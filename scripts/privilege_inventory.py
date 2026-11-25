"""Record which privileges each catalog knows: docs/api/privileges-<catalog>.txt.

Sources (the running images; run on the HOST, needs docker):

    lakekeeper  assignment types per object from the server's management OpenAPI
                (``/api-docs/management/v1/openapi.json``, lakekeeper-authz on port 8282)
    polaris     the grant enums of the management model jar (``CatalogPrivilege``,
                ``NamespacePrivilege``, ``TablePrivilege``, ``ViewPrivilege``, ``PolicyPrivilege``)
    unity       the ``Privilege`` enum of the server model jar
    rustfs      IAM actions: candidates found in the binary (``admin:...``, ``s3:...``), kept
                only if RustFS itself accepts them in an IAM policy (it refuses unknown actions)
    gravitino   the ``Privilege.Name`` enum of the API jar

Each file starts with ``# image: <compose image>``; tests/security/test_privileges.py fails when the
compose file names another image - re-run this script after every catalog update.

Usage: python3 scripts/privilege_inventory.py [<catalog> ...]   (default: all)
"""

from __future__ import annotations

import json
import pathlib
import re
import struct
import subprocess
import tempfile
import urllib.request
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPOSE = ROOT / ".devcontainer" / "docker-compose.yml"
OUT = ROOT / "docs" / "api"
POLARIS_JAR = "/deployments/lib/main/org.apache.polaris.polaris-api-management-model-1.9.0.jar"
UNITY_JAR = "/home/unitycatalog/server/target/models/target/unitycatalog-servermodels-0.7.0.jar"
GRAVITINO_JAR = "/opt/gravitino/libs/gravitino-api-1.3.1.jar"


def image(service: str) -> str:
    """Image of a compose service (the version the inventory belongs to).

    Args:
        service: Compose service name.

    Returns:
        The image reference, e.g. ``apache/polaris:1.9.0``.
    """
    text = COMPOSE.read_text()
    block = text.split(f"\n  {service}:\n", 1)[1]
    return re.search(r"^\s+image:\s*(\S+)", block, re.MULTILINE)[1]


def enum_constants(data: bytes) -> list[str]:
    """Names of the enum constants of a Java class file (fields flagged ACC_ENUM).

    Args:
        data: The class file.

    Returns:
        The constant names in declaration order.

    Raises:
        ValueError: an unknown constant pool tag (not a class file this parser knows).

    Raises:
        ValueError: an unknown constant pool tag (not a class file this parser knows).
    """
    count = struct.unpack(">H", data[8:10])[0]
    pool: list[str | None] = [None]
    pos, i = 10, 1
    while i < count:
        tag = data[pos]
        if tag == 1:
            n = struct.unpack(">H", data[pos + 1 : pos + 3])[0]
            pool.append(data[pos + 3 : pos + 3 + n].decode("utf-8", "replace"))
            pos += 3 + n
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            pool.append(None)
            pos += 5
        elif tag in (5, 6):
            pool += [None, None]
            pos += 9
            i += 1
        elif tag in (7, 8, 16, 19, 20):
            pool.append(None)
            pos += 3
        elif tag == 15:
            pool.append(None)
            pos += 4
        else:
            raise ValueError(f"unknown constant pool tag {tag}")
        i += 1
    pos += 6
    pos += 2 + 2 * struct.unpack(">H", data[pos : pos + 2])[0]
    fields = struct.unpack(">H", data[pos : pos + 2])[0]
    pos += 2
    out = []
    for _ in range(fields):
        flags, name, _desc, attrs = struct.unpack(">HHHH", data[pos : pos + 8])
        pos += 8
        for _ in range(attrs):
            pos += 6 + struct.unpack(">I", data[pos + 2 : pos + 6])[0]
        if flags & 0x4000:
            out.append(pool[name])
    return out


def _jar(container: str, path: str) -> zipfile.ZipFile:
    """Copy a jar out of a container.

    Args:
        container: Container name.
        path: Jar path inside the container.

    Returns:
        The opened jar.
    """
    target = pathlib.Path(tempfile.mkdtemp()) / "x.jar"
    subprocess.run(["docker", "cp", f"{container}:{path}", str(target)], check=True)
    return zipfile.ZipFile(target)


def lakekeeper() -> list[str]:
    """Lakekeeper: ``<object>: <assignment type>`` from the management OpenAPI.

    Returns:
        The inventory lines.
    """
    with urllib.request.urlopen("http://localhost:8282/api-docs/management/v1/openapi.json") as r:
        schemas = json.load(r)["components"]["schemas"]
    return sorted(
        f"{name.removesuffix('Assignment')}: {t}"
        for name, s in schemas.items()
        if name.endswith("Assignment") and "discriminator" in s
        for t in s["discriminator"]["mapping"]
    )


def polaris() -> list[str]:
    """Polaris: ``<grant type>: <privilege>`` from the management model's enums.

    Returns:
        The inventory lines.
    """
    z = _jar("iceberg-lab-polaris-1", POLARIS_JAR)
    out = []
    for n in z.namelist():
        if n.endswith("Privilege.class") and "/model/" in n:
            kind = n.rsplit("/", 1)[1].removesuffix("Privilege.class")
            out += [f"{kind}: {p}" for p in enum_constants(z.read(n))]
    return sorted(out)


def unity() -> list[str]:
    """Unity: the server model's ``Privilege`` enum.

    Returns:
        The inventory lines.
    """
    z = _jar("iceberg-lab-unitycatalog-authz-1", UNITY_JAR)
    data = z.read("io/unitycatalog/server/model/Privilege.class")
    return sorted(enum_constants(data))


def rustfs() -> list[str]:
    """RustFS: IAM actions found in the binary that RustFS accepts in a policy.

    Strings in the binary are concatenated, so every prefix of a match is a candidate;
    RustFS's own policy validation decides (run inside the dev container).

    Returns:
        The inventory lines.
    """
    raw = subprocess.run(
        ["docker", "exec", "iceberg-lab-rustfs-1", "sh", "-c",
         "grep -aoE '(admin|s3|kms):[A-Z][A-Za-z]{2,60}' /usr/bin/rustfs | sort -u"],
        check=True, capture_output=True, text=True,
    ).stdout.split()  # fmt: skip
    candidates = sorted({m[: len(p) + 1 + k] for m in raw for p in [m.split(":")[0]]
                         for k in range(3, len(m) - len(p))})  # fmt: skip
    out = subprocess.run(
        ["docker", "exec", "-i", "-w", "/workspaces/iceberg_lab", "iceberg-lab-dev-1",
         "bash", "-lc", "CATALOG=rustfs uv run python scripts/privilege_inventory.py --validate-rustfs"],
        input=json.dumps(candidates), check=True, capture_output=True, text=True,
    ).stdout  # fmt: skip
    accepted = json.loads(out.strip().splitlines()[-1])
    # A concatenation can make a longer valid action out of two: keep only the accepted
    # candidates, which are real action names by RustFS's own validation.
    return sorted(set(accepted))


def gravitino() -> list[str]:
    """Gravitino: the API's ``Privilege.Name`` enum.

    Returns:
        The inventory lines.
    """
    z = _jar("iceberg-lab-gravitino-1", GRAVITINO_JAR)
    data = z.read("org/apache/gravitino/authorization/Privilege$Name.class")
    return sorted(enum_constants(data))


SOURCES = {
    "lakekeeper": ("lakekeeper-authz", lakekeeper),
    "polaris": ("polaris", polaris),
    "unity": ("unitycatalog-authz", unity),
    "rustfs": ("rustfs", rustfs),
    "gravitino": ("gravitino", gravitino),
}


def validate_rustfs() -> None:
    """In the dev container: print the candidate actions (JSON on stdin) RustFS accepts."""
    import sys

    sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "tests" / "lab")]
    from authz_backends import RustFSAuthz

    adm, ok = RustFSAuthz()._admin_client(), []
    for action in json.load(sys.stdin):
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": [action], "Resource": ["arn:aws:s3:::*"]}],
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(policy, f)
        try:
            adm.policy_add("lab-privilege-inventory", f.name)
            ok.append(action)
        except Exception:  # noqa: BLE001, S110 - refused: not an action RustFS knows
            pass
    try:
        adm.policy_remove("lab-privilege-inventory")
    except Exception:  # noqa: BLE001, S110 - nothing was added
        pass
    print(json.dumps(ok))


def main(only: list[str]) -> None:
    """Write docs/api/privileges-<catalog>.txt for the given catalogs (default: all).

    Args:
        only: Catalog names; empty for all.
    """
    for catalog, (service, read) in SOURCES.items():
        if only and catalog not in only:
            continue
        lines = read()
        path = OUT / f"privileges-{catalog}.txt"
        path.write_text(f"# image: {image(service)}\n" + "\n".join(lines) + "\n")
        print(f"{path.relative_to(ROOT)}: {len(lines)} privileges")


if __name__ == "__main__":
    import sys

    validate_rustfs() if "--validate-rustfs" in sys.argv else main(sys.argv[1:])
