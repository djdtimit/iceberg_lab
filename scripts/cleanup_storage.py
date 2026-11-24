"""Remove test leftovers of one catalog before a full run (called by scripts/run_all.sh).

Leftovers slow RustFS down: listing a bucket with ~20,000 objects takes ~18 s, and large
writes/reads then run into timeouts (test_rustfs.py, test_performance.py). Two phases:

1. Catalog: drop leftover test namespaces (``test_<8 hex>`` and others, see TEST_NS) in the main warehouses, with
   purge where the catalog supports it.
2. Storage: delete objects under the catalog's own storage roots that belong to NO table or
   view of the catalog any more (orphans: dropped without purge, soft-deleted, aborted writes).

Safety rules - nothing that is still in use is deleted:
- an object is kept if it lies under the location of a live table/view, or is referenced by a
  live table (all snapshots: data and delete files, manifests, metadata files);
- objects younger than ``--min-age-hours`` (default 3) are kept;
- only the roots of this catalog are swept; Unity's second (authz) instance shares the
  bucket path, so the managed storage of every other Unity catalog id is excluded;
- if listing the catalog fails anywhere, phase 2 is skipped (no delete without a full view);
- a cloud catalog (``PROFILE.remote``) gets phase 1 only.

Usage (dev container): ``CATALOG=polaris uv run python scripts/cleanup_storage.py [--apply]``
Without ``--apply`` it only reports what it would do.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from urllib.parse import urlparse

import boto3
import requests
from pyiceberg.exceptions import NamespaceNotEmptyError, RESTError

from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSES, catalog_headers
from iceberg_lab.engines import pyiceberg_catalog

#: Namespaces the tests create (test_<8 hex>; test_sts_expiry: exp_<6 hex>; permission tests: az_/priv_/inv_/idp_<6 hex>;
#: features: feat_<8 hex>) and those of manual
#: debugging sessions in the lab (<word>_<4-6 hex>, probe, pyns, ...). Notebook namespaces (nb_*) stay.
TEST_NS = re.compile(
    r"^(test_[0-9a-f]{8}|exp_[0-9a-f]{6}"
    r"|az_[0-9a-f]{6}|(probe|dbg|race|sig|flaky|purge|lks|anon|flk|trdel|plan|iop|nest|vac|ow|tn|cap|dk3c|dk3p"
    r"|pgp|pf|priv|inv|idp|pm|rbac_probe|ex|pr|rx|ry)_[0-9a-f]{4,6}"
    r"|feat_[0-9a-f]{8}(_r)?"
    r"|probe|pyns|statprobe|rbprobe|txprobe|neutral_probe)$"
)


def split(uri: str) -> tuple[str, str]:
    """``s3://bucket/key`` -> ``(bucket, key)``.

    Args:
        uri: S3 URI.

    Returns:
        ``(bucket, key)``.
    """
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def roots() -> list[tuple[str, str]]:
    """(bucket, key prefix) swept for this catalog - only paths no other catalog writes to.

    Returns:
        The roots.
    """
    if PROFILE.name == "lakekeeper":
        return [(SETTINGS.s3_bucket, f"{w}/") for w in WAREHOUSES]
    bucket, prefix = split(PROFILE.location)
    return [(bucket, prefix.rstrip("/") + "/" if prefix else "")]


def foreign_prefixes() -> list[str]:
    """Unity: managed storage of other UC catalogs (authz instance) under the same root.

    Returns:
        The prefixes to leave alone.
    """
    if PROFILE.name != "unity":
        return []
    base = SETTINGS.catalog_uri.split("/api/")[0] if "/api/" in SETTINGS.catalog_uri else None
    base = base or "http://unitycatalog:8080"
    r = requests.get(
        f"{base}/api/2.1/unity-catalog/catalogs/{PROFILE.warehouse_sts}",
        headers=catalog_headers(),
        timeout=10,
    )
    r.raise_for_status()
    own = r.json()["id"]
    _, prefix = split(PROFILE.location)
    managed = f"{prefix.rstrip('/')}/__unitystorage/catalogs/"
    return [f"{managed}{own}/", managed]  # [0] own (kept sweepable), [1] everything else


def drop_tree(cat, ns: tuple[str, ...], log) -> None:
    """Drop views, tables (purge, else drop) and children of a namespace, then it.

    Args:
        cat: PyIceberg catalog.
        ns: Namespace.
        log: Print function.
    """
    for child in cat.list_namespaces(ns):
        drop_tree(cat, child, log)
    for view in cat.list_views(ns):
        cat.drop_view(view)
    for ident in cat.list_tables(ns):
        try:
            cat.purge_table(ident)
        except Exception:  # noqa: BLE001 - catalogs without purge (RustFS: 406)
            cat.drop_table(ident)
    try:
        cat.drop_namespace(ns)
    except NamespaceNotEmptyError:  # RustFS rename ghost - harmless, files are swept below
        log(f"  namespace {'.'.join(ns)} stays (catalog reports it not empty)")


def raw_table(cat, ident) -> dict:
    """The catalog's LoadTable response as plain JSON (no PyIceberg model).

    Args:
        cat: PyIceberg catalog.
        ident: Table identifier ``namespace.table``.

    Returns:
        The LoadTable answer.
    """
    from pyiceberg.catalog.rest import Endpoints

    url = cat.url(Endpoints.load_table, prefixed=True, **cat._split_identifier_for_path(ident))
    r = cat._session.get(url)
    r.raise_for_status()
    return r.json()


def live_objects(cat, ns: tuple[str, ...], prefixes: set[str], files: set[str]) -> None:
    """Collect locations of all tables/views and every file a table references.

    Args:
        cat: PyIceberg catalog.
        ns: Namespace.
        prefixes: Collected table/view locations (filled).
        files: Collected referenced files (filled).
    """
    for child in cat.list_namespaces(ns):
        live_objects(cat, child, prefixes, files)
    for ident in cat.list_views(ns) if ns else []:
        view = cat.load_view(ident)
        prefixes.add(view.metadata.location.rstrip("/") + "/")
    for ident in cat.list_tables(ns) if ns else []:
        try:
            t = cat.load_table(ident)
        except Exception:  # noqa: BLE001 - PyIceberg cannot model it (v3 variant, geometry
            # with CRS ...): keep its whole location; a real catalog error re-raises below
            md = raw_table(cat, ident)["metadata"]
            prefixes.add(md["location"].rstrip("/") + "/")
            files.update(e["metadata-file"] for e in md.get("metadata-log", []))
            continue
        prefixes.add(t.location().rstrip("/") + "/")
        files.add(t.metadata_location)
        for entry in t.metadata.metadata_log:
            files.add(entry.metadata_file)
        seen: set[str] = set()
        for snap in t.metadata.snapshots:
            files.add(snap.manifest_list)
            try:
                manifests = snap.manifests(t.io)
            except FileNotFoundError:  # broken table (e.g. DuckDB lost commit): nothing to keep
                continue
            for m in manifests:
                if m.manifest_path in seen:
                    continue
                seen.add(m.manifest_path)
                files.add(m.manifest_path)
                try:
                    entries = m.fetch_manifest_entry(t.io, discard_deleted=False)
                except FileNotFoundError:
                    continue
                files.update(e.data_file.file_path for e in entries)
        for stat in t.metadata.statistics + t.metadata.partition_statistics:
            files.add(stat.statistics_path)


def main() -> int:
    """Drop leftover test namespaces, then delete unreferenced objects below the catalog's roots (dry run by default).

    Returns:
        Exit code 0.

    Raises:
        RESTError: a warehouse cannot be opened for another reason than "not found".
    """
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="really drop/delete (default: dry run)")
    ap.add_argument("--min-age-hours", type=float, default=3.0)
    args = ap.parse_args()

    def log(msg: str) -> None:
        """Print immediately.

        Args:
            msg: Message.
        """
        print(msg, flush=True)

    mode = "" if args.apply else " (dry run)"
    log(f"cleanup {PROFILE.name}{mode}")

    # Phase 1: leftover test namespaces
    cats = {}
    missing = []
    for w in WAREHOUSES:
        try:
            cats[w] = pyiceberg_catalog(w)
        except RESTError as e:
            # Lakekeeper/Polaris: NotFoundException; Unity: "Catalog not found".
            if not any(t in str(e) for t in ("NotFoundException", "Catalog not found")):
                raise
            # A fresh stack: the tests create this warehouse themselves (e.g. polaris_sts).
            missing.append(w)
            log(f"  {w}: warehouse not created yet")
    for w, cat in cats.items():
        left = [ns for ns in cat.list_namespaces() if TEST_NS.match(ns[-1])]
        log(f"  {w}: {len(left)} leftover test namespaces")
        for ns in left if args.apply else []:
            try:
                drop_tree(cat, ns, log)
            except Exception as e:  # noqa: BLE001 - e.g. drop-protected tables; report, go on
                log(f"  {'.'.join(ns)}: not dropped ({type(e).__name__}: {str(e)[:120]})")

    # Phase 2: orphans in storage
    if PROFILE.remote:  # a cloud catalog's storage is the provider's, not the lab's RustFS
        log("storage: cloud catalog - only its test namespaces are dropped")
        return 0
    if missing:
        log("  storage sweep skipped - not every warehouse exists yet")
        return 0
    prefixes: set[str] = set()
    files: set[str] = set()
    try:
        for cat in cats.values():
            live_objects(cat, (), prefixes, files)
        foreign = foreign_prefixes()
    except Exception as e:  # noqa: BLE001 - no complete view of the catalog: delete nothing
        log(f"  storage sweep skipped - catalog listing failed: {type(e).__name__}: {e}")
        return 0
    live_prefixes = [split(p) for p in prefixes]
    live_files = {split(f) for f in files}
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(hours=args.min_age_hours)

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{SETTINGS.s3_host_port}",
        aws_access_key_id=SETTINGS.s3_access_key,
        aws_secret_access_key=SETTINGS.s3_secret_key,
        region_name="us-east-1",
    )
    for bucket, root in roots():
        keep = young = 0
        orphans: list[str] = []
        size = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=root):
            for o in page.get("Contents", []):
                key = o["Key"]
                if (
                    foreign
                    and key.startswith(foreign[1])
                    and not key.startswith(foreign[0])
                    or (bucket, key) in live_files
                    or any(b == bucket and key.startswith(p) for b, p in live_prefixes)
                ):
                    keep += 1
                elif o["LastModified"] > cutoff:
                    young += 1
                else:
                    orphans.append(key)
                    size += o["Size"]
        log(
            f"  s3://{bucket}/{root}: {keep} in use, {young} younger than "
            f"{args.min_age_hours:g} h, {len(orphans)} orphans ({size / 1e6:.0f} MB)"
            + (" -> deleted" if args.apply and orphans else "")
        )
        for i in range(0, len(orphans), 1000) if args.apply else []:
            batch = [{"Key": k} for k in orphans[i : i + 1000]]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
    return 0


if __name__ == "__main__":
    sys.exit(main())
