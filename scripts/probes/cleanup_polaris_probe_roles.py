"""Remove the principal/catalog roles probes and aborted tests left behind (Polaris)."""
import sys

import requests

sys.path[:0] = ["tests", "tests/lab"]
from authz_backends import PolarisAuthz  # noqa: E402
from iceberg_lab.config import WAREHOUSE_STS  # noqa: E402

p = PolarisAuthz()
h = p._h()
api = f"{p.base}/api/management/v1"
keep = {"az_alice", "service_admin", "catalog_admin", "lab_readers"}
for r in requests.get(f"{api}/principal-roles", headers=h, timeout=10).json()["roles"]:
    name = r["name"]
    if name not in keep and name.split("_")[0] in ("pm", "priv", "rbac", "inv", "probe"):
        print("drop principal role", name, requests.delete(f"{api}/principal-roles/{name}", headers=h, timeout=10).status_code)
for r in requests.get(f"{api}/catalogs/{WAREHOUSE_STS}/catalog-roles", headers=h, timeout=10).json()["roles"]:
    name = r["name"]
    if name not in keep and name.split("_")[0] in ("pm", "priv", "rbac", "inv", "probe"):
        print("drop catalog role", name, requests.delete(f"{api}/catalogs/{WAREHOUSE_STS}/catalog-roles/{name}", headers=h, timeout=10).status_code)
roles = requests.get(f"{api}/principals/az_bob/principal-roles", headers=h, timeout=10).json()["roles"]
print("az_bob roles now:", [r["name"] for r in roles])
