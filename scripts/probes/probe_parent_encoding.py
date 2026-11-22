import uuid, requests
from urllib.parse import quote
from iceberg_lab.config import PROFILE, SETTINGS, WAREHOUSE_STS, catalog_auth, catalog_headers
from iceberg_lab.engines import pyiceberg_catalog
cat = pyiceberg_catalog(WAREHOUSE_STS)
parent = "probe_" + uuid.uuid4().hex[:5]; cat.create_namespace(parent); cat.create_namespace((parent, "Grüße"))
cfg = requests.get(f"{SETTINGS.catalog_uri}/v1/config", params={"warehouse": WAREHOUSE_STS}, headers=catalog_headers(), auth=catalog_auth()).json()
prefix = cfg.get("overrides", {}).get("prefix") or cfg.get("defaults", {}).get("prefix")
url = f"{SETTINGS.catalog_uri}/v1/{prefix}/namespaces"
# requests encodes params itself, exactly once:
r = requests.get(url, params={"parent": f"{parent}\x1fGrüße"}, headers=catalog_headers(), auth=catalog_auth(), timeout=10)
print(PROFILE.name, "raw, encoded once:", r.status_code, r.text[:100])
# what PyIceberg sends:
import pyiceberg.catalog.rest as rest, inspect
src = inspect.getsource(rest.RestCatalog.list_namespaces)
print("  pyiceberg list_namespaces encodes parent with:", [l.strip() for l in src.splitlines() if "parent" in l][:3])
try: print("  pyiceberg:", cat.list_namespaces((parent, "Grüße")))
except Exception as e: print("  pyiceberg:", type(e).__name__, str(e)[:90])
requests.delete(f"{url}/{quote(parent)}%1F{quote('Grüße')}", headers=catalog_headers(), auth=catalog_auth()); cat.drop_namespace(parent)
