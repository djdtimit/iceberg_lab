import json, sys, tempfile, uuid, pyarrow as pa
sys.path[:0] = ["/workspaces/iceberg_lab/tests", "/workspaces/iceberg_lab/tests/lab"]
from minio import MinioAdmin
from minio.credentials import StaticProvider
from authz_backends import Conn, pyiceberg_for
from iceberg_lab.config import SETTINGS, WAREHOUSE_STS
from iceberg_lab.engines import pyiceberg_catalog
root = MinioAdmin(endpoint="rustfs:9000", credentials=StaticProvider(SETTINGS.s3_access_key, SETTINGS.s3_secret_key), secure=False)
def quiet(f, *a, **k):
    try: return f(*a, **k)
    except ValueError as e:
        if "AEAD" not in str(e): raise
adm = pyiceberg_catalog(WAREHOUSE_STS); sfx = uuid.uuid4().hex[:5]
ok, no = f"probe_ok_{sfx}", f"probe_no_{sfx}"
for n in (ok, no):
    adm.create_namespace(n); adm.create_table(f"{n}.t", schema=pa.schema([("id", pa.int64())]))
ACTIONS = ["admin:GetTableNamespace", "admin:GetTable", "admin:GetTableMetadata", "admin:GetTableMetadataLocation"]
FORMATS = {
    "s3 bucket": ["arn:aws:s3:::labtables"],
    "s3 bucket/*": ["arn:aws:s3:::labtables/*"],
    "s3 other bucket": ["arn:aws:s3:::warehouse"],
}
USER, SECRET = "az-scope", "az-scope-secret"
for label, res in FORMATS.items():
    pol = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["admin:GetTableCatalog", "admin:GetTableBucket"], "Resource": ["arn:aws:s3:::*"]},
        {"Effect": "Allow", "Action": ACTIONS, "Resource": res}]}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh: json.dump(pol, fh)
    try:
        quiet(root.user_add, USER, SECRET); quiet(root.policy_add, "az-scope", fh.name); quiet(root.policy_set, "az-scope", user=USER)
    except Exception as e:
        print(f"{label:22} policy refused: {str(e)[:100]}"); continue
    cat = pyiceberg_for(Conn(uri=SETTINGS.catalog_uri, warehouse=WAREHOUSE_STS, auth="sigv4", keys=(USER, SECRET), vended=False))
    res_ok = res_no = None
    try: cat.load_table(f"{ok}.t"); res_ok = "load OK"
    except Exception as e: res_ok = type(e).__name__
    try: cat.load_table(f"{no}.t"); res_no = "load OK"
    except Exception as e: res_no = type(e).__name__
    print(f"{label:22} granted ns: {res_ok:18} other ns: {res_no}")
    quiet(root.user_remove, USER); quiet(root.policy_remove, "az-scope")
for n in (ok, no):
    adm.drop_table(f"{n}.t"); adm.drop_namespace(n)
