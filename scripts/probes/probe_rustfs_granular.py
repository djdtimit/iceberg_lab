import json, sys, tempfile, uuid, pyarrow as pa
sys.path[:0] = ["/workspaces/iceberg_lab/tests", "/workspaces/iceberg_lab/tests/lab"]
from minio import MinioAdmin
from minio.credentials import StaticProvider
from authz_backends import Conn, pyiceberg_for
from iceberg_lab.config import SETTINGS, WAREHOUSE_STS
root = MinioAdmin(endpoint="rustfs:9000", credentials=StaticProvider(SETTINGS.s3_access_key, SETTINGS.s3_secret_key), secure=False)
USER, SECRET = "az-granular", "az-granular-secret"
ACTIONS = ["admin:GetTableCatalog", "admin:GetTableBucket", "admin:GetTableNamespace", "admin:SetTableNamespace",
           "admin:UpdateTableNamespaceProperties", "admin:DeleteTableNamespace", "admin:CreateTable", "admin:GetTable",
           "admin:GetTableMetadata", "admin:GetTableMetadataLocation", "admin:SetTableMetadata", "admin:CommitTable",
           "admin:DeleteTable", "admin:RegisterTable"]
pol = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ACTIONS, "Resource": ["arn:aws:s3:::*"]},
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": ["arn:aws:s3:::labtables/*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::labtables"]}]}
def quiet(f, *a, **k):
    try: return f(*a, **k)
    except ValueError as e:  # the minio client cannot decrypt RustFS answers (AEAD 2) - effect is checked separately
        if "AEAD" not in str(e): raise
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh: json.dump(pol, fh)
quiet(root.user_add, USER, SECRET); quiet(root.policy_add, "az-granular", fh.name); quiet(root.policy_set, "az-granular", user=USER)
print("user:", root.user_info(USER))
cat = pyiceberg_for(Conn(uri=SETTINGS.catalog_uri, warehouse=WAREHOUSE_STS, auth="sigv4", keys=(USER, SECRET), vended=False))
ns = "probe_" + uuid.uuid4().hex[:5]
def step(name, fn):
    try: r = fn(); print(f"  {name:28} OK {str(r)[:60] if r is not None else ''}")
    except Exception as e: print(f"  {name:28} {type(e).__name__}: {str(e)[:110]}")
step("list namespaces", cat.list_namespaces)
step("create namespace", lambda: cat.create_namespace(ns))
step("create table", lambda: cat.create_table(f"{ns}.t", schema=pa.schema([("id", pa.int64())])))
step("append", lambda: cat.load_table(f"{ns}.t").append(pa.table({"id": [1]})))
step("read", lambda: cat.load_table(f"{ns}.t").scan().to_arrow().num_rows)
step("list tables", lambda: cat.list_tables(ns))
step("rename table", lambda: cat.rename_table(f"{ns}.t", f"{ns}.t2"))
step("drop table", lambda: cat.drop_table(f"{ns}.t2"))
step("drop namespace", lambda: cat.drop_namespace(ns))
self_adm = MinioAdmin(endpoint="rustfs:9000", credentials=StaticProvider(USER, SECRET), secure=False)
step("SELF-ESCALATE consoleAdmin", lambda: self_adm.policy_set("consoleAdmin", user=USER))
print("  after escalation attempt:", root.user_info(USER))
quiet(root.user_remove, USER); quiet(root.policy_remove, "az-granular")
print("removed test user:", USER)
