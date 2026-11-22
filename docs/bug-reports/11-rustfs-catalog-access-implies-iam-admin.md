# [RustFS] Catalog access requires `admin:*`, which lets any catalog user make themselves storage administrator

> **WITHDRAWN (2026-11-17) - not a RustFS bug, a lab misconfiguration. Do not file.**
> RustFS has granular catalog actions (`admin:GetTableCatalog`, `admin:GetTableBucket`,
> `admin:GetTableNamespace`, `admin:SetTableNamespace`, `admin:CreateTable`, `admin:GetTable`,
> `admin:GetTableMetadata`, `admin:SetTableMetadata`, `admin:CommitTable`, `admin:DeleteTable`,
> ...). With only these (resource `arn:aws:s3:::<bucket>/*`) the catalog works and a user can NOT
> attach `consoleAdmin` to herself (403, checked: scripts/probes/probe_rustfs_granular.py). The lab
> had granted `admin:*`, which includes IAM administration. What remains is documented in the
> README: catalog rights can be scoped per bucket (warehouse), not per namespace or table (checked
> with several resource forms: scripts/probes/probe_rustfs_scope.py), and the documentation lists
> only part of the actions (rename needs one that is neither documented nor found).


**Component:** RustFS built-in Iceberg REST catalog ("table buckets", `/iceberg/v1`) and IAM
**Versions:** RustFS 1.0.5
**Severity:** privilege escalation - every user who may use the catalog at all can obtain full
access to all buckets

## Summary

The built-in Iceberg catalog only accepts IAM users whose policy allows `admin:*` (narrower
actions, even `s3:*` or `*`, are answered 403 - checked). `admin:*` also covers RustFS's IAM
administration. A user who was given catalog access for a single table can therefore attach the
built-in `consoleAdmin` policy to themselves and then read and write every bucket.

## Steps to reproduce

1. IAM user `az-alice` with the policy needed for the catalog plus read access to one table's
   prefix:
   ```json
   {"Version": "2012-10-17", "Statement": [
     {"Effect": "Allow", "Action": ["admin:*"], "Resource": ["arn:aws:s3:::*"]},
     {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::labtables/tables/<uuid>/*"]}
   ]}
   ```
2. With `az-alice`'s own keys: `mc admin policy attach <alias> consoleAdmin --user az-alice`
   (or the MinIO admin API `policy_set("consoleAdmin", user="az-alice")`).
3. Read another table's data files with `az-alice`'s keys.

## Expected

Catalog access does not require administrative rights - or at least the catalog can be used with
a policy that excludes IAM administration (e.g. dedicated `s3tables:*`-style actions, as AWS S3
Tables has). Step 2 is refused.

## Actual

Step 2 succeeds (`az-alice`'s policy is replaced by `consoleAdmin`), step 3 reads the other table's
data. Separately, the same `admin:*` lets a reader drop, rename and create any table and drop
namespaces through the catalog.

## Notes

- Lab tests: `tests/security/test_security.py::test_cannot_grant_oneself_rights` and
  `test_reader_cannot_change_the_catalog` (CATALOG=rustfs). The test puts the user's lab policy
  back afterwards.
- Table property changes are refused for a reader (the catalog checks S3 write access to the
  metadata), so not every operation is open.
