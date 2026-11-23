# [Apache Iceberg] REST fixture: concurrent `POST /transactions/commit` answered 204, changes dropped

**Component:** `iceberg-open-api` test fixture (`RESTCatalogServer` / `RESTCatalogAdapter`, image
`apache/iceberg-rest-fixture`), JDBC backend (the image default)
**Version:** image `apache/iceberg-rest-fixture@sha256:009f9f6897cc…` (Iceberg `main`,
commit c75374c, 2026-11-10)

## Summary

When several clients commit to the same table through `POST /v1/transactions/commit` at the same
time, the server answers **204 to all of them but keeps only one change**. The others are
dropped silently. The same race through the table endpoint
(`POST /v1/namespaces/{ns}/tables/{table}`) is handled correctly: the losers get 409 and can retry.

The server log shows why: the losing commits hit the CAS conflict in the JDBC backend, the
server retries them itself (`Tasks ... Retrying task after failure: attempt=1`), the retry finds
`Nothing to commit` against the refreshed metadata and the request is answered as a success.

Clients that commit through this endpoint lose data without any error. In the lab, DuckDB (which
commits every write through `/transactions/commit`) loses appends: 4 concurrent `INSERT`s all
report success, the table afterwards holds one of them.

## Steps to reproduce

8 threads, each sets its own table property, with an `assert-table-uuid` requirement:

```python
def commit(k):  # via /transactions/commit
    return requests.post(
        f"{base}/v1/transactions/commit",
        json={
            "table-changes": [
                {
                    "identifier": {"namespace": [ns], "name": "t"},
                    "requirements": [{"type": "assert-table-uuid", "uuid": table_uuid}],
                    "updates": [{"action": "set-properties", "updates": {f"k{k}": "v"}}],
                }
            ]
        },
    )
```

| endpoint | answered success | properties kept |
|---|---|---|
| `POST /v1/namespaces/{ns}/tables/t` | 5 of 8 (3 x 409) | 5 of 8 |
| `POST /v1/transactions/commit` | **8 of 8 (204)** | **1 of 8** |

(Repeated: same result. Lakekeeper 0.15.1 answers 409 to the losers on both endpoints.)

## Expected

A transaction whose commit cannot be applied is answered with 409 `CommitFailedException`, as on
the table endpoint - or the server retry re-applies the request's updates to the refreshed
metadata (after re-validating the requirements) instead of reporting "nothing to commit".

## Notes

- Lab tests: `tests/catalog/test_catalog_consistency.py::test_parallel_transaction_commits_are_not_lost`
  (all catalogs that advertise the endpoint), `tests/engines/test_concurrency.py::test_concurrent_appends[duckdb-v2]`
  (CATALOG=reference).
- The fixture is the reference implementation many clients test against; a client that relies
  on 204 meaning "committed" passes its tests here although its changes are lost.
