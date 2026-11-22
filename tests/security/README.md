# tests/security – Authentication, authorization and security

Who may do what, and how far vended credentials reach: rights per user and table, roles and
inherited grants, single privileges with inventory, attacks on login, storage location, secrets and
names, reach and expiry of the STS keys, access modes (vended credentials, remote signing), web UI,
outage of the token endpoint and every engine's connection without lab help. The setup per catalog
is in `tests/lab/authz_backends.py`.

| File | Content | Test functions |
|---|---|---|
| [`test_access_delegation.py`](test_access_delegation.py) | Which engine works with which access-delegation mode - and how far does it reach? | 6 |
| [`test_authz.py`](test_authz.py) | Authentication and authorization - the same scenarios on every catalog. | 15 |
| [`test_idp_outage.py`](test_idp_outage.py) | Token endpoint outage (base test, catalogs with bearer tokens; Python and Java REST clients). | 3 |
| [`test_privileges.py`](test_privileges.py) | Which privileges the catalogs offer, and what each common one allows (base, every catalog). | 4 |
| [`test_rbac.py`](test_rbac.py) | Roles and inherited grants - the same scenarios on every catalog (base). | 5 |
| [`test_security.py`](test_security.py) | Security of the catalogs (base tests, area "Sicherheit"): the same attacks on every catalog. | 13 |
| [`test_stock_connection.py`](test_stock_connection.py) | Connection without lab help (base, every engine, every catalog). | 1 |
| [`test_sts_expiry.py`](test_sts_expiry.py) | Long-lived clients after the vended STS credentials EXPIRED - on every catalog that vends. | 1 |
| [`test_sts_scope.py`](test_sts_scope.py) | How far do vended STS credentials reach? (``sts`` warehouse of every catalog that vends) | 9 |
| [`test_ui.py`](test_ui.py) | Web UI of the catalog (base, every catalog; no click tests). | 2 |

## Running

```bash
CATALOG=lakekeeper uv run pytest tests/security            # whole folder (CATALOG: lakekeeper|polaris|rustfs|unity)
CATALOG=polaris REPORT_DIR=/tmp/x uv run pytest tests/security/<file>.py -k <filter>   # targeted, own report
```

Always run targeted runs with `REPORT_DIR`, otherwise they overwrite the report of the last full run.

Every file describes at the top what it checks; every test describes in its docstring under
`Checks:` exactly what is checked, and its arguments under `Args:`. Overview: [README](../../README.md),
test structure: [docs/test-architecture.md](../../docs/test-architecture.md).
