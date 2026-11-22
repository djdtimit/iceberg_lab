"""Iceberg lab helpers: settings, bootstrap and engine adapters for four REST catalogs on RustFS.

The catalog under test is chosen with ``CATALOG`` (lakekeeper, polaris, rustfs, unity).

Modules:
    config      - catalog profiles, endpoints/credentials (environment-overridable), warehouses
    bootstrap   - idempotent setup of bucket and catalog (warehouses, Polaris catalog, ...)
    engines     - one adapter per query engine with a shared read/write interface
    datatypes   - catalog of all Iceberg v2/v3 types with test values and normalisation
    keycloak    - Keycloak clients, scopes and token audiences of the permission tests
    notebook    - helpers for the example notebooks (the same notebooks on every catalog)
"""
