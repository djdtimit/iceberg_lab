"""Feature inventory: every endpoint of the four catalogs -> a capability -> tests (evidence).

Source of the endpoints: ``docs/api/<catalog>-<version>.txt`` (extracted from each catalog's API
description, see the file headers). Rule (docs/test-architecture.md): a capability that at least
two catalogs offer is a BASE capability (same test for all, adapter per catalog); a capability
of exactly one catalog is SPECIFIC. ``kind`` is derived, not written down.

Each capability:
    title      what it does (shown in docs/feature-inventory.md)
    endpoints  catalog -> regex matched against "METHOD /path" lines of that catalog's file
    tests      test ids (``file::test``) that prove it; empty = not tested yet (listed as open)
    note       why something is not tested, or how a catalog does it differently
    not_implemented
               catalog -> {status, test, gap}: the catalog DESCRIBES the capability in its API,
               but the server does not implement it at all (shown as ◐). ``test`` proves it
               with a known gap whose reason contains ``gap``; when the catalog adds it, that
               test reports "gap fixed" and the entry is removed. Partial failures stay ✓ with
               a note.

tests/selftest/test_feature_inventory.py checks: every endpoint belongs to exactly one capability, every
listed test exists, every ◐ entry is backed by its test's known gap, and the overview
docs/feature-inventory.md is up to date.
"""

from __future__ import annotations

ICEBERG = r"(/catalog)?(/api/catalog)?/v1/"  # Iceberg REST prefix (Lakekeeper /catalog, Polaris /api/catalog)
RFS = r"ANY /iceberg/v1/"
UC = r"/api/2\.1/unity-catalog/"
UCC = r"/api/1\.0/unity-control/"
LK = r"/management/v1/"
PM = r"/api/management/v1/"
PC = r"/api/catalog/polaris/v1/\{prefix\}/"

CAPABILITIES: dict[str, dict] = {
    # ===================================================================== Iceberg REST spec
    "iceberg_config": {
        "title": "Iceberg REST: fetch configuration (/v1/config)",
        "endpoints": {
            "lakekeeper": rf"^GET {ICEBERG}config$",
            "polaris": rf"^GET {ICEBERG}config$",
            "rustfs": rf"^{RFS}config$",
        },
        "also": [
            "unity"
        ],  # Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml)
        "tests": ["test_scan_planning::test_plan_endpoint_is_advertised"],
        "note": "Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml, used by all tests).",
    },
    "iceberg_namespaces": {
        "title": "Iceberg REST: create, list, properties, drop namespaces",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}\{{prefix\}}/namespaces(/\{{namespace\}}(/properties)?)?$",
            "polaris": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}\{{prefix\}}/namespaces(/\{{namespace\}}(/properties)?)?$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces(/\{{namespace\}}(/properties)?)?$",
        },
        "also": [
            "unity"
        ],  # Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml)
        "tests": ["test_namespaces::test_namespace"],
    },
    "iceberg_tables": {
        "title": "Iceberg REST: create, load, commit, drop, rename tables",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}\{{prefix\}}/(namespaces/\{{namespace\}}/tables(/\{{table\}})?|tables/rename)$",
            "polaris": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}\{{prefix\}}/(namespaces/\{{namespace\}}/tables(/\{{table\}})?|tables/rename)$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/(namespaces/\{{namespace\}}/tables(/\{{table\}})?|tables/rename)$",
        },
        "also": [
            "unity"
        ],  # Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml)
        "tests": ["test_operations::test_operation", "test_interop::test_write_then_read"],
    },
    "iceberg_views": {
        "title": "Iceberg REST: create, load, replace, drop, rename views",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}\{{prefix\}}/(namespaces/\{{namespace\}}/views(/\{{view\}})?|views/rename)$",
            "polaris": rf"^(GET|POST|HEAD|DELETE) {ICEBERG}(\{{prefix\}})?(/namespaces/\{{namespace\}}/views(/\{{view\}})?|/views/rename)?$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/views(/\{{view\}})?$",
        },
        "also": [
            "unity"
        ],  # Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml)
        "tests": ["test_views::test_create_view", "test_views::test_read_view"],
    },
    "iceberg_register": {
        "title": "Iceberg REST: register a table (or view) from existing metadata",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/register(-view)?$",
            "polaris": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/register(-view)?$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/register$",
        },
        "tests": ["test_catalog_features::test_register_table"],
    },
    "iceberg_transactions": {
        "title": "Iceberg REST: transaction across several tables (/transactions/commit)",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}\{{prefix\}}/transactions/commit$",
            "polaris": rf"^POST {ICEBERG}\{{prefix\}}/transactions/commit$",
        },
        "tests": ["test_transactions::test_multi_table_commit"],
    },
    "iceberg_credentials": {
        "title": "Fetch short-lived credentials for a table",
        "endpoints": {
            "lakekeeper": rf"^GET {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/credentials$",
            "polaris": rf"^GET {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/credentials$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/credentials$",
            "unity": rf"^POST {UC}temporary-table-credentials$",
        },
        "tests": [
            "test_catalog_features::test_load_credentials",
            "test_sts_scope::test_write_and_delete_inside_own_table",
        ],
        "note": "Unity additionally with its own API (temporary-table-credentials); RustFS: route exists but vends no credentials.",
    },
    "iceberg_metrics": {
        "title": "Iceberg REST: report scan/commit metrics",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/metrics$",
            "polaris": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/metrics$",
        },
        "also": [
            "unity"
        ],  # Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml)
        "tests": ["test_catalog_features::test_metrics_report"],
    },
    "iceberg_scan_planning": {
        "title": "Iceberg REST: server-side scan planning",
        "endpoints": {
            "lakekeeper": rf"^(POST|GET|DELETE) {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/(plan(/\{{plan-id\}})?|tasks)$",
        },
        "also": [],  # only Lakekeeper describes it; the others answer 404/501 (base test on all)
        "tests": ["test_scan_planning::test_server_side_plan_with_projection_and_filter"],
        "not_implemented": {
            "lakekeeper": {
                "status": "404, not advertised in /v1/config",
                "test": "test_scan_planning::test_server_side_plan_with_projection_and_filter",
                "gap": "does not implement server-side scan planning",
            },
        },
        "note": "Base test on all catalogs; none implements it (404/501).",
    },
    "oauth_tokens": {
        "title": "Catalog issues access tokens",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}oauth/tokens$",
            "polaris": r"^POST /api/catalog/v1/oauth/tokens$",
            "unity": rf"^POST {UCC}auth/(tokens|logout)$",
        },
        "tests": ["test_authz::test_engine_reads"],
        "note": "Lakekeeper: endpoint from the specification; in the lab the tokens come from Keycloak. Unity: token exchange (unity/token_shim.py).",
    },
    "iceberg_remote_signing": {
        "title": "Iceberg REST: catalog signs S3 requests (remote signing)",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/sign$",
            "polaris": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/sign$",
        },
        "tests": ["test_catalog_features::test_remote_signing"],
        "not_implemented": {
            "polaris": {
                "status": "501, not advertised in /v1/config",
                "test": "test_catalog_features::test_remote_signing",
                "gap": "Polaris 1.9 has the Iceberg signing endpoint in its API but answers 501",
            },
        },
    },
    "unregister": {
        "title": "Unregister a table from the catalog (files stay)",
        "endpoints": {
            "lakekeeper": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/unregister$"
        },
        "tests": ["test_catalog_features::test_lakekeeper_unregister"],
        "not_implemented": {
            "lakekeeper": {
                "status": "404, not advertised in /v1/config",
                "test": "test_catalog_features::test_lakekeeper_unregister",
                "gap": "describes POST .../tables/{t}/unregister in its API, but the server answers 404",
            },
        },
    },
    "openlineage": {
        "title": "Receive OpenLineage events",
        "endpoints": {"polaris": r"^POST /api/openlineage/v1/lineage(/batch)?$"},
        "tests": ["test_catalog_features::test_polaris_openlineage"],
    },
    # ===================================================================== catalog management
    "catalog_lifecycle": {
        "title": "Create, list, show, drop warehouses/catalogs",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|DELETE) {LK}warehouse(/\{{warehouse_id\}})?$",
            "polaris": rf"^(GET|POST|DELETE) {PM}catalogs(/\{{catalogName\}})?$",
            "rustfs": rf"^{RFS}buckets/\{{warehouse\}}$",
            "unity": rf"^(GET|POST|DELETE) {UC}catalogs(/\{{name\}})?$",
        },
        "tests": [
            "test_catalog_admin::test_catalog_lifecycle",
            "test_catalog_features::test_rustfs_table_bucket_lifecycle",
        ],
        "note": "RustFS: table bucket = S3 bucket with the catalog enabled; cannot be disabled after a table exists (special-feature test).",
    },
    "catalog_update": {
        "title": "Alter a warehouse/catalog (rename, properties)",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/rename$",
            "polaris": rf"^PUT {PM}catalogs/\{{catalogName\}}$",
            "unity": rf"^PATCH {UC}catalogs/\{{name\}}$",
        },
        "tests": ["test_catalog_admin::test_catalog_update"],
        "note": "Lakekeeper and Unity: rename; Polaris cannot rename, only change properties.",
    },
    "catalog_deactivate": {
        "title": "Disable and re-enable a warehouse/catalog",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/(activate|deactivate)$",
        },
        "also": ["rustfs"],  # RustFS: DELETE/PUT /iceberg/v1/buckets/{b} schaltet ab/an
        "tests": ["test_catalog_admin::test_catalog_deactivate"],
        "note": "RustFS disables and enables table buckets through DELETE/PUT /buckets/{b} (see catalog_lifecycle).",
    },
    "storage_config": {
        "title": "Configure the catalog's storage access (profile, credentials, locations)",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/(storage|storage-credential)$",
            "unity": rf"^(GET|POST|PATCH|DELETE) {UC}(credentials|external-locations)(/\{{name\}})?$",
        },
        "also": ["polaris"],  # Polaris: storageConfigInfo in PUT /api/management/v1/catalogs/{name}
        "tests": ["test_catalog_admin::test_storage_config"],
        "note": "Polaris: the storage configuration is part of PUT /catalogs/{name} (catalog_update).",
    },
    "storage_validation": {
        "title": "Validate storage access before creating",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}(warehouse-creation-validation|warehouse/\{{warehouse_id\}}/storage/validate-(access|credential|profile))$",
        },
        "tests": ["test_catalog_features::test_lakekeeper_storage_validation"],
    },
    "users": {
        "title": "Manage users/principals (create, list, alter, drop)",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|PUT|DELETE) {LK}(user(/\{{user_id\}})?|search/user)$",
            "polaris": rf"^(GET|POST|PUT|DELETE) {PM}principals(/\{{principalName\}})?$",
            "unity": rf"^(GET|POST|PUT|DELETE) {UCC}scim2/Users(/\{{id\}})?$",
        },
        "also": [
            "rustfs"
        ],  # RustFS: IAM users through /rustfs/admin/v3 (not part of the catalog API)
        "tests": ["test_catalog_admin::test_users"],
        "note": "RustFS: IAM users through the RustFS admin API (not part of the catalog API).",
    },
    "whoami": {
        "title": "Who am I (logged-in identity)",
        "endpoints": {
            "lakekeeper": rf"^GET {LK}whoami$",
            "unity": rf"^GET {UCC}scim2/Me$",
        },
        "tests": ["test_authz::test_whoami"],
    },
    "roles": {
        "title": "Manage and assign roles",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|PUT|DELETE) {LK}(role(?!.*/actions$).*|search/role|user/\{{user_id\}}/roles.*)$",
            "polaris": rf"^(GET|POST|PUT|DELETE) {PM}(principal-roles.*|catalogs/\{{catalogName\}}/catalog-roles(/\{{catalogRoleName\}})?(/principal-roles)?|principals/\{{principalName\}}/principal-roles.*)$",
        },
        "also": [
            "rustfs"
        ],  # RustFS: IAM groups through the admin API (not in the Iceberg API text)
        "tests": [
            "test_rbac::test_role_grant_reaches_every_member",
            "test_rbac::test_removed_member_loses_access",
            "test_rbac::test_roles_add_up",
            "test_rbac::test_idp_role_is_a_catalog_role",
        ],
        "note": "RustFS: IAM groups with a group policy (admin API). Unity Catalog OSS 0.7: no roles or groups (SCIM /Groups answers 500) - checked by test_rbac on every run.",
    },
    "web_ui": {
        "title": "Web UI of the catalog",
        "endpoints": {},
        "also": ["lakekeeper", "polaris", "rustfs", "unity"],
        "tests": ["test_ui::test_catalog_has_ui", "test_ui::test_ui_data_needs_login"],
        "note": "Lakekeeper /ui/ and the RustFS console (:9001) from the server; Polaris Console (apache/polaris-tools, built in the lab) and Unity UI (unitycatalog-ui) as separate containers.",
    },
    "grants": {
        "title": "Grant, revoke, list privileges",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}(.*/)?grants(/.*)?$",
            "polaris": rf"^(GET|PUT|POST) {PM}catalogs/\{{catalogName\}}/catalog-roles/\{{catalogRoleName\}}/grants$",
            "unity": rf"^(GET|PATCH) {UC}permissions/\{{securable_type\}}/\{{full_name\}}$",
        },
        "tests": [
            "test_authz::test_revoke_takes_effect",
            "test_authz::test_select_grant_cannot_append",
            "test_privileges::test_privilege_allows_its_action",
            "test_privileges::test_action_needs_its_privilege",
            "test_privileges::test_recorded_privileges_are_accepted",
        ],
    },
    "permission_check": {
        "title": "Query allowed actions (what may I do here?)",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}(action/batch-check|.*/actions)$",
        },
        "tests": ["test_catalog_features::test_lakekeeper_permission_check"],
    },
    "credential_rotation": {
        "title": "Reset/rotate a principal's credentials",
        "endpoints": {
            "polaris": rf"^POST {PM}principals/\{{principalName\}}/(reset|rotate)$",
        },
        "tests": ["test_authz::test_engine_reads"],
        "note": "reset with fixed credentials is used in the authorization setup; rotate untested.",
    },
    "projects": {
        "title": "Manage projects (tenants)",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|DELETE) {LK}(project(-list)?|project/\{{project_id\}}(/rename)?|project/rename)$",
        },
        "tests": ["test_catalog_features::test_lakekeeper_projects"],
    },
    "server_info": {
        "title": "Server/metastore information",
        "endpoints": {
            "lakekeeper": rf"^GET {LK}info$",
            "unity": rf"^GET {UC}metastore_summary$",
        },
        "tests": ["test_catalog_admin::test_server_info"],
    },
    "bootstrap": {
        "title": "Set up the server for the first time (bootstrap)",
        "endpoints": {"lakekeeper": rf"^POST {LK}bootstrap$"},
        "tests": [],
        "untested_reason": "Part of every setup (bootstrap.py, authz_backends.py): without bootstrap no test runs.",
    },
    "maintenance_mode": {
        "title": "Put the server into maintenance mode",
        "endpoints": {"lakekeeper": rf"^POST {LK}server/maintenance-mode$"},
        "tests": ["test_catalog_features::test_lakekeeper_maintenance_mode"],
    },
    "endpoint_statistics": {
        "title": "Call statistics per endpoint",
        "endpoints": {"lakekeeper": rf"^POST {LK}endpoint-statistics$"},
        "tests": ["test_catalog_features::test_lakekeeper_endpoint_statistics"],
    },
    "warehouse_statistics": {
        "title": "Statistics of a warehouse (number of tables/views)",
        "endpoints": {"lakekeeper": rf"^GET {LK}warehouse/\{{warehouse_id\}}/statistics$"},
        "tests": ["test_catalog_features::test_warehouse_statistics"],
    },
    "tags": {
        "title": "Tags on catalog objects (definitions, assignment, permissions)",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST|PUT|DELETE) {LK}(tag-definition(?!.*/(grants|actions)).*|.*/tags(/.*)?|.*/column-tags)$"
        },
        "tests": ["test_catalog_features::test_tags"],
    },
    "protection": {
        "title": "Deletion protection for warehouse, namespace, table, view",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}warehouse/\{{warehouse_id\}}(/.*)?/protection$"
        },
        "tests": ["test_catalog_features::test_drop_protection"],
    },
    "soft_delete": {
        "title": "Trash bin: show and restore dropped tables, delete profile",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}warehouse/\{{warehouse_id\}}/(deleted-tabulars(/undrop)?|delete-profile)$"
        },
        "tests": ["test_catalog_features::test_soft_delete_and_undrop"],
        "note": "Unity deletes managed tables with a delay (7 days) but offers no API to restore them.",
    },
    "background_tasks": {
        "title": "Show and control the catalog's background tasks",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}(project/)?(task/.*|warehouse/\{{warehouse_id\}}/task/.*)$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/maintenance/(jobs/\{{job\}}(/heartbeat|/quarantine)?|scheduler(/run)?|worker/run)$",
        },
        "tests": ["test_catalog_admin::test_background_tasks"],
    },
    "task_queue_config": {
        "title": "Configure the task queues (drop, purge, log cleanup)",
        "endpoints": {
            "lakekeeper": rf"^(GET|POST) {LK}(project|warehouse/\{{warehouse_id\}})/task-queue/.*$"
        },
        "tests": ["test_catalog_features::test_lakekeeper_task_queue_config"],
    },
    "table_maintenance_by_catalog": {
        "title": "Table maintenance by the catalog (configuration, metadata)",
        "endpoints": {
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/maintenance/(config|metadata)$"
        },
        "tests": ["test_catalog_features::test_rustfs_table_maintenance"],
    },
    "search": {
        "title": "Search tables and views",
        "endpoints": {"lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/search-tabular$"},
        "tests": ["test_catalog_features::test_search_tables"],
    },
    "namespace_move": {
        "title": "Move a namespace",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/namespace/\{{namespace_id\}}/move$"
        },
        "tests": ["test_catalog_features::test_namespace_move"],
    },
    "format_version_policy": {
        "title": "Format-version policy of a warehouse",
        "endpoints": {
            "lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/format-version-policy$"
        },
        "tests": ["test_catalog_features::test_format_version_policy"],
    },
    "managed_by": {
        "title": "Mark a warehouse as externally managed",
        "endpoints": {"lakekeeper": rf"^POST {LK}warehouse/\{{warehouse_id\}}/managed-by$"},
        "tests": ["test_catalog_features::test_lakekeeper_managed_by"],
    },
    # ===================================================================== tables beyond Iceberg
    "non_iceberg_tables": {
        "title": "Non-Iceberg tables in the catalog (generic tables, Delta)",
        "endpoints": {
            "polaris": rf"^(GET|POST|DELETE) {PC}namespaces/\{{namespace\}}/generic-tables(/\{{generic-table\}})?$",
            "unity": rf"^(GET|POST|DELETE) {UC}(tables(/\{{full_name\}})?|staging-tables|delta/preview/commits)$",
        },
        "tests": ["test_catalog_features::test_generic_tables"],
        "note": "Lakekeeper: permissions/tags/protection for generic tables exist, but no endpoint to create them (404).",
    },
    "external_table_sync": {
        "title": "Adopt/sync a table from externally written metadata",
        "endpoints": {
            "polaris": rf"^POST {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/notifications$",
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/catalog/external(/sync)?$",
        },
        "tests": ["test_catalog_features::test_external_table_sync"],
    },
    "policies": {
        "title": "Create policies, attach them to objects, inherit them",
        "endpoints": {
            "polaris": rf"^(GET|POST|PUT|DELETE) {PC}(namespaces/\{{namespace\}}/policies.*|applicable-policies)$"
        },
        "tests": ["test_catalog_features::test_policies"],
    },
    "federation": {
        "title": "Integrate a foreign catalog (federation)",
        "endpoints": {},
        "also": [
            "polaris"
        ],  # Polaris: catalog type EXTERNAL through POST /api/management/v1/catalogs
        "tests": [
            "test_catalog_features::test_federation",
            "test_catalog_features::test_federation_direct",
        ],
        "note": "Polaris: catalog type EXTERNAL through POST /catalogs (covered by catalog_lifecycle).",
    },
    "unity_own_object_api": {
        "title": "Own object API next to Iceberg REST (schemas through the Unity API)",
        "endpoints": {"unity": rf"^(GET|POST|PATCH|DELETE) {UC}schemas(/\{{full_name\}})?$"},
        "tests": ["test_catalog_features::test_unity_schema_api"],
    },
    "volumes": {
        "title": "Volumes (files managed by the catalog)",
        "endpoints": {
            "unity": rf"^(GET|POST|PATCH|DELETE) {UC}(volumes(/\{{name\}})?|temporary-volume-credentials)$"
        },
        "tests": ["test_catalog_features::test_unity_volumes"],
    },
    "functions": {
        "title": "Register functions in the catalog",
        "endpoints": {
            "lakekeeper": rf"^GET {ICEBERG}\{{prefix\}}/namespaces/\{{namespace\}}/functions(/\{{function\}})?$",
            "unity": rf"^(GET|POST|DELETE) {UC}functions(/\{{name\}})?$",
        },
        "tests": [
            "test_catalog_features::test_catalog_functions",
            "test_catalog_features::test_unity_functions",
        ],
        "not_implemented": {
            "lakekeeper": {
                "status": "404, not advertised in /v1/config",
                "test": "test_catalog_features::test_catalog_functions",
                "gap": "describes GET .../namespaces/{ns}/functions in its API, but the server answers 404",
            },
        },
    },
    "models": {
        "title": "ML models and model versions",
        "endpoints": {
            "unity": rf"^(GET|POST|PATCH|DELETE) {UC}(models.*|temporary-model-version-credentials)$"
        },
        "tests": ["test_catalog_features::test_unity_models"],
    },
    "path_credentials": {
        "title": "Temporary credentials for a storage path",
        "endpoints": {"unity": rf"^POST {UC}temporary-path-credentials$"},
        "tests": ["test_catalog_features::test_unity_temporary_path_credentials"],
    },
    "table_refs_api": {
        "title": "Branches/tags of a table through an own API",
        "endpoints": {
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/refs(/\{{ref\}})?$"
        },
        "tests": ["test_catalog_features::test_rustfs_refs_api"],
    },
    "metadata_location_api": {
        "title": "Read/set a table's metadata location directly",
        "endpoints": {
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/metadata-location$"
        },
        "tests": ["test_catalog_features::test_rustfs_metadata_location_api"],
    },
    "table_admin": {
        "title": "Export/import, diagnose, restore, reset a table",
        "endpoints": {
            "rustfs": rf"^{RFS}\{{warehouse\}}/namespaces/\{{namespace\}}/tables/\{{table\}}/catalog/(export|import|diagnostics|recovery|rollback)$"
        },
        "tests": ["test_catalog_features::test_rustfs_table_admin"],
    },
    "catalog_backup": {
        "title": "Back up, restore, export, import, migrate the catalog",
        "endpoints": {
            "rustfs": rf"^{RFS}\{{warehouse\}}/catalog/(backup(/export|/import|/prepare)?|restore|directory|warehouse-index/backfill)$"
        },
        "tests": ["test_catalog_features::test_rustfs_catalog_backup"],
        "note": "RustFS: directory status and index backfill work, GET backup answers 501.",
    },
}

CATALOGS = ("lakekeeper", "polaris", "rustfs", "unity")
API_FILES = {
    "lakekeeper": "lakekeeper-0.15.1.txt",
    "polaris": "polaris-1.9.0.txt",
    "rustfs": "rustfs-1.0.6.txt",
    "unity": "unity-0.7.0.txt",
}


def offered_by(cap: dict) -> list[str]:
    """Catalogs that offer a capability (have endpoints for it, or are named in the note).

    Args:
        cap: Capability entry.

    Returns:
        The catalogs.
    """
    return [c for c in CATALOGS if c in cap["endpoints"]] + list(cap.get("also", ()))


def works_on(cap: dict) -> list[str]:
    """Catalogs that offer a capability and implement it (offered minus ◐).

    Args:
        cap: Capability entry.

    Returns:
        The catalogs.
    """
    return [c for c in offered_by(cap) if c not in cap.get("not_implemented", {})]


def kind(name: str, cap: dict) -> str:
    """BASE if two or more catalogs offer it - or if the Iceberg REST spec defines it (the
    standard asks it of every catalog) - else SPECIFIC (docs/test-architecture.md).

    Args:
        name: Capability key.
        cap: Capability entry.

    Returns:
        ``Base`` or ``Specific``.
    """
    if name.startswith("iceberg_") or len(set(offered_by(cap))) >= 2:
        return "Base"
    return "Specific"


def render() -> str:
    """docs/feature-inventory.md (generated - do not edit by hand).

    Returns:
        The Markdown text.
    """
    lines = [
        "# Feature inventory of the catalogs",
        "",
        "Generated from `tests/lab/feature_inventory.py` (`uv run python -m feature_inventory`",
        "in the folder `tests/lab/`). Source of the endpoints: `docs/api/` (extracted per catalog",
        "from its API description). Rule: a capability that at least two catalogs offer or that",
        "the Iceberg REST specification defines is **Base** (the same test for all); a",
        "capability of exactly one catalog is **Specific**. `tests/selftest/test_feature_inventory.py`",
        "checks that every endpoint belongs to exactly one capability.",
        "",
        "Symbols: **✓** described in the API and implemented (whether it works in detail is shown",
        "by the tests) · **◐** described in the API, but the server does not implement it (status",
        "code and proving test in the note) · **–** not offered. Partial failures stay ✓ with a",
        "note.",
        "",
        "| | " + " | ".join(c.capitalize() if c != "rustfs" else "RustFS" for c in CATALOGS) + " |",
        "|---|" + "---|" * len(CATALOGS),
        "| Capabilities described (of "
        + str(len(CAPABILITIES))
        + ") | "
        + " | ".join(str(sum(c in offered_by(x) for x in CAPABILITIES.values())) for c in CATALOGS)
        + " |",
        "| of which implemented | "
        + " | ".join(str(sum(c in works_on(x) for x in CAPABILITIES.values())) for c in CATALOGS)
        + " |",
        "",
        "| Capability | Kind | Lakekeeper | Polaris | RustFS | Unity | Evidence (tests) | Note |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, cap in CAPABILITIES.items():
        offered = set(offered_by(cap))
        missing = cap.get("not_implemented", {})
        cells = ["◐" if c in missing else "✓" if c in offered else "–" for c in CATALOGS]
        tests = "<br>".join(f"`{t}`" for t in cap["tests"]) or (
            "– *(" + cap["untested_reason"] + ")*" if "untested_reason" in cap else "**open**"
        )
        gaps = [
            f"{c.capitalize() if c != 'rustfs' else 'RustFS'}: described, not implemented "
            f"({m['status']}; `{m['test']}`)."
            for c, m in missing.items()
        ]
        note = " ".join(x for x in (*gaps, cap.get("note", "")) if x)
        lines.append(
            f"| {cap['title']} | {kind(name, cap)} | "
            + " | ".join(cells)
            + f" | {tests} | {note} |"
        )
    lines += ["", "## Endpoints per capability", ""]
    for name, cap in CAPABILITIES.items():
        lines.append(f"### {cap['title']} (`{name}`)")
        for cat in CATALOGS:
            eps = [e for e in endpoints(cat) if cat in cap["endpoints"] and _match(cap, cat, e)]
            if eps:
                lines.append(f"- **{cat}**: " + ", ".join(f"`{e}`" for e in eps))
        for why in (cap.get("also_note"),):
            if why:
                lines.append(f"- {why}")
        lines.append("")
    return "\n".join(lines) + "\n"


def endpoints(catalog: str) -> list[str]:
    """``METHOD /path`` lines of a catalog's API file (comments stripped).

    Args:
        catalog: Catalog name.

    Returns:
        The endpoint lines.
    """
    from pathlib import Path

    f = Path(__file__).resolve().parents[2] / "docs" / "api" / API_FILES[catalog]
    out = []
    for line in f.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            out.append(line.split("  #")[0].strip())
    return out


def _match(cap: dict, catalog: str, endpoint: str) -> bool:
    """Whether the capability's regex for the catalog matches the endpoint.

    Args:
        cap: Capability entry.
        catalog: Catalog name.
        endpoint: ``METHOD /path``.

    Returns:
        Whether it matches.
    """
    import re

    pattern = cap["endpoints"].get(catalog)
    return bool(pattern and re.search(pattern, endpoint))


def unmapped(catalog: str) -> dict[str, list[str]]:
    """Endpoint -> matching capabilities, for endpoints with zero or several matches.

    Args:
        catalog: Catalog name.

    Returns:
        The problem endpoints.
    """
    bad = {}
    for e in endpoints(catalog):
        hits = [n for n, cap in CAPABILITIES.items() if _match(cap, catalog, e)]
        if len(hits) != 1:
            bad[e] = hits
    return bad


if __name__ == "__main__":
    from pathlib import Path

    out = Path(__file__).resolve().parents[2] / "docs" / "feature-inventory.md"
    out.write_text(render())
    print(f"written {out}")
