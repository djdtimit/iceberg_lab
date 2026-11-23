# Feature inventory of the catalogs

Generated from `tests/lab/feature_inventory.py` (`uv run python -m feature_inventory`
in the folder `tests/lab/`). Source of the endpoints: `docs/api/` (extracted per catalog
from its API description). Rule: a capability that at least two catalogs offer or that
the Iceberg REST specification defines is **Base** (the same test for all); a
capability of exactly one catalog is **Specific**. `tests/selftest/test_feature_inventory.py`
checks that every endpoint belongs to exactly one capability.

Symbols: **✓** described in the API and implemented (whether it works in detail is shown
by the tests) · **◐** described in the API, but the server does not implement it (status
code and proving test in the note) · **–** not offered. Partial failures stay ✓ with a
note.

| | Lakekeeper | Polaris | RustFS | Unity | Reference |
|---|---|---|---|---|---|
| Capabilities described (of 54) | 39 | 23 | 18 | 21 | 10 |
| of which implemented | 36 | 22 | 18 | 21 | 10 |

| Capability | Kind | Lakekeeper | Polaris | RustFS | Unity | Reference | Evidence (tests) | Note |
|---|---|---|---|---|---|---|---|---|
| Iceberg REST: fetch configuration (/v1/config) | Base | ✓ | ✓ | ✓ | ✓ | ✓ | `test_scan_planning::test_plan_endpoint_is_advertised` | Unity: Iceberg REST under /api/2.1/unity-catalog/iceberg (not described in all.yaml, used by all tests). |
| Iceberg REST: create, list, properties, drop namespaces | Base | ✓ | ✓ | ✓ | ✓ | ✓ | `test_namespaces::test_namespace` |  |
| Iceberg REST: create, load, commit, drop, rename tables | Base | ✓ | ✓ | ✓ | ✓ | ✓ | `test_operations::test_operation`<br>`test_interop::test_write_then_read` |  |
| Iceberg REST: create, load, replace, drop, rename views | Base | ✓ | ✓ | ✓ | ✓ | ✓ | `test_views::test_create_view`<br>`test_views::test_read_view` |  |
| Iceberg REST: register a table (or view) from existing metadata | Base | ✓ | ✓ | ✓ | – | ✓ | `test_catalog_features::test_register_table` |  |
| Iceberg REST: transaction across several tables (/transactions/commit) | Base | ✓ | ✓ | – | – | ✓ | `test_transactions::test_multi_table_commit` |  |
| Fetch short-lived credentials for a table | Base | ✓ | ✓ | ✓ | ✓ | – | `test_catalog_features::test_load_credentials`<br>`test_sts_scope::test_write_and_delete_inside_own_table` | Unity additionally with its own API (temporary-table-credentials); RustFS: route exists but vends no credentials. |
| Iceberg REST: report scan/commit metrics | Base | ✓ | ✓ | – | ✓ | ✓ | `test_catalog_features::test_metrics_report` |  |
| Iceberg REST: server-side scan planning | Base | ◐ | – | – | – | ✓ | `test_scan_planning::test_server_side_plan_with_projection_and_filter` | Lakekeeper: described, not implemented (404, not advertised in /v1/config; `test_scan_planning::test_server_side_plan_with_projection_and_filter`). Base test on all catalogs; only the Iceberg REST fixture implements it, the others answer 404/501. |
| Catalog issues access tokens | Base | ✓ | ✓ | – | ✓ | ✓ | `test_authz::test_engine_reads` | Lakekeeper: endpoint from the specification; in the lab the tokens come from Keycloak. Unity: token exchange (unity/token_shim.py). Reference: served by the fixture, not tested (no authentication in the lab). |
| Iceberg REST: catalog signs S3 requests (remote signing) | Base | ✓ | ◐ | – | – | – | `test_catalog_features::test_remote_signing` | Polaris: described, not implemented (501, not advertised in /v1/config; `test_catalog_features::test_remote_signing`). |
| Unregister a table from the catalog (files stay) | Base | ◐ | – | – | – | ✓ | `test_catalog_features::test_unregister` | Lakekeeper: described, not implemented (404, not advertised in /v1/config; `test_catalog_features::test_unregister`). |
| Receive OpenLineage events | Specific | – | ✓ | – | – | – | `test_catalog_features::test_polaris_openlineage` |  |
| Create, list, show, drop warehouses/catalogs | Base | ✓ | ✓ | ✓ | ✓ | – | `test_catalog_admin::test_catalog_lifecycle`<br>`test_catalog_features::test_rustfs_table_bucket_lifecycle` | RustFS: table bucket = S3 bucket with the catalog enabled; cannot be disabled after a table exists (special-feature test). |
| Alter a warehouse/catalog (rename, properties) | Base | ✓ | ✓ | – | ✓ | – | `test_catalog_admin::test_catalog_update` | Lakekeeper and Unity: rename; Polaris cannot rename, only change properties. |
| Disable and re-enable a warehouse/catalog | Base | ✓ | – | ✓ | – | – | `test_catalog_admin::test_catalog_deactivate` | RustFS disables and enables table buckets through DELETE/PUT /buckets/{b} (see catalog_lifecycle). |
| Configure the catalog's storage access (profile, credentials, locations) | Base | ✓ | ✓ | – | ✓ | – | `test_catalog_admin::test_storage_config` | Polaris: the storage configuration is part of PUT /catalogs/{name} (catalog_update). |
| Validate storage access before creating | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_storage_validation` |  |
| Manage users/principals (create, list, alter, drop) | Base | ✓ | ✓ | ✓ | ✓ | – | `test_catalog_admin::test_users` | RustFS: IAM users through the RustFS admin API (not part of the catalog API). |
| Who am I (logged-in identity) | Base | ✓ | – | – | ✓ | – | `test_authz::test_whoami` |  |
| Manage and assign roles | Base | ✓ | ✓ | ✓ | – | – | `test_rbac::test_role_grant_reaches_every_member`<br>`test_rbac::test_removed_member_loses_access`<br>`test_rbac::test_roles_add_up`<br>`test_rbac::test_idp_role_is_a_catalog_role` | RustFS: IAM groups with a group policy (admin API). Unity Catalog OSS 0.7: no roles or groups (its API describes only SCIM /Users and /Me; /Groups answers 500) - checked by test_rbac on every run. |
| Web UI of the catalog | Base | ✓ | ✓ | ✓ | ✓ | – | `test_ui::test_catalog_has_ui`<br>`test_ui::test_ui_data_needs_login` | Lakekeeper /ui/ and the RustFS console (:9001) from the server; Polaris Console (apache/polaris-tools, built in the lab) and Unity UI (unitycatalog-ui) as separate containers. |
| Grant, revoke, list privileges | Base | ✓ | ✓ | – | ✓ | – | `test_authz::test_revoke_takes_effect`<br>`test_authz::test_select_grant_cannot_append`<br>`test_privileges::test_privilege_allows_its_action`<br>`test_privileges::test_action_needs_its_privilege`<br>`test_privileges::test_recorded_privileges_are_accepted` |  |
| Query allowed actions (what may I do here?) | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_permission_check` |  |
| Reset/rotate a principal's credentials | Specific | – | ✓ | – | – | – | `test_authz::test_engine_reads` | reset with fixed credentials is used in the authorization setup; rotate untested. |
| Manage projects (tenants) | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_projects` |  |
| Server/metastore information | Base | ✓ | – | – | ✓ | – | `test_catalog_admin::test_server_info` |  |
| Set up the server for the first time (bootstrap) | Specific | ✓ | – | – | – | – | – *(Part of every setup (bootstrap.py, authz_backends.py): without bootstrap no test runs.)* |  |
| Put the server into maintenance mode | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_maintenance_mode` |  |
| Call statistics per endpoint | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_endpoint_statistics` |  |
| Statistics of a warehouse (number of tables/views) | Specific | ✓ | – | – | – | – | `test_catalog_features::test_warehouse_statistics` |  |
| Tags on catalog objects (definitions, assignment, permissions) | Specific | ✓ | – | – | – | – | `test_catalog_features::test_tags` |  |
| Deletion protection for warehouse, namespace, table, view | Specific | ✓ | – | – | – | – | `test_catalog_features::test_drop_protection` |  |
| Trash bin: show and restore dropped tables, delete profile | Specific | ✓ | – | – | – | – | `test_catalog_features::test_soft_delete_and_undrop` | Unity deletes managed tables with a delay (7 days) but offers no API to restore them. |
| Show and control the catalog's background tasks | Base | ✓ | – | ✓ | – | – | `test_catalog_admin::test_background_tasks` |  |
| Configure the task queues (drop, purge, log cleanup) | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_task_queue_config` |  |
| Table maintenance by the catalog (configuration, metadata) | Specific | – | – | ✓ | – | – | `test_catalog_features::test_rustfs_table_maintenance` |  |
| Search tables and views | Specific | ✓ | – | – | – | – | `test_catalog_features::test_search_tables` |  |
| Move a namespace | Specific | ✓ | – | – | – | – | `test_catalog_features::test_namespace_move` |  |
| Format-version policy of a warehouse | Specific | ✓ | – | – | – | – | `test_catalog_features::test_format_version_policy` |  |
| Mark a warehouse as externally managed | Specific | ✓ | – | – | – | – | `test_catalog_features::test_lakekeeper_managed_by` |  |
| Non-Iceberg tables in the catalog (generic tables, Delta) | Base | – | ✓ | – | ✓ | – | `test_catalog_features::test_generic_tables` | Lakekeeper: permissions/tags/protection for generic tables exist, but no endpoint to create them (404). |
| Adopt/sync a table from externally written metadata | Base | – | ✓ | ✓ | – | – | `test_catalog_features::test_external_table_sync` |  |
| Create policies, attach them to objects, inherit them | Specific | – | ✓ | – | – | – | `test_catalog_features::test_policies` |  |
| Integrate a foreign catalog (federation) | Specific | – | ✓ | – | – | – | `test_catalog_features::test_federation`<br>`test_catalog_features::test_federation_direct` | Polaris: catalog type EXTERNAL through POST /catalogs (covered by catalog_lifecycle). |
| Own object API next to Iceberg REST (schemas through the Unity API) | Specific | – | – | – | ✓ | – | `test_catalog_features::test_unity_schema_api` |  |
| Volumes (files managed by the catalog) | Specific | – | – | – | ✓ | – | `test_catalog_features::test_unity_volumes` |  |
| Register functions in the catalog | Base | ◐ | – | – | ✓ | – | `test_catalog_features::test_catalog_functions`<br>`test_catalog_features::test_unity_functions` | Lakekeeper: described, not implemented (404, not advertised in /v1/config; `test_catalog_features::test_catalog_functions`). |
| ML models and model versions | Specific | – | – | – | ✓ | – | `test_catalog_features::test_unity_models` |  |
| Temporary credentials for a storage path | Specific | – | – | – | ✓ | – | `test_catalog_features::test_unity_temporary_path_credentials` |  |
| Branches/tags of a table through an own API | Specific | – | – | ✓ | – | – | `test_catalog_features::test_rustfs_refs_api` |  |
| Read/set a table's metadata location directly | Specific | – | – | ✓ | – | – | `test_catalog_features::test_rustfs_metadata_location_api` |  |
| Export/import, diagnose, restore, reset a table | Specific | – | – | ✓ | – | – | `test_catalog_features::test_rustfs_table_admin` |  |
| Back up, restore, export, import, migrate the catalog | Specific | – | – | ✓ | – | – | `test_catalog_features::test_rustfs_catalog_backup` | RustFS: directory status and index backfill work, GET backup answers 501. |

## Endpoints per capability

### Iceberg REST: fetch configuration (/v1/config) (`iceberg_config`)
- **lakekeeper**: `GET /catalog/v1/config`
- **polaris**: `GET /api/catalog/v1/config`
- **rustfs**: `ANY /iceberg/v1/config`
- **reference**: `GET /v1/config`

### Iceberg REST: create, list, properties, drop namespaces (`iceberg_namespaces`)
- **lakekeeper**: `DELETE /catalog/v1/{prefix}/namespaces/{namespace}`, `GET /catalog/v1/{prefix}/namespaces`, `GET /catalog/v1/{prefix}/namespaces/{namespace}`, `HEAD /catalog/v1/{prefix}/namespaces/{namespace}`, `POST /catalog/v1/{prefix}/namespaces`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/properties`
- **polaris**: `DELETE /api/catalog/v1/{prefix}/namespaces/{namespace}`, `GET /api/catalog/v1/{prefix}/namespaces`, `GET /api/catalog/v1/{prefix}/namespaces/{namespace}`, `HEAD /api/catalog/v1/{prefix}/namespaces/{namespace}`, `POST /api/catalog/v1/{prefix}/namespaces`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/properties`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/properties`
- **reference**: `DELETE /v1/{prefix}/namespaces/{namespace}`, `GET /v1/{prefix}/namespaces`, `GET /v1/{prefix}/namespaces/{namespace}`, `HEAD /v1/{prefix}/namespaces/{namespace}`, `POST /v1/{prefix}/namespaces`, `POST /v1/{prefix}/namespaces/{namespace}/properties`

### Iceberg REST: create, load, commit, drop, rename tables (`iceberg_tables`)
- **lakekeeper**: `DELETE /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/tables`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `HEAD /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /catalog/v1/{prefix}/tables/rename`
- **polaris**: `DELETE /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/tables`, `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `HEAD /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/tables`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /api/catalog/v1/{prefix}/tables/rename`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}`, `ANY /iceberg/v1/{warehouse}/tables/rename`
- **reference**: `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`, `GET /v1/{prefix}/namespaces/{namespace}/tables`, `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`, `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /v1/{prefix}/namespaces/{namespace}/tables`, `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`, `POST /v1/{prefix}/tables/rename`

### Iceberg REST: create, load, replace, drop, rename views (`iceberg_views`)
- **lakekeeper**: `DELETE /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/views`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `HEAD /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/views`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /catalog/v1/{prefix}/views/rename`
- **polaris**: `DELETE /api/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/views`, `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `HEAD /api/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/views`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /api/catalog/v1/{prefix}/views/rename`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/views`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/views/{view}`
- **reference**: `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`, `GET /v1/{prefix}/namespaces/{namespace}/views`, `GET /v1/{prefix}/namespaces/{namespace}/views/{view}`, `HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /v1/{prefix}/namespaces/{namespace}/views`, `POST /v1/{prefix}/namespaces/{namespace}/views/{view}`, `POST /v1/{prefix}/views/rename`

### Iceberg REST: register a table (or view) from existing metadata (`iceberg_register`)
- **lakekeeper**: `POST /catalog/v1/{prefix}/namespaces/{namespace}/register`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/register-view`
- **polaris**: `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/register`, `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/register-view`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/register`
- **reference**: `POST /v1/{prefix}/namespaces/{namespace}/register`, `POST /v1/{prefix}/namespaces/{namespace}/register-view`

### Iceberg REST: transaction across several tables (/transactions/commit) (`iceberg_transactions`)
- **lakekeeper**: `POST /catalog/v1/{prefix}/transactions/commit`
- **polaris**: `POST /api/catalog/v1/{prefix}/transactions/commit`
- **reference**: `POST /v1/{prefix}/transactions/commit`

### Fetch short-lived credentials for a table (`iceberg_credentials`)
- **lakekeeper**: `GET /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`
- **polaris**: `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/credentials`
- **unity**: `POST /api/2.1/unity-catalog/temporary-table-credentials`

### Iceberg REST: report scan/commit metrics (`iceberg_metrics`)
- **lakekeeper**: `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`
- **polaris**: `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`
- **reference**: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`

### Iceberg REST: server-side scan planning (`iceberg_scan_planning`)
- **lakekeeper**: `DELETE /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`, `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks`
- **reference**: `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`, `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`, `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`, `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks`

### Catalog issues access tokens (`oauth_tokens`)
- **lakekeeper**: `POST /catalog/v1/oauth/tokens`
- **polaris**: `POST /api/catalog/v1/oauth/tokens`
- **unity**: `POST /api/1.0/unity-control/auth/logout`, `POST /api/1.0/unity-control/auth/tokens`
- **reference**: `POST /v1/oauth/tokens`

### Iceberg REST: catalog signs S3 requests (remote signing) (`iceberg_remote_signing`)
- **lakekeeper**: `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign`
- **polaris**: `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign`

### Unregister a table from the catalog (files stay) (`unregister`)
- **lakekeeper**: `POST /catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister`
- **reference**: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister`

### Receive OpenLineage events (`openlineage`)
- **polaris**: `POST /api/openlineage/v1/lineage`, `POST /api/openlineage/v1/lineage/batch`

### Create, list, show, drop warehouses/catalogs (`catalog_lifecycle`)
- **lakekeeper**: `DELETE /management/v1/warehouse/{warehouse_id}`, `GET /management/v1/warehouse`, `GET /management/v1/warehouse/{warehouse_id}`, `POST /management/v1/warehouse`
- **polaris**: `DELETE /api/management/v1/catalogs/{catalogName}`, `GET /api/management/v1/catalogs`, `GET /api/management/v1/catalogs/{catalogName}`, `POST /api/management/v1/catalogs`
- **rustfs**: `ANY /iceberg/v1/buckets/{warehouse}`
- **unity**: `DELETE /api/2.1/unity-catalog/catalogs/{name}`, `GET /api/2.1/unity-catalog/catalogs`, `GET /api/2.1/unity-catalog/catalogs/{name}`, `POST /api/2.1/unity-catalog/catalogs`

### Alter a warehouse/catalog (rename, properties) (`catalog_update`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/rename`
- **polaris**: `PUT /api/management/v1/catalogs/{catalogName}`
- **unity**: `PATCH /api/2.1/unity-catalog/catalogs/{name}`

### Disable and re-enable a warehouse/catalog (`catalog_deactivate`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/activate`, `POST /management/v1/warehouse/{warehouse_id}/deactivate`

### Configure the catalog's storage access (profile, credentials, locations) (`storage_config`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/storage`, `POST /management/v1/warehouse/{warehouse_id}/storage-credential`
- **unity**: `DELETE /api/2.1/unity-catalog/credentials/{name}`, `DELETE /api/2.1/unity-catalog/external-locations/{name}`, `GET /api/2.1/unity-catalog/credentials`, `GET /api/2.1/unity-catalog/credentials/{name}`, `GET /api/2.1/unity-catalog/external-locations`, `GET /api/2.1/unity-catalog/external-locations/{name}`, `PATCH /api/2.1/unity-catalog/credentials/{name}`, `PATCH /api/2.1/unity-catalog/external-locations/{name}`, `POST /api/2.1/unity-catalog/credentials`, `POST /api/2.1/unity-catalog/external-locations`

### Validate storage access before creating (`storage_validation`)
- **lakekeeper**: `POST /management/v1/warehouse-creation-validation`, `POST /management/v1/warehouse/{warehouse_id}/storage/validate-access`, `POST /management/v1/warehouse/{warehouse_id}/storage/validate-credential`, `POST /management/v1/warehouse/{warehouse_id}/storage/validate-profile`

### Manage users/principals (create, list, alter, drop) (`users`)
- **lakekeeper**: `DELETE /management/v1/user/{user_id}`, `GET /management/v1/user`, `GET /management/v1/user/{user_id}`, `POST /management/v1/search/user`, `POST /management/v1/user`, `PUT /management/v1/user/{user_id}`
- **polaris**: `DELETE /api/management/v1/principals/{principalName}`, `GET /api/management/v1/principals`, `GET /api/management/v1/principals/{principalName}`, `POST /api/management/v1/principals`, `PUT /api/management/v1/principals/{principalName}`
- **unity**: `DELETE /api/1.0/unity-control/scim2/Users/{id}`, `GET /api/1.0/unity-control/scim2/Users`, `GET /api/1.0/unity-control/scim2/Users/{id}`, `POST /api/1.0/unity-control/scim2/Users`, `PUT /api/1.0/unity-control/scim2/Users/{id}`

### Who am I (logged-in identity) (`whoami`)
- **lakekeeper**: `GET /management/v1/whoami`
- **unity**: `GET /api/1.0/unity-control/scim2/Me`

### Manage and assign roles (`roles`)
- **lakekeeper**: `DELETE /management/v1/role/{role_id}`, `DELETE /management/v1/role/{role_id}/members/{member_type}/{member_id}`, `GET /management/v1/role`, `GET /management/v1/role/{role_id}`, `GET /management/v1/role/{role_id}/member-of`, `GET /management/v1/role/{role_id}/member-of/transitive`, `GET /management/v1/role/{role_id}/members`, `GET /management/v1/role/{role_id}/members/transitive`, `GET /management/v1/role/{role_id}/metadata`, `GET /management/v1/user/{user_id}/roles`, `GET /management/v1/user/{user_id}/roles/transitive`, `POST /management/v1/role`, `POST /management/v1/role/{role_id}`, `POST /management/v1/role/{role_id}/members`, `POST /management/v1/search/role`, `PUT /management/v1/role/{role_id}/source-system`
- **polaris**: `DELETE /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}`, `DELETE /api/management/v1/principal-roles/{principalRoleName}`, `DELETE /api/management/v1/principal-roles/{principalRoleName}/catalog-roles/{catalogName}/{catalogRoleName}`, `DELETE /api/management/v1/principals/{principalName}/principal-roles/{principalRoleName}`, `GET /api/management/v1/catalogs/{catalogName}/catalog-roles`, `GET /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}`, `GET /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}/principal-roles`, `GET /api/management/v1/principal-roles`, `GET /api/management/v1/principal-roles/{principalRoleName}`, `GET /api/management/v1/principal-roles/{principalRoleName}/catalog-roles/{catalogName}`, `GET /api/management/v1/principal-roles/{principalRoleName}/principals`, `GET /api/management/v1/principals/{principalName}/principal-roles`, `POST /api/management/v1/catalogs/{catalogName}/catalog-roles`, `POST /api/management/v1/principal-roles`, `PUT /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}`, `PUT /api/management/v1/principal-roles/{principalRoleName}`, `PUT /api/management/v1/principal-roles/{principalRoleName}/catalog-roles/{catalogName}`, `PUT /api/management/v1/principals/{principalName}/principal-roles`

### Web UI of the catalog (`web_ui`)

### Grant, revoke, list privileges (`grants`)
- **lakekeeper**: `GET /management/v1/grants`, `GET /management/v1/grants/grantable-privileges`, `GET /management/v1/project/grants`, `GET /management/v1/project/grants/grantable-privileges`, `GET /management/v1/server/grants`, `GET /management/v1/server/grants/grantable-privileges`, `GET /management/v1/tag-definition/{tag_definition_id}/grants`, `GET /management/v1/tag-definition/{tag_definition_id}/grants/grantable-privileges`, `GET /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants`, `GET /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants/grantable-privileges`, `GET /management/v1/warehouse/{warehouse_id}/grants`, `GET /management/v1/warehouse/{warehouse_id}/grants/grantable-privileges`, `GET /management/v1/warehouse/{warehouse_id}/grants/subtree`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/grantable-privileges`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/grants`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/grants/grantable-privileges`, `GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/grants`, `GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/grants/grantable-privileges`, `POST /management/v1/project/grants`, `POST /management/v1/server/grants`, `POST /management/v1/tag-definition/{tag_definition_id}/grants`, `POST /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/grants`, `POST /management/v1/warehouse/{warehouse_id}/grants`, `POST /management/v1/warehouse/{warehouse_id}/grants/subtree/revoke`, `POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants`, `POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/grants/subtree/revoke`, `POST /management/v1/warehouse/{warehouse_id}/table/{table_id}/grants`, `POST /management/v1/warehouse/{warehouse_id}/view/{view_id}/grants`
- **polaris**: `GET /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants`, `POST /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants`, `PUT /api/management/v1/catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants`
- **unity**: `GET /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`, `PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`

### Query allowed actions (what may I do here?) (`permission_check`)
- **lakekeeper**: `GET /management/v1/project/actions`, `GET /management/v1/role/{role_id}/actions`, `GET /management/v1/server/actions`, `GET /management/v1/tag-definition/{tag_definition_id}/actions`, `GET /management/v1/user/{user_id}/actions`, `GET /management/v1/warehouse/{warehouse_id}/actions`, `GET /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/actions`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/actions`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/actions`, `GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/actions`, `POST /management/v1/action/batch-check`

### Reset/rotate a principal's credentials (`credential_rotation`)
- **polaris**: `POST /api/management/v1/principals/{principalName}/reset`, `POST /api/management/v1/principals/{principalName}/rotate`

### Manage projects (tenants) (`projects`)
- **lakekeeper**: `DELETE /management/v1/project`, `DELETE /management/v1/project/{project_id}`, `GET /management/v1/project`, `GET /management/v1/project-list`, `GET /management/v1/project/{project_id}`, `POST /management/v1/project`, `POST /management/v1/project/rename`, `POST /management/v1/project/{project_id}/rename`

### Server/metastore information (`server_info`)
- **lakekeeper**: `GET /management/v1/info`
- **unity**: `GET /api/2.1/unity-catalog/metastore_summary`

### Set up the server for the first time (bootstrap) (`bootstrap`)
- **lakekeeper**: `POST /management/v1/bootstrap`

### Put the server into maintenance mode (`maintenance_mode`)
- **lakekeeper**: `POST /management/v1/server/maintenance-mode`

### Call statistics per endpoint (`endpoint_statistics`)
- **lakekeeper**: `POST /management/v1/endpoint-statistics`

### Statistics of a warehouse (number of tables/views) (`warehouse_statistics`)
- **lakekeeper**: `GET /management/v1/warehouse/{warehouse_id}/statistics`

### Tags on catalog objects (definitions, assignment, permissions) (`tags`)
- **lakekeeper**: `DELETE /management/v1/tag-definition/{tag_definition_id}`, `DELETE /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags/{tag_name}`, `DELETE /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags/{tag_name}`, `DELETE /management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags/{tag_name}`, `DELETE /management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/{tag_name}`, `DELETE /management/v1/warehouse/{warehouse_id}/tags/{tag_name}`, `DELETE /management/v1/warehouse/{warehouse_id}/view/{view_id}/tags/{tag_name}`, `GET /management/v1/tag-definition`, `GET /management/v1/tag-definition/{tag_definition_id}`, `GET /management/v1/tag-definition/{tag_definition_id}/attachments`, `GET /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/column-tags`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/tags`, `GET /management/v1/warehouse/{warehouse_id}/tags`, `GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/tags`, `POST /management/v1/tag-definition`, `POST /management/v1/tag-definition/{tag_definition_id}`, `PUT /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/tags/{tag_name}`, `PUT /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/tags/{tag_name}`, `PUT /management/v1/warehouse/{warehouse_id}/table/{table_id}/column/{column_name}/tags/{tag_name}`, `PUT /management/v1/warehouse/{warehouse_id}/table/{table_id}/tags/{tag_name}`, `PUT /management/v1/warehouse/{warehouse_id}/tags/{tag_name}`, `PUT /management/v1/warehouse/{warehouse_id}/view/{view_id}/tags/{tag_name}`

### Deletion protection for warehouse, namespace, table, view (`protection`)
- **lakekeeper**: `GET /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/protection`, `GET /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection`, `GET /management/v1/warehouse/{warehouse_id}/table/{table_id}/protection`, `GET /management/v1/warehouse/{warehouse_id}/view/{view_id}/protection`, `POST /management/v1/warehouse/{warehouse_id}/generic-table/{generic_table_id}/protection`, `POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/protection`, `POST /management/v1/warehouse/{warehouse_id}/protection`, `POST /management/v1/warehouse/{warehouse_id}/table/{table_id}/protection`, `POST /management/v1/warehouse/{warehouse_id}/view/{view_id}/protection`

### Trash bin: show and restore dropped tables, delete profile (`soft_delete`)
- **lakekeeper**: `GET /management/v1/warehouse/{warehouse_id}/deleted-tabulars`, `POST /management/v1/warehouse/{warehouse_id}/delete-profile`, `POST /management/v1/warehouse/{warehouse_id}/deleted-tabulars/undrop`

### Show and control the catalog's background tasks (`background_tasks`)
- **lakekeeper**: `GET /management/v1/project/task/by-id/{task_id}`, `GET /management/v1/warehouse/{warehouse_id}/task/by-id/{task_id}`, `POST /management/v1/project/task/control`, `POST /management/v1/project/task/list`, `POST /management/v1/warehouse/{warehouse_id}/task/control`, `POST /management/v1/warehouse/{warehouse_id}/task/list`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/heartbeat`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}/quarantine`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/scheduler/run`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/worker/run`

### Configure the task queues (drop, purge, log cleanup) (`task_queue_config`)
- **lakekeeper**: `GET /management/v1/project/task-queue/task_log_cleanup/config`, `GET /management/v1/warehouse/{warehouse_id}/task-queue/soft_deletion/config`, `GET /management/v1/warehouse/{warehouse_id}/task-queue/tabular_purge/config`, `POST /management/v1/project/task-queue/task_log_cleanup/config`, `POST /management/v1/warehouse/{warehouse_id}/task-queue/soft_deletion/config`, `POST /management/v1/warehouse/{warehouse_id}/task-queue/tabular_purge/config`

### Table maintenance by the catalog (configuration, metadata) (`table_maintenance_by_catalog`)
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata`

### Search tables and views (`search`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/search-tabular`

### Move a namespace (`namespace_move`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/namespace/{namespace_id}/move`

### Format-version policy of a warehouse (`format_version_policy`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/format-version-policy`

### Mark a warehouse as externally managed (`managed_by`)
- **lakekeeper**: `POST /management/v1/warehouse/{warehouse_id}/managed-by`

### Non-Iceberg tables in the catalog (generic tables, Delta) (`non_iceberg_tables`)
- **polaris**: `DELETE /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}`, `GET /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/generic-tables`, `GET /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}`, `POST /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/generic-tables`
- **unity**: `DELETE /api/2.1/unity-catalog/tables/{full_name}`, `GET /api/2.1/unity-catalog/delta/preview/commits`, `GET /api/2.1/unity-catalog/tables`, `GET /api/2.1/unity-catalog/tables/{full_name}`, `POST /api/2.1/unity-catalog/delta/preview/commits`, `POST /api/2.1/unity-catalog/staging-tables`, `POST /api/2.1/unity-catalog/tables`

### Adopt/sync a table from externally written metadata (`external_table_sync`)
- **polaris**: `POST /api/catalog/v1/{prefix}/namespaces/{namespace}/tables/{table}/notifications`
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync`

### Create policies, attach them to objects, inherit them (`policies`)
- **polaris**: `DELETE /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}`, `GET /api/catalog/polaris/v1/{prefix}/applicable-policies`, `GET /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies`, `GET /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}`, `POST /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies`, `POST /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings`, `PUT /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}`, `PUT /api/catalog/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings`

### Integrate a foreign catalog (federation) (`federation`)

### Own object API next to Iceberg REST (schemas through the Unity API) (`unity_own_object_api`)
- **unity**: `DELETE /api/2.1/unity-catalog/schemas/{full_name}`, `GET /api/2.1/unity-catalog/schemas`, `GET /api/2.1/unity-catalog/schemas/{full_name}`, `PATCH /api/2.1/unity-catalog/schemas/{full_name}`, `POST /api/2.1/unity-catalog/schemas`

### Volumes (files managed by the catalog) (`volumes`)
- **unity**: `DELETE /api/2.1/unity-catalog/volumes/{name}`, `GET /api/2.1/unity-catalog/volumes`, `GET /api/2.1/unity-catalog/volumes/{name}`, `PATCH /api/2.1/unity-catalog/volumes/{name}`, `POST /api/2.1/unity-catalog/temporary-volume-credentials`, `POST /api/2.1/unity-catalog/volumes`

### Register functions in the catalog (`functions`)
- **lakekeeper**: `GET /catalog/v1/{prefix}/namespaces/{namespace}/functions`, `GET /catalog/v1/{prefix}/namespaces/{namespace}/functions/{function}`
- **unity**: `DELETE /api/2.1/unity-catalog/functions/{name}`, `GET /api/2.1/unity-catalog/functions`, `GET /api/2.1/unity-catalog/functions/{name}`, `POST /api/2.1/unity-catalog/functions`

### ML models and model versions (`models`)
- **unity**: `DELETE /api/2.1/unity-catalog/models/{full_name}`, `DELETE /api/2.1/unity-catalog/models/{full_name}/versions/{version}`, `GET /api/2.1/unity-catalog/models`, `GET /api/2.1/unity-catalog/models/{full_name}`, `GET /api/2.1/unity-catalog/models/{full_name}/versions`, `GET /api/2.1/unity-catalog/models/{full_name}/versions/{version}`, `PATCH /api/2.1/unity-catalog/models/{full_name}`, `PATCH /api/2.1/unity-catalog/models/{full_name}/versions/{version}`, `PATCH /api/2.1/unity-catalog/models/{full_name}/versions/{version}/finalize`, `POST /api/2.1/unity-catalog/models`, `POST /api/2.1/unity-catalog/models/versions`, `POST /api/2.1/unity-catalog/temporary-model-version-credentials`

### Temporary credentials for a storage path (`path_credentials`)
- **unity**: `POST /api/2.1/unity-catalog/temporary-path-credentials`

### Branches/tags of a table through an own API (`table_refs_api`)
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}`

### Read/set a table's metadata location directly (`metadata_location_api`)
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location`

### Export/import, diagnose, restore, reset a table (`table_admin`)
- **rustfs**: `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery`, `ANY /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback`

### Back up, restore, export, import, migrate the catalog (`catalog_backup`)
- **rustfs**: `ANY /iceberg/v1/{warehouse}/catalog/backup`, `ANY /iceberg/v1/{warehouse}/catalog/backup/export`, `ANY /iceberg/v1/{warehouse}/catalog/backup/import`, `ANY /iceberg/v1/{warehouse}/catalog/backup/prepare`, `ANY /iceberg/v1/{warehouse}/catalog/directory`, `ANY /iceberg/v1/{warehouse}/catalog/restore`, `ANY /iceberg/v1/{warehouse}/catalog/warehouse-index/backfill`

