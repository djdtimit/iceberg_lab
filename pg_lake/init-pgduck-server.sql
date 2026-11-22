-- S3 credentials pgduck_server (DuckDB) uses for RustFS.
-- pg_lake does not use catalog-vended credentials for writable REST tables, so it always needs these.
-- No scope: every bucket of the lab lives on RustFS (warehouse, labtables, unitylab).
CREATE SECRET s3_rustfs(
        TYPE s3,
        use_ssl false,
        key_id 'rustfsadmin',
        secret 'rustfsadmin',
        url_style 'path',
        endpoint 'rustfs:9000',
        region 'us-east-1'
);
