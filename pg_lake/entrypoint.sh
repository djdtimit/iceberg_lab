#!/usr/bin/env bash
# Start pgduck_server (as the postgres user) in the background, then hand over to the
# official postgres entrypoint, which runs initdb + /docker-entrypoint-initdb.d on first start.
#
# pgduck_server gets its OWN socket directory: sharing /var/run/postgresql does not work,
# because the postgres entrypoint's first-start initialisation removes the foreign
# .s.PGSQL.5332 socket from there ("could not start query engine").
set -euo pipefail

PGDUCK_SOCKET_DIR=/var/run/pgduck
mkdir -p "$PGDUCK_SOCKET_DIR" /var/lib/pgduck/cache
chown -R postgres:postgres "$PGDUCK_SOCKET_DIR" /var/lib/pgduck

# DuckDB's AWS credential chain probes the EC2 metadata endpoint (169.254.169.254) on start.
# Outside AWS that connection hangs, and pgduck_server never opens its socket - Postgres then
# reports "could not start query engine". The S3 secret comes from init.sql anyway.
export AWS_EC2_METADATA_DISABLED=true

# --memory_limit: DuckDB's default is 80 % of the host RAM, far too much next to Spark/Trino
# on a 15 GB lab machine. Override with PGDUCK_MEMORY_LIMIT.
# PG_MAJOR: set by the official postgres image the Dockerfile builds on.
gosu postgres "/usr/lib/postgresql/${PG_MAJOR}/bin/pgduck_server" \
    --cache_dir /var/lib/pgduck/cache \
    --unix_socket_directory "$PGDUCK_SOCKET_DIR" \
    --unix_socket_group postgres \
    --port 5332 \
    --memory_limit "${PGDUCK_MEMORY_LIMIT:-2GB}" \
    --init_file_path /etc/pgduck/init.sql &

exec docker-entrypoint.sh "$@"
