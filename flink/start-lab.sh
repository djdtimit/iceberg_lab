#!/usr/bin/env bash
# Start a local Flink cluster (JobManager + TaskManager) and the SQL Gateway in the foreground.
set -euo pipefail
/opt/flink/bin/start-cluster.sh
# idle-timeout: the default (10 min) drops a test session that waits while other engines run
exec /opt/flink/bin/sql-gateway.sh start-foreground \
    -Dsql-gateway.session.idle-timeout=2h \
    -Dsql-gateway.endpoint.rest.address=0.0.0.0 \
    -Dsql-gateway.endpoint.rest.port=8083
