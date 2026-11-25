# Postgres 19 + pg_lake built from source - the latest releases: PostgreSQL 19.0 (official
# image) and pg_lake v3.7.1 (git tag, with its DuckDB submodule). pg_lake publishes no runnable
# images (its docker/ setup is a development environment that compiles Postgres 16-19 itself).
#
# Build (alone - DuckDB's link step needs ~3 GB RAM per job, JOBS=2 keeps it under ~7 GB;
# the first build takes ~1.5-2 h, ccache makes rebuilds after a later failure quick):
#   docker compose -f .devcontainer/docker-compose.yml --profile pglake build pg-lake
#
# Stage "build" compiles vcpkg's Azure SDK (pg_lake links it statically) and pg_lake with
# pgduck_server against the image's own server headers (postgresql-server-dev-19 from the
# PGDG repository the official image already uses). Stage 2 copies only the installed files.
# entrypoint.sh starts pgduck_server next to Postgres; init-pgduck-server.sql holds its S3
# secret for RustFS, init-postgres.sql creates the extension on first start.
ARG PG_IMAGE=postgres:19.0-trixie

FROM ${PG_IMAGE} AS build
ARG PG_LAKE_REF=v3.7.1
ARG VCPKG_VERSION=2025.10.17
ARG JOBS=2
# Build dependencies (pg_lake docs/building-from-source.md, Debian list).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        "postgresql-server-dev-${PG_MAJOR}" \
        build-essential cmake ninja-build git curl ca-certificates zip unzip tar pkg-config \
        python3 python3-dev flex bison perl patch libtool \
        libreadline-dev zlib1g-dev libxml2-dev libxslt1-dev libicu-dev libssl-dev \
        libgeos-dev libproj-dev libgdal-dev libjson-c-dev libprotobuf-c-dev protobuf-c-compiler \
        uuid-dev libossp-uuid-dev liblz4-dev liblzma-dev libsnappy-dev libjansson-dev \
        libcurl4-openssl-dev libpam0g-dev libkrb5-dev libnuma-dev \
    && rm -rf /var/lib/apt/lists/*
# Parallelism everywhere (make, cmake/ninja, vcpkg): ninja would otherwise use every core.
ENV NCORES=${JOBS} MAKEFLAGS=-j${JOBS} CMAKE_BUILD_PARALLEL_LEVEL=${JOBS} VCPKG_MAX_CONCURRENCY=${JOBS}
WORKDIR /build
RUN git clone --depth 1 --branch "${VCPKG_VERSION}" https://github.com/Microsoft/vcpkg.git \
    && ./vcpkg/bootstrap-vcpkg.sh -disableMetrics \
    && ./vcpkg/vcpkg install --clean-after-build \
        azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl \
    && rm -rf vcpkg/downloads vcpkg/buildtrees vcpkg/packages
ENV VCPKG_TOOLCHAIN_PATH=/build/vcpkg/scripts/buildsystems/vcpkg.cmake
# pgduck_server links with `pg_config --libs` (-lpgcommon -lpgport -lselinux -lzstd ...).
# Debian keeps libpgcommon.a/libpgport.a in the pkglibdir (/usr/lib/postgresql/<major>/lib),
# not in pg_config's libdir - LIBRARY_PATH lets the linker find them.
# The shallow submodule clone has no git tags, so DuckDB would call itself v0.0.1 and
# pgduck_server would fetch its extensions (httpfs, spatial, ...) from a URL that does not
# exist ("could not start query engine"): OVERRIDE_GIT_DESCRIBE sets the version pg_lake
# pins (DUCKDB_VERSION in duckdb_pglake/Makefile; EXT_FLAGS keeps its extension list), and
# the build fails if libduckdb.so does not carry it. ccache (cache mount below) makes a
# rebuild skip DuckDB's ~1.5 h compile when only a later step changes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libselinux1-dev libzstd-dev ccache \
    && rm -rf /var/lib/apt/lists/*
ENV LIBRARY_PATH=/usr/lib/postgresql/${PG_MAJOR}/lib \
    CCACHE_DIR=/root/.ccache CCACHE_MAXSIZE=5G \
    CMAKE_C_COMPILER_LAUNCHER=ccache CMAKE_CXX_COMPILER_LAUNCHER=ccache
RUN --mount=type=cache,target=/root/.ccache \
    git clone --depth 1 --branch "${PG_LAKE_REF}" --recurse-submodules --shallow-submodules \
        https://github.com/snowflake-labs/pg_lake.git \
    && cd pg_lake \
    && duckdb_version=$(sed -n "s/^DUCKDB_VERSION=//p" duckdb_pglake/Makefile) \
    && test -n "$duckdb_version" \
    && make install NCORES="${JOBS}" \
        EXT_FLAGS="-DBUILD_EXTENSIONS='tpch;json;icu' -DOVERRIDE_GIT_DESCRIBE=v${duckdb_version}" \
    && grep -aq "v${duckdb_version}" "$(pg_config --libdir)/libduckdb.so" \
    && cd / && rm -rf /build/pg_lake  # ~30 GB build tree; the layer keeps only the install
# pg_lake installs libavro and libduckdb into the system libdir (pg_config --libdir), not
# under /usr/lib/postgresql - collect them for the runtime stage (arch-neutral path).
# libduckdb.so carries debug info (1.2 GB -> 84 MB stripped); only crash stack traces lose
# their line numbers.
RUN mkdir /pglake-libs && cp -a "$(pg_config --libdir)"/libavro.so* \
        "$(pg_config --libdir)"/libduckdb.so /pglake-libs/ \
    && strip --strip-debug /pglake-libs/libduckdb.so

FROM ${PG_IMAGE}
# Runtime libraries of pg_lake/pgduck_server (no -dev packages, no compilers).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgeos-c1t64 libproj25 libgdal36 libjson-c5 libprotobuf-c1 libossp-uuid16 \
        liblz4-1 libsnappy1v5 libjansson4 libcurl4t64 libxslt1.1 libnuma1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/lib/postgresql /usr/lib/postgresql
COPY --from=build /usr/share/postgresql /usr/share/postgresql
COPY --from=build /pglake-libs/ /usr/local/lib/
RUN ldconfig
# Fail the build if a pg_lake library or pgduck_server misses a shared library.
RUN missing=$(for f in /usr/lib/postgresql/${PG_MAJOR}/lib/*.so /usr/lib/postgresql/${PG_MAJOR}/bin/pgduck_server; do \
        ldd "$f" | grep "not found" | sed "s|^|$f: |"; done); \
    if [ -n "$missing" ]; then echo "$missing"; exit 1; fi

COPY entrypoint.sh /usr/local/bin/pg-lake-entrypoint.sh
COPY init-pgduck-server.sql /etc/pgduck/init.sql
COPY init-postgres.sql /docker-entrypoint-initdb.d/10-pg_lake.sql

ENTRYPOINT ["pg-lake-entrypoint.sh"]
CMD ["postgres", \
     "-c", "shared_preload_libraries=pg_extension_base", \
     "-c", "max_worker_processes=32", \
     "-c", "pg_lake_iceberg.enable_object_store_catalog=off", \
     "-c", "pg_lake_engine.host=host=/var/run/pgduck port=5332", \
     "-c", "pg_lake_iceberg.default_location_prefix=s3://warehouse/pg_lake/"]
