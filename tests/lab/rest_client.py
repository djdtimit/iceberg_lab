"""Raw Iceberg REST client for catalog-level tests: no engine or client library in between,
so a library bug can never be booked as a catalog gap."""

from __future__ import annotations

import requests

from iceberg_lab.config import (
    SETTINGS,
    WAREHOUSE_STS,
    catalog_auth,
    catalog_headers,
    rest_prefix_path,
)


class Rest:
    """Raw Iceberg REST calls on the warehouse under test (prefix and auth resolved once)."""

    def __init__(self, warehouse: str = WAREHOUSE_STS):
        """Read ``/v1/config`` of the warehouse: prefix, defaults/overrides and advertised endpoints.

        Args:
            warehouse: Warehouse (catalog) name.
        """
        self.headers = catalog_headers(SETTINGS) | {"Content-Type": "application/json"}
        self.auth = catalog_auth(SETTINGS)  # SigV4 (RustFS); None otherwise
        self.base = SETTINGS.catalog_uri.rstrip("/")
        r = requests.get(
            f"{self.base}/v1/config",
            params={"warehouse": warehouse},
            headers=self.headers,
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()
        self.config = r.json()
        merged = self.config.get("defaults", {}) | self.config.get("overrides", {})
        #: The catalog's prefix (Lakekeeper: the warehouse id), None without one.
        self.prefix = merged.get("prefix")
        #: "/<prefix>" or "" - the prefix is optional (Iceberg REST fixture: none).
        self.prefix_path = rest_prefix_path(self.config)
        self.properties = merged
        self.endpoints = set(self.config.get("endpoints") or [])

    def url(self, path: str) -> str:
        """Full URL of a path below ``/v1[/<prefix>]/``.

        Args:
            path: Path below the prefix.

        Returns:
            The URL.
        """
        return f"{self.base}/v1{self.prefix_path}/{path.lstrip('/')}"

    def call(self, method: str, path: str, **kw) -> requests.Response:
        """Raw catalog call with the catalog's authentication.

        Args:
            method: HTTP method.
            path: Path below the prefix.
            kw: Passed to ``requests.request`` (headers are merged).

        Returns:
            The response.
        """
        headers = self.headers | kw.pop("headers", {})
        return requests.request(
            method, self.url(path), headers=headers, auth=self.auth, timeout=30, **kw
        )

    def advertises(self, endpoint: str) -> bool:
        """Whether ``/v1/config`` lists the endpoint.

        Args:
            endpoint: ``METHOD /v1/...``.

        Returns:
            True if listed.
        """
        return endpoint in self.endpoints
