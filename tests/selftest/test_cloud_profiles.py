"""Self-tests of the cloud catalog profiles read from ``LAB_<NAME>_*`` (``config.remote_profile``).

No local run sets these variables, so a mistake here shows only when someone connects a cloud
catalog - with a credential sent to the wrong host in the worst case. Offline, fake values.
"""

from __future__ import annotations

import pytest

from iceberg_lab.config import remote_profile


def test_unset_uri_is_no_profile():
    """Without ``URI`` the cloud catalog is simply not configured.

    Checks:
        * no variables, or only other catalogs' variables -> None.
    """
    assert remote_profile("databricks", {}) is None
    assert remote_profile("databricks", {"LAB_SNOWFLAKE_OPEN_URI": "https://x"}) is None


def test_token_profile():
    """A bearer token gives a token profile on the URI's host and path.

    Checks:
        * base URL and path split from the URI (trailing slash dropped);
        * auth ``token`` with the token, no OAuth credential, scope or token endpoint.
    """
    p = remote_profile(
        "databricks",
        {
            "LAB_DATABRICKS_URI": "https://dbc.example.com/api/2.1/unity-catalog/iceberg-rest/",
            "LAB_DATABRICKS_WAREHOUSE": "main",
            "LAB_DATABRICKS_TOKEN": "dapi-x",
        },
    )
    assert (p.base_url, p.path, p.warehouse_sts) == (
        "https://dbc.example.com",
        "/api/2.1/unity-catalog/iceberg-rest",
        "main",
    )
    assert (p.auth, p.token, p.oauth_credential, p.oauth_scope, p.token_path) == (
        "token",
        "dapi-x",
        None,
        None,
        None,
    )
    assert p.remote


def test_credential_profile_takes_the_catalogs_defaults():
    """OAuth client credentials take the catalog's default scope and token endpoint.

    Checks:
        * Databricks: scope ``all-apis``, token endpoint ``/oidc/v1/token`` on the URI's host;
        * Snowflake Open Catalog: endpoint below the REST URI (``<path>/v1/oauth/tokens``);
        * an explicit ``SCOPE`` wins over the default.
    """
    db = remote_profile(
        "databricks",
        {
            "LAB_DATABRICKS_URI": "https://dbc.example.com/api/2.1/unity-catalog/iceberg-rest",
            "LAB_DATABRICKS_WAREHOUSE": "main",
            "LAB_DATABRICKS_CREDENTIAL": "id:secret",
        },
    )
    assert (db.auth, db.oauth_credential, db.oauth_scope, db.token_path, db.token) == (
        "oauth2",
        "id:secret",
        "all-apis",
        "/oidc/v1/token",
        None,
    )
    env = {
        "LAB_SNOWFLAKE_OPEN_URI": "https://acct.example.com/polaris/api/catalog",
        "LAB_SNOWFLAKE_OPEN_WAREHOUSE": "wh",
        "LAB_SNOWFLAKE_OPEN_CREDENTIAL": "id:secret",
    }
    so = remote_profile("snowflake_open", env)
    assert so.token_path == "/polaris/api/catalog/v1/oauth/tokens"
    assert so.oauth_scope == "PRINCIPAL_ROLE:ALL"
    own = remote_profile("snowflake_open", env | {"LAB_SNOWFLAKE_OPEN_SCOPE": "PRINCIPAL_ROLE:r"})
    assert own.oauth_scope == "PRINCIPAL_ROLE:r"


def test_scope_template_needs_the_role():
    """Snowflake Horizon builds the scope from ``ROLE``; without it the profile is refused.

    Checks:
        * with ``ROLE`` -> scope ``session:role:<ROLE>``;
        * without -> ValueError naming ``ROLE``.
    """
    env = {
        "LAB_SNOWFLAKE_HORIZON_URI": "https://acct.example.com/polaris/api/catalog",
        "LAB_SNOWFLAKE_HORIZON_WAREHOUSE": "db",
        "LAB_SNOWFLAKE_HORIZON_CREDENTIAL": "pat",
    }
    p = remote_profile("snowflake_horizon", env | {"LAB_SNOWFLAKE_HORIZON_ROLE": "ANALYST"})
    assert p.oauth_scope == "session:role:ANALYST"
    with pytest.raises(ValueError, match="ROLE"):
        remote_profile("snowflake_horizon", env)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"LAB_DATABRICKS_TOKEN": "t"}, "WAREHOUSE"),  # no warehouse
        ({"LAB_DATABRICKS_WAREHOUSE": "main"}, "TOKEN or"),  # neither token nor credential
        (
            {
                "LAB_DATABRICKS_WAREHOUSE": "main",
                "LAB_DATABRICKS_CREDENTIAL": "id:secret",
                "LAB_DATABRICKS_TOKEN_URL": "https://evil.example.org/token",
            },
            "TOKEN_URL must be on dbc.example.com",
        ),
    ],
)
def test_incomplete_or_foreign_settings_are_refused(extra, message):
    """A half-configured catalog fails at once, and the secret never goes to another host.

    Checks:
        * a missing warehouse, or neither token nor credential -> ValueError;
        * a token endpoint on another host than the URI -> ValueError (the client secret
          would be sent there).

    Args:
        extra: Variables besides the URI.
        message: Part of the expected error.
    """
    env = {"LAB_DATABRICKS_URI": "https://dbc.example.com/api/2.1/unity-catalog/iceberg-rest"}
    with pytest.raises(ValueError, match=message):
        remote_profile("databricks", env | extra)
