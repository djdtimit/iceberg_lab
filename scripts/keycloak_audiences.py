"""Set up the per-service token scopes in a running Keycloak (idempotent).

See src/iceberg_lab/keycloak.py. Run inside the dev container:

    uv run python scripts/keycloak_audiences.py
"""

from iceberg_lab.keycloak import ensure_service_scopes

if __name__ == "__main__":
    for line in ensure_service_scopes():
        print(line)
    print("ok")
