FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

# Java 17 for PySpark
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /usr/local/bin/

# Keep the venv outside the bind-mounted workspace so it does not clash with a host .venv
ENV UV_PROJECT_ENVIRONMENT=/home/vscode/.venv \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system \
    PATH=/home/vscode/.venv/bin:$PATH

# Pre-create cache dirs so named volumes mounted there are owned by vscode, not root
RUN mkdir -p /home/vscode/.cache/uv /home/vscode/.ivy2 && chown -R vscode:vscode /home/vscode/.cache /home/vscode/.ivy2
