FROM python:3.13-slim

# Install uv (Python package manager), curl (for healthcheck),
# and system deps for DuckDB spatial extension
RUN pip install --no-cache-dir uv && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        libgeos-dev \
        libgdal-dev \
        libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Pre-install the MCP server package so container startup is fast
RUN uv tool install mcp-server-motherduck

# Copy config files
COPY duckdb-mcp-init.sql /config/duckdb-mcp-init.sql
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# MCP server port (SSE/stream transport)
EXPOSE 8082

ENTRYPOINT ["/entrypoint.sh"]
