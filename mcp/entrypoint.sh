#!/bin/bash
set -e

# Template the init SQL with actual env var values
INIT_SQL="/tmp/duckdb-mcp-init.sql"
sed \
  -e "s|\${GARAGE_KEY_ID}|${GARAGE_KEY_ID}|g" \
  -e "s|\${GARAGE_SECRET_KEY}|${GARAGE_SECRET_KEY}|g" \
  /config/duckdb-mcp-init.sql > "${INIT_SQL}"

# Air-gap mode: load extensions from a mounted volume instead of downloading
if [ -n "$DUCKDB_EXTENSION_DIR" ]; then
  sed -i \
    -e "1i SET extension_directory = '${DUCKDB_EXTENSION_DIR}';" \
    -e "1i SET autoinstall_known_extensions = false;" \
    -e "1i SET autoload_known_extensions = false;" \
    -e 's/^INSTALL .*;//' \
    "${INIT_SQL}"
fi

# Wait for LakeKeeper to be ready before starting
echo "Waiting for LakeKeeper..."
until curl -sf http://lakekeeper:8181/health > /dev/null 2>&1; do
  sleep 2
done
echo "LakeKeeper is ready."

echo "=== MCP Server starting ==="
echo "  DB path:   ${MCP_DB_PATH:-:memory:}"
echo "  Transport: ${MCP_TRANSPORT:-http}"
echo "  Host:      ${MCP_HOST:-0.0.0.0}"
echo "  Port:      ${MCP_PORT:-8082}"
echo "==========================="

# Start the MCP server
exec uvx mcp-server-motherduck \
  --db-path "${MCP_DB_PATH:-:memory:}" \
  --transport "${MCP_TRANSPORT:-http}" \
  --host "${MCP_HOST:-0.0.0.0}" \
  --port "${MCP_PORT:-8082}" \
  --read-write \
  --init-sql "${INIT_SQL}" \
  "$@"
