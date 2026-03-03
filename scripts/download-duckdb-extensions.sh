#!/bin/bash
# Download DuckDB extensions for air-gapped deployment.
#
# Creates the versioned directory structure that DuckDB expects when
# configured with SET extension_directory = '<path>'.
#
# Usage:
#   ./scripts/download-duckdb-extensions.sh [output-dir]
#
# Example:
#   ./scripts/download-duckdb-extensions.sh ./duckdb-extensions
#
# Then in docker-compose.override.yml, mount the directory and set:
#   environment:
#     DUCKDB_EXTENSION_DIR: /duckdb-extensions
#   volumes:
#     - ./duckdb-extensions:/duckdb-extensions:ro
set -e

OUTPUT_DIR="${1:-./duckdb-extensions}"
mkdir -p "$OUTPUT_DIR"

echo "Downloading DuckDB extensions to $OUTPUT_DIR ..."

python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"SET extension_directory = '$OUTPUT_DIR'\")
for ext in ['httpfs', 'iceberg', 'spatial']:
    print(f'  Installing {ext}...', end=' ', flush=True)
    conn.execute(f'INSTALL {ext}')
    print('done')
print(f'DuckDB version: {duckdb.__version__}')
"

echo ""
echo "Extensions saved to $OUTPUT_DIR"
echo "Directory structure:"
find "$OUTPUT_DIR" -type f | sort
