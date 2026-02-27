# Spatial Lakehouse

A containerized geospatial data lakehouse for local development on Apple Silicon, designed for eventual migration to production S3 and as the foundation for an interactive geospatial AI interface.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Your AI Interface                     │
│              (generates SQL, calls DuckDB)               │
└────────────────────────┬────────────────────────────────┘
                         │ SQL
          ┌──────────────┴──────────────┐
          │                             │
  ┌───────▼────────┐          ┌────────▼─────────┐
  │   DuckDB +     │          │  SedonaSpark     │
  │   Spatial +    │          │  (heavy profile) │
  │   Iceberg ext  │          │  ETL · raster ·  │
  │                │          │  maintenance     │
  │  interactive   │          │  batch jobs      │
  └───────┬────────┘          └────────┬─────────┘
          │ REST                       │ REST
          └──────────┬─────────────────┘
                     │
           ┌─────────▼──────────┐
           │    LakeKeeper      │
           │  Iceberg REST      │──── PostgreSQL
           │  Catalog           │     (metadata)
           └─────────┬──────────┘
                     │ S3 API
           ┌─────────▼──────────┐
           │      Garage        │
           │  S3-compatible     │
           │  object storage    │
           └────────────────────┘
           (swap to real S3 later)
```

## Quick Start

```bash
# 1. Start the core stack (Garage + LakeKeeper + Postgres + DuckDB)
docker compose up -d

# 2. One-time bootstrap (creates secrets, bucket, warehouse)
./bootstrap.sh

# 3. Open DuckDB with spatial + Iceberg ready
docker compose exec duckdb duckdb -init /config/init.sql
```

## Usage

### DuckDB — Interactive spatial queries (daily driver)

```bash
docker compose exec duckdb duckdb -init /config/init.sql
```

```sql
-- Verify spatial extension
SELECT ST_Point(-104.99, 39.74) AS denver;

-- List Iceberg tables
SHOW ALL TABLES;

-- Read GeoParquet directly from Garage
SELECT * FROM 's3://lakehouse/my-data.geoparquet' LIMIT 10;

-- Query Iceberg tables through LakeKeeper
SELECT * FROM lakehouse.my_namespace.my_table;
```

### SedonaSpark — Heavy spatial ETL (on demand)

```bash
# Start Sedona (adds ~8GB RAM)
docker compose --profile heavy up sedona -d

# Open Jupyter at http://localhost:8888
```

```python
# In a Jupyter notebook
from sedona.spark import SedonaContext

sedona = SedonaContext.builder().getOrCreate()

# Create an Iceberg table with geometry
sedona.sql("""
  CREATE NAMESPACE IF NOT EXISTS lakehouse.spatial
""")
sedona.sql("""
  CREATE TABLE lakehouse.spatial.buildings (
    id STRING,
    name STRING,
    geometry BINARY
  ) USING iceberg
  TBLPROPERTIES('format-version'='3')
""")
```

### Stopping

```bash
docker compose down                        # core stack
docker compose --profile heavy down        # if Sedona was running
docker compose down -v                     # nuke all data + start fresh
```

## File Structure

```
spatial-lakehouse/
├── docker-compose.yml        # Service definitions
├── bootstrap.sh              # One-time init (run after first `up`)
├── garage.toml               # Garage S3 config
├── create-warehouse.json     # LakeKeeper warehouse definition
├── duckdb-init.sql           # DuckDB startup: extensions + catalog
├── sedona-defaults.conf      # Spark config for SedonaSpark
├── notebooks/                # Shared notebook directory
├── .env                      # Auto-generated credentials (after bootstrap)
└── README.md
```

## Endpoints

| Service            | URL                          | Notes                          |
|--------------------|------------------------------|--------------------------------|
| Garage S3 API      | http://localhost:3900        | S3-compatible endpoint         |
| Garage Admin API   | http://localhost:3903        | Cluster management             |
| LakeKeeper UI      | http://localhost:8181        | Warehouse + table browser      |
| LakeKeeper Catalog | http://localhost:8181/catalog | Iceberg REST API               |
| Jupyter (Sedona)   | http://localhost:8888        | heavy profile only             |
| Spark Master UI    | http://localhost:8080        | heavy profile only             |

## M1 Mac Notes

- All images publish `arm64` variants
- Core stack (no Sedona): ~2GB RAM
- With Sedona: +8GB RAM (configurable via `DRIVER_MEM` / `EXECUTOR_MEM`)
- DuckDB spatial queries run in milliseconds — ideal for AI query loops
- SedonaSpark is intentionally behind a `--profile heavy` gate to save resources

## Migration to Production S3

When ready to point at real S3:

1. Update `create-warehouse.json` with your real S3 bucket, region, and credentials
2. Update the warehouse in LakeKeeper UI (or recreate via API)
3. Update `duckdb-init.sql` S3 secret with real AWS credentials
4. Update `sedona-defaults.conf` S3 endpoint + keys
5. Garage can be removed from the compose file

Your Iceberg tables, schemas, and SQL all stay the same.

## Known Limitations

- **DuckDB ↔ LakeKeeper ↔ Garage**: DuckDB's Iceberg extension supports S3-backed
  REST catalogs. If you hit credential issues, check that LakeKeeper's warehouse
  storage profile has `path-style-access: true` and Garage's key has read/write.
- **Iceberg v3 native geometry types**: Neither DuckDB nor SedonaSpark fully supports
  Iceberg v3 native `GEOMETRY` columns end-to-end yet (SEDONA-744). Geometry is
  stored as WKB in binary columns. This is the industry state-of-the-art as of
  early 2026.
- **DuckDB Iceberg writes**: Write support is new (v1.3+). Stick to SedonaSpark
  for production table creation and heavy writes.
- **Iceberg table maintenance**: Compaction, snapshot expiry, and orphan file
  cleanup must be run via SedonaSpark or PyIceberg — neither DuckDB nor LakeKeeper
  handles this automatically.
