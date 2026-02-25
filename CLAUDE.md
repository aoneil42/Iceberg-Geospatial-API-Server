# Iceberg Geospatial API Server — Implementation Guide

## Project Overview

Build a containerized Python service that serves geospatial data stored in Apache Iceberg tables through two API surfaces:

1. **OGC API Features** (via pygeoapi) — standards-based geospatial API serving GeoJSON, GeoArrow/Arrow IPC, and HTML
2. **Esri GeoServices REST** (via FastAPI) — `/FeatureServer` endpoints serving Esri PBF (protobuf) and Esri JSON for ArcGIS clients

Both API surfaces share a common **Iceberg Query Service** module that handles all data access through PyIceberg + DuckDB with its spatial extension.

```
                    ┌──────────────┐
                    │   pygeoapi   │ ← OGC API Features
                    │   :5000      │   (GeoJSON, GeoArrow, HTML)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  Iceberg     │ ← PyIceberg + DuckDB spatial
                    │  Query       │   (shared Python module)
                    │  Service     │
                    └──────▲───────┘
                           │
                    ┌──────┴───────┐
                    │   FastAPI    │ ← Esri GeoServices
                    │   :8001      │   (PBF + Esri JSON + GeoJSON)
                    │  /rest/      │
                    └──────────────┘
```

---

## Architecture Principles

- **One query engine.** All spatial filtering, attribute queries, pagination, and aggregation happen in DuckDB against Arrow tables produced by PyIceberg. No query logic is duplicated between API surfaces.
- **One language.** Everything is Python. No Node.js (Koop.js is not used).
- **Binary-first serialization for large data.** Iceberg tables are large by nature (millions–billions of rows). Even after bbox filtering and partition pruning, responses routinely contain tens of thousands of features. PBF (Esri clients) and GeoArrow/Arrow IPC (analytical clients) are Phase 2 priorities, not afterthoughts.
- **Shared module, separate processes.** The Iceberg Query Service is a Python package imported by both pygeoapi and the FastAPI app. They run as separate containers (or processes) for independent scaling and failure isolation.

---

## Project Structure

```
iceberg-geo-api/
├── docker-compose.yml
├── Dockerfile.pygeoapi
├── Dockerfile.geoservices
├── pyproject.toml                    # Shared Python project / monorepo
├── README.md
│
├── src/
│   └── iceberg_geo/
│       ├── __init__.py
│       │
│       ├── query/                    # === ICEBERG QUERY SERVICE (shared) ===
│       │   ├── __init__.py
│       │   ├── catalog.py            # PyIceberg catalog connection management
│       │   ├── engine.py             # Core query engine: DuckDB spatial queries against Iceberg scans
│       │   ├── models.py             # Pydantic models for query params, results, feature schemas
│       │   └── geometry.py           # WKB decode/encode, coordinate transforms, Shapely helpers
│       │
│       ├── pygeoapi_provider/        # === PYGEOAPI PROVIDER PLUGIN ===
│       │   ├── __init__.py
│       │   └── iceberg.py            # BaseProvider implementation for pygeoapi
│       │
│       ├── geoservices/              # === FASTAPI GEOSERVICES APP ===
│       │   ├── __init__.py
│       │   ├── app.py                # FastAPI application, route definitions
│       │   ├── routes/
│       │   │   ├── __init__.py
│       │   │   ├── feature_server.py # /rest/services/{id}/FeatureServer routes
│       │   │   └── query.py          # /FeatureServer/{layer}/query handler
│       │   ├── serializers/
│       │   │   ├── __init__.py
│       │   │   ├── esri_json.py      # Arrow/DuckDB result → Esri JSON
│       │   │   ├── esri_pbf.py       # Arrow/DuckDB result → Esri FeatureCollection PBF
│       │   │   └── geojson.py        # Arrow/DuckDB result → GeoJSON (f=geojson)
│       │   ├── metadata.py           # Service/layer metadata from Iceberg table schema
│       │   └── proto/
│       │       ├── FeatureCollection.proto    # Esri's published .proto (vendored)
│       │       └── FeatureCollection_pb2.py   # protoc-generated Python classes
│       │
│       └── formatters/               # === PYGEOAPI CUSTOM FORMATTERS ===
│           ├── __init__.py
│           └── geoarrow.py           # GeoArrow/Arrow IPC output formatter for pygeoapi
│
├── config/
│   ├── pygeoapi-config.yml           # pygeoapi configuration
│   └── catalog.yml                   # Iceberg catalog connection config
│
├── tests/
│   ├── conftest.py                   # Shared fixtures: sample Iceberg tables, DuckDB instances
│   ├── test_query_engine.py
│   ├── test_pygeoapi_provider.py
│   ├── test_geoservices_query.py
│   ├── test_esri_pbf_encoder.py
│   ├── test_geoarrow_formatter.py
│   └── data/
│       └── sample_features.parquet   # Test fixture data
│
└── scripts/
    ├── generate_proto.sh             # protoc compilation for Esri PBF
    ├── seed_test_data.py             # Create sample Iceberg table with geometry
    └── healthcheck.py
```

---

## Container Architecture

### docker-compose.yml

```yaml
services:

  pygeoapi:
    build:
      context: .
      dockerfile: Dockerfile.pygeoapi
    ports:
      - "5000:5000"
    volumes:
      - ./config/pygeoapi-config.yml:/app/config/pygeoapi-config.yml:ro
      - ./config/catalog.yml:/app/config/catalog.yml:ro
      - iceberg-warehouse:/data/warehouse    # shared Iceberg warehouse
    environment:
      - PYGEOAPI_CONFIG=/app/config/pygeoapi-config.yml
      - ICEBERG_CATALOG_CONFIG=/app/config/catalog.yml
      - PYGEOAPI_OPENAPI=/app/config/openapi.yml
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:5000/')"]
      interval: 30s
      timeout: 10s
      retries: 3

  geoservices:
    build:
      context: .
      dockerfile: Dockerfile.geoservices
    ports:
      - "8001:8001"
    volumes:
      - ./config/catalog.yml:/app/config/catalog.yml:ro
      - iceberg-warehouse:/data/warehouse
    environment:
      - ICEBERG_CATALOG_CONFIG=/app/config/catalog.yml
      - GEOSERVICES_HOST=0.0.0.0
      - GEOSERVICES_PORT=8001
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8001/rest/info')"]
      interval: 30s
      timeout: 10s
      retries: 3

  # --- Optional: local Iceberg catalog for development ---
  rest-catalog:
    image: apache/iceberg-rest:1.9.0
    ports:
      - "8181:8181"
    environment:
      - CATALOG_WAREHOUSE=file:///data/warehouse
      - CATALOG_IO__IMPL=org.apache.iceberg.io.FileIO
    volumes:
      - iceberg-warehouse:/data/warehouse

volumes:
  iceberg-warehouse:
```

### Dockerfile.pygeoapi

```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir ".[pygeoapi]"

# Generate OpenAPI doc from config at build time
# (or do it at runtime via entrypoint)
COPY config/ config/

EXPOSE 5000

CMD ["pygeoapi", "serve", "--server-config", "/app/config/pygeoapi-config.yml"]
```

### Dockerfile.geoservices

```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    libproj-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir ".[geoservices]"

# Generate protobuf Python classes if not already committed
RUN protoc --python_out=src/iceberg_geo/geoservices/proto/ \
    --proto_path=src/iceberg_geo/geoservices/proto/ \
    src/iceberg_geo/geoservices/proto/FeatureCollection.proto || true

EXPOSE 8001

CMD ["uvicorn", "iceberg_geo.geoservices.app:app", \
     "--host", "0.0.0.0", "--port", "8001"]
```

### pyproject.toml

```toml
[project]
name = "iceberg-geo-api"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "pyiceberg[pyarrow,duckdb]>=0.11.0",
    "duckdb>=1.1.0",
    "shapely>=2.0",
    "pyproj>=3.6",
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
pygeoapi = [
    "pygeoapi>=0.22.0",
    "geoarrow-pyarrow>=0.1.0",
]
geoservices = [
    "fastapi>=0.110",
    "uvicorn[standard]>=0.29",
    "protobuf>=4.25",
]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "httpx>=0.27",           # async test client for FastAPI
    "pyarrow>=15.0",
    "geopandas>=0.14",
]

[tool.setuptools.packages.find]
where = ["src"]
```

---

## Component Implementation Details

### 1. Iceberg Query Service (`src/iceberg_geo/query/`)

This is the shared core. Both pygeoapi and FastAPI import from here. It never imports from either API surface.

#### catalog.py — Catalog Connection

```python
"""
Manage PyIceberg catalog connections.

Reads config from ICEBERG_CATALOG_CONFIG env var or passed dict.
Supports REST, SQL, Glue, Hive catalogs per PyIceberg.
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import yaml, os

_catalog = None

def get_catalog():
    """Singleton catalog instance."""
    global _catalog
    if _catalog is None:
        config_path = os.environ.get("ICEBERG_CATALOG_CONFIG", "config/catalog.yml")
        with open(config_path) as f:
            config = yaml.safe_load(f)
        _catalog = load_catalog(**config["catalog"])
    return _catalog

def get_table(namespace: str, table_name: str) -> Table:
    """Load an Iceberg table by namespace.table_name."""
    catalog = get_catalog()
    return catalog.load_table(f"{namespace}.{table_name}")

def list_tables(namespace: str) -> list[str]:
    """List available tables in a namespace."""
    catalog = get_catalog()
    return [t[1] for t in catalog.list_tables(namespace)]
```

#### engine.py — Core Query Engine

```python
"""
Core query engine. Translates query parameters into DuckDB SQL
against Arrow tables produced by PyIceberg scans.

This is the ONLY place where DuckDB queries are constructed and executed.
Both pygeoapi and the GeoServices endpoint call these functions.
"""

import duckdb
import pyarrow as pa
from pyiceberg.table import Table
from pyiceberg.expressions import (
    AlwaysTrue, And, GreaterThanOrEqual, LessThanOrEqual
)
from shapely import wkb, box
from .models import QueryParams, QueryResult, FeatureSchema

# DuckDB spatial extension - load once per connection
def _get_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    return conn

def get_table_schema(table: Table) -> FeatureSchema:
    """
    Extract feature schema from Iceberg table metadata.

    Returns field names, types, geometry column name, spatial reference,
    and extent (from Iceberg partition stats or full scan if needed).
    """
    schema = table.schema()
    # Identify geometry column: look for binary column named 'geometry'/'geom'/'wkb_geometry'
    # or Iceberg v3 geometry logical type
    # Build FeatureSchema with field definitions, geometry column, SRID
    ...

def query_features(table: Table, params: QueryParams) -> QueryResult:
    """
    Execute a spatial query against an Iceberg table.

    Pipeline:
    1. Build PyIceberg row_filter from bbox (for partition pruning)
    2. Execute scan → Arrow table
    3. Register Arrow table in DuckDB
    4. Build and execute DuckDB SQL with spatial filters,
       attribute filters, field selection, sorting, pagination
    5. Return QueryResult with Arrow table of matching features

    This function handles:
    - bbox filtering (Iceberg partition pruning + DuckDB ST_Intersects)
    - Property/attribute filtering (WHERE clause)
    - CQL2 filter translation (Phase 2)
    - Field selection (SELECT specific columns)
    - Sorting (ORDER BY)
    - Pagination (LIMIT/OFFSET)
    - Count-only queries (SELECT COUNT(*))
    - IDs-only queries (SELECT objectid_field)
    """
    conn = _get_connection()

    # --- Step 1: PyIceberg scan with partition pruning ---
    row_filter = AlwaysTrue()
    selected_fields = _resolve_fields(table, params)

    if params.bbox:
        # If table has spatial partition stats, PyIceberg can prune partitions
        # The bbox filter here is coarse — DuckDB refines it in Step 4
        # For tables with xz2 spatial partitions, this is very effective
        pass

    scan = table.scan(
        row_filter=row_filter,
        selected_fields=selected_fields,
    )
    arrow_table = scan.to_arrow()

    if arrow_table.num_rows == 0:
        return QueryResult.empty(params)

    # --- Step 2: Register in DuckDB ---
    conn.register("features", arrow_table)

    # --- Step 3: Build SQL ---
    geom_col = _detect_geometry_column(arrow_table.schema)
    select_clause = _build_select(params, geom_col)
    where_clauses = []

    # Spatial filter
    if params.bbox:
        xmin, ymin, xmax, ymax = params.bbox
        where_clauses.append(
            f"ST_Intersects(ST_GeomFromWKB({geom_col}), "
            f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
        )

    if params.geometry_filter:
        # Esri-style geometry filter (envelope, polygon, etc.)
        # Convert to WKT, use ST_Intersects / ST_Contains / ST_Within
        # based on params.spatial_rel
        ...

    # Attribute filter
    if params.where:
        # SECURITY: parameterize or sanitize the WHERE clause
        # DuckDB SQL injection is a real risk here
        sanitized = _sanitize_where(params.where)
        where_clauses.append(f"({sanitized})")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Count-only shortcut
    if params.return_count_only:
        result = conn.execute(
            f"SELECT COUNT(*) as cnt FROM features WHERE {where_sql}"
        ).fetchone()
        return QueryResult(count=result[0], features=None)

    # IDs-only shortcut
    if params.return_ids_only:
        ...

    # Order
    order_sql = ""
    if params.order_by:
        order_sql = f"ORDER BY {_sanitize_order(params.order_by)}"

    # Pagination
    limit_sql = f"LIMIT {params.limit}" if params.limit else ""
    offset_sql = f"OFFSET {params.offset}" if params.offset else ""

    sql = f"""
        SELECT {select_clause}
        FROM features
        WHERE {where_sql}
        {order_sql}
        {limit_sql}
        {offset_sql}
    """

    result_arrow = conn.execute(sql).fetch_arrow_table()

    # --- Step 4: Check if more results exist (exceededTransferLimit) ---
    exceeded = False
    if params.limit:
        count_result = conn.execute(
            f"SELECT COUNT(*) FROM features WHERE {where_sql}"
        ).fetchone()
        exceeded = count_result[0] > (params.offset or 0) + params.limit

    return QueryResult(
        features=result_arrow,
        geometry_column=geom_col,
        exceeded_transfer_limit=exceeded,
        count=result_arrow.num_rows,
    )


def _detect_geometry_column(schema: pa.Schema) -> str:
    """Find the geometry column in an Arrow schema.

    Looks for:
    1. Column with GeoArrow extension type metadata
    2. Column named geometry/geom/wkb_geometry/shape with binary type
    3. First large_binary column
    """
    ...

def _sanitize_where(where: str) -> str:
    """
    Sanitize a SQL WHERE clause from user input.

    CRITICAL SECURITY: This is the primary SQL injection vector.
    Approach:
    - Parse with a simple expression grammar (or DuckDB's own parser)
    - Allowlist of functions and operators
    - Reject semicolons, DDL keywords, system functions
    - Parameterize literal values where possible

    For Phase 1, a conservative allowlist approach is safest.
    For Phase 2, consider using sqlglot or DuckDB's prepared statements.
    """
    ...

def _sanitize_order(order_by: str) -> str:
    """Sanitize ORDER BY clause. Only allow column names + ASC/DESC."""
    ...

def _build_select(params: QueryParams, geom_col: str) -> str:
    """Build SELECT clause from requested fields."""
    if params.out_fields == "*" or not params.out_fields:
        return "*"
    fields = [f.strip() for f in params.out_fields.split(",")]
    if geom_col not in fields and params.return_geometry:
        fields.append(geom_col)
    return ", ".join(fields)

def _resolve_fields(table: Table, params: QueryParams) -> tuple[str, ...]:
    """Map requested fields to Iceberg column names for scan projection."""
    ...
```

#### models.py — Shared Data Models

```python
"""
Pydantic models shared across both API surfaces.
These models are API-agnostic — they represent query semantics, not wire formats.
"""

from pydantic import BaseModel, Field
from typing import Optional
import pyarrow as pa

class QueryParams(BaseModel):
    """Unified query parameters. Both pygeoapi and GeoServices
    translate their API-specific params into this model."""

    # Spatial
    bbox: Optional[tuple[float, float, float, float]] = None
    geometry_filter: Optional[str] = None   # WKT geometry for spatial filter
    spatial_rel: str = "intersects"          # intersects | contains | within

    # Attribute
    where: Optional[str] = None             # SQL WHERE clause (GeoServices)

    # Fields
    out_fields: Optional[str] = None        # comma-separated field names, or "*"
    return_geometry: bool = True

    # Pagination
    limit: Optional[int] = 1000
    offset: Optional[int] = 0

    # Sorting
    order_by: Optional[str] = None

    # Response modifiers
    return_count_only: bool = False
    return_ids_only: bool = False

    # Output spatial reference (EPSG code)
    out_sr: Optional[int] = None

    # CQL2 filter (Phase 2)
    cql2_filter: Optional[dict] = None


class QueryResult(BaseModel):
    """Result from the query engine. Carries Arrow table + metadata."""

    class Config:
        arbitrary_types_allowed = True

    features: Optional[pa.Table] = None
    geometry_column: str = "geometry"
    count: int = 0
    exceeded_transfer_limit: bool = False

    @classmethod
    def empty(cls, params: QueryParams) -> "QueryResult":
        return cls(features=None, count=0)


class FeatureSchema(BaseModel):
    """Schema description for an Iceberg table exposed as a feature layer."""

    table_identifier: str               # namespace.table_name
    geometry_column: str = "geometry"
    geometry_type: str = "Polygon"       # Point, LineString, Polygon, MultiPolygon, etc.
    srid: int = 4326
    fields: list[dict]                   # [{name, type, alias}, ...]
    extent: Optional[dict] = None        # {xmin, ymin, xmax, ymax}
    id_field: str = "objectid"           # or auto-generated
    max_record_count: int = 10000
```

#### geometry.py — Geometry Utilities

```python
"""
Geometry conversion and coordinate utilities.

Handles:
- WKB ↔ Shapely geometry conversion
- Coordinate extraction for PBF encoding
- Coordinate system transformation
- Geometry type detection and mapping
"""

from shapely import wkb
from shapely.geometry import mapping as geojson_mapping
import pyproj
from typing import Optional

def wkb_to_geojson(wkb_bytes: bytes) -> dict:
    """Decode WKB to GeoJSON geometry dict."""
    geom = wkb.loads(wkb_bytes)
    return geojson_mapping(geom)

def wkb_to_coords(wkb_bytes: bytes) -> dict:
    """
    Decode WKB to raw coordinate arrays for PBF encoding.

    Returns:
        {
            "type": "Polygon",  # Shapely geometry type
            "rings": [[(x, y), (x, y), ...], ...],  # for polygons
            # or "coordinates": [(x, y), ...] for points/lines
        }
    """
    geom = wkb.loads(wkb_bytes)
    ...

def transform_coords(
    coords: list,
    from_srid: int,
    to_srid: int,
) -> list:
    """Reproject coordinates using pyproj."""
    if from_srid == to_srid:
        return coords
    transformer = pyproj.Transformer.from_crs(
        f"EPSG:{from_srid}", f"EPSG:{to_srid}", always_xy=True
    )
    ...

def detect_geometry_type(wkb_sample: bytes) -> str:
    """Detect geometry type from a sample WKB value."""
    geom = wkb.loads(wkb_sample)
    return geom.geom_type

ESRI_GEOMETRY_TYPE_MAP = {
    "Point": "esriGeometryPoint",
    "MultiPoint": "esriGeometryMultipoint",
    "LineString": "esriGeometryPolyline",
    "MultiLineString": "esriGeometryPolyline",
    "Polygon": "esriGeometryPolygon",
    "MultiPolygon": "esriGeometryPolygon",
}
```

---

### 2. pygeoapi Provider Plugin (`src/iceberg_geo/pygeoapi_provider/`)

#### iceberg.py

```python
"""
pygeoapi BaseProvider implementation for Apache Iceberg tables.

Registered in pygeoapi-config.yml as:
    provider:
        name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider

This provider translates pygeoapi's query interface into calls
to the shared Iceberg Query Service.
"""

from pygeoapi.provider.base import BaseProvider, ProviderQueryError
from iceberg_geo.query.catalog import get_table
from iceberg_geo.query.engine import query_features, get_table_schema
from iceberg_geo.query.models import QueryParams
from iceberg_geo.query.geometry import wkb_to_geojson

import logging

LOGGER = logging.getLogger(__name__)


class IcebergProvider(BaseProvider):
    """pygeoapi provider for Apache Iceberg tables with geometry columns."""

    def __init__(self, provider_def):
        """
        Initialize the Iceberg provider.

        provider_def comes from pygeoapi-config.yml and should include:
            data: "namespace.table_name"    # Iceberg table identifier
            id_field: "objectid"            # unique ID field
            geometry:
                x_field: null               # not used — geometry is WKB column
                y_field: null
            options:
                geometry_column: "geometry"  # name of WKB geometry column
                catalog_config: "/app/config/catalog.yml"  # optional override
        """
        super().__init__(provider_def)

        table_id = provider_def["data"]
        parts = table_id.split(".")
        self.namespace = parts[0]
        self.table_name = parts[1]
        self.table = get_table(self.namespace, self.table_name)

        options = provider_def.get("options", {})
        self.geometry_column = options.get("geometry_column", "geometry")

        # Introspect schema for field definitions
        self._schema = get_table_schema(self.table)
        self.fields = {
            f["name"]: {"type": f["type"]}
            for f in self._schema.fields
            if f["name"] != self.geometry_column
        }

    def get_fields(self):
        """Return field definitions for the collection."""
        return self.fields

    def query(self, offset=0, limit=10, resulttype="results",
              bbox=None, datetime_=None, properties=None,
              sortby=None, select_properties=None,
              skip_geometry=False, q=None, filterq=None,
              crs_transform_spec=None, **kwargs):
        """
        Execute a query against the Iceberg table.

        Translates pygeoapi query params to shared QueryParams model,
        calls the query engine, and converts results to GeoJSON
        FeatureCollection.
        """
        params = QueryParams(
            bbox=tuple(bbox) if bbox else None,
            limit=limit,
            offset=offset,
            return_geometry=not skip_geometry,
            return_count_only=(resulttype == "hits"),
        )

        # Property filters → simple WHERE clause
        if properties:
            where_parts = []
            for prop in properties:
                name, value = prop["property"], prop["value"]
                where_parts.append(f"{name} = '{value}'")
            params.where = " AND ".join(where_parts)

        # Sort
        if sortby:
            order_parts = []
            for s in sortby:
                direction = "ASC" if s.get("order", "A") == "A" else "DESC"
                order_parts.append(f"{s['property']} {direction}")
            params.order_by = ", ".join(order_parts)

        # Field selection
        if select_properties:
            params.out_fields = ",".join(select_properties)

        # CQL2 filter (Phase 2)
        if filterq:
            params.cql2_filter = filterq

        # Execute
        result = query_features(self.table, params)

        if resulttype == "hits":
            return self._format_hits(result)

        return self._format_feature_collection(result, skip_geometry)

    def get(self, identifier, **kwargs):
        """Retrieve a single feature by ID."""
        params = QueryParams(
            where=f"{self._schema.id_field} = '{identifier}'",
            limit=1,
        )
        result = query_features(self.table, params)
        if result.count == 0:
            raise ProviderQueryError(f"Feature {identifier} not found")

        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column
        )
        return features[0]

    def _format_feature_collection(self, result, skip_geometry=False):
        """Convert QueryResult to pygeoapi-expected dict."""
        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column, skip_geometry
        )
        return {
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": result.count,
            "numberReturned": len(features),
        }

    def _format_hits(self, result):
        """Return count-only response."""
        return {"numberMatched": result.count}

    def _arrow_to_geojson_features(self, arrow_table, geom_col,
                                    skip_geometry=False):
        """
        Convert Arrow table to list of GeoJSON Feature dicts.

        For each row:
        1. Extract all non-geometry columns as properties
        2. Decode WKB geometry column to GeoJSON geometry via Shapely
        3. Build Feature dict with id from id_field
        """
        if arrow_table is None or arrow_table.num_rows == 0:
            return []

        features = []
        id_field = self._schema.id_field

        # Convert to Python dicts (for moderate result sets this is fine;
        # for very large sets, consider streaming or chunked conversion)
        table_dict = arrow_table.to_pydict()
        num_rows = arrow_table.num_rows

        for i in range(num_rows):
            properties = {}
            geometry = None

            for col_name in table_dict:
                if col_name == geom_col:
                    if not skip_geometry:
                        wkb_bytes = table_dict[col_name][i]
                        if wkb_bytes:
                            geometry = wkb_to_geojson(wkb_bytes)
                else:
                    val = table_dict[col_name][i]
                    # Convert pyarrow types to JSON-safe Python types
                    properties[col_name] = _to_json_safe(val)

            feature = {
                "type": "Feature",
                "id": str(table_dict.get(id_field, [i])[i]),
                "geometry": geometry,
                "properties": properties,
            }
            features.append(feature)

        return features


def _to_json_safe(val):
    """Convert pyarrow scalar values to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None  # skip binary fields in properties
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
```

---

### 3. FastAPI GeoServices Endpoint (`src/iceberg_geo/geoservices/`)

#### app.py

```python
"""
FastAPI application implementing a minimal Esri GeoServices REST API.

Endpoints implemented (covers ~90% of ArcGIS map visualization needs):
- /rest/info
- /rest/services/{service_id}/FeatureServer
- /rest/services/{service_id}/FeatureServer/{layer_id}
- /rest/services/{service_id}/FeatureServer/{layer_id}/query

The service_id maps to an Iceberg namespace, and layer_id maps to
a table within that namespace (0-indexed from the list of tables).
"""

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .routes import feature_server, query

app = FastAPI(
    title="Iceberg GeoServices",
    description="Esri GeoServices REST API backed by Apache Iceberg",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Mount routes
app.include_router(feature_server.router, prefix="/rest/services")

@app.get("/rest/info")
async def rest_info():
    """ArcGIS REST service directory info."""
    return {
        "currentVersion": 11.0,
        "fullVersion": "11.0.0",
        "owningSystemUrl": "",
        "authInfo": {"isTokenBasedSecurity": False},
    }
```

#### routes/feature_server.py

```python
"""
FeatureServer routes.

Implements the subset of Esri GeoServices REST that ArcGIS clients
need for map visualization.
"""

from fastapi import APIRouter, Request, Response, Query
from iceberg_geo.query.catalog import get_table, list_tables
from iceberg_geo.query.engine import get_table_schema
from ..metadata import build_service_metadata, build_layer_metadata

router = APIRouter()


@router.get("/{service_id}/FeatureServer")
@router.post("/{service_id}/FeatureServer")
async def feature_server_info(service_id: str, f: str = "json"):
    """
    Service-level metadata.

    ArcGIS clients call this to discover layers, spatial reference,
    and capabilities. Returned once when a layer is added to a map.
    """
    tables = list_tables(service_id)
    metadata = build_service_metadata(service_id, tables)

    if f == "json":
        return metadata
    # HTML rendering for browser access (optional)
    ...


@router.get("/{service_id}/FeatureServer/{layer_id}")
@router.post("/{service_id}/FeatureServer/{layer_id}")
async def layer_info(service_id: str, layer_id: int, f: str = "json"):
    """
    Layer-level metadata.

    Returns field definitions, geometry type, extent, objectIdField,
    maxRecordCount, supportedQueryFormats, etc.
    """
    tables = list_tables(service_id)
    table_name = tables[layer_id]
    table = get_table(service_id, table_name)
    schema = get_table_schema(table)
    metadata = build_layer_metadata(schema, layer_id)

    return metadata


@router.get("/{service_id}/FeatureServer/{layer_id}/query")
@router.post("/{service_id}/FeatureServer/{layer_id}/query")
async def query_layer(
    request: Request,
    service_id: str,
    layer_id: int,
    # --- GeoServices query parameters ---
    where: str = "1=1",
    geometry: str = None,
    geometryType: str = "esriGeometryEnvelope",
    spatialRel: str = "esriSpatialRelIntersects",
    outFields: str = "*",
    outSR: int = None,
    returnGeometry: bool = True,
    returnCountOnly: bool = False,
    returnIdsOnly: bool = False,
    resultOffset: int = 0,
    resultRecordCount: int = None,
    orderByFields: str = None,
    f: str = "json",    # json | geojson | pbf
):
    """
    Feature query — the workhorse endpoint.

    Translates GeoServices query params to shared QueryParams,
    executes via the Iceberg Query Service, then serializes
    to the requested format.
    """
    from iceberg_geo.query.catalog import get_table, list_tables
    from iceberg_geo.query.engine import query_features, get_table_schema
    from iceberg_geo.query.models import QueryParams
    from ..serializers import esri_json, esri_pbf, geojson

    # Resolve table
    tables = list_tables(service_id)
    table_name = tables[layer_id]
    table = get_table(service_id, table_name)
    schema = get_table_schema(table)

    # Parse geometry filter
    bbox = None
    geometry_wkt = None
    if geometry:
        bbox, geometry_wkt = _parse_esri_geometry(
            geometry, geometryType
        )

    # Map GeoServices spatial rel to engine spatial rel
    spatial_rel_map = {
        "esriSpatialRelIntersects": "intersects",
        "esriSpatialRelContains": "contains",
        "esriSpatialRelWithin": "within",
    }

    # Build shared query params
    params = QueryParams(
        bbox=bbox,
        geometry_filter=geometry_wkt,
        spatial_rel=spatial_rel_map.get(spatialRel, "intersects"),
        where=where if where != "1=1" else None,
        out_fields=outFields,
        return_geometry=returnGeometry,
        return_count_only=returnCountOnly,
        return_ids_only=returnIdsOnly,
        limit=resultRecordCount or schema.max_record_count,
        offset=resultOffset,
        order_by=orderByFields,
        out_sr=outSR,
    )

    # Execute query
    result = query_features(table, params)

    # Serialize based on requested format
    if f == "pbf":
        pbf_bytes = esri_pbf.serialize(result, schema)
        return Response(
            content=pbf_bytes,
            media_type="application/x-protobuf",
        )
    elif f == "geojson":
        return geojson.serialize(result)
    else:  # f == "json" (default)
        return esri_json.serialize(result, schema)


def _parse_esri_geometry(geometry_str: str, geometry_type: str):
    """
    Parse an Esri geometry parameter into bbox and/or WKT.

    Handles:
    - Envelope: {"xmin":..., "ymin":..., "xmax":..., "ymax":...}
    - Point: {"x":..., "y":...}
    - Polygon: {"rings": [...]}
    - Plain bbox string: "xmin,ymin,xmax,ymax"

    Returns (bbox_tuple, wkt_string).
    For envelopes, returns bbox and None for WKT.
    For polygons/points, returns None for bbox and WKT.
    """
    import json
    from shapely.geometry import shape, box

    # Try JSON parse
    try:
        geom = json.loads(geometry_str)
    except (json.JSONDecodeError, TypeError):
        # Try comma-separated bbox
        parts = [float(x) for x in geometry_str.split(",")]
        if len(parts) == 4:
            return tuple(parts), None
        raise ValueError(f"Cannot parse geometry: {geometry_str}")

    # Esri envelope
    if "xmin" in geom:
        bbox = (geom["xmin"], geom["ymin"], geom["xmax"], geom["ymax"])
        return bbox, None

    # Esri point
    if "x" in geom:
        from shapely.geometry import Point
        pt = Point(geom["x"], geom["y"])
        return None, pt.wkt

    # Esri polygon (rings)
    if "rings" in geom:
        from shapely.geometry import Polygon
        poly = Polygon(geom["rings"][0])
        return None, poly.wkt

    raise ValueError(f"Unsupported geometry type: {geometry_type}")
```

#### serializers/esri_json.py

```python
"""
Serialize QueryResult → Esri JSON response.

Esri JSON is the native JSON format for ArcGIS Feature Services.
It differs from GeoJSON in geometry representation:
- Polygons use {"rings": [[[x,y],...], ...]}
- Polylines use {"paths": [[[x,y],...], ...]}
- Points use {"x": val, "y": val}
- SpatialReference is an object: {"wkid": 4326}
"""

from iceberg_geo.query.models import QueryResult, FeatureSchema
from iceberg_geo.query.geometry import wkb_to_coords, ESRI_GEOMETRY_TYPE_MAP


def serialize(result: QueryResult, schema: FeatureSchema) -> dict:
    """Convert QueryResult to Esri JSON FeatureSet response."""

    if result.return_count_only or result.features is None:
        return {
            "count": result.count,
        }

    esri_geom_type = ESRI_GEOMETRY_TYPE_MAP.get(
        schema.geometry_type, "esriGeometryPolygon"
    )

    fields = _build_field_definitions(schema)

    features = []
    table_dict = result.features.to_pydict()
    geom_col = result.geometry_column

    for i in range(result.features.num_rows):
        attributes = {}
        geometry = None

        for col_name in table_dict:
            if col_name == geom_col:
                wkb_bytes = table_dict[col_name][i]
                if wkb_bytes:
                    geometry = _wkb_to_esri_geometry(wkb_bytes)
            else:
                attributes[col_name] = _to_esri_value(table_dict[col_name][i])

        features.append({
            "attributes": attributes,
            "geometry": geometry,
        })

    return {
        "objectIdFieldName": schema.id_field,
        "geometryType": esri_geom_type,
        "spatialReference": {"wkid": schema.srid},
        "fields": fields,
        "features": features,
        "exceededTransferLimit": result.exceeded_transfer_limit,
    }


def _wkb_to_esri_geometry(wkb_bytes: bytes) -> dict:
    """Convert WKB to Esri JSON geometry representation."""
    from shapely import wkb
    geom = wkb.loads(wkb_bytes)
    geom_type = geom.geom_type

    if geom_type == "Point":
        return {"x": geom.x, "y": geom.y}
    elif geom_type in ("Polygon", "MultiPolygon"):
        rings = []
        polys = [geom] if geom_type == "Polygon" else list(geom.geoms)
        for poly in polys:
            rings.append(list(poly.exterior.coords))
            for interior in poly.interiors:
                rings.append(list(interior.coords))
        return {"rings": rings}
    elif geom_type in ("LineString", "MultiLineString"):
        paths = []
        lines = [geom] if geom_type == "LineString" else list(geom.geoms)
        for line in lines:
            paths.append(list(line.coords))
        return {"paths": paths}
    elif geom_type == "MultiPoint":
        return {"points": [list(p.coords[0]) for p in geom.geoms]}

    return None


def _build_field_definitions(schema: FeatureSchema) -> list[dict]:
    """Build Esri field definition array from schema."""
    type_map = {
        "string": "esriFieldTypeString",
        "int32": "esriFieldTypeInteger",
        "int64": "esriFieldTypeInteger",
        "float": "esriFieldTypeSingle",
        "double": "esriFieldTypeDouble",
        "boolean": "esriFieldTypeSmallInteger",
        "date": "esriFieldTypeDate",
        "timestamp": "esriFieldTypeDate",
    }
    fields = []
    for f in schema.fields:
        esri_type = type_map.get(f["type"], "esriFieldTypeString")
        fields.append({
            "name": f["name"],
            "type": esri_type,
            "alias": f.get("alias", f["name"]),
        })
    return fields


def _to_esri_value(val):
    """Convert Python value to Esri JSON safe value."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
```

#### serializers/esri_pbf.py

```python
"""
Serialize QueryResult → Esri FeatureCollection PBF (Protocol Buffers).

This is the performance-critical serializer for Iceberg-scale data.
The ArcGIS JS API requests PBF by default (f=pbf).

Key differences from JSON encoding:
1. Coordinates are quantized to integers with a Transform
2. Coordinates are delta-encoded (each = current - previous)
3. Geometry rings/paths use a flat coords array + lengths array
4. Attributes use protobuf Value oneof types (not JSON strings)

References:
- Spec: https://github.com/Esri/arcgis-pbf/tree/main/proto/FeatureCollection
- Decoder reference (JS): https://github.com/rowanwins/arcgis-pbf-parser
- Decoder reference (R/Rust): https://r.esri.com/arcpbf/

IMPORTANT: The .proto file must be compiled first:
    protoc --python_out=. FeatureCollection.proto
"""

from iceberg_geo.query.models import QueryResult, FeatureSchema
from iceberg_geo.query.geometry import wkb_to_coords
from .proto import FeatureCollection_pb2 as pb
from shapely import wkb
import struct

# Quantization resolution — controls coordinate precision
# Higher = more precision but larger integers
# Esri typically uses values that give ~0.01m precision
QUANTIZE_RESOLUTION = 1e8  # for geographic (lon/lat) coordinates


def serialize(result: QueryResult, schema: FeatureSchema) -> bytes:
    """
    Serialize a QueryResult to Esri FeatureCollection PBF bytes.

    Returns raw protobuf bytes suitable for Response(content=...,
    media_type="application/x-protobuf").
    """
    if result.features is None or result.features.num_rows == 0:
        return _serialize_empty(result, schema)

    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult
    feature_result = query_result.featureResult

    # --- Spatial reference ---
    feature_result.spatialReference.wkid = schema.srid
    feature_result.spatialReference.latestWkid = schema.srid

    # --- Fields ---
    _build_fields(feature_result, schema)

    # --- Geometry type ---
    geom_type_map = {
        "Point": pb.FeatureCollectionPBuffer.esriGeometryTypePoint,
        "MultiPoint": pb.FeatureCollectionPBuffer.esriGeometryTypeMultipoint,
        "LineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "MultiLineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "Polygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
        "MultiPolygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    }
    feature_result.geometryType = geom_type_map.get(
        schema.geometry_type,
        pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    )

    # --- Compute Transform from data extent ---
    # Must scan all geometries to determine coordinate bounds
    table_dict = result.features.to_pydict()
    geom_col = result.geometry_column
    all_coords = []

    # First pass: collect all coordinates to compute bounds
    geometries = []
    for i in range(result.features.num_rows):
        wkb_bytes = table_dict[geom_col][i]
        if wkb_bytes:
            geom = wkb.loads(wkb_bytes)
            geometries.append(geom)
            bounds = geom.bounds  # (minx, miny, maxx, maxy)
            all_coords.extend([(bounds[0], bounds[1]), (bounds[2], bounds[3])])
        else:
            geometries.append(None)

    if not all_coords:
        return _serialize_empty(result, schema)

    xs = [c[0] for c in all_coords]
    ys = [c[1] for c in all_coords]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)

    # Transform: maps world coords to integer space
    # quantize_origin is upper-left by default
    x_range = x_max - x_min if x_max != x_min else 1.0
    y_range = y_max - y_min if y_max != y_min else 1.0
    x_scale = x_range / QUANTIZE_RESOLUTION
    y_scale = y_range / QUANTIZE_RESOLUTION

    transform = feature_result.transform
    transform.quantizeOriginPostion = (
        pb.FeatureCollectionPBuffer.Transform.kQuantizeOriginPostionUpperLeft
    )
    transform.scale.xScale = x_scale
    transform.scale.yScale = y_scale
    transform.translate.xTranslate = x_min
    transform.translate.yTranslate = y_max  # upper-left origin

    # --- Features ---
    # Second pass: encode features with quantized, delta-encoded coords
    field_names = [f["name"] for f in schema.fields]

    for i in range(result.features.num_rows):
        feature = feature_result.features.add()

        # Attributes (in field order)
        for field_name in field_names:
            if field_name in table_dict:
                val = table_dict[field_name][i]
                _set_value(feature.attributes.add(), val,
                          _get_field_type(schema, field_name))

        # Geometry
        geom = geometries[i]
        if geom is not None:
            _encode_geometry(
                feature.geometry, geom,
                x_min, y_max, x_scale, y_scale
            )

    # --- Exceeded transfer limit ---
    feature_result.exceededTransferLimit = result.exceeded_transfer_limit

    return fc.SerializeToString()


def _encode_geometry(
    pb_geom,
    shapely_geom,
    x_translate: float,
    y_translate: float,
    x_scale: float,
    y_scale: float,
):
    """
    Encode a Shapely geometry into an Esri PBF Geometry message.

    Steps:
    1. Extract coordinate rings/paths from shapely geometry
    2. Quantize: int_x = round((world_x - x_translate) / x_scale)
                 int_y = round((world_y - y_translate) / y_scale)
       (note: y_translate is the max Y for upper-left origin,
        so y values become negative integers)
    3. Delta-encode: each coord = current - previous
    4. Set lengths array (vertex count per ring/path)
    5. Set coords array (flat delta-encoded sint32 values)
    """
    geom_type = shapely_geom.geom_type
    coord_arrays = _extract_coord_arrays(shapely_geom)

    # Set geometry type
    type_map = {
        "Point": pb.FeatureCollectionPBuffer.esriGeometryTypePoint,
        "MultiPoint": pb.FeatureCollectionPBuffer.esriGeometryTypeMultipoint,
        "LineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "MultiLineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "Polygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
        "MultiPolygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    }
    pb_geom.geometryType = type_map.get(
        geom_type,
        pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    )

    all_delta_coords = []
    lengths = []

    for ring_coords in coord_arrays:
        lengths.append(len(ring_coords))
        prev_x, prev_y = 0, 0

        for (wx, wy) in ring_coords:
            # Quantize
            qx = round((wx - x_translate) / x_scale)
            qy = round((wy - y_translate) / y_scale)
            # Delta encode
            dx = qx - prev_x
            dy = qy - prev_y
            all_delta_coords.extend([dx, dy])
            prev_x, prev_y = qx, qy

    pb_geom.lengths.extend(lengths)
    pb_geom.coords.extend(all_delta_coords)


def _extract_coord_arrays(geom):
    """Extract coordinate arrays from a Shapely geometry.

    Returns list of rings/paths, where each is a list of (x, y) tuples.
    For Point: [[(x, y)]]
    For LineString: [[(x1,y1), (x2,y2), ...]]
    For Polygon: [exterior_ring, interior_ring1, ...]
    For Multi*: flattened list of all parts
    """
    geom_type = geom.geom_type

    if geom_type == "Point":
        return [[(geom.x, geom.y)]]
    elif geom_type == "MultiPoint":
        return [[(p.x, p.y)] for p in geom.geoms]
    elif geom_type == "LineString":
        return [list(geom.coords)]
    elif geom_type == "MultiLineString":
        return [list(line.coords) for line in geom.geoms]
    elif geom_type == "Polygon":
        rings = [list(geom.exterior.coords)]
        for interior in geom.interiors:
            rings.append(list(interior.coords))
        return rings
    elif geom_type == "MultiPolygon":
        rings = []
        for poly in geom.geoms:
            rings.append(list(poly.exterior.coords))
            for interior in poly.interiors:
                rings.append(list(interior.coords))
        return rings
    return []


def _set_value(pb_value, python_val, field_type: str):
    """Set a protobuf Value message from a Python value."""
    if python_val is None:
        # PBF doesn't have explicit null — convention is to skip or use default
        # Esri uses a sentinel: empty string for strings, 0 for numbers
        return

    if hasattr(python_val, "as_py"):
        python_val = python_val.as_py()

    if field_type in ("string", "esriFieldTypeString"):
        pb_value.string_value = str(python_val) if python_val is not None else ""
    elif field_type in ("int32", "esriFieldTypeSmallInteger"):
        pb_value.sint_value = int(python_val)
    elif field_type in ("int64", "esriFieldTypeInteger", "esriFieldTypeOID"):
        pb_value.int64_value = int(python_val)
    elif field_type in ("float", "esriFieldTypeSingle"):
        pb_value.float_value = float(python_val)
    elif field_type in ("double", "esriFieldTypeDouble"):
        pb_value.double_value = float(python_val)
    elif field_type in ("boolean",):
        pb_value.bool_value = bool(python_val)
    else:
        pb_value.string_value = str(python_val)


def _build_fields(feature_result, schema: FeatureSchema):
    """Add field definitions to the PBF FeatureResult."""
    type_map = {
        "string": pb.FeatureCollectionPBuffer.esriFieldTypeString,
        "int32": pb.FeatureCollectionPBuffer.esriFieldTypeSmallInteger,
        "int64": pb.FeatureCollectionPBuffer.esriFieldTypeInteger,
        "float": pb.FeatureCollectionPBuffer.esriFieldTypeSingle,
        "double": pb.FeatureCollectionPBuffer.esriFieldTypeDouble,
        "date": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
        "timestamp": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
    }
    for f in schema.fields:
        field = feature_result.fields.add()
        field.name = f["name"]
        field.alias = f.get("alias", f["name"])
        field.fieldType = type_map.get(
            f["type"],
            pb.FeatureCollectionPBuffer.esriFieldTypeString,
        )


def _get_field_type(schema: FeatureSchema, field_name: str) -> str:
    """Look up field type from schema."""
    for f in schema.fields:
        if f["name"] == field_name:
            return f["type"]
    return "string"


def _serialize_empty(result: QueryResult, schema: FeatureSchema) -> bytes:
    """Serialize an empty result set."""
    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult

    if result.return_count_only:
        query_result.countResult.count = result.count
        return fc.SerializeToString()

    feature_result = query_result.featureResult
    feature_result.spatialReference.wkid = schema.srid
    _build_fields(feature_result, schema)
    feature_result.exceededTransferLimit = False
    return fc.SerializeToString()
```

#### metadata.py

```python
"""
Build Esri GeoServices metadata responses from Iceberg table schemas.

These are the relatively static JSON responses for /FeatureServer
and /FeatureServer/{layer_id} — called once when a layer is added
to an ArcGIS map.
"""

from iceberg_geo.query.models import FeatureSchema
from iceberg_geo.query.geometry import ESRI_GEOMETRY_TYPE_MAP


def build_service_metadata(namespace: str, table_names: list[str]) -> dict:
    """Build /FeatureServer response."""
    layers = []
    for i, name in enumerate(table_names):
        layers.append({
            "id": i,
            "name": name,
            "type": "Feature Layer",
            "geometryType": "esriGeometryPolygon",  # refined per-layer
        })

    return {
        "currentVersion": 11.0,
        "serviceDescription": f"Iceberg-backed feature service: {namespace}",
        "hasVersionedData": False,
        "supportsDisconnectedEditing": False,
        "supportedQueryFormats": "JSON, geoJSON, PBF",
        "maxRecordCount": 10000,
        "capabilities": "Query",
        "layers": layers,
        "tables": [],
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
    }


def build_layer_metadata(schema: FeatureSchema, layer_id: int) -> dict:
    """Build /FeatureServer/{layer_id} response."""

    esri_geom_type = ESRI_GEOMETRY_TYPE_MAP.get(
        schema.geometry_type, "esriGeometryPolygon"
    )

    # Build field array
    type_map = {
        "string": "esriFieldTypeString",
        "int32": "esriFieldTypeInteger",
        "int64": "esriFieldTypeInteger",
        "float": "esriFieldTypeSingle",
        "double": "esriFieldTypeDouble",
        "boolean": "esriFieldTypeSmallInteger",
        "date": "esriFieldTypeDate",
        "timestamp": "esriFieldTypeDate",
    }

    fields = [{
        "name": schema.id_field,
        "type": "esriFieldTypeOID",
        "alias": schema.id_field,
        "sqlType": "sqlTypeInteger",
    }]

    for f in schema.fields:
        if f["name"] == schema.id_field:
            continue
        fields.append({
            "name": f["name"],
            "type": type_map.get(f["type"], "esriFieldTypeString"),
            "alias": f.get("alias", f["name"]),
        })

    return {
        "currentVersion": 11.0,
        "id": layer_id,
        "name": schema.table_identifier.split(".")[-1],
        "type": "Feature Layer",
        "geometryType": esri_geom_type,
        "objectIdField": schema.id_field,
        "fields": fields,
        "extent": {
            "xmin": schema.extent["xmin"] if schema.extent else -180,
            "ymin": schema.extent["ymin"] if schema.extent else -90,
            "xmax": schema.extent["xmax"] if schema.extent else 180,
            "ymax": schema.extent["ymax"] if schema.extent else 90,
            "spatialReference": {"wkid": schema.srid},
        },
        "maxRecordCount": schema.max_record_count,
        "supportedQueryFormats": "JSON, geoJSON, PBF",
        "capabilities": "Query",
        "advancedQueryCapabilities": {
            "supportsDistinct": True,
            "supportsOrderBy": True,
            "supportsPagination": True,
            "supportsQueryWithResultType": True,
            "supportsReturningGeometryCentroid": False,
            "supportsStatistics": False,  # Phase 3
        },
        "hasAttachments": False,
        "htmlPopupType": "esriServerHTMLPopupTypeAsHTMLText",
    }
```

---

### 4. pygeoapi Configuration

#### config/pygeoapi-config.yml

```yaml
server:
    bind:
        host: 0.0.0.0
        port: 5000
    url: http://localhost:5000
    mimetype: application/json; charset=UTF-8
    encoding: utf-8
    gzip: true
    languages:
        - en-US
    cors: true

logging:
    level: INFO

metadata:
    identification:
        title: Iceberg Geospatial API
        description: OGC API Features backed by Apache Iceberg
        keywords:
            - iceberg
            - geospatial
            - ogc api
        keywords_type: theme
        terms_of_service: null
        url: https://github.com/your-org/iceberg-geo-api
    license:
        name: Apache-2.0
        url: https://www.apache.org/licenses/LICENSE-2.0
    provider:
        name: Your Organization
        url: https://your-org.com
    contact:
        name: Admin
        email: admin@your-org.com

resources:
    # Each Iceberg table becomes a pygeoapi collection.
    # Add entries here for each table to expose.

    example-parcels:
        type: collection
        title: Parcels
        description: National parcel boundaries from Iceberg
        keywords:
            - parcels
            - boundaries
        extents:
            spatial:
                bbox: [-180, -90, 180, 90]
                crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
        providers:
            - type: feature
              name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider
              data: default.parcels       # namespace.table_name
              id_field: parcel_id
              options:
                  geometry_column: geometry

    example-sensors:
        type: collection
        title: Sensor Observations
        description: Environmental sensor readings
        keywords:
            - sensors
            - observations
        extents:
            spatial:
                bbox: [-180, -90, 180, 90]
                crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
            temporal:
                begin: 2020-01-01T00:00:00Z
                end: null
        providers:
            - type: feature
              name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider
              data: default.sensor_observations
              id_field: observation_id
              options:
                  geometry_column: location
```

#### config/catalog.yml

```yaml
# PyIceberg catalog configuration.
# See: https://py.iceberg.apache.org/configuration/

catalog:
    name: default
    type: rest               # rest | sql | glue | hive | dynamodb
    uri: http://rest-catalog:8181

# For local dev with SQLite catalog:
# catalog:
#     name: default
#     type: sql
#     uri: sqlite:////data/warehouse/catalog.db
#     warehouse: file:///data/warehouse

# For AWS Glue:
# catalog:
#     name: default
#     type: glue
#     warehouse: s3://your-bucket/warehouse
```

---

### 5. Esri PBF Proto File

Vendor the `.proto` file from https://github.com/Esri/arcgis-pbf/blob/main/proto/FeatureCollection/FeatureCollection.proto into `src/iceberg_geo/geoservices/proto/FeatureCollection.proto`.

Compile with:

```bash
protoc \
    --python_out=src/iceberg_geo/geoservices/proto/ \
    --proto_path=src/iceberg_geo/geoservices/proto/ \
    src/iceberg_geo/geoservices/proto/FeatureCollection.proto
```

This generates `FeatureCollection_pb2.py`. Commit the generated file so the build doesn't require protoc at runtime.

---

### 6. GeoArrow Formatter for pygeoapi (Phase 2)

#### src/iceberg_geo/formatters/geoarrow.py

```python
"""
GeoArrow / Arrow IPC output formatter for pygeoapi.

Registers media type: application/vnd.apache.arrow.stream
Clients request via Accept header or ?f=arrow

This avoids the expensive GeoJSON serialization path entirely —
data goes from Iceberg (Parquet/Arrow) through DuckDB (Arrow)
to the client as Arrow IPC with GeoArrow geometry encoding.
"""

# Phase 2 implementation.
# Register as a pygeoapi formatter plugin:
#
# In pygeoapi-config.yml:
#   server:
#     extra_formats:
#       - name: arrow
#         mimetype: application/vnd.apache.arrow.stream
#         formatter: iceberg_geo.formatters.geoarrow.GeoArrowFormatter
#
# The formatter receives the provider result (GeoJSON dict) and
# must convert back to Arrow. For maximum efficiency, the provider
# should pass through the raw Arrow table via a side channel
# (e.g., storing it on the result dict under a private key)
# so we avoid GeoJSON → Arrow round-tripping.

import pyarrow as pa
import pyarrow.ipc as ipc
from io import BytesIO


class GeoArrowFormatter:
    """Format query results as Arrow IPC stream with GeoArrow geometry."""

    mimetype = "application/vnd.apache.arrow.stream"

    def write(self, result: dict, **kwargs) -> bytes:
        """
        Convert result to Arrow IPC bytes.

        If the result contains a _raw_arrow_table key (set by the
        Iceberg provider to avoid round-tripping), use it directly.
        Otherwise, convert from GeoJSON features.
        """
        if "_raw_arrow_table" in result:
            arrow_table = result["_raw_arrow_table"]
        else:
            arrow_table = _geojson_to_arrow(result)

        # Serialize to Arrow IPC stream format
        sink = BytesIO()
        writer = ipc.new_stream(sink, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()
        return sink.getvalue()


def _geojson_to_arrow(geojson_result: dict) -> pa.Table:
    """
    Convert GeoJSON FeatureCollection to Arrow table.
    Fallback path — prefer passing raw Arrow tables.
    """
    # Use geoarrow-pyarrow for geometry encoding
    ...
```

---

## Testing Strategy

### Test fixtures (tests/conftest.py)

```python
"""
Shared test fixtures.

Creates an in-memory Iceberg catalog with sample tables
containing geometry columns for testing all components.
"""

import pytest
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from shapely.geometry import Point, Polygon, box
from shapely import wkb as wkb_mod
import tempfile, os


@pytest.fixture(scope="session")
def iceberg_catalog(tmp_path_factory):
    """Create a temporary SQLite-backed Iceberg catalog with test data."""
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{warehouse}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    catalog.create_namespace("test")

    # Create a points table (sensor observations)
    _create_points_table(catalog, warehouse)

    # Create a polygons table (parcels)
    _create_polygons_table(catalog, warehouse)

    return catalog


def _create_points_table(catalog, warehouse):
    """Create test.sensor_points with ~1000 random points."""
    import random
    random.seed(42)

    n = 1000
    geometries = []
    for _ in range(n):
        pt = Point(
            random.uniform(-120, -70),  # US longitude range
            random.uniform(25, 50),      # US latitude range
        )
        geometries.append(wkb_mod.dumps(pt))

    table = pa.table({
        "objectid": pa.array(range(n), type=pa.int64()),
        "sensor_id": pa.array([f"S{i:04d}" for i in range(n)]),
        "temperature": pa.array(
            [random.uniform(-10, 45) for _ in range(n)],
            type=pa.float64(),
        ),
        "geometry": pa.array(geometries, type=pa.large_binary()),
    })

    # Write as Iceberg table
    catalog.create_table("test.sensor_points", schema=table.schema)
    iceberg_table = catalog.load_table("test.sensor_points")
    iceberg_table.append(table)


def _create_polygons_table(catalog, warehouse):
    """Create test.parcels with ~100 rectangular polygons."""
    import random
    random.seed(43)

    n = 100
    geometries = []
    for _ in range(n):
        x = random.uniform(-120, -70)
        y = random.uniform(25, 50)
        size = random.uniform(0.01, 0.1)
        poly = box(x, y, x + size, y + size)
        geometries.append(wkb_mod.dumps(poly))

    table = pa.table({
        "objectid": pa.array(range(n), type=pa.int64()),
        "parcel_id": pa.array([f"P{i:06d}" for i in range(n)]),
        "area_sqm": pa.array(
            [random.uniform(100, 50000) for _ in range(n)],
            type=pa.float64(),
        ),
        "zoning": pa.array(
            [random.choice(["R1", "R2", "C1", "C2", "I1"]) for _ in range(n)]
        ),
        "geometry": pa.array(geometries, type=pa.large_binary()),
    })

    catalog.create_table("test.parcels", schema=table.schema)
    iceberg_table = catalog.load_table("test.parcels")
    iceberg_table.append(table)


@pytest.fixture
def sample_query_params():
    """Default query params for testing."""
    from iceberg_geo.query.models import QueryParams
    return QueryParams(limit=10)
```

### Key test areas

```bash
# Run all tests
pytest tests/ -v

# Test the query engine in isolation
pytest tests/test_query_engine.py -v
# - bbox filtering returns correct features
# - WHERE clause filtering works
# - Pagination (limit/offset) works correctly
# - Count-only queries return correct count
# - Empty result sets handled gracefully
# - SQL injection attempts are rejected

# Test PBF encoder specifically
pytest tests/test_esri_pbf_encoder.py -v
# - Point geometry encoding roundtrips correctly
# - Polygon ring encoding with delta coords
# - Transform quantization produces valid integers
# - Multi-geometry encoding
# - Empty geometry handling
# - Attribute type mapping (string, int, float, date)
# - Validate output against arcgis-pbf-parser (if available)

# Test the FastAPI GeoServices endpoint
pytest tests/test_geoservices_query.py -v
# - /rest/info returns valid response
# - /FeatureServer returns layer list
# - /FeatureServer/0 returns field definitions
# - /FeatureServer/0/query?f=json returns valid Esri JSON
# - /FeatureServer/0/query?f=pbf returns valid PBF
# - /FeatureServer/0/query?f=geojson returns valid GeoJSON
# - bbox spatial filter works
# - where clause filter works
# - outFields projection works
# - Pagination works
# - returnCountOnly works

# Test pygeoapi provider
pytest tests/test_pygeoapi_provider.py -v
# - query() returns valid FeatureCollection
# - get() returns single feature
# - bbox filtering
# - Property filtering
# - Field selection
# - Pagination
```

---

## Security Considerations

### SQL Injection in WHERE Clauses

The `where` parameter in GeoServices queries accepts SQL-like expressions from the client. This is the **primary attack vector**.

**Phase 1 approach (conservative):**
- Allowlist of operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `IN`, `BETWEEN`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`
- Reject any input containing: `;`, `--`, `/*`, `DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER`, `EXEC`, `UNION`, system function names
- Reject subqueries (parenthesized SELECT)
- Use parameterized queries where possible (DuckDB supports `$1`, `$2` style parameters)

**Phase 2 approach (robust):**
- Use `sqlglot` to parse the WHERE clause into an AST
- Walk the AST and validate that it only contains allowed expression types
- Reconstruct the SQL from the validated AST
- This prevents all injection vectors including encoded characters

### Container Security

- Run containers as non-root user
- Read-only filesystem except for DuckDB temp directory
- No network egress except to Iceberg catalog and object storage
- Health check endpoints do not expose internal state

---

## Development Workflow

```bash
# Local development (no containers)
pip install -e ".[pygeoapi,geoservices,dev]"

# Start local Iceberg REST catalog
docker run -p 8181:8181 apache/iceberg-rest:1.9.0

# Seed test data
python scripts/seed_test_data.py

# Run pygeoapi
ICEBERG_CATALOG_CONFIG=config/catalog.yml pygeoapi serve --server-config config/pygeoapi-config.yml

# Run GeoServices endpoint (separate terminal)
ICEBERG_CATALOG_CONFIG=config/catalog.yml uvicorn iceberg_geo.geoservices.app:app --port 8001 --reload

# Run tests
pytest tests/ -v

# Build and run with Docker Compose
docker compose up --build

# Test endpoints
curl http://localhost:5000/                              # pygeoapi landing page
curl http://localhost:5000/collections                   # list collections
curl "http://localhost:5000/collections/example-parcels/items?limit=5"  # query features
curl http://localhost:8001/rest/info                     # GeoServices info
curl "http://localhost:8001/rest/services/default/FeatureServer/0/query?where=1=1&outFields=*&f=json&resultRecordCount=5"
```

---

## Phase Checklist

### Phase 1 — MVP (read-only, JSON formats)
- [ ] Project scaffolding (pyproject.toml, Dockerfiles, docker-compose)
- [ ] Iceberg Query Service: catalog connection, basic query engine
- [ ] pygeoapi provider: query(), get(), bbox filter, pagination
- [ ] FastAPI GeoServices: /FeatureServer, /FeatureServer/0, /FeatureServer/0/query
- [ ] Esri JSON serializer
- [ ] GeoJSON serializer (for both pygeoapi and GeoServices f=geojson)
- [ ] pygeoapi-config.yml with at least one collection
- [ ] WHERE clause sanitization (conservative allowlist)
- [ ] Integration tests with sample Iceberg tables
- [ ] Docker Compose runs end-to-end

### Phase 2 — Performance (binary formats, advanced queries)
- [ ] Esri PBF encoder (protoc compilation, quantization, delta encoding)
- [ ] GeoArrow/Arrow IPC formatter for pygeoapi
- [ ] DuckDB spatial extension integration for complex spatial filters
- [ ] CQL2 filter translation for pygeoapi
- [ ] Iceberg partition pruning for spatially partitioned tables
- [ ] Cursor-based pagination option
- [ ] WHERE clause sanitization (sqlglot AST approach)
- [ ] Performance benchmarks: PBF vs JSON at 10k, 50k, 100k features

### Phase 3 — Polish
- [ ] Caching layer for collection metadata and frequently accessed extents
- [ ] Iceberg time-travel: expose snapshots as temporal queries
- [ ] GeoServices statistics (returnStatisticsOnly, groupByFieldsForStatistics)
- [ ] GeoServices generateRenderer (class breaks, unique values)
- [ ] Tile generation from Iceberg data
- [ ] Monitoring/observability (request latency, query performance, cache hit rates)
