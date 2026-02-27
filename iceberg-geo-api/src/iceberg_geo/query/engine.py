"""
Core query engine. Translates query parameters into DuckDB SQL
against Arrow tables produced by PyIceberg scans.

This is the ONLY place where DuckDB queries are constructed and executed.
Both pygeoapi and the GeoServices endpoint call these functions.
"""

import logging
import re
import threading

import duckdb
import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.table import Table
from shapely import wkb
from shapely.geometry import box as shapely_box

from .geometry import detect_geometry_type
from .models import FeatureSchema, QueryParams, QueryResult

logger = logging.getLogger(__name__)

# Cache Arrow tables in memory, keyed by (table_identifier, snapshot_id).
# Invalidates automatically when a new Iceberg snapshot appears.
_arrow_cache: dict[tuple[str, int], pa.Table] = {}
_arrow_cache_lock = threading.Lock()
_schema_cache: dict[str, FeatureSchema] = {}

# Allowlisted SQL tokens for WHERE clause sanitization
_ALLOWED_OPERATORS = {
    "=", "!=", "<>", "<", ">", "<=", ">=",
    "AND", "OR", "NOT", "IN", "BETWEEN",
    "LIKE", "IS", "NULL",
}

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|EXEC|EXECUTE|UNION|"
    r"TRUNCATE|GRANT|REVOKE|MERGE|CALL|COPY|ATTACH|DETACH|PRAGMA)\b",
    re.IGNORECASE,
)

_FORBIDDEN_PATTERNS = re.compile(r"(--|/\*|\*/|;)")

_HAS_SPATIAL = None


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection, loading the spatial extension if available."""
    global _HAS_SPATIAL
    conn = duckdb.connect()
    if _HAS_SPATIAL is None:
        try:
            conn.install_extension("spatial")
            conn.load_extension("spatial")
            _HAS_SPATIAL = True
        except Exception:
            logger.warning(
                "DuckDB spatial extension not available. "
                "Spatial queries will use Shapely fallback."
            )
            _HAS_SPATIAL = False
    elif _HAS_SPATIAL:
        try:
            conn.load_extension("spatial")
        except Exception:
            _HAS_SPATIAL = False
    return conn


def get_table_schema(table: Table) -> FeatureSchema:
    """
    Extract feature schema from Iceberg table metadata.

    Returns field names, types, geometry column name, spatial reference,
    and extent. Results are cached per table identifier.
    """
    schema = table.schema()
    raw_name = table.name()
    if isinstance(raw_name, (tuple, list)):
        table_identifier = ".".join(str(part) for part in raw_name)
    else:
        table_identifier = str(raw_name)

    # Return cached schema if available
    if table_identifier in _schema_cache:
        return _schema_cache[table_identifier]

    # Map Iceberg/Arrow types to simple type strings
    type_map = {
        "string": "string",
        "large_string": "string",
        "utf8": "string",
        "int32": "int32",
        "int64": "int64",
        "float": "float",
        "float32": "float",
        "double": "double",
        "float64": "double",
        "bool": "boolean",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "binary": "binary",
        "large_binary": "binary",
    }

    geom_col = _detect_geometry_column_from_iceberg(schema)
    id_field = _detect_id_field(schema)

    fields = []
    for field in schema.fields:
        field_name = field.name
        if field_name == geom_col:
            continue

        # Get type string
        iceberg_type = str(field.field_type).lower()

        # Complex types (struct, map, list) are always serialized as strings
        simple_type = "string"
        if not any(prefix in iceberg_type for prefix in ("struct", "map<", "list<")):
            for key, val in type_map.items():
                if key in iceberg_type:
                    simple_type = val
                    break

        fields.append({
            "name": field_name,
            "type": simple_type,
            "alias": field_name,
        })

    # Detect geometry type and compute extent from cached Arrow table
    geometry_type = "Polygon"
    extent = None
    try:
        arrow_table = _get_cached_arrow_table(table)
        if arrow_table.num_rows > 0:
            wkb_bytes = arrow_table.column(geom_col)[0].as_py()
            if wkb_bytes:
                geometry_type = detect_geometry_type(wkb_bytes)

            # Compute extent using DuckDB for speed
            try:
                conn = _get_connection()
                conn.register("_extent_tmp", arrow_table)
                row = conn.execute(
                    f'SELECT MIN(ST_XMin(ST_GeomFromWKB("{geom_col}"))),'
                    f'       MIN(ST_YMin(ST_GeomFromWKB("{geom_col}"))),'
                    f'       MAX(ST_XMax(ST_GeomFromWKB("{geom_col}"))),'
                    f'       MAX(ST_YMax(ST_GeomFromWKB("{geom_col}")))'
                    f' FROM _extent_tmp'
                ).fetchone()
                if row and row[0] is not None:
                    extent = {
                        "xmin": row[0], "ymin": row[1],
                        "xmax": row[2], "ymax": row[3],
                    }
            except Exception as e:
                logger.warning("Failed to compute extent for %s: %s", table_identifier, e)
    except Exception:
        pass

    # Adaptive max record count: fewer for polygons (heavy geometry)
    # 500 for polygons keeps PBF serialization under ~1s per request
    max_records = 500 if geometry_type in ("Polygon", "MultiPolygon") else 10000

    result = FeatureSchema(
        table_identifier=table_identifier,
        geometry_column=geom_col,
        geometry_type=geometry_type,
        srid=4326,
        fields=fields,
        extent=extent,
        id_field=id_field,
        max_record_count=max_records,
    )
    _schema_cache[table_identifier] = result
    return result


def _get_cached_arrow_table(table: Table) -> pa.Table:
    """Load the Arrow table from cache or scan from Iceberg.

    Caches the full table scan in memory, keyed by table identifier
    and snapshot ID. This avoids repeated S3 reads for the same data.

    Also computes and caches bbox pre-filter columns (__bbox_xmin, etc.)
    using DuckDB for fast spatial pre-filtering on subsequent queries.
    These columns enable cheap numeric comparisons before the expensive
    ST_GeomFromWKB + ST_Intersects check.
    """
    raw_name = table.name()
    if isinstance(raw_name, (tuple, list)):
        table_id = ".".join(str(part) for part in raw_name)
    else:
        table_id = str(raw_name)

    snapshot = table.current_snapshot()
    snapshot_id = snapshot.snapshot_id if snapshot else 0
    cache_key = (table_id, snapshot_id)

    with _arrow_cache_lock:
        if cache_key in _arrow_cache:
            return _arrow_cache[cache_key]

    # Not cached — scan from Iceberg
    arrow_table = table.scan(row_filter=AlwaysTrue()).to_arrow()

    # Compute bbox pre-filter columns for spatial queries
    arrow_table = _add_bbox_columns(arrow_table)

    with _arrow_cache_lock:
        # Evict old snapshots of the same table
        for key in list(_arrow_cache):
            if key[0] == table_id and key != cache_key:
                del _arrow_cache[key]
        _arrow_cache[cache_key] = arrow_table

    logger.info("Cached %s (snapshot %s): %d rows", table_id, snapshot_id, arrow_table.num_rows)
    return arrow_table


def _add_bbox_columns(arrow_table: pa.Table) -> pa.Table:
    """Pre-compute bounding box columns for fast spatial pre-filtering.

    Adds __bbox_xmin, __bbox_ymin, __bbox_xmax, __bbox_ymax columns
    to the Arrow table using DuckDB. This avoids calling ST_GeomFromWKB
    on every row during every spatial query — instead, cheap numeric
    comparisons filter out most non-matching rows first.
    """
    import time as _time

    geom_col = _detect_geometry_column(arrow_table.schema)
    logger.info(
        "Computing bbox columns for %d rows (geom_col=%s, HAS_SPATIAL=%s)",
        arrow_table.num_rows, geom_col, _HAS_SPATIAL,
    )

    if not _HAS_SPATIAL:
        # Try to init spatial if not yet checked
        try:
            conn = _get_connection()
        except Exception:
            pass

    if not _HAS_SPATIAL:
        logger.warning("Skipping bbox columns: DuckDB spatial not available")
        return arrow_table

    try:
        t0 = _time.perf_counter()
        conn = _get_connection()
        conn.register("_bbox_tmp", arrow_table)
        result = conn.execute(f"""
            SELECT *,
                ST_XMin(ST_GeomFromWKB("{geom_col}")) AS __bbox_xmin,
                ST_YMin(ST_GeomFromWKB("{geom_col}")) AS __bbox_ymin,
                ST_XMax(ST_GeomFromWKB("{geom_col}")) AS __bbox_xmax,
                ST_YMax(ST_GeomFromWKB("{geom_col}")) AS __bbox_ymax
            FROM _bbox_tmp
        """).fetch_arrow_table()
        elapsed = _time.perf_counter() - t0
        logger.info(
            "Added bbox pre-filter columns to %d rows in %.1fs",
            result.num_rows, elapsed,
        )
        return result
    except Exception as e:
        logger.warning("Failed to add bbox columns: %s", e)
        return arrow_table


def query_features(table: Table, params: QueryParams) -> QueryResult:
    """
    Execute a spatial query against an Iceberg table.

    Pipeline:
    1. Build PyIceberg row_filter from bbox (for partition pruning)
    2. Execute scan -> Arrow table
    3. Register Arrow table in DuckDB
    4. Build and execute DuckDB SQL with spatial filters,
       attribute filters, field selection, sorting, pagination
    5. Return QueryResult with Arrow table of matching features
    """
    conn = _get_connection()

    # --- Step 1: Load Arrow table (cached after first scan) ---
    arrow_table = _get_cached_arrow_table(table)

    if arrow_table.num_rows == 0:
        return QueryResult.empty(params)

    # --- Step 2: Register in DuckDB ---
    conn.register("features", arrow_table)

    # --- Step 3: Build SQL ---
    #
    # All queries use a CTE that assigns deterministic global OIDs first,
    # then applies filters on the numbered result. This guarantees that
    # a feature's __oid is always the same regardless of which query
    # returns it (critical for QGIS identify: returnIdsOnly + objectIds).
    geom_col = _detect_geometry_column(arrow_table.schema)
    schema_info = get_table_schema(table)

    where_clauses = []
    needs_shapely_spatial_filter = False
    shapely_filter_geom = None

    # Spatial filter — bbox
    # Use bbox pre-filter columns if available (numeric comparisons are
    # ~100x faster than ST_GeomFromWKB + ST_Intersects on every row).
    has_bbox_cols = "__bbox_xmin" in [f.name for f in arrow_table.schema]
    if params.bbox:
        xmin, ymin, xmax, ymax = params.bbox
        if _HAS_SPATIAL:
            if has_bbox_cols:
                # Fast bbox pre-filter: eliminates most rows cheaply
                where_clauses.append(
                    f"__bbox_xmax >= {xmin} AND __bbox_xmin <= {xmax} "
                    f"AND __bbox_ymax >= {ymin} AND __bbox_ymin <= {ymax}"
                )
            else:
                # Fallback: expensive per-row WKB parsing
                where_clauses.append(
                    f"ST_Intersects(ST_GeomFromWKB(\"{geom_col}\"), "
                    f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
                )
        else:
            needs_shapely_spatial_filter = True
            shapely_filter_geom = shapely_box(xmin, ymin, xmax, ymax)

    # Spatial filter — geometry (WKT)
    if params.geometry_filter:
        if _HAS_SPATIAL:
            spatial_fn = {
                "intersects": "ST_Intersects",
                "contains": "ST_Contains",
                "within": "ST_Within",
            }.get(params.spatial_rel, "ST_Intersects")
            where_clauses.append(
                f"{spatial_fn}(ST_GeomFromWKB(\"{geom_col}\"), "
                f"ST_GeomFromText('{params.geometry_filter}'))"
            )
        else:
            from shapely import wkt as wkt_mod

            needs_shapely_spatial_filter = True
            shapely_filter_geom = wkt_mod.loads(params.geometry_filter)

    # Attribute filter
    if params.where:
        sanitized = _sanitize_where(params.where)
        where_clauses.append(f"({sanitized})")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Global OID CTE — used by all query paths below
    numbered_cte = """
        WITH numbered AS (
            SELECT (ROW_NUMBER() OVER () - 1)::INTEGER AS __oid, *
            FROM features
        )
    """

    # Count-only shortcut
    if params.return_count_only:
        if needs_shapely_spatial_filter and shapely_filter_geom is not None:
            all_arrow = conn.execute(
                f"{numbered_cte} SELECT \"{geom_col}\" FROM numbered WHERE {where_sql}"
            ).fetch_arrow_table()
            filtered = _apply_shapely_spatial_filter(
                all_arrow, geom_col, shapely_filter_geom, params.spatial_rel
            )
            return QueryResult(count=filtered.num_rows, features=None)
        result = conn.execute(
            f"{numbered_cte} SELECT COUNT(*) as cnt FROM numbered WHERE {where_sql}"
        ).fetchone()
        return QueryResult(count=result[0], features=None)

    # IDs-only — return global OIDs matching the filter
    if params.return_ids_only:
        result_arrow = conn.execute(
            f"{numbered_cte} SELECT __oid FROM numbered WHERE {where_sql}"
        ).fetch_arrow_table()
        return QueryResult(
            features=result_arrow,
            geometry_column=geom_col,
            count=result_arrow.num_rows,
        )

    # objectIds filter — fetch specific features by global OID
    if params.object_ids is not None:
        select_clause = _build_select(params, geom_col, arrow_table.schema)
        oid_list = ",".join(str(int(oid)) for oid in params.object_ids)
        sql = f"""
            {numbered_cte}
            SELECT __oid, {select_clause}
            FROM numbered
            WHERE __oid IN ({oid_list})
        """
        result_arrow = conn.execute(sql).fetch_arrow_table()
        return QueryResult(
            features=result_arrow,
            geometry_column=geom_col,
            count=result_arrow.num_rows,
            exceeded_transfer_limit=False,
        )

    # Build SELECT
    select_clause = _build_select(params, geom_col, arrow_table.schema)

    # Order
    order_sql = ""
    if params.order_by:
        order_sql = f"ORDER BY {_sanitize_order(params.order_by)}"

    # Pagination
    limit_sql = f"LIMIT {int(params.limit)}" if params.limit else ""
    offset_sql = f"OFFSET {int(params.offset)}" if params.offset else ""

    sql = f"""
        {numbered_cte}
        SELECT __oid, {select_clause}
        FROM numbered
        WHERE {where_sql}
        {order_sql}
        {limit_sql}
        {offset_sql}
    """

    result_arrow = conn.execute(sql).fetch_arrow_table()

    # --- Step 4: Apply Shapely spatial filter if DuckDB spatial not available ---
    if needs_shapely_spatial_filter and shapely_filter_geom is not None:
        result_arrow = _apply_shapely_spatial_filter(
            result_arrow, geom_col, shapely_filter_geom, params.spatial_rel
        )

    # --- Step 5: Check if more results exist (exceededTransferLimit) ---
    exceeded = False
    if params.limit and not needs_shapely_spatial_filter:
        count_result = conn.execute(
            f"{numbered_cte} SELECT COUNT(*) FROM numbered WHERE {where_sql}"
        ).fetchone()
        exceeded = count_result[0] > (params.offset or 0) + params.limit
    elif params.limit and needs_shapely_spatial_filter:
        exceeded = result_arrow.num_rows >= params.limit

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
    known_names = {"geometry", "geom", "wkb_geometry", "shape", "location"}

    # Check for known geometry column names with binary type
    for i, field in enumerate(schema):
        if field.name.lower() in known_names and _is_binary_type(field.type):
            return field.name

    # Check for GeoArrow extension type metadata
    for i, field in enumerate(schema):
        metadata = field.metadata
        if metadata:
            for key in metadata:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                if "geo" in key_str.lower() or "arrow" in key_str.lower():
                    return field.name

    # Fallback: first large_binary or binary column
    for field in schema:
        if _is_binary_type(field.type):
            return field.name

    return "geometry"


def _detect_geometry_column_from_iceberg(schema) -> str:
    """Find geometry column from an Iceberg schema."""
    known_names = {"geometry", "geom", "wkb_geometry", "shape", "location"}

    for field in schema.fields:
        field_type = str(field.field_type).lower()
        if field.name.lower() in known_names and "binary" in field_type:
            return field.name

    # Fallback: first binary column
    for field in schema.fields:
        field_type = str(field.field_type).lower()
        if "binary" in field_type:
            return field.name

    return "geometry"


def _detect_id_field(schema) -> str:
    """Detect the ID field from an Iceberg schema."""
    known_id_names = {"objectid", "id", "fid", "gid", "ogc_fid"}
    for field in schema.fields:
        if field.name.lower() in known_id_names:
            return field.name
    # Return first integer field as fallback
    for field in schema.fields:
        field_type = str(field.field_type).lower()
        if "int" in field_type:
            return field.name
    return "objectid"


def _is_binary_type(arrow_type) -> bool:
    """Check if an Arrow type is binary-like."""
    return (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    )


def _sanitize_where(where: str) -> str:
    """
    Sanitize a SQL WHERE clause from user input.

    Uses a conservative allowlist approach:
    - Reject forbidden keywords (DDL, DML)
    - Reject dangerous patterns (comments, semicolons)
    - Allow only safe expressions
    """
    if not where or where.strip() == "":
        return "1=1"

    # Check for forbidden patterns
    if _FORBIDDEN_PATTERNS.search(where):
        raise ValueError(f"Forbidden pattern in WHERE clause: {where}")

    # Check for forbidden keywords
    if _FORBIDDEN_KEYWORDS.search(where):
        raise ValueError(f"Forbidden keyword in WHERE clause: {where}")

    # Check for subqueries
    if re.search(r"\bSELECT\b", where, re.IGNORECASE):
        raise ValueError(f"Subqueries not allowed in WHERE clause: {where}")

    return where


def _sanitize_order(order_by: str) -> str:
    """Sanitize ORDER BY clause. Only allow column names + ASC/DESC."""
    if not order_by:
        return ""

    # Check for forbidden patterns
    if _FORBIDDEN_PATTERNS.search(order_by):
        raise ValueError(f"Forbidden pattern in ORDER BY: {order_by}")

    if _FORBIDDEN_KEYWORDS.search(order_by):
        raise ValueError(f"Forbidden keyword in ORDER BY: {order_by}")

    # Validate format: comma-separated "column_name [ASC|DESC]"
    parts = [p.strip() for p in order_by.split(",")]
    sanitized = []
    for part in parts:
        tokens = part.split()
        if len(tokens) == 0:
            continue
        col_name = tokens[0]
        # Column name must be alphanumeric/underscore
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            raise ValueError(f"Invalid column name in ORDER BY: {col_name}")
        direction = ""
        if len(tokens) > 1:
            direction = tokens[1].upper()
            if direction not in ("ASC", "DESC"):
                raise ValueError(f"Invalid sort direction: {direction}")
        sanitized.append(f'"{col_name}" {direction}'.strip())

    return ", ".join(sanitized)


def _apply_shapely_spatial_filter(
    arrow_table: pa.Table,
    geom_col: str,
    filter_geom,
    spatial_rel: str = "intersects",
) -> pa.Table:
    """
    Apply spatial filtering using Shapely when DuckDB spatial is unavailable.

    This is a fallback for environments where the DuckDB spatial extension
    cannot be installed. Production deployments should use DuckDB spatial.
    """
    if arrow_table.num_rows == 0:
        return arrow_table

    geom_data = arrow_table.column(geom_col).to_pylist()
    keep_indices = []

    spatial_fn = {
        "intersects": lambda g, f: g.intersects(f),
        "contains": lambda g, f: f.contains(g),
        "within": lambda g, f: g.within(f),
    }.get(spatial_rel, lambda g, f: g.intersects(f))

    for i, wkb_bytes in enumerate(geom_data):
        if wkb_bytes is None:
            continue
        geom = wkb.loads(wkb_bytes)
        if spatial_fn(geom, filter_geom):
            keep_indices.append(i)

    if not keep_indices:
        return arrow_table.slice(0, 0)

    return arrow_table.take(keep_indices)


# Internal columns that should never appear in query results
_INTERNAL_COLS = {"__bbox_xmin", "__bbox_ymin", "__bbox_xmax", "__bbox_ymax"}


# Columns that _build_select should never include in the SELECT clause.
# __oid is always added by the query template (SELECT __oid, ...) so must
# not be duplicated. __bbox_* are internal pre-filter columns.
_EXCLUDED_FROM_SELECT = {
    "__oid", "__bbox_xmin", "__bbox_ymin", "__bbox_xmax", "__bbox_ymax",
}


def _build_select(
    params: QueryParams, geom_col: str, schema: pa.Schema
) -> str:
    """Build SELECT clause from requested fields.

    Excludes __oid (already in the query template) and internal
    __bbox_* columns that should never be returned to clients.
    """
    if params.out_fields == "*" or not params.out_fields:
        if not params.return_geometry:
            # Select all columns except geometry and excluded columns
            cols = [
                f'"{f.name}"'
                for f in schema
                if f.name != geom_col and f.name not in _EXCLUDED_FROM_SELECT
            ]
            return ", ".join(cols) if cols else "1 AS _dummy"
        # Select all columns except excluded columns
        cols = [
            f'"{f.name}"'
            for f in schema
            if f.name not in _EXCLUDED_FROM_SELECT
        ]
        return ", ".join(cols) if cols else "1 AS _dummy"

    fields = [f.strip() for f in params.out_fields.split(",")]
    # Quote field names for safety, exclude internal/duplicate columns
    quoted = [f'"{f}"' for f in fields if f not in _EXCLUDED_FROM_SELECT]
    if geom_col not in fields and params.return_geometry:
        quoted.append(f'"{geom_col}"')
    # If only __oid was requested (and we excluded it), return geometry only
    if not quoted and params.return_geometry:
        quoted.append(f'"{geom_col}"')
    if not quoted:
        return "1 AS _dummy"
    return ", ".join(quoted)
