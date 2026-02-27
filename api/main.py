"""FastAPI backend for live spatial queries against the Iceberg lakehouse."""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading

import duckdb
import urllib.request
from fastapi import FastAPI, File, Form, Query, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(docs_url="/api/docs", openapi_url="/api/openapi.json")

CATALOG_URL = os.environ.get("CATALOG_URL", "http://lakekeeper:8181/catalog")

_conn: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()
_catalog_prefix: str | None = None

_VALID_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_catalog_prefix() -> str:
    """Discover the warehouse prefix from LakeKeeper's /v1/config endpoint."""
    global _catalog_prefix
    if _catalog_prefix:
        return _catalog_prefix
    url = f"{CATALOG_URL}/v1/config?warehouse=lakehouse"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    _catalog_prefix = data["defaults"]["prefix"]
    return _catalog_prefix


def _get_metadata_location(namespace: str, layer: str) -> str:
    """Query LakeKeeper REST API for the latest metadata location of a table."""
    prefix = _get_catalog_prefix()
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces/{namespace}/tables/{layer}"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return data["metadata-location"]


def _init_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL spatial; LOAD spatial;")

    key_id = os.environ["GARAGE_KEY_ID"]
    secret = os.environ["GARAGE_SECRET_KEY"]

    conn.execute(
        f"""
        CREATE SECRET garage_s3 (
            TYPE S3,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            REGION 'garage',
            ENDPOINT 'garage:3900',
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )

    return conn


@app.on_event("startup")
def startup() -> None:
    global _conn
    _conn = _init_connection()


# ---------------------------------------------------------------------------
# Namespace / table discovery
# ---------------------------------------------------------------------------


@app.get("/api/namespaces")
def list_namespaces() -> list[str]:
    """List available Iceberg namespaces."""
    prefix = _get_catalog_prefix()
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return [ns[0] if isinstance(ns, list) else ns for ns in data["namespaces"]]


@app.get("/api/tables/{namespace}")
def list_tables(namespace: str) -> list[str]:
    """List tables in a namespace."""
    if not _VALID_NAME.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    prefix = _get_catalog_prefix()
    url = f"{CATALOG_URL}/v1/{prefix}/namespaces/{namespace}/tables"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer dummy"})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return [
        ident.get("name", ident[-1] if isinstance(ident, list) else str(ident))
        for ident in data.get("identifiers", [])
    ]


# ---------------------------------------------------------------------------
# Bounding box
# ---------------------------------------------------------------------------


def _compute_bbox(source: str) -> tuple[float, float, float, float] | None:
    """Compute bounding box for a table source using MIN/MAX.

    Note: ST_Extent() is buggy with iceberg_scan in DuckDB — it returns a
    single-point bbox instead of the full extent.  The MIN/MAX approach on
    individual geometries works correctly.
    """
    sql = (
        f"SELECT MIN(ST_XMin(g)), MIN(ST_YMin(g)), "
        f"MAX(ST_XMax(g)), MAX(ST_YMax(g)) "
        f"FROM (SELECT ST_GeomFromWKB(geometry) AS g FROM {source})"
    )
    with _lock:
        row = _conn.execute(sql).fetchone()  # type: ignore[union-attr]
    if row and row[0] is not None:
        return (row[0], row[1], row[2], row[3])
    return None


@app.get("/api/bbox/{namespace}")
def get_bbox(namespace: str) -> dict:
    """Get the aggregate bounding box for all geometry in a namespace."""
    if not _VALID_NAME.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )

    tables = list_tables(namespace)
    extents: list[tuple[float, float, float, float]] = []
    for table_name in tables:
        try:
            metadata_loc = _get_metadata_location(namespace, table_name)
        except Exception:
            continue

        source = f"iceberg_scan('{metadata_loc}')"
        try:
            ext = _compute_bbox(source)
            if ext:
                extents.append(ext)
        except Exception:
            continue

    if not extents:
        return JSONResponse(
            status_code=404,
            content={"error": f"No data found for namespace {namespace}"},
        )

    bbox = [
        min(e[0] for e in extents),
        min(e[1] for e in extents),
        max(e[2] for e in extents),
        max(e[3] for e in extents),
    ]
    return {"bbox": bbox}


@app.get("/api/bbox/{namespace}/{table_name}")
def get_table_bbox(namespace: str, table_name: str) -> dict:
    """Get the bounding box for a single table."""
    if not _VALID_NAME.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(table_name):
        return JSONResponse(
            status_code=400, content={"error": "Invalid table name"}
        )

    try:
        metadata_loc = _get_metadata_location(namespace, table_name)
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Catalog lookup failed: {e}"},
        )

    source = f"iceberg_scan('{metadata_loc}')"
    try:
        ext = _compute_bbox(source)
        if ext:
            return {"bbox": list(ext)}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Extent query failed: {e}"},
        )

    return JSONResponse(
        status_code=404,
        content={"error": f"No data found for {namespace}.{table_name}"},
    )


# ---------------------------------------------------------------------------
# Feature queries
# ---------------------------------------------------------------------------


@app.get("/api/features/{namespace}/{layer}")
def get_features(
    namespace: str,
    layer: str,
    bbox: str | None = Query(default=None, description="minx,miny,maxx,maxy"),
    limit: int | None = Query(default=None, ge=1),
) -> Response:
    if not _VALID_NAME.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(layer):
        return JSONResponse(
            status_code=400, content={"error": "Invalid layer name"}
        )

    # Discover the latest metadata location from LakeKeeper
    try:
        metadata_loc = _get_metadata_location(namespace, layer)
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": f"Catalog lookup failed: {e}"},
        )

    source = f"iceberg_scan('{metadata_loc}')"

    # Introspect columns: detect geometry type and flatten STRUCTs so that
    # the output GeoParquet only has flat (non-nested) columns — GeoArrow
    # deck.gl layers don't handle nested structs well.
    with _lock:
        cols_info = _conn.execute(  # type: ignore[union-attr]
            f"SELECT column_name, column_type "
            f"FROM (DESCRIBE SELECT * FROM {source} LIMIT 0)"
        ).fetchall()

    col_map: dict[str, str] = {c[0]: c[1] for c in cols_info}
    geom_col_type = col_map.get("geometry", "BLOB").upper()

    # Build geometry expression based on actual column type
    if "GEOMETRY" in geom_col_type:
        geom_expr = "ST_AsWKB(geometry) AS geometry"
        geom_from = "ST_GeomFromWKB(ST_AsWKB(geometry))"
    else:
        geom_expr = "ST_GeomFromWKB(geometry) AS geometry"
        geom_from = "ST_GeomFromWKB(geometry)"

    # Build column list, flattening any STRUCT columns into their fields.
    # DuckDB's "col.*" expands a STRUCT into its child columns.
    # GeoArrow / deck.gl layers don't handle nested Arrow structs.
    select_parts = [geom_expr]
    for cname, ctype in cols_info:
        if cname == "geometry":
            continue
        if ctype.upper().startswith("STRUCT"):
            select_parts.append(f"{cname}.*")
        else:
            select_parts.append(cname)

    conditions: list[str] = []

    if bbox:
        parts = bbox.split(",")
        if len(parts) != 4:
            return JSONResponse(
                status_code=400,
                content={"error": "bbox must be minx,miny,maxx,maxy"},
            )
        minx, miny, maxx, maxy = (float(p) for p in parts)
        conditions.append(
            f"ST_Intersects({geom_from}, "
            f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
        )

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = f" LIMIT {limit}" if limit else ""

    # Put geometry first (matches the column order readGeoParquet expects),
    # and convert WKB binary → DuckDB GEOMETRY so COPY TO writes GeoParquet.
    sql = f"SELECT {', '.join(select_parts)} FROM {source}{where}{limit_clause}"

    # Use DuckDB's native Parquet writer (produces correct GeoParquet encoding)
    fd, tmppath = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        with _lock:
            _conn.execute(  # type: ignore[union-attr]
                f"COPY ({sql}) TO '{tmppath}' (FORMAT PARQUET)"
            )
        with open(tmppath, "rb") as f:
            data = f.read()
    finally:
        os.unlink(tmppath)

    return Response(
        content=data,
        media_type="application/x-parquet",
    )


# ---------------------------------------------------------------------------
# Upload — ingest GeoJSON or GeoParquet into the Iceberg lakehouse
# ---------------------------------------------------------------------------


_pyiceberg_catalog = None
_pyiceberg_lock = threading.Lock()


def _get_pyiceberg_catalog():
    """Lazy-init a PyIceberg REST catalog connection."""
    global _pyiceberg_catalog
    if _pyiceberg_catalog is not None:
        return _pyiceberg_catalog

    from pyiceberg.catalog import load_catalog

    _pyiceberg_catalog = load_catalog(
        "rest",
        **{
            "uri": CATALOG_URL,
            "warehouse": "lakehouse",
            "token": "dummy",
            "s3.access-key-id": os.environ["GARAGE_KEY_ID"],
            "s3.secret-access-key": os.environ["GARAGE_SECRET_KEY"],
            "s3.endpoint": "http://garage:3900",
            "s3.region": "garage",
            "s3.path-style-access": "true",
            "s3.remote-signing-enabled": "false",
        },
    )
    return _pyiceberg_catalog


def _detect_geom_column_geoparquet(tmp_path: str) -> tuple[str, str]:
    """
    Detect geometry column name and encoding from GeoParquet metadata.

    Returns (column_name, encoding) — encoding is typically "WKB".
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(tmp_path)
    raw_meta = pf.schema_arrow.metadata or {}
    geo_meta = json.loads(raw_meta.get(b"geo", b"{}"))
    geom_col = geo_meta.get("primary_column", "geometry")

    column_meta = geo_meta.get("columns", {}).get(geom_col, {})
    encoding = column_meta.get("encoding", "WKB")

    return geom_col, encoding


# ---------------------------------------------------------------------------
# Upload form — HTML UI for file uploads
# ---------------------------------------------------------------------------

_UPLOAD_FORM_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Upload to Lakehouse</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #f5f5f5; display: flex; justify-content: center; padding: 40px 16px; }
  .card { background: #fff; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,0.1);
          padding: 32px; max-width: 520px; width: 100%; }
  h1 { font-size: 20px; color: #333; margin-bottom: 4px; }
  .subtitle { font-size: 13px; color: #888; margin-bottom: 24px; }
  label { display: block; font-size: 13px; font-weight: 600; color: #555; margin-bottom: 4px; }
  input[type="text"], select { width: 100%; padding: 8px 10px; border: 1px solid #ccc;
    border-radius: 6px; font-size: 14px; margin-bottom: 16px; }
  input[type="text"]:focus, select:focus { outline: none; border-color: #1e90ff;
    box-shadow: 0 0 0 2px rgba(30,144,255,0.15); }
  .file-area { border: 2px dashed #ccc; border-radius: 8px; padding: 24px; text-align: center;
    margin-bottom: 16px; cursor: pointer; transition: border-color 0.2s; }
  .file-area:hover, .file-area.dragover { border-color: #1e90ff; background: #f0f8ff; }
  .file-area input { display: none; }
  .file-area p { color: #888; font-size: 14px; }
  .file-area .selected { color: #333; font-weight: 500; }
  .check-row { display: flex; align-items: center; gap: 8px; margin-bottom: 20px; }
  .check-row input { cursor: pointer; }
  .check-row label { margin: 0; font-weight: 400; cursor: pointer; }
  button { width: 100%; padding: 10px; background: #1e90ff; color: #fff; border: none;
    border-radius: 6px; font-size: 15px; font-weight: 600; cursor: pointer;
    transition: background 0.2s; }
  button:hover { background: #0b7dda; }
  button:disabled { background: #aaa; cursor: not-allowed; }
  .result { margin-top: 16px; padding: 12px; border-radius: 6px; font-size: 13px;
    white-space: pre-wrap; word-break: break-all; }
  .result.ok { background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; }
  .result.err { background: #fbe9e7; color: #c62828; border: 1px solid #ef9a9a; }
  .home-link { display: inline-block; margin-top: 16px; font-size: 13px; color: #1e90ff; }
</style>
</head>
<body>
<div class="card">
  <h1>Upload to Lakehouse</h1>
  <p class="subtitle">Ingest GeoJSON or GeoParquet into the Iceberg catalog</p>

  <label for="namespace">Namespace</label>
  <input type="text" id="namespace" placeholder="e.g. colorado" required />

  <label for="table_name">Table name</label>
  <input type="text" id="table_name" placeholder="e.g. buildings" required />

  <div class="file-area" id="drop-zone">
    <input type="file" id="files" multiple accept=".geojson,.json,.parquet,.geoparquet" />
    <p id="file-label">Drop files here or <u>browse</u><br>
       <span style="font-size:12px;color:#aaa">.geojson &middot; .parquet &middot; .geoparquet</span></p>
  </div>

  <div class="check-row">
    <input type="checkbox" id="append" />
    <label for="append">Append to existing table</label>
  </div>

  <button id="submit-btn" type="button">Upload</button>
  <div id="result"></div>
  <a class="home-link" href="/">&larr; Back to map</a>
</div>
<script>
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("files");
const fileLabel = document.getElementById("file-label");

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("dragover", e => { e.preventDefault(); dropZone.classList.add("dragover"); });
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
dropZone.addEventListener("drop", e => {
  e.preventDefault(); dropZone.classList.remove("dragover");
  fileInput.files = e.dataTransfer.files;
  showSelected();
});
fileInput.addEventListener("change", showSelected);

function showSelected() {
  const n = fileInput.files.length;
  if (n === 0) { fileLabel.innerHTML = 'Drop files here or <u>browse</u>'; return; }
  const names = Array.from(fileInput.files).map(f => f.name).join(", ");
  fileLabel.innerHTML = `<span class="selected">${n} file${n>1?"s":""}: ${names}</span>`;
}

document.getElementById("submit-btn").addEventListener("click", async () => {
  const ns = document.getElementById("namespace").value.trim();
  const tn = document.getElementById("table_name").value.trim();
  const ap = document.getElementById("append").checked;
  const fl = fileInput.files;
  const res = document.getElementById("result");
  const btn = document.getElementById("submit-btn");

  if (!ns || !tn) { res.className="result err"; res.textContent="Namespace and table name are required."; return; }
  if (!fl.length) { res.className="result err"; res.textContent="Select at least one file."; return; }

  const form = new FormData();
  for (const f of fl) form.append("files", f);

  const params = new URLSearchParams({ namespace: ns, table_name: tn, append: ap });
  btn.disabled = true; btn.textContent = "Uploading...";
  res.className = ""; res.textContent = "";

  try {
    const resp = await fetch(`/api/upload?${params}`, { method: "POST", body: form });
    const data = await resp.json();
    if (resp.ok) {
      res.className = "result ok";
      res.textContent = `${data.created ? "Created" : "Appended to"} ${ns}.${tn}\\n`
        + `${data.rows.toLocaleString()} rows from ${data.files_processed} file(s)\\n`
        + `Columns: ${data.columns.join(", ")}`;
    } else {
      res.className = "result err";
      res.textContent = data.error || JSON.stringify(data);
    }
  } catch (e) {
    res.className = "result err";
    res.textContent = "Request failed: " + e.message;
  } finally {
    btn.disabled = false; btn.textContent = "Upload";
  }
});
</script>
</body>
</html>
"""


@app.get("/api/upload", response_class=HTMLResponse, include_in_schema=False)
def upload_form():
    """Serve the upload UI form."""
    return _UPLOAD_FORM_HTML


@app.post("/api/upload")
async def upload_dataset(
    namespace: str = Query(..., description="Iceberg namespace (created if missing)"),
    table_name: str = Query(..., description="Table name within the namespace"),
    append: bool = Query(default=False, description="Append to existing table"),
    files: list[UploadFile] = File(
        ..., description="GeoJSON or GeoParquet files to upload"
    ),
) -> dict:
    """
    Upload one or more GeoJSON / GeoParquet files into an Iceberg table.

    Auto-detects file format, schema, and geometry column.
    Creates the namespace and table if they do not already exist.
    """
    # --- Validate names ---
    if not _VALID_NAME.match(namespace):
        return JSONResponse(
            status_code=400, content={"error": "Invalid namespace name"}
        )
    if not _VALID_NAME.match(table_name):
        return JSONResponse(
            status_code=400, content={"error": "Invalid table name"}
        )

    if not files:
        return JSONResponse(
            status_code=400, content={"error": "No files provided"}
        )

    # --- Read all uploaded files into Arrow tables ---
    import pyarrow as pa

    arrow_tables: list[pa.Table] = []

    for upload_file in files:
        filename = (upload_file.filename or "").lower()
        if filename.endswith((".geojson", ".json")):
            fmt = "geojson"
        elif filename.endswith((".parquet", ".geoparquet")):
            fmt = "geoparquet"
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        f"Unsupported file: {upload_file.filename}. "
                        "Upload .geojson or .parquet/.geoparquet files."
                    )
                },
            )

        suffix = ".geojson" if fmt == "geojson" else ".parquet"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "wb") as tmp:
                while chunk := await upload_file.read(1024 * 1024):
                    tmp.write(chunk)
            arrow_tables.append(_read_upload(tmp_path, fmt))
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # Concatenate all tables (they must share the same schema)
    if len(arrow_tables) == 1:
        combined = arrow_tables[0]
    else:
        try:
            combined = pa.concat_tables(arrow_tables, promote_options="default")
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        f"Schema mismatch across uploaded files: {e}. "
                        "All files must share the same schema."
                    )
                },
            )

    # --- Write to Iceberg via PyIceberg ---
    catalog = _get_pyiceberg_catalog()

    # Create namespace if needed
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # already exists

    # Create or load the Iceberg table
    table_id = f"{namespace}.{table_name}"
    created = False
    try:
        ice_table = catalog.create_table(table_id, schema=combined.schema)
        created = True
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "alreadyexists" in msg:
            if not append:
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": (
                            f"Table {table_id} already exists. "
                            "Set append=true to add data to it."
                        )
                    },
                )
            # Load existing table for append
            ice_table = catalog.load_table(table_id)
        else:
            return JSONResponse(
                status_code=500,
                content={"error": f"Failed to create table: {e}"},
            )

    # Strip remote signing (Garage incompatibility)
    if ice_table.io.properties.get("s3.remote-signing-enabled") == "true":
        ice_table.io.properties["s3.remote-signing-enabled"] = "false"
        ice_table.io.properties.pop("s3.signer", None)
        ice_table.io.properties.pop("s3.signer.endpoint", None)
        ice_table.io.properties.pop("s3.signer.uri", None)

    # Append data
    ice_table.append(combined)

    # Invalidate the catalog prefix cache so new tables are immediately visible
    global _catalog_prefix
    _catalog_prefix = None

    return {
        "status": "ok",
        "namespace": namespace,
        "table": table_name,
        "created": created,
        "rows": combined.num_rows,
        "files_processed": len(files),
        "columns": [f.name for f in combined.schema],
        "schema": {f.name: str(f.type) for f in combined.schema},
    }


def _read_upload(tmp_path: str, fmt: str):
    """
    Read an uploaded GeoJSON or GeoParquet into a normalised Arrow table.

    Returns a PyArrow Table with:
      - ``geometry`` column as WKB binary (first column)
      - All other columns preserved
    """
    import pyarrow as pa

    if fmt == "geojson":
        return _read_geojson(tmp_path)
    else:
        return _read_geoparquet(tmp_path)


def _read_geojson(tmp_path: str):
    """Read GeoJSON via DuckDB ST_Read → Arrow table with WKB geometry."""
    with _lock:
        _conn.execute("DROP TABLE IF EXISTS __upload")  # type: ignore[union-attr]
        _conn.execute(  # type: ignore[union-attr]
            f"CREATE TEMP TABLE __upload AS "
            f"SELECT * FROM ST_Read('{tmp_path}')"
        )
        # ST_Read produces a 'geom' column of type GEOMETRY
        arrow_table = _conn.execute(  # type: ignore[union-attr]
            "SELECT ST_AsWKB(geom) AS geometry, "
            "* EXCLUDE (geom) FROM __upload"
        ).fetch_arrow_table()
        _conn.execute("DROP TABLE IF EXISTS __upload")  # type: ignore[union-attr]
    return arrow_table


def _read_geoparquet(tmp_path: str):
    """Read GeoParquet via DuckDB → Arrow table with WKB geometry."""
    geom_col, encoding = _detect_geom_column_geoparquet(tmp_path)

    with _lock:
        _conn.execute("DROP TABLE IF EXISTS __upload")  # type: ignore[union-attr]
        _conn.execute(  # type: ignore[union-attr]
            f"CREATE TEMP TABLE __upload AS "
            f"SELECT * FROM read_parquet('{tmp_path}')"
        )

        # Build the SELECT: convert geometry to WKB, keep everything else
        cols = _conn.execute(  # type: ignore[union-attr]
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = '__upload' ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        col_types = {c[0]: c[1] for c in cols}

        if geom_col in col_names:
            dtype = col_types[geom_col].upper()

            if dtype == "GEOMETRY":
                # DuckDB auto-parsed GeoParquet → already GEOMETRY type
                geom_expr = f"ST_AsWKB({geom_col}) AS geometry"
            elif encoding.upper() == "WKT" or dtype == "VARCHAR":
                geom_expr = (
                    f"ST_AsWKB(ST_GeomFromText({geom_col})) AS geometry"
                )
            else:
                # Raw WKB binary (BLOB)
                geom_expr = (
                    f"ST_AsWKB(ST_GeomFromWKB({geom_col})) AS geometry"
                )

            other_cols = [c for c in col_names if c != geom_col]
            select = ", ".join([geom_expr] + other_cols)
        else:
            # No geometry detected — pass through as-is
            select = "*"

        arrow_table = _conn.execute(  # type: ignore[union-attr]
            f"SELECT {select} FROM __upload"
        ).fetch_arrow_table()
        _conn.execute("DROP TABLE IF EXISTS __upload")  # type: ignore[union-attr]

    return arrow_table


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
