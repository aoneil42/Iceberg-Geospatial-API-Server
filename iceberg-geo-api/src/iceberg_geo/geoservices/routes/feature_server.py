"""
FeatureServer routes.

Implements the subset of Esri GeoServices REST that ArcGIS clients
need for map visualization.

ArcGIS clients send query parameters via:
- GET with URL query parameters
- POST with application/x-www-form-urlencoded body

Both must be handled. _get_query_params() merges both sources.
"""

import json
import logging

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from iceberg_geo.query.catalog import get_table, list_tables
from iceberg_geo.query.engine import get_table_schema, query_features
from iceberg_geo.query.models import QueryParams

from ..html import (
    render_feature_server,
    render_layer,
    render_query_form,
    render_query_results,
)
from ..metadata import build_layer_metadata, build_service_metadata
from ..serializers import esri_json, esri_pbf, geojson

logger = logging.getLogger(__name__)
router = APIRouter()


async def _get_query_params(request: Request) -> dict:
    """Merge query string and form body params.

    ArcGIS Pro sends POST with form-encoded body for query requests.
    Query string params take precedence over form body.
    """
    params = dict(request.query_params)

    # For POST requests, also parse the form body
    if request.method == "POST":
        content_type = request.headers.get("content-type", "")
        if "form" in content_type or "urlencoded" in content_type:
            try:
                form_data = await request.form()
                # Form params fill in anything not already in query string
                for key, value in form_data.items():
                    if key not in params:
                        params[key] = value
            except Exception as e:
                logger.warning("Failed to parse form body: %s", e)

    return params


def _base_url(request: Request) -> str:
    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "localhost")
    proto = request.headers.get("x-forwarded-proto", "http")
    root = request.scope.get("root_path", "")
    return f"{proto}://{host}{root}"


def _wants_html(request: Request, f: str | None) -> bool:
    if f == "html":
        return True
    if f and f != "html":
        return False
    accept = request.headers.get("accept", "")
    return "text/html" in accept


@router.get("/{service_id}/FeatureServer")
@router.post("/{service_id}/FeatureServer")
async def feature_server_info(request: Request, service_id: str, f: str = None):
    """
    Service-level metadata.

    ArcGIS clients call this to discover layers, spatial reference,
    and capabilities.
    """
    tables = list_tables(service_id)
    schemas = []
    for table_name in tables:
        table = get_table(service_id, table_name)
        schemas.append(get_table_schema(table))
    metadata = build_service_metadata(service_id, tables, schemas)

    if _wants_html(request, f):
        return HTMLResponse(
            render_feature_server(_base_url(request), service_id, metadata)
        )

    return metadata


@router.get("/{service_id}/FeatureServer/{layer_id}")
@router.post("/{service_id}/FeatureServer/{layer_id}")
async def layer_info(request: Request, service_id: str, layer_id: int, f: str = None):
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

    if _wants_html(request, f):
        return HTMLResponse(
            render_layer(_base_url(request), service_id, layer_id, metadata)
        )

    return metadata


@router.get("/{service_id}/FeatureServer/{layer_id}/query")
@router.post("/{service_id}/FeatureServer/{layer_id}/query")
async def query_layer(request: Request, service_id: str, layer_id: int):
    """
    Feature query — the workhorse endpoint.

    Translates GeoServices query params to shared QueryParams,
    executes via the Iceberg Query Service, then serializes
    to the requested format.

    Reads parameters from both URL query string and POST form body
    (ArcPro uses both depending on the request).
    """
    # Merge query string + form body params
    p = await _get_query_params(request)

    def _str(key, default=None):
        return p.get(key, default)

    def _bool(key, default=False):
        val = p.get(key)
        if val is None:
            return default
        if isinstance(val, bool):
            return val
        return str(val).lower() in ("true", "1", "yes")

    def _int(key, default=None):
        val = p.get(key)
        if val is None:
            return default
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def _float(key, default=None):
        val = p.get(key)
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    f = _str("f")
    where = _str("where", "1=1")
    objectIds = _str("objectIds")
    geometry_param = _str("geometry")
    geometryType = _str("geometryType", "esriGeometryEnvelope")
    spatialRel = _str("spatialRel", "esriSpatialRelIntersects")
    outFields = _str("outFields", "*")
    outSR = _str("outSR")
    returnGeometry = _bool("returnGeometry", True)
    returnCountOnly = _bool("returnCountOnly", False)
    returnIdsOnly = _bool("returnIdsOnly", False)
    resultOffset = _int("resultOffset", 0)
    resultRecordCount = _int("resultRecordCount")
    orderByFields = _str("orderByFields")
    maxAllowableOffset = _float("maxAllowableOffset")

    # Show query form if browser navigates directly (no query params submitted)
    if _wants_html(request, f) and not p.get("where"):
        tables = list_tables(service_id)
        layer_name = tables[layer_id]
        return HTMLResponse(
            render_query_form(
                _base_url(request), service_id, layer_id, layer_name
            )
        )

    # Resolve table
    tables = list_tables(service_id)
    table_name = tables[layer_id]
    table = get_table(service_id, table_name)
    schema = get_table_schema(table)

    # Parse spatial reference parameters (ArcPro sends JSON objects, not integers)
    parsed_out_sr = _parse_spatial_ref(outSR)

    # Parse geometry filter
    bbox = None
    geometry_wkt = None
    if geometry_param:
        bbox, geometry_wkt = _parse_esri_geometry(geometry_param, geometryType)

    # Map GeoServices spatial rel to engine spatial rel
    spatial_rel_map = {
        "esriSpatialRelIntersects": "intersects",
        "esriSpatialRelEnvelopeIntersects": "intersects",
        "esriSpatialRelContains": "contains",
        "esriSpatialRelWithin": "within",
    }

    # Parse objectIds if provided
    parsed_object_ids = None
    if objectIds:
        parsed_object_ids = [int(x.strip()) for x in objectIds.split(",") if x.strip()]

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
        object_ids=parsed_object_ids,
        limit=resultRecordCount or schema.max_record_count,
        offset=resultOffset,
        order_by=orderByFields,
        out_sr=parsed_out_sr,
    )

    # Execute query
    result = query_features(table, params)

    # HTML table rendering
    if f == "html":
        result_dict = esri_json.serialize(result, schema)
        return HTMLResponse(
            render_query_results(
                _base_url(request),
                service_id,
                layer_id,
                table_name,
                result_dict,
            )
        )

    # Serialize based on requested format
    if f == "pbf":
        pbf_bytes = esri_pbf.serialize(
            result, schema, max_allowable_offset=maxAllowableOffset
        )
        return Response(
            content=pbf_bytes,
            media_type="application/x-protobuf",
        )
    elif f == "geojson":
        return geojson.serialize(result)
    else:
        return esri_json.serialize(result, schema)


def _parse_spatial_ref(sr: str | None) -> int | None:
    """
    Parse a spatial reference parameter from Esri clients.

    ArcGIS Pro sends outSR/inSR as a JSON spatial reference object like:
      {"wkid":4326,"latestWkid":4326,"xyTolerance":...}
    Plain WKID integers (e.g. "4326") are also accepted.

    Returns the WKID as an integer, or None.
    """
    if sr is None:
        return None
    # Try plain integer first
    try:
        return int(sr)
    except (ValueError, TypeError):
        pass
    # Try JSON spatial reference object
    try:
        obj = json.loads(sr)
        if isinstance(obj, dict):
            return int(obj.get("latestWkid") or obj.get("wkid") or 4326)
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _parse_esri_geometry(geometry_str: str, geometry_type: str):
    """
    Parse an Esri geometry parameter into bbox and/or WKT.

    Handles:
    - Envelope: {"xmin":..., "ymin":..., "xmax":..., "ymax":...}
    - Point: {"x":..., "y":...}
    - Polygon: {"rings": [...]}
    - Plain bbox string: "xmin,ymin,xmax,ymax"

    Returns (bbox_tuple, wkt_string).
    """
    from shapely.geometry import Point, Polygon

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
        pt = Point(geom["x"], geom["y"])
        return None, pt.wkt

    # Esri polygon (rings)
    if "rings" in geom:
        poly = Polygon(geom["rings"][0])
        return None, poly.wkt

    raise ValueError(f"Unsupported geometry type: {geometry_type}")
