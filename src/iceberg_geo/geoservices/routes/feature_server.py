"""
FeatureServer routes.

Implements the subset of Esri GeoServices REST that ArcGIS clients
need for map visualization.
"""

import json

from fastapi import APIRouter, Request, Response

from iceberg_geo.query.catalog import get_table, list_tables
from iceberg_geo.query.engine import get_table_schema, query_features
from iceberg_geo.query.models import QueryParams

from ..metadata import build_layer_metadata, build_service_metadata
from ..serializers import esri_json, esri_pbf, geojson

router = APIRouter()


@router.get("/{service_id}/FeatureServer")
@router.post("/{service_id}/FeatureServer")
async def feature_server_info(service_id: str, f: str = "json"):
    """
    Service-level metadata.

    ArcGIS clients call this to discover layers, spatial reference,
    and capabilities.
    """
    tables = list_tables(service_id)
    metadata = build_service_metadata(service_id, tables)
    return metadata


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
    f: str = "json",
):
    """
    Feature query — the workhorse endpoint.

    Translates GeoServices query params to shared QueryParams,
    executes via the Iceberg Query Service, then serializes
    to the requested format.
    """
    # Resolve table
    tables = list_tables(service_id)
    table_name = tables[layer_id]
    table = get_table(service_id, table_name)
    schema = get_table_schema(table)

    # Parse geometry filter
    bbox = None
    geometry_wkt = None
    if geometry:
        bbox, geometry_wkt = _parse_esri_geometry(geometry, geometryType)

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
    else:
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
