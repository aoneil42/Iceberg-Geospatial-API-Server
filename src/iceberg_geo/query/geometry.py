"""
Geometry conversion and coordinate utilities.

Handles:
- WKB <-> Shapely geometry conversion
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


def wkb_to_shapely(wkb_bytes: bytes):
    """Decode WKB to Shapely geometry."""
    return wkb.loads(wkb_bytes)


def wkb_to_coords(wkb_bytes: bytes) -> dict:
    """
    Decode WKB to raw coordinate arrays for PBF encoding.

    Returns dict with 'type' and coordinate arrays appropriate
    for the geometry type.
    """
    geom = wkb.loads(wkb_bytes)
    geom_type = geom.geom_type

    if geom_type == "Point":
        return {
            "type": "Point",
            "coordinates": (geom.x, geom.y),
        }
    elif geom_type == "MultiPoint":
        return {
            "type": "MultiPoint",
            "coordinates": [(p.x, p.y) for p in geom.geoms],
        }
    elif geom_type == "LineString":
        return {
            "type": "LineString",
            "paths": [list(geom.coords)],
        }
    elif geom_type == "MultiLineString":
        return {
            "type": "MultiLineString",
            "paths": [list(line.coords) for line in geom.geoms],
        }
    elif geom_type == "Polygon":
        rings = [list(geom.exterior.coords)]
        for interior in geom.interiors:
            rings.append(list(interior.coords))
        return {
            "type": "Polygon",
            "rings": rings,
        }
    elif geom_type == "MultiPolygon":
        rings = []
        for poly in geom.geoms:
            rings.append(list(poly.exterior.coords))
            for interior in poly.interiors:
                rings.append(list(interior.coords))
        return {
            "type": "MultiPolygon",
            "rings": rings,
        }

    return {"type": geom_type, "coordinates": []}


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
    transformed = []
    for coord in coords:
        if isinstance(coord, (list, tuple)) and len(coord) >= 2:
            if isinstance(coord[0], (list, tuple)):
                # Nested coordinate arrays (rings/paths)
                transformed.append(transform_coords(coord, from_srid, to_srid))
            else:
                x, y = transformer.transform(coord[0], coord[1])
                transformed.append((x, y))
        else:
            transformed.append(coord)
    return transformed


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
