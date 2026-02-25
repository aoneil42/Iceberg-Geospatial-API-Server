"""
Serialize QueryResult -> Esri JSON response.

Esri JSON is the native JSON format for ArcGIS Feature Services.
It differs from GeoJSON in geometry representation:
- Polygons use {"rings": [[[x,y],...], ...]}
- Polylines use {"paths": [[[x,y],...], ...]}
- Points use {"x": val, "y": val}
- SpatialReference is an object: {"wkid": 4326}
"""

from shapely import wkb

from iceberg_geo.query.geometry import ESRI_GEOMETRY_TYPE_MAP
from iceberg_geo.query.models import FeatureSchema, QueryResult


def serialize(result: QueryResult, schema: FeatureSchema) -> dict:
    """Convert QueryResult to Esri JSON FeatureSet response."""

    if result.features is None:
        return {"count": result.count}

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

        features.append(
            {
                "attributes": attributes,
                "geometry": geometry,
            }
        )

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
        fields.append(
            {
                "name": f["name"],
                "type": esri_type,
                "alias": f.get("alias", f["name"]),
            }
        )
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
