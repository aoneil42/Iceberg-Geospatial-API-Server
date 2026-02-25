"""
Build Esri GeoServices metadata responses from Iceberg table schemas.

These are the relatively static JSON responses for /FeatureServer
and /FeatureServer/{layer_id} — called once when a layer is added
to an ArcGIS map.
"""

from iceberg_geo.query.geometry import ESRI_GEOMETRY_TYPE_MAP
from iceberg_geo.query.models import FeatureSchema


def build_service_metadata(namespace: str, table_names: list[str]) -> dict:
    """Build /FeatureServer response."""
    layers = []
    for i, name in enumerate(table_names):
        layers.append(
            {
                "id": i,
                "name": name,
                "type": "Feature Layer",
                "geometryType": "esriGeometryPolygon",
            }
        )

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

    fields = [
        {
            "name": schema.id_field,
            "type": "esriFieldTypeOID",
            "alias": schema.id_field,
            "sqlType": "sqlTypeInteger",
        }
    ]

    for f in schema.fields:
        if f["name"] == schema.id_field:
            continue
        fields.append(
            {
                "name": f["name"],
                "type": type_map.get(f["type"], "esriFieldTypeString"),
                "alias": f.get("alias", f["name"]),
            }
        )

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
            "supportsStatistics": False,
        },
        "hasAttachments": False,
        "htmlPopupType": "esriServerHTMLPopupTypeAsHTMLText",
    }
