"""
Serialize QueryResult -> GeoJSON FeatureCollection.

Used when f=geojson is requested from the GeoServices endpoint.
"""

from iceberg_geo.query.geometry import wkb_to_geojson
from iceberg_geo.query.models import QueryResult


def serialize(result: QueryResult) -> dict:
    """Convert QueryResult to GeoJSON FeatureCollection."""

    if result.features is None or result.features.num_rows == 0:
        return {
            "type": "FeatureCollection",
            "features": [],
        }

    table_dict = result.features.to_pydict()
    geom_col = result.geometry_column
    features = []

    for i in range(result.features.num_rows):
        properties = {}
        geometry = None

        for col_name in table_dict:
            if col_name == geom_col:
                wkb_bytes = table_dict[col_name][i]
                if wkb_bytes:
                    geometry = wkb_to_geojson(wkb_bytes)
            else:
                val = table_dict[col_name][i]
                properties[col_name] = _to_json_safe(val)

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": properties,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def _to_json_safe(val):
    """Convert pyarrow scalar values to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
