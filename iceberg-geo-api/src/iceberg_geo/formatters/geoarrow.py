"""
GeoArrow / Arrow IPC output formatter for pygeoapi.

Registers media type: application/vnd.apache.arrow.stream
Clients request via Accept header or ?f=arrow

This avoids the expensive GeoJSON serialization path entirely —
data goes from Iceberg (Parquet/Arrow) through DuckDB (Arrow)
to the client as Arrow IPC with GeoArrow geometry encoding.
"""

from io import BytesIO

import pyarrow as pa
import pyarrow.ipc as ipc


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
    features = geojson_result.get("features", [])
    if not features:
        return pa.table({})

    # Collect all property keys from first feature
    sample = features[0]
    prop_keys = list(sample.get("properties", {}).keys())

    columns = {key: [] for key in prop_keys}
    geometries = []

    for feature in features:
        props = feature.get("properties", {})
        for key in prop_keys:
            columns[key].append(props.get(key))

        geom = feature.get("geometry")
        if geom:
            from shapely.geometry import shape
            from shapely import wkb as wkb_mod

            geom_obj = shape(geom)
            geometries.append(wkb_mod.dumps(geom_obj))
        else:
            geometries.append(None)

    arrays = {}
    for key, values in columns.items():
        arrays[key] = pa.array(values)

    arrays["geometry"] = pa.array(geometries, type=pa.large_binary())

    return pa.table(arrays)
