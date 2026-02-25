"""Tests for the GeoArrow formatter."""

import pyarrow as pa
import pyarrow.ipc as ipc
from io import BytesIO

from iceberg_geo.formatters.geoarrow import GeoArrowFormatter, _geojson_to_arrow


class TestGeoArrowFormatter:
    """Test GeoArrow IPC formatter."""

    def test_formatter_mimetype(self):
        formatter = GeoArrowFormatter()
        assert formatter.mimetype == "application/vnd.apache.arrow.stream"

    def test_format_raw_arrow_table(self):
        formatter = GeoArrowFormatter()
        table = pa.table(
            {
                "id": pa.array([1, 2, 3]),
                "name": pa.array(["a", "b", "c"]),
                "geometry": pa.array([b"\x00", b"\x01", b"\x02"], type=pa.large_binary()),
            }
        )
        result = {"_raw_arrow_table": table}
        ipc_bytes = formatter.write(result)

        # Verify it's valid Arrow IPC
        reader = ipc.open_stream(BytesIO(ipc_bytes))
        read_table = reader.read_all()
        assert read_table.num_rows == 3
        assert "id" in read_table.column_names

    def test_format_geojson_features(self):
        formatter = GeoArrowFormatter()
        result = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-100, 35],
                    },
                    "properties": {
                        "name": "test_point",
                        "value": 42.0,
                    },
                },
            ],
        }
        ipc_bytes = formatter.write(result)

        reader = ipc.open_stream(BytesIO(ipc_bytes))
        read_table = reader.read_all()
        assert read_table.num_rows == 1
        assert "name" in read_table.column_names
        assert "geometry" in read_table.column_names


class TestGeoJsonToArrow:
    """Test GeoJSON to Arrow conversion."""

    def test_empty_features(self):
        result = {"features": []}
        table = _geojson_to_arrow(result)
        assert table.num_rows == 0

    def test_point_features(self):
        result = {
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [1.0, 2.0],
                    },
                    "properties": {"id": 1, "label": "A"},
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [3.0, 4.0],
                    },
                    "properties": {"id": 2, "label": "B"},
                },
            ]
        }
        table = _geojson_to_arrow(result)
        assert table.num_rows == 2
        assert "id" in table.column_names
        assert "label" in table.column_names
        assert "geometry" in table.column_names

    def test_null_geometry(self):
        result = {
            "features": [
                {
                    "type": "Feature",
                    "geometry": None,
                    "properties": {"id": 1},
                },
            ]
        }
        table = _geojson_to_arrow(result)
        assert table.num_rows == 1
        geom_val = table.column("geometry")[0].as_py()
        assert geom_val is None
