"""Tests for the Esri PBF serializer."""

import pytest
import pyarrow as pa
from shapely import wkb as wkb_mod
from shapely.geometry import LineString, MultiPolygon, Point, Polygon, box

from iceberg_geo.geoservices.proto import FeatureCollection_pb2 as pb
from iceberg_geo.geoservices.serializers.esri_pbf import (
    _encode_geometry,
    _extract_coord_arrays,
    serialize,
)
from iceberg_geo.query.models import FeatureSchema, QueryResult


def _make_schema(**kwargs) -> FeatureSchema:
    """Helper to create a FeatureSchema for tests."""
    defaults = {
        "table_identifier": "test.features",
        "geometry_column": "geometry",
        "geometry_type": "Point",
        "srid": 4326,
        "fields": [
            {"name": "objectid", "type": "int64", "alias": "objectid"},
            {"name": "name", "type": "string", "alias": "name"},
        ],
        "id_field": "objectid",
    }
    defaults.update(kwargs)
    return FeatureSchema(**defaults)


def _make_point_result(n=3) -> tuple[QueryResult, FeatureSchema]:
    """Create a QueryResult with point features."""
    schema = _make_schema(geometry_type="Point")
    points = [
        Point(-100 + i, 35 + i) for i in range(n)
    ]
    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "name": pa.array([f"point_{i}" for i in range(n)]),
            "geometry": pa.array(
                [wkb_mod.dumps(p) for p in points], type=pa.large_binary()
            ),
        }
    )
    result = QueryResult(
        features=table,
        geometry_column="geometry",
        count=n,
    )
    return result, schema


def _make_polygon_result(n=3) -> tuple[QueryResult, FeatureSchema]:
    """Create a QueryResult with polygon features."""
    schema = _make_schema(geometry_type="Polygon")
    polygons = [
        box(-100 + i, 35 + i, -99 + i, 36 + i) for i in range(n)
    ]
    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "name": pa.array([f"poly_{i}" for i in range(n)]),
            "geometry": pa.array(
                [wkb_mod.dumps(p) for p in polygons], type=pa.large_binary()
            ),
        }
    )
    result = QueryResult(
        features=table,
        geometry_column="geometry",
        count=n,
    )
    return result, schema


class TestExtractCoordArrays:
    """Test coordinate extraction from Shapely geometries."""

    def test_point(self):
        p = Point(1.0, 2.0)
        arrays = _extract_coord_arrays(p)
        assert arrays == [[(1.0, 2.0)]]

    def test_linestring(self):
        line = LineString([(0, 0), (1, 1), (2, 0)])
        arrays = _extract_coord_arrays(line)
        assert len(arrays) == 1
        assert len(arrays[0]) == 3

    def test_polygon(self):
        poly = box(0, 0, 1, 1)
        arrays = _extract_coord_arrays(poly)
        assert len(arrays) == 1  # one exterior ring
        assert len(arrays[0]) == 5  # closed ring (5 vertices)

    def test_polygon_with_hole(self):
        exterior = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        hole = [(2, 2), (8, 2), (8, 8), (2, 8), (2, 2)]
        poly = Polygon(exterior, [hole])
        arrays = _extract_coord_arrays(poly)
        assert len(arrays) == 2  # exterior + one hole

    def test_multipolygon(self):
        p1 = box(0, 0, 1, 1)
        p2 = box(2, 2, 3, 3)
        mp = MultiPolygon([p1, p2])
        arrays = _extract_coord_arrays(mp)
        assert len(arrays) == 2  # two polygons, no holes


class TestSerialize:
    """Test full PBF serialization."""

    def test_serialize_points(self):
        result, schema = _make_point_result(3)
        pbf_bytes = serialize(result, schema)
        assert len(pbf_bytes) > 0

        # Deserialize and verify
        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        features = fc.queryResult.featureResult.features
        assert len(features) == 3

    def test_serialize_polygons(self):
        result, schema = _make_polygon_result(3)
        pbf_bytes = serialize(result, schema)
        assert len(pbf_bytes) > 0

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        features = fc.queryResult.featureResult.features
        assert len(features) == 3

    def test_serialize_has_spatial_reference(self):
        result, schema = _make_point_result(1)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        sr = fc.queryResult.featureResult.spatialReference
        assert sr.wkid == 4326

    def test_serialize_has_fields(self):
        result, schema = _make_point_result(1)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        fields = fc.queryResult.featureResult.fields
        field_names = [f.name for f in fields]
        assert "objectid" in field_names
        assert "name" in field_names

    def test_serialize_has_transform(self):
        result, schema = _make_point_result(3)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        transform = fc.queryResult.featureResult.transform
        assert transform.scale.xScale > 0
        assert transform.scale.yScale > 0

    def test_serialize_empty_result(self):
        schema = _make_schema()
        result = QueryResult(features=None, count=0)
        pbf_bytes = serialize(result, schema)
        assert len(pbf_bytes) > 0

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        # Should have fields but no features
        features = fc.queryResult.featureResult.features
        assert len(features) == 0

    def test_attributes_encoded(self):
        result, schema = _make_point_result(1)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        feature = fc.queryResult.featureResult.features[0]
        # Should have attributes
        assert len(feature.attributes) == 2  # objectid + name

    def test_geometry_has_coords(self):
        result, schema = _make_point_result(1)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        feature = fc.queryResult.featureResult.features[0]
        geom = feature.geometry
        # Point should have coords (2 values: dx, dy)
        assert len(geom.coords) == 2
        assert len(geom.lengths) == 1  # one "ring" of 1 point

    def test_polygon_geometry_encoding(self):
        result, schema = _make_polygon_result(1)
        pbf_bytes = serialize(result, schema)

        fc = pb.FeatureCollectionPBuffer()
        fc.ParseFromString(pbf_bytes)
        feature = fc.queryResult.featureResult.features[0]
        geom = feature.geometry
        # Polygon ring should have lengths and coords
        assert len(geom.lengths) >= 1
        assert len(geom.coords) >= 2


class TestEncodeGeometry:
    """Test geometry encoding to PBF format."""

    def test_point_encoding(self):
        pb_geom = pb.FeatureCollectionPBuffer.Geometry()
        point = Point(10.0, 20.0)
        _encode_geometry(pb_geom, point, 0.0, 100.0, 1e-8, 1e-8)

        assert len(pb_geom.lengths) == 1
        assert pb_geom.lengths[0] == 1
        assert len(pb_geom.coords) == 2

    def test_polygon_encoding(self):
        pb_geom = pb.FeatureCollectionPBuffer.Geometry()
        poly = box(0, 0, 1, 1)
        _encode_geometry(pb_geom, poly, 0.0, 1.0, 1e-8, 1e-8)

        assert len(pb_geom.lengths) == 1  # one ring
        assert pb_geom.lengths[0] == 5  # closed polygon ring
        assert len(pb_geom.coords) == 10  # 5 vertices * 2 coords

    def test_delta_encoding(self):
        """Verify coords are delta-encoded."""
        pb_geom = pb.FeatureCollectionPBuffer.Geometry()
        line = LineString([(0, 0), (1, 0), (1, 1)])
        _encode_geometry(pb_geom, line, 0.0, 1.0, 1e-8, 1e-8)

        # First point: dx=0, dy=-1e8 (upper-left origin)
        # Second: dx=1e8, dy=0
        # Third: dx=0, dy=-1e8
        coords = list(pb_geom.coords)
        assert len(coords) == 6  # 3 points * 2
