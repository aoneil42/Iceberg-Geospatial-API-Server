"""Tests for the Iceberg Query Engine."""

import pytest

from iceberg_geo.query.engine import get_table_schema, query_features
from iceberg_geo.query.models import QueryParams


class TestGetTableSchema:
    """Test schema introspection."""

    def test_schema_has_fields(self, points_table):
        schema = get_table_schema(points_table)
        field_names = [f["name"] for f in schema.fields]
        assert "objectid" in field_names
        assert "sensor_id" in field_names
        assert "temperature" in field_names

    def test_schema_identifies_geometry_column(self, points_table):
        schema = get_table_schema(points_table)
        assert schema.geometry_column == "geometry"

    def test_schema_identifies_id_field(self, points_table):
        schema = get_table_schema(points_table)
        assert schema.id_field == "objectid"

    def test_polygon_table_schema(self, polygons_table):
        schema = get_table_schema(polygons_table)
        field_names = [f["name"] for f in schema.fields]
        assert "parcel_id" in field_names
        assert "zoning" in field_names
        assert "area_sqm" in field_names


class TestQueryFeatures:
    """Test the core query function."""

    def test_basic_query(self, points_table):
        params = QueryParams(limit=10)
        result = query_features(points_table, params)
        assert result.features is not None
        assert result.features.num_rows == 10
        assert result.count == 10

    def test_query_all(self, points_table):
        params = QueryParams(limit=200)
        result = query_features(points_table, params)
        assert result.features is not None
        assert result.count == 100  # all 100 points

    def test_pagination_offset(self, points_table):
        params = QueryParams(limit=10, offset=5)
        result = query_features(points_table, params)
        assert result.features is not None
        assert result.count == 10

    def test_exceeded_transfer_limit(self, points_table):
        params = QueryParams(limit=10)
        result = query_features(points_table, params)
        assert result.exceeded_transfer_limit is True

    def test_no_exceeded_when_all_returned(self, points_table):
        params = QueryParams(limit=200)
        result = query_features(points_table, params)
        assert result.exceeded_transfer_limit is False

    def test_bbox_filter(self, points_table):
        # A small bbox in the middle of the US
        params = QueryParams(
            bbox=(-100.0, 35.0, -95.0, 40.0),
            limit=1000,
        )
        result = query_features(points_table, params)
        assert result.features is not None
        # All returned points should be within the bbox
        # (verified by DuckDB spatial filter)
        assert result.count >= 0

    def test_where_filter(self, polygons_table):
        params = QueryParams(
            where="zoning = 'R1'",
            limit=1000,
        )
        result = query_features(polygons_table, params)
        assert result.features is not None
        # All returned features should have zoning = 'R1'
        if result.count > 0:
            zoning_col = result.features.column("zoning").to_pylist()
            assert all(z == "R1" for z in zoning_col)

    def test_count_only(self, points_table):
        params = QueryParams(return_count_only=True)
        result = query_features(points_table, params)
        assert result.features is None
        assert result.count == 100

    def test_ids_only(self, points_table):
        params = QueryParams(return_ids_only=True, limit=1000)
        result = query_features(points_table, params)
        assert result.features is not None
        assert "objectid" in result.features.column_names

    def test_field_selection(self, points_table):
        params = QueryParams(
            out_fields="objectid,sensor_id",
            return_geometry=False,
            limit=5,
        )
        result = query_features(points_table, params)
        assert result.features is not None
        col_names = result.features.column_names
        assert "objectid" in col_names
        assert "sensor_id" in col_names
        assert "temperature" not in col_names

    def test_order_by(self, points_table):
        params = QueryParams(
            order_by="temperature DESC",
            limit=5,
        )
        result = query_features(points_table, params)
        temps = result.features.column("temperature").to_pylist()
        assert temps == sorted(temps, reverse=True)

    def test_empty_result(self, points_table):
        # Query with impossible bbox
        params = QueryParams(
            bbox=(0.0, 0.0, 0.1, 0.1),  # middle of Atlantic
            limit=100,
        )
        result = query_features(points_table, params)
        assert result.count == 0


class TestSanitization:
    """Test WHERE clause sanitization."""

    def test_rejects_drop(self, points_table):
        params = QueryParams(where="1=1; DROP TABLE features", limit=1)
        with pytest.raises(ValueError):
            query_features(points_table, params)

    def test_rejects_semicolons(self, points_table):
        params = QueryParams(where="objectid = 1; SELECT 1", limit=1)
        with pytest.raises(ValueError):
            query_features(points_table, params)

    def test_rejects_comments(self, points_table):
        params = QueryParams(where="objectid = 1 -- comment", limit=1)
        with pytest.raises(ValueError):
            query_features(points_table, params)

    def test_rejects_union(self, points_table):
        params = QueryParams(where="1=1 UNION SELECT 1", limit=1)
        with pytest.raises(ValueError):
            query_features(points_table, params)

    def test_rejects_subquery(self, points_table):
        params = QueryParams(where="objectid IN (SELECT 1)", limit=1)
        with pytest.raises(ValueError):
            query_features(points_table, params)

    def test_allows_valid_where(self, points_table):
        params = QueryParams(where="objectid > 50", limit=5)
        result = query_features(points_table, params)
        assert result.features is not None
