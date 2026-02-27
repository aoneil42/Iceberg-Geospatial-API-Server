"""Tests for the pygeoapi Iceberg provider."""

import pytest


@pytest.fixture
def points_provider(iceberg_catalog):
    """Create an IcebergProvider for the test points table."""
    from iceberg_geo.pygeoapi_provider.iceberg import IcebergProvider

    provider_def = {
        "name": "iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider",
        "type": "feature",
        "data": "test.sensor_points",
        "id_field": "objectid",
        "options": {
            "geometry_column": "geometry",
        },
    }
    return IcebergProvider(provider_def)


@pytest.fixture
def polygons_provider(iceberg_catalog):
    """Create an IcebergProvider for the test polygons table."""
    from iceberg_geo.pygeoapi_provider.iceberg import IcebergProvider

    provider_def = {
        "name": "iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider",
        "type": "feature",
        "data": "test.parcels",
        "id_field": "objectid",
        "options": {
            "geometry_column": "geometry",
        },
    }
    return IcebergProvider(provider_def)


class TestProviderInit:
    """Test provider initialization."""

    def test_fields_populated(self, points_provider):
        fields = points_provider.get_fields()
        assert "objectid" in fields or "sensor_id" in fields
        assert "temperature" in fields or "sensor_id" in fields

    def test_geometry_column_set(self, points_provider):
        assert points_provider.geometry_column == "geometry"


class TestProviderQuery:
    """Test provider query method."""

    def test_basic_query(self, points_provider):
        result = points_provider.query(limit=5)
        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 5
        assert result["numberReturned"] == 5

    def test_query_with_bbox(self, points_provider):
        result = points_provider.query(
            bbox=[-100, 35, -95, 40],
            limit=100,
        )
        assert result["type"] == "FeatureCollection"
        # All features should be within bbox
        for feature in result["features"]:
            if feature["geometry"]:
                coords = feature["geometry"]["coordinates"]
                assert -100 <= coords[0] <= -95
                assert 35 <= coords[1] <= 40

    def test_query_hits(self, points_provider):
        result = points_provider.query(resulttype="hits")
        assert "numberMatched" in result
        assert result["numberMatched"] == 100

    def test_query_pagination(self, points_provider):
        page1 = points_provider.query(limit=5, offset=0)
        page2 = points_provider.query(limit=5, offset=5)

        ids1 = {f["id"] for f in page1["features"]}
        ids2 = {f["id"] for f in page2["features"]}
        # Pages should return different features
        assert len(ids1.intersection(ids2)) == 0

    def test_query_property_filter(self, polygons_provider):
        result = polygons_provider.query(
            properties=[{"property": "zoning", "value": "R1"}],
            limit=100,
        )
        for feature in result["features"]:
            assert feature["properties"]["zoning"] == "R1"

    def test_query_skip_geometry(self, points_provider):
        result = points_provider.query(limit=5, skip_geometry=True)
        for feature in result["features"]:
            assert feature["geometry"] is None

    def test_query_select_properties(self, points_provider):
        result = points_provider.query(
            select_properties=["sensor_id"],
            limit=5,
        )
        for feature in result["features"]:
            assert "sensor_id" in feature["properties"]

    def test_query_sort(self, points_provider):
        result = points_provider.query(
            sortby=[{"property": "temperature", "order": "D"}],
            limit=5,
        )
        temps = [f["properties"]["temperature"] for f in result["features"]]
        assert temps == sorted(temps, reverse=True)

    def test_feature_has_geometry(self, points_provider):
        result = points_provider.query(limit=1)
        feature = result["features"][0]
        assert feature["geometry"] is not None
        assert feature["geometry"]["type"] == "Point"
        assert "coordinates" in feature["geometry"]

    def test_polygon_feature_has_geometry(self, polygons_provider):
        result = polygons_provider.query(limit=1)
        feature = result["features"][0]
        assert feature["geometry"] is not None
        assert feature["geometry"]["type"] == "Polygon"


class TestProviderGet:
    """Test getting individual features."""

    def test_get_feature_by_id(self, points_provider):
        feature = points_provider.get("0")
        assert feature["type"] == "Feature"
        assert feature["geometry"] is not None

    def test_get_nonexistent_feature(self, points_provider):
        from pygeoapi.provider.base import ProviderQueryError

        with pytest.raises(ProviderQueryError):
            points_provider.get("999999")
