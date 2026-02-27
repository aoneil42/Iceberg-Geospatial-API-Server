"""Tests for the FastAPI GeoServices endpoint."""

import pytest
from fastapi.testclient import TestClient

from iceberg_geo.geoservices.app import app


@pytest.fixture
def client():
    """Create a FastAPI test client."""
    return TestClient(app)


class TestRestInfo:
    """Test /rest/info endpoint."""

    def test_rest_info(self, client):
        response = client.get("/rest/info")
        assert response.status_code == 200
        data = response.json()
        assert data["currentVersion"] == 11.0
        assert "authInfo" in data


class TestFeatureServer:
    """Test /FeatureServer endpoint."""

    def test_feature_server_info(self, client):
        response = client.get("/rest/services/test/FeatureServer")
        assert response.status_code == 200
        data = response.json()
        assert "layers" in data
        assert data["currentVersion"] == 11.0
        assert len(data["layers"]) >= 1

    def test_layer_info(self, client):
        response = client.get("/rest/services/test/FeatureServer/0")
        assert response.status_code == 200
        data = response.json()
        assert "fields" in data
        assert "objectIdField" in data
        assert "geometryType" in data
        assert data["type"] == "Feature Layer"
        assert "extent" in data
        assert "maxRecordCount" in data


class TestQuery:
    """Test /FeatureServer/{layer}/query endpoint."""

    def test_basic_query_json(self, client):
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 5,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        assert "fields" in data
        assert "geometryType" in data
        assert "spatialReference" in data
        assert len(data["features"]) == 5

    def test_query_geojson(self, client):
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "geojson",
                "resultRecordCount": 5,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        for f in data["features"]:
            assert f["type"] == "Feature"
            assert "geometry" in f
            assert "properties" in f

    def test_query_pbf(self, client):
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "pbf",
                "resultRecordCount": 5,
            },
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-protobuf"
        assert len(response.content) > 0

    def test_query_with_bbox(self, client):
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "geometry": "-100,35,-95,40",
                "geometryType": "esriGeometryEnvelope",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 100,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "features" in data

    def test_query_with_where(self, client):
        # Layer 0 = parcels (alphabetical order)
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "zoning = 'R1'",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 100,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
        for feat in data["features"]:
            assert feat["attributes"]["zoning"] == "R1"

    def test_query_out_fields(self, client):
        # Layer 0 = parcels, use its columns
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "objectid,parcel_id",
                "f": "json",
                "resultRecordCount": 5,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "features" in data

    def test_query_return_count_only(self, client):
        # Layer 0 = parcels (50 rows), Layer 1 = sensor_points (100 rows)
        response = client.get(
            "/rest/services/test/FeatureServer/1/query",
            params={
                "where": "1=1",
                "returnCountOnly": True,
                "f": "json",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert data["count"] == 100

    def test_query_pagination(self, client):
        response1 = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 5,
                "resultOffset": 0,
            },
        )
        response2 = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 5,
                "resultOffset": 5,
            },
        )
        data1 = response1.json()
        data2 = response2.json()
        # Pages should return different features
        ids1 = {f["attributes"]["objectid"] for f in data1["features"]}
        ids2 = {f["attributes"]["objectid"] for f in data2["features"]}
        assert len(ids1.intersection(ids2)) == 0

    def test_query_exceeded_transfer_limit(self, client):
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 5,
            },
        )
        data = response.json()
        assert data["exceededTransferLimit"] is True

    def test_esri_json_feature_structure(self, client):
        # Layer 1 = sensor_points (Point geometry)
        response = client.get(
            "/rest/services/test/FeatureServer/1/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 1,
            },
        )
        data = response.json()
        feature = data["features"][0]
        assert "attributes" in feature
        assert "geometry" in feature
        # Point geometry should have x and y
        geom = feature["geometry"]
        assert "x" in geom
        assert "y" in geom

    def test_query_envelope_geometry(self, client):
        import json

        envelope = json.dumps(
            {"xmin": -100, "ymin": 35, "xmax": -95, "ymax": 40}
        )
        response = client.get(
            "/rest/services/test/FeatureServer/0/query",
            params={
                "geometry": envelope,
                "geometryType": "esriGeometryEnvelope",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": 100,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "features" in data
