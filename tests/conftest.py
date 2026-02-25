"""
Shared test fixtures.

Creates an in-memory Iceberg catalog with sample tables
containing geometry columns for testing all components.
"""

import os
import random
import tempfile

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from shapely import wkb as wkb_mod
from shapely.geometry import Point, box


@pytest.fixture(scope="session")
def warehouse_path(tmp_path_factory):
    """Create a temporary warehouse directory."""
    return str(tmp_path_factory.mktemp("warehouse"))


@pytest.fixture(scope="session")
def iceberg_catalog(warehouse_path):
    """Create a temporary SQLite-backed Iceberg catalog with test data."""
    catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    catalog.create_namespace("test")

    _create_points_table(catalog)
    _create_polygons_table(catalog)

    return catalog


@pytest.fixture(autouse=True)
def setup_catalog(iceberg_catalog):
    """Set the test catalog as the global catalog for all tests."""
    from iceberg_geo.query.catalog import set_catalog, reset_catalog

    set_catalog(iceberg_catalog)
    yield
    reset_catalog()


@pytest.fixture
def points_table(iceberg_catalog):
    """Load the test points table."""
    return iceberg_catalog.load_table("test.sensor_points")


@pytest.fixture
def polygons_table(iceberg_catalog):
    """Load the test polygons table."""
    return iceberg_catalog.load_table("test.parcels")


@pytest.fixture
def sample_query_params():
    """Default query params for testing."""
    from iceberg_geo.query.models import QueryParams

    return QueryParams(limit=10)


def _create_points_table(catalog):
    """Create test.sensor_points with 100 random points."""
    random.seed(42)

    n = 100
    geometries = []
    for _ in range(n):
        pt = Point(
            random.uniform(-120, -70),
            random.uniform(25, 50),
        )
        geometries.append(wkb_mod.dumps(pt))

    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "sensor_id": pa.array([f"S{i:04d}" for i in range(n)]),
            "temperature": pa.array(
                [random.uniform(-10, 45) for _ in range(n)],
                type=pa.float64(),
            ),
            "geometry": pa.array(geometries, type=pa.large_binary()),
        }
    )

    catalog.create_table("test.sensor_points", schema=table.schema)
    iceberg_table = catalog.load_table("test.sensor_points")
    iceberg_table.append(table)


def _create_polygons_table(catalog):
    """Create test.parcels with 50 rectangular polygons."""
    random.seed(43)

    n = 50
    geometries = []
    for _ in range(n):
        x = random.uniform(-120, -70)
        y = random.uniform(25, 50)
        size = random.uniform(0.01, 0.1)
        poly = box(x, y, x + size, y + size)
        geometries.append(wkb_mod.dumps(poly))

    table = pa.table(
        {
            "objectid": pa.array(range(n), type=pa.int64()),
            "parcel_id": pa.array([f"P{i:06d}" for i in range(n)]),
            "area_sqm": pa.array(
                [random.uniform(100, 50000) for _ in range(n)],
                type=pa.float64(),
            ),
            "zoning": pa.array(
                [random.choice(["R1", "R2", "C1", "C2", "I1"]) for _ in range(n)]
            ),
            "geometry": pa.array(geometries, type=pa.large_binary()),
        }
    )

    catalog.create_table("test.parcels", schema=table.schema)
    iceberg_table = catalog.load_table("test.parcels")
    iceberg_table.append(table)
