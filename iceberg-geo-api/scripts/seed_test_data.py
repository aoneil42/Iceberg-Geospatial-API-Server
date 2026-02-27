#!/usr/bin/env python3
"""
Create sample Iceberg tables with geometry columns for development.

Usage:
    python scripts/seed_test_data.py

Requires a running Iceberg REST catalog (see docker-compose.yml).
Set ICEBERG_CATALOG_CONFIG to point to catalog.yml.
"""

import os
import random
import sys

import pyarrow as pa
from shapely import wkb as wkb_mod
from shapely.geometry import Point, box

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from iceberg_geo.query.catalog import get_catalog


def seed_points(catalog, namespace="default", table_name="sensor_observations"):
    """Create a table of random sensor point observations."""
    random.seed(42)
    n = 1000

    geometries = []
    for _ in range(n):
        pt = Point(
            random.uniform(-120, -70),
            random.uniform(25, 50),
        )
        geometries.append(wkb_mod.dumps(pt))

    table = pa.table(
        {
            "observation_id": pa.array(range(n), type=pa.int64()),
            "sensor_id": pa.array([f"S{i:04d}" for i in range(n)]),
            "temperature": pa.array(
                [random.uniform(-10, 45) for _ in range(n)],
                type=pa.float64(),
            ),
            "humidity": pa.array(
                [random.uniform(10, 95) for _ in range(n)],
                type=pa.float64(),
            ),
            "location": pa.array(geometries, type=pa.large_binary()),
        }
    )

    full_name = f"{namespace}.{table_name}"
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    try:
        catalog.drop_table(full_name)
    except Exception:
        pass

    catalog.create_table(full_name, schema=table.schema)
    iceberg_table = catalog.load_table(full_name)
    iceberg_table.append(table)
    print(f"Created {full_name} with {n} sensor observations")


def seed_parcels(catalog, namespace="default", table_name="parcels"):
    """Create a table of random parcel polygons."""
    random.seed(43)
    n = 500

    geometries = []
    for _ in range(n):
        x = random.uniform(-120, -70)
        y = random.uniform(25, 50)
        size = random.uniform(0.01, 0.1)
        poly = box(x, y, x + size, y + size)
        geometries.append(wkb_mod.dumps(poly))

    table = pa.table(
        {
            "parcel_id": pa.array([f"P{i:06d}" for i in range(n)]),
            "area_sqm": pa.array(
                [random.uniform(100, 50000) for _ in range(n)],
                type=pa.float64(),
            ),
            "zoning": pa.array(
                [random.choice(["R1", "R2", "C1", "C2", "I1"]) for _ in range(n)]
            ),
            "assessed_value": pa.array(
                [random.uniform(50000, 2000000) for _ in range(n)],
                type=pa.float64(),
            ),
            "geometry": pa.array(geometries, type=pa.large_binary()),
        }
    )

    full_name = f"{namespace}.{table_name}"
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    try:
        catalog.drop_table(full_name)
    except Exception:
        pass

    catalog.create_table(full_name, schema=table.schema)
    iceberg_table = catalog.load_table(full_name)
    iceberg_table.append(table)
    print(f"Created {full_name} with {n} parcels")


def main():
    catalog = get_catalog()
    seed_points(catalog)
    seed_parcels(catalog)
    print("Seed data complete!")


if __name__ == "__main__":
    main()
