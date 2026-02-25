"""
Manage PyIceberg catalog connections.

Reads config from ICEBERG_CATALOG_CONFIG env var or passed dict.
Supports REST, SQL, Glue, Hive catalogs per PyIceberg.
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import yaml
import os

_catalog = None


def get_catalog():
    """Singleton catalog instance."""
    global _catalog
    if _catalog is None:
        config_path = os.environ.get(
            "ICEBERG_CATALOG_CONFIG", "config/catalog.yml"
        )
        with open(config_path) as f:
            config = yaml.safe_load(f)
        _catalog = load_catalog(**config["catalog"])
    return _catalog


def set_catalog(catalog):
    """Override the catalog instance (used for testing)."""
    global _catalog
    _catalog = catalog


def reset_catalog():
    """Reset the singleton catalog (used for testing)."""
    global _catalog
    _catalog = None


def get_table(namespace: str, table_name: str) -> Table:
    """Load an Iceberg table by namespace.table_name."""
    catalog = get_catalog()
    return catalog.load_table(f"{namespace}.{table_name}")


def list_tables(namespace: str) -> list[str]:
    """List available tables in a namespace."""
    catalog = get_catalog()
    return [t[1] for t in catalog.list_tables(namespace)]
