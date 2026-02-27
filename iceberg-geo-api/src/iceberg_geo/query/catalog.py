"""
Manage PyIceberg catalog connections.

Reads config from ICEBERG_CATALOG_CONFIG env var or passed dict.
Supports REST, SQL, Glue, Hive catalogs per PyIceberg.

Supports ${ENV_VAR} interpolation in YAML string values so that
secrets (e.g. S3 credentials) can be injected via environment
variables rather than hard-coded in the config file.
"""

import re

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import yaml
import os

_catalog = None

_ENV_RE = re.compile(r"\$\{(\w+)\}")


def _resolve_env_vars(value):
    """Replace ${VAR} placeholders with environment variable values."""
    if isinstance(value, str):
        return _ENV_RE.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), value
        )
    return value


def get_catalog():
    """Singleton catalog instance."""
    global _catalog
    if _catalog is None:
        config_path = os.environ.get(
            "ICEBERG_CATALOG_CONFIG", "config/catalog.yml"
        )
        with open(config_path) as f:
            config = yaml.safe_load(f)
        catalog_config = {
            k: _resolve_env_vars(v) for k, v in config["catalog"].items()
        }
        _catalog = load_catalog(**catalog_config)
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
    table = catalog.load_table(f"{namespace}.{table_name}")
    # Disable remote signing if the catalog server (e.g. LakeKeeper) enables
    # it — Garage S3 is incompatible with the REST signer proxy.
    if table.io.properties.get("s3.remote-signing-enabled") == "true":
        table.io.properties["s3.remote-signing-enabled"] = "false"
        table.io.properties.pop("s3.signer", None)
        table.io.properties.pop("s3.signer.endpoint", None)
        table.io.properties.pop("s3.signer.uri", None)
    return table


def list_namespaces() -> list[str]:
    """List available namespaces in the catalog."""
    catalog = get_catalog()
    return [ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()]


def list_tables(namespace: str) -> list[str]:
    """List available tables in a namespace."""
    catalog = get_catalog()
    return [t[1] for t in catalog.list_tables(namespace)]
