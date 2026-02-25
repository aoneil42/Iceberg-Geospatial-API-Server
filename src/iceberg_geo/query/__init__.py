"""Iceberg Query Service — shared data access layer."""

from .catalog import get_catalog, get_table, list_tables
from .engine import query_features, get_table_schema
from .models import QueryParams, QueryResult, FeatureSchema

__all__ = [
    "get_catalog",
    "get_table",
    "list_tables",
    "query_features",
    "get_table_schema",
    "QueryParams",
    "QueryResult",
    "FeatureSchema",
]
