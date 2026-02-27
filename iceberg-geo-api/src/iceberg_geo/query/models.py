"""
Pydantic models shared across both API surfaces.
These models are API-agnostic — they represent query semantics, not wire formats.
"""

from pydantic import BaseModel, Field
from typing import Optional
import pyarrow as pa


class QueryParams(BaseModel):
    """Unified query parameters. Both pygeoapi and GeoServices
    translate their API-specific params into this model."""

    # Spatial
    bbox: Optional[tuple[float, float, float, float]] = None
    geometry_filter: Optional[str] = None  # WKT geometry for spatial filter
    spatial_rel: str = "intersects"  # intersects | contains | within

    # Attribute
    where: Optional[str] = None  # SQL WHERE clause (GeoServices)

    # Fields
    out_fields: Optional[str] = None  # comma-separated field names, or "*"
    return_geometry: bool = True

    # Pagination
    limit: Optional[int] = 1000
    offset: Optional[int] = 0

    # Sorting
    order_by: Optional[str] = None

    # Response modifiers
    return_count_only: bool = False
    return_ids_only: bool = False

    # Object ID filter (list of integer OIDs to fetch)
    object_ids: Optional[list[int]] = None

    # Output spatial reference (EPSG code)
    out_sr: Optional[int] = None

    # CQL2 filter (Phase 2)
    cql2_filter: Optional[dict] = None


class QueryResult(BaseModel):
    """Result from the query engine. Carries Arrow table + metadata."""

    model_config = {"arbitrary_types_allowed": True}

    features: Optional[pa.Table] = None
    geometry_column: str = "geometry"
    count: int = 0
    exceeded_transfer_limit: bool = False

    @classmethod
    def empty(cls, params: "QueryParams") -> "QueryResult":
        return cls(features=None, count=0)


class FeatureSchema(BaseModel):
    """Schema description for an Iceberg table exposed as a feature layer."""

    table_identifier: str  # namespace.table_name
    geometry_column: str = "geometry"
    geometry_type: str = "Polygon"  # Point, LineString, Polygon, MultiPolygon, etc.
    srid: int = 4326
    fields: list[dict]  # [{name, type, alias}, ...]
    extent: Optional[dict] = None  # {xmin, ymin, xmax, ymax}
    id_field: str = "objectid"  # or auto-generated
    max_record_count: int = 10000
