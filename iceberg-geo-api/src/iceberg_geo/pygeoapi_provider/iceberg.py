"""
pygeoapi BaseProvider implementation for Apache Iceberg tables.

Registered in pygeoapi-config.yml as:
    provider:
        name: iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider

This provider translates pygeoapi's query interface into calls
to the shared Iceberg Query Service.
"""

import logging

from pygeoapi.provider.base import BaseProvider, ProviderQueryError

from iceberg_geo.query.catalog import get_table
from iceberg_geo.query.engine import get_table_schema, query_features
from iceberg_geo.query.geometry import wkb_to_geojson
from iceberg_geo.query.models import QueryParams

LOGGER = logging.getLogger(__name__)


class IcebergProvider(BaseProvider):
    """pygeoapi provider for Apache Iceberg tables with geometry columns."""

    def __init__(self, provider_def):
        """
        Initialize the Iceberg provider.

        provider_def comes from pygeoapi-config.yml and should include:
            data: "namespace.table_name"
            id_field: "objectid"
            options:
                geometry_column: "geometry"
        """
        super().__init__(provider_def)

        table_id = provider_def["data"]
        parts = table_id.split(".")
        self.namespace = parts[0]
        self.table_name = parts[1]
        self.table = get_table(self.namespace, self.table_name)

        options = provider_def.get("options", {})
        self.geometry_column = options.get("geometry_column", "geometry")

        self._schema = get_table_schema(self.table)
        self._fields = {
            f["name"]: {"type": f["type"]}
            for f in self._schema.fields
            if f["name"] != self.geometry_column
        }

    def get_fields(self):
        """Return field definitions for the collection."""
        return self.fields

    def query(
        self,
        offset=0,
        limit=10,
        resulttype="results",
        bbox=None,
        datetime_=None,
        properties=None,
        sortby=None,
        select_properties=None,
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec=None,
        **kwargs,
    ):
        """
        Execute a query against the Iceberg table.

        Translates pygeoapi query params to shared QueryParams model,
        calls the query engine, and converts results to GeoJSON
        FeatureCollection.
        """
        params = QueryParams(
            bbox=tuple(bbox) if bbox else None,
            limit=limit,
            offset=offset,
            return_geometry=not skip_geometry,
            return_count_only=(resulttype == "hits"),
        )

        # Property filters -> simple WHERE clause
        if properties:
            where_parts = []
            for prop in properties:
                name, value = prop["property"], prop["value"]
                # Use quoted values safely
                safe_value = str(value).replace("'", "''")
                where_parts.append(f"{name} = '{safe_value}'")
            params.where = " AND ".join(where_parts)

        # Sort
        if sortby:
            order_parts = []
            for s in sortby:
                direction = "ASC" if s.get("order", "A") == "A" else "DESC"
                order_parts.append(f"{s['property']} {direction}")
            params.order_by = ", ".join(order_parts)

        # Field selection
        if select_properties:
            params.out_fields = ",".join(select_properties)

        # CQL2 filter (Phase 2)
        if filterq:
            params.cql2_filter = filterq

        # Execute
        result = query_features(self.table, params)

        if resulttype == "hits":
            return self._format_hits(result)

        return self._format_feature_collection(result, skip_geometry)

    def get(self, identifier, **kwargs):
        """Retrieve a single feature by ID."""
        params = QueryParams(
            where=f"{self._schema.id_field} = '{identifier}'",
            limit=1,
        )
        result = query_features(self.table, params)
        if result.count == 0:
            raise ProviderQueryError(f"Feature {identifier} not found")

        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column
        )
        return features[0]

    def _format_feature_collection(self, result, skip_geometry=False):
        """Convert QueryResult to pygeoapi-expected dict."""
        features = self._arrow_to_geojson_features(
            result.features, result.geometry_column, skip_geometry
        )
        return {
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": result.count,
            "numberReturned": len(features),
        }

    def _format_hits(self, result):
        """Return count-only response."""
        return {"numberMatched": result.count}

    def _arrow_to_geojson_features(
        self, arrow_table, geom_col, skip_geometry=False
    ):
        """
        Convert Arrow table to list of GeoJSON Feature dicts.

        For each row:
        1. Extract all non-geometry columns as properties
        2. Decode WKB geometry column to GeoJSON geometry via Shapely
        3. Build Feature dict with id from id_field
        """
        if arrow_table is None or arrow_table.num_rows == 0:
            return []

        features = []
        id_field = self._schema.id_field
        table_dict = arrow_table.to_pydict()
        num_rows = arrow_table.num_rows

        for i in range(num_rows):
            properties = {}
            geometry = None

            for col_name in table_dict:
                if col_name == geom_col:
                    if not skip_geometry:
                        wkb_bytes = table_dict[col_name][i]
                        if wkb_bytes:
                            geometry = wkb_to_geojson(wkb_bytes)
                else:
                    val = table_dict[col_name][i]
                    properties[col_name] = _to_json_safe(val)

            feature = {
                "type": "Feature",
                "id": str(table_dict.get(id_field, list(range(num_rows)))[i]),
                "geometry": geometry,
                "properties": properties,
            }
            features.append(feature)

        return features


def _to_json_safe(val):
    """Convert pyarrow scalar values to JSON-serializable Python types."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return None
    if hasattr(val, "as_py"):
        return val.as_py()
    return val
