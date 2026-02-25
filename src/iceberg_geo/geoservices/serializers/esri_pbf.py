"""
Serialize QueryResult -> Esri FeatureCollection PBF (Protocol Buffers).

This is the performance-critical serializer for Iceberg-scale data.
The ArcGIS JS API requests PBF by default (f=pbf).

Key differences from JSON encoding:
1. Coordinates are quantized to integers with a Transform
2. Coordinates are delta-encoded (each = current - previous)
3. Geometry rings/paths use a flat coords array + lengths array
4. Attributes use protobuf Value oneof types (not JSON strings)

References:
- Spec: https://github.com/Esri/arcgis-pbf/tree/main/proto/FeatureCollection
"""

from shapely import wkb

from iceberg_geo.query.models import FeatureSchema, QueryResult

try:
    from ..proto import FeatureCollection_pb2 as pb

    HAS_PROTO = True
except ImportError:
    HAS_PROTO = False
    pb = None

# Quantization resolution — controls coordinate precision
QUANTIZE_RESOLUTION = 1e8


def serialize(result: QueryResult, schema: FeatureSchema) -> bytes:
    """
    Serialize a QueryResult to Esri FeatureCollection PBF bytes.

    Returns raw protobuf bytes suitable for Response(content=...,
    media_type="application/x-protobuf").
    """
    if not HAS_PROTO:
        raise ImportError(
            "Protobuf classes not generated. Run scripts/generate_proto.sh first."
        )

    if result.features is None or result.features.num_rows == 0:
        return _serialize_empty(result, schema)

    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult
    feature_result = query_result.featureResult

    # Spatial reference
    feature_result.spatialReference.wkid = schema.srid
    feature_result.spatialReference.lastestWkid = schema.srid

    # Fields
    _build_fields(feature_result, schema)

    # Geometry type
    geom_type_map = {
        "Point": pb.FeatureCollectionPBuffer.esriGeometryTypePoint,
        "MultiPoint": pb.FeatureCollectionPBuffer.esriGeometryTypeMultipoint,
        "LineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "MultiLineString": pb.FeatureCollectionPBuffer.esriGeometryTypePolyline,
        "Polygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
        "MultiPolygon": pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    }
    feature_result.geometryType = geom_type_map.get(
        schema.geometry_type,
        pb.FeatureCollectionPBuffer.esriGeometryTypePolygon,
    )

    # Compute Transform from data extent
    table_dict = result.features.to_pydict()
    geom_col = result.geometry_column
    all_coords = []

    # First pass: collect all geometries and bounds
    geometries = []
    for i in range(result.features.num_rows):
        wkb_bytes = table_dict[geom_col][i]
        if wkb_bytes:
            geom = wkb.loads(wkb_bytes)
            geometries.append(geom)
            bounds = geom.bounds
            all_coords.extend([(bounds[0], bounds[1]), (bounds[2], bounds[3])])
        else:
            geometries.append(None)

    if not all_coords:
        return _serialize_empty(result, schema)

    xs = [c[0] for c in all_coords]
    ys = [c[1] for c in all_coords]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)

    x_range = x_max - x_min if x_max != x_min else 1.0
    y_range = y_max - y_min if y_max != y_min else 1.0
    x_scale = x_range / QUANTIZE_RESOLUTION
    y_scale = y_range / QUANTIZE_RESOLUTION

    transform = feature_result.transform
    transform.quantizeOriginPostion = pb.FeatureCollectionPBuffer.upperLeft
    transform.scale.xScale = x_scale
    transform.scale.yScale = y_scale
    transform.translate.xTranslate = x_min
    transform.translate.yTranslate = y_max  # upper-left origin

    # Second pass: encode features
    field_names = [f["name"] for f in schema.fields]

    for i in range(result.features.num_rows):
        feature = feature_result.features.add()

        # Attributes (in field order)
        for field_name in field_names:
            if field_name in table_dict:
                val = table_dict[field_name][i]
                _set_value(
                    feature.attributes.add(),
                    val,
                    _get_field_type(schema, field_name),
                )

        # Geometry
        geom = geometries[i]
        if geom is not None:
            _encode_geometry(
                feature.geometry, geom, x_min, y_max, x_scale, y_scale
            )

    feature_result.exceededTransferLimit = result.exceeded_transfer_limit

    return fc.SerializeToString()


def _encode_geometry(
    pb_geom,
    shapely_geom,
    x_translate: float,
    y_translate: float,
    x_scale: float,
    y_scale: float,
):
    """
    Encode a Shapely geometry into an Esri PBF Geometry message.

    Steps:
    1. Extract coordinate rings/paths from shapely geometry
    2. Quantize to integers
    3. Delta-encode
    4. Set lengths + coords arrays
    """
    coord_arrays = _extract_coord_arrays(shapely_geom)

    all_delta_coords = []
    lengths = []

    for ring_coords in coord_arrays:
        lengths.append(len(ring_coords))
        prev_x, prev_y = 0, 0

        for wx, wy in ring_coords:
            # Quantize
            qx = round((wx - x_translate) / x_scale)
            qy = round((wy - y_translate) / y_scale)
            # Delta encode
            dx = qx - prev_x
            dy = qy - prev_y
            all_delta_coords.extend([dx, dy])
            prev_x, prev_y = qx, qy

    pb_geom.lengths.extend(lengths)
    pb_geom.coords.extend(all_delta_coords)


def _extract_coord_arrays(geom):
    """Extract coordinate arrays from a Shapely geometry.

    Returns list of rings/paths, where each is a list of (x, y) tuples.
    """
    geom_type = geom.geom_type

    if geom_type == "Point":
        return [[(geom.x, geom.y)]]
    elif geom_type == "MultiPoint":
        return [[(p.x, p.y)] for p in geom.geoms]
    elif geom_type == "LineString":
        return [list(geom.coords)]
    elif geom_type == "MultiLineString":
        return [list(line.coords) for line in geom.geoms]
    elif geom_type == "Polygon":
        rings = [list(geom.exterior.coords)]
        for interior in geom.interiors:
            rings.append(list(interior.coords))
        return rings
    elif geom_type == "MultiPolygon":
        rings = []
        for poly in geom.geoms:
            rings.append(list(poly.exterior.coords))
            for interior in poly.interiors:
                rings.append(list(interior.coords))
        return rings
    return []


def _set_value(pb_value, python_val, field_type: str):
    """Set a protobuf Value message from a Python value."""
    if python_val is None:
        return

    if hasattr(python_val, "as_py"):
        python_val = python_val.as_py()

    if python_val is None:
        return

    if field_type in ("string", "esriFieldTypeString"):
        pb_value.string_value = str(python_val)
    elif field_type in ("int32", "esriFieldTypeSmallInteger"):
        pb_value.sint_value = int(python_val)
    elif field_type in ("int64", "esriFieldTypeInteger", "esriFieldTypeOID"):
        pb_value.int64_value = int(python_val)
    elif field_type in ("float", "esriFieldTypeSingle"):
        pb_value.float_value = float(python_val)
    elif field_type in ("double", "esriFieldTypeDouble"):
        pb_value.double_value = float(python_val)
    elif field_type in ("boolean",):
        pb_value.bool_value = bool(python_val)
    else:
        pb_value.string_value = str(python_val)


def _build_fields(feature_result, schema: FeatureSchema):
    """Add field definitions to the PBF FeatureResult."""
    type_map = {
        "string": pb.FeatureCollectionPBuffer.esriFieldTypeString,
        "int32": pb.FeatureCollectionPBuffer.esriFieldTypeSmallInteger,
        "int64": pb.FeatureCollectionPBuffer.esriFieldTypeInteger,
        "float": pb.FeatureCollectionPBuffer.esriFieldTypeSingle,
        "double": pb.FeatureCollectionPBuffer.esriFieldTypeDouble,
        "date": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
        "timestamp": pb.FeatureCollectionPBuffer.esriFieldTypeDate,
    }
    for f in schema.fields:
        field = feature_result.fields.add()
        field.name = f["name"]
        field.alias = f.get("alias", f["name"])
        field.fieldType = type_map.get(
            f["type"],
            pb.FeatureCollectionPBuffer.esriFieldTypeString,
        )


def _get_field_type(schema: FeatureSchema, field_name: str) -> str:
    """Look up field type from schema."""
    for f in schema.fields:
        if f["name"] == field_name:
            return f["type"]
    return "string"


def _serialize_empty(result: QueryResult, schema: FeatureSchema) -> bytes:
    """Serialize an empty result set."""
    fc = pb.FeatureCollectionPBuffer()
    query_result = fc.queryResult

    if result.features is None and result.count > 0:
        # Count-only result
        query_result.countResult.count = result.count
        return fc.SerializeToString()

    feature_result = query_result.featureResult
    feature_result.spatialReference.wkid = schema.srid
    _build_fields(feature_result, schema)
    feature_result.exceededTransferLimit = False
    return fc.SerializeToString()
