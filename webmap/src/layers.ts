import {
  GeoArrowScatterplotLayer,
  GeoArrowPathLayer,
  GeoArrowSolidPolygonLayer,
} from "@geoarrow/deck.gl-layers";
import type { GeoArrowPickingInfo } from "@geoarrow/deck.gl-layers";
import type { Table } from "apache-arrow";
import type { Layer, Color, PickingInfo } from "@deck.gl/core";

// ---------------------------------------------------------------------------
// Geometry type detection from Arrow/GeoArrow metadata
// ---------------------------------------------------------------------------

export type GeomType = "point" | "line" | "polygon" | "unknown";

/**
 * Detect the geometry type from GeoArrow extension metadata on the geometry
 * column. Returns "point", "line", "polygon", or "unknown".
 */
export function detectGeomType(table: Table): GeomType {
  const geomField = table.schema.fields.find((f) => f.name === "geometry");
  if (!geomField) return "unknown";

  const extName =
    geomField.metadata.get("ARROW:extension:name")?.toLowerCase() ?? "";

  if (extName.includes("point")) return "point";
  if (extName.includes("linestring")) return "line";
  if (extName.includes("polygon")) return "polygon";
  return "unknown";
}

// ---------------------------------------------------------------------------
// Color palettes
// ---------------------------------------------------------------------------

const CATEGORY_COLORS: Record<string, Color> = {
  park: [34, 139, 34, 255],
  school: [30, 144, 255, 255],
  hospital: [220, 20, 60, 255],
  restaurant: [255, 165, 0, 255],
  gas_station: [128, 128, 128, 255],
  trailhead: [107, 142, 35, 255],
  campground: [139, 69, 19, 255],
  viewpoint: [148, 103, 189, 255],
  water_tower: [0, 191, 255, 255],
  fire_station: [255, 69, 0, 255],
};

const PARCEL_COLORS: Record<string, Color> = {
  residential: [65, 105, 225, 100],
  commercial: [255, 140, 0, 100],
  industrial: [169, 169, 169, 100],
  agricultural: [34, 139, 34, 100],
  public: [148, 103, 189, 100],
};

const LINE_COLORS: Record<string, Color> = {
  road: [80, 80, 80, 255],
  trail: [139, 90, 43, 255],
  highway: [220, 20, 60, 255],
  path: [107, 142, 35, 255],
  creek: [30, 144, 255, 255],
};

const DEFAULT_COLOR: Color = [100, 100, 100, 255];

/** Default polygon fill — visible blue at 63% opacity (used for datasets
 *  without a recognised category column like `parcel_type`). */
const DEFAULT_POLYGON_FILL: Color = [30, 144, 255, 160];

export type FeatureClickHandler = (info: Record<string, unknown>) => void;

const GEOM_LABEL: Record<GeomType, string> = {
  point: "Point",
  line: "Line",
  polygon: "Polygon",
  unknown: "Feature",
};

function pickingHandler(
  onClick?: FeatureClickHandler
): (info: PickingInfo) => void {
  return (info: PickingInfo) => {
    const geoInfo = info as GeoArrowPickingInfo;
    if (!geoInfo.object || !onClick) return;
    const row = geoInfo.object.toJSON();

    // Derive type label from layer id (e.g. "colorado/points" → "colorado Point")
    const layerId = (info.layer?.id as string) ?? "";
    const slash = layerId.lastIndexOf("/");
    const ns = slash >= 0 ? layerId.slice(0, slash) : "";
    const tableName = slash >= 0 ? layerId.slice(slash + 1) : layerId;

    const props: Record<string, unknown> = {
      type: ns ? `${ns}/${tableName}` : tableName,
    };
    for (const [key, value] of Object.entries(row)) {
      if (key !== "geometry") props[key] = value;
    }
    onClick(props);
  };
}

// ---------------------------------------------------------------------------
// Layer builders
// ---------------------------------------------------------------------------

export function buildPointLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "points"
): Layer {
  return new GeoArrowScatterplotLayer({
    id,
    data: table,
    visible,
    getFillColor: ({ index, data }) => {
      const category = data.data.getChild("category")?.get(index);
      return (CATEGORY_COLORS[category] ?? DEFAULT_COLOR) as Color;
    },
    getRadius: 300,
    radiusMinPixels: 3,
    radiusMaxPixels: 15,
    pickable: true,
    onClick: pickingHandler(onClick),
  });
}

export function buildLineLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "lines"
): Layer {
  return new GeoArrowPathLayer({
    id,
    data: table,
    visible,
    getColor: ({ index, data }) => {
      const name: string = data.data.getChild("name")?.get(index) ?? "";
      const lineType = name.split("_")[0];
      return (LINE_COLORS[lineType] ?? DEFAULT_COLOR) as Color;
    },
    getWidth: 2,
    widthMinPixels: 1,
    widthMaxPixels: 5,
    pickable: true,
    onClick: pickingHandler(onClick),
  });
}

export function buildPolygonLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id: string = "polygons"
): Layer {
  // Check if the table has a `parcel_type` column (colorado-style data).
  // If it does, use the per-feature colour accessor; otherwise use a
  // static fill colour so the polygons are clearly visible on any basemap.
  const hasParcelType = table.schema.fields.some(
    (f) => f.name === "parcel_type"
  );

  const fillColor: GeoArrowSolidPolygonLayer["props"]["getFillColor"] =
    hasParcelType
      ? ({ index, data }) => {
          const parcelType = data.data.getChild("parcel_type")?.get(index);
          return (PARCEL_COLORS[parcelType] ?? DEFAULT_POLYGON_FILL) as Color;
        }
      : DEFAULT_POLYGON_FILL;

  return new GeoArrowSolidPolygonLayer({
    id,
    data: table,
    visible,
    getFillColor: fillColor,
    pickable: true,
    onClick: pickingHandler(onClick),
    // Disable validation — the assert(isPolygonVector) can throw during
    // renderLayers for valid data when internal type detection is strict.
    _validate: false,
    // Enable normalization to handle all winding orders / coordinate edge cases.
    _normalize: true,
    // Log earcut timing to console for debugging.
    metrics: true,
  });
}

// ---------------------------------------------------------------------------
// Auto layer — picks the right builder based on geometry type
// ---------------------------------------------------------------------------

/**
 * Build the appropriate deck.gl layer based on the geometry type detected
 * from the Arrow table's GeoArrow extension metadata.
 */
export function buildAutoLayer(
  table: Table,
  visible: boolean,
  onClick?: FeatureClickHandler,
  id?: string
): Layer {
  const geomType = detectGeomType(table);
  switch (geomType) {
    case "point":
      return buildPointLayer(table, visible, onClick, id);
    case "line":
      return buildLineLayer(table, visible, onClick, id);
    case "polygon":
      return buildPolygonLayer(table, visible, onClick, id);
    default:
      // Fall back to polygon layer for unknown types
      return buildPolygonLayer(table, visible, onClick, id);
  }
}
