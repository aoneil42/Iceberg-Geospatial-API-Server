import { loadGeoParquet } from "./geoarrow";
import type { Table } from "apache-arrow";

const API_BASE = "/api";

/** Feature limits per geometry type.
 *  Points/lines are cheap to render; polygons require earcut triangulation
 *  which is O(n·vertices) so we keep that limit much lower.
 *  10K polygons ≈ 2–3s earcut; 50K was causing timeouts where moveend
 *  reloads restart earcut before it finishes, making polygons never render. */
export const MAX_FEATURES_POINT = 200_000;
export const MAX_FEATURES_LINE = 200_000;
export const MAX_FEATURES_POLYGON = 10_000;

export type Bbox = [number, number, number, number];

/**
 * Load a layer as a GeoArrow Arrow Table.
 * Optionally pass a viewport bbox and a per-geometry-type feature limit.
 */
export async function loadLayer(
  namespace: string,
  layer: string,
  bbox?: Bbox,
  maxFeatures: number = MAX_FEATURES_POINT
): Promise<Table> {
  const params = new URLSearchParams({ limit: String(maxFeatures) });
  if (bbox) {
    params.set("bbox", bbox.join(","));
  }
  return loadGeoParquet(
    `${API_BASE}/features/${namespace}/${layer}?${params}`
  );
}

export async function fetchNamespaces(): Promise<string[]> {
  const resp = await fetch(`${API_BASE}/namespaces`);
  if (!resp.ok) throw new Error(`Failed to fetch namespaces: ${resp.status}`);
  return resp.json();
}

export async function fetchTables(namespace: string): Promise<string[]> {
  const resp = await fetch(`${API_BASE}/tables/${namespace}`);
  if (!resp.ok) throw new Error(`Failed to fetch tables: ${resp.status}`);
  return resp.json();
}

export async function fetchBbox(
  namespace: string
): Promise<Bbox> {
  const resp = await fetch(`${API_BASE}/bbox/${namespace}`);
  if (!resp.ok) throw new Error(`Failed to fetch bbox: ${resp.status}`);
  const data = await resp.json();
  return data.bbox;
}

export async function fetchTableBbox(
  namespace: string,
  table: string
): Promise<Bbox> {
  const resp = await fetch(`${API_BASE}/bbox/${namespace}/${table}`);
  if (!resp.ok)
    throw new Error(`Failed to fetch table bbox: ${resp.status}`);
  const data = await resp.json();
  return data.bbox;
}
