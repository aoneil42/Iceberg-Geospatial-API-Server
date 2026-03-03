import { loadGeoParquet } from "./geoarrow";
import type { Table } from "apache-arrow";

const API_BASE = "/api";

/** Feature limits per geometry type.
 *  The GeoArrow pipeline handles large datasets well; the real OOM guard is
 *  MAX_RESPONSE_BYTES (256 MB) in geoarrow.ts.  Earcut cooldowns in main.ts
 *  prevent moveend reloads from restarting triangulation mid-flight. */
export const MAX_FEATURES_POINT = 200_000;
export const MAX_FEATURES_LINE = 200_000;
export const MAX_FEATURES_POLYGON = 2_000_000;

export type Bbox = [number, number, number, number];

/**
 * Expand a bbox by a factor in each direction (1.5 = 50% padding).
 * Clamps longitude to [-180, 180] and latitude to [-90, 90].
 */
export function expandBbox(bbox: Bbox, factor: number): Bbox {
  const dx = (bbox[2] - bbox[0]) * (factor - 1) / 2;
  const dy = (bbox[3] - bbox[1]) * (factor - 1) / 2;
  return [
    Math.max(-180, bbox[0] - dx),
    Math.max(-90, bbox[1] - dy),
    Math.min(180, bbox[2] + dx),
    Math.min(90, bbox[3] + dy),
  ];
}

/**
 * Load a layer as a GeoArrow Arrow Table.
 * Optionally pass a viewport bbox, per-geometry-type feature limit,
 * and simplification tolerance (in degrees).
 */
export async function loadLayer(
  namespace: string,
  layer: string,
  bbox?: Bbox,
  maxFeatures: number = MAX_FEATURES_POINT,
  simplify?: number,
  aggregate?: { resolution: number }
): Promise<Table> {
  const params = new URLSearchParams({ limit: String(maxFeatures) });
  if (bbox) {
    params.set("bbox", bbox.join(","));
  }
  if (simplify !== undefined && simplify > 0) {
    params.set("simplify", String(simplify));
  }
  if (aggregate) {
    params.set("mode", "aggregate");
    params.set("resolution", String(aggregate.resolution));
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

/** Namespace path as array of segments, e.g. ["colorado", "water"] */
export type NamespacePath = string[];

export async function fetchNamespaceTree(): Promise<NamespacePath[]> {
  const resp = await fetch(`${API_BASE}/namespaces/tree`);
  if (!resp.ok)
    throw new Error(`Failed to fetch namespace tree: ${resp.status}`);
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
