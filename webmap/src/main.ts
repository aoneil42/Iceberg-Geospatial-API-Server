import "./style.css";
import {
  loadBasemapConfig,
  initMap,
  setLayers,
  getMap,
  getOverlay,
  getBasemaps,
  getCurrentBasemapIndex,
  switchBasemap,
  pickObjectsInRect,
  flyToBounds,
  getViewportBbox,
  onMoveEnd,
} from "./map";
import {
  loadLayer,
  fetchNamespaces,
  fetchNamespaceTree,
  fetchTables,
  fetchTableBbox,
  expandBbox,
  MAX_FEATURES_POINT,
  MAX_FEATURES_LINE,
  MAX_FEATURES_POLYGON,
} from "./queries";
import type { Bbox } from "./queries";
import { buildAutoLayer, buildAggregateLayer, detectGeomType } from "./layers";
import type { FeatureClickHandler, GeomType } from "./layers";
import type { GeoArrowPickingInfo } from "@geoarrow/deck.gl-layers";
import type { Table, Vector } from "apache-arrow";
import {
  initCatalogBrowser,
  buildCatalogTree,
  updateTreeLayerCount,
  setTreeLayerLoading,
  setTreeLayerChecked,
  setStatus,
  showPopup,
  hidePopup,
  initIdentifyToggle,
  initBasemapPicker,
  showIdentifyPanel,
  hideIdentifyPanel,
  addIdentifyResult,
  clearIdentifyResults,
  deactivateIdentifyButton,
  debugLog,
} from "./ui";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** Loaded Arrow tables keyed by "namespace/layer" */
const tables = new Map<string, Table>();
/** Detected geometry type per loaded table key */
const geomTypes = new Map<string, GeomType>();
/** Set of currently visible layer keys ("namespace/layer") */
const visibleSet = new Set<string>();
/** Viewport bbox used for the most recent load, per layer key */
const loadedBbox = new Map<string, Bbox | undefined>();
/** Keys currently being loaded — prevents moveend from re-triggering loads. */
const loadingKeys = new Set<string>();
/** Cooldown guard for polygon layers — prevents moveend from reloading a layer
 *  while earcut triangulation is likely still running.  If we reload the data
 *  during earcut, deck.gl receives a new table reference → restarts earcut
 *  from scratch → the layer never finishes rendering. */
const earcutCooldownKeys = new Set<string>();
/** Zoom level at which each layer was last loaded */
const loadedZoom = new Map<string, number>();
/** Layer keys currently displayed in aggregate (grid-binned) mode */
const aggregatedKeys = new Set<string>();

/** Below this zoom level, request server-side grid aggregation */
const AGGREGATE_ZOOM_THRESHOLD = 11;

/** Map zoom level to grid cell size in degrees */
function getAggregateResolution(zoom: number): number {
  if (zoom <= 3) return 5.0;
  if (zoom <= 5) return 2.0;
  if (zoom <= 7) return 0.5;
  if (zoom <= 9) return 0.1;
  return 0.05; // zoom 10
}

let identifyActive = false;
let lastMouseX = 0;
let lastMouseY = 0;

// ---------------------------------------------------------------------------
// Layer rebuild
// ---------------------------------------------------------------------------

/** Draw-order rank: polygons at bottom, lines in middle, points on top */
const GEOM_ORDER: Record<GeomType, number> = {
  polygon: 0,
  unknown: 1,
  line: 2,
  point: 3,
};

function rebuildLayers() {
  // Collect all visible layer keys, sorted by geometry draw order
  const visibleKeys = [...visibleSet].filter((k) => tables.has(k));
  visibleKeys.sort((a, b) => {
    const ga = geomTypes.get(a) ?? "unknown";
    const gb = geomTypes.get(b) ?? "unknown";
    return GEOM_ORDER[ga] - GEOM_ORDER[gb];
  });

  debugLog(`rebuildLayers: ${visibleKeys.length} visible: ${visibleKeys.map((k) => `${k}(${geomTypes.get(k)})`).join(", ")}`);

  const layers: ReturnType<typeof buildAutoLayer>[] = [];
  for (const key of visibleKeys) {
    try {
      const t = tables.get(key)!;
      let l;
      if (aggregatedKeys.has(key)) {
        l = buildAggregateLayer(t, true, handleClick, key);
        debugLog(`  → ${key}: aggregate bubble layer (${t.numRows} cells)`);
      } else {
        debugLog(`  building ${key}: ${t.numRows} rows, ${t.batches.length} batches`);
        l = buildAutoLayer(t, true, handleClick, key);
        debugLog(`  → ${key}: ${l.constructor.name} created OK`);
      }
      layers.push(l);
    } catch (e) {
      debugLog(`  → ${key}: FAILED: ${(e as Error).message}`, "err");
      setStatus(`Error building ${key}: ${(e as Error).message}`);
      // Don't throw — keep other layers working
    }
  }

  debugLog(`setLayers(${layers.length} layers)`);
  setLayers(layers);

  // Check sub-layer state after earcut should have completed
  for (const key of visibleKeys) {
    const gt = geomTypes.get(key);
    if (gt === "polygon") {
      debugCheckDeckLayers(key);
    }
  }
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

const GEOM_ABBREV: Record<GeomType, string> = {
  point: "pts",
  line: "lines",
  polygon: "polys",
  unknown: "feat",
};

function updateStatusBar() {
  let total = 0;
  const parts: string[] = [];

  for (const key of visibleSet) {
    const table = tables.get(key);
    if (!table) continue;
    const n = table.numRows;
    total += n;
    const isAgg = aggregatedKeys.has(key);
    const gt = geomTypes.get(key) ?? "unknown";
    const suffix = isAgg ? "clusters" : GEOM_ABBREV[gt];
    parts.push(`${key}: ${n.toLocaleString()} ${suffix}`);
  }

  if (total === 0) {
    setStatus("No layers visible");
    return;
  }

  setStatus(
    `${total.toLocaleString()} features \u2014 ${parts.join(" \u00b7 ")}`
  );
}

// ---------------------------------------------------------------------------
// Click / Identify
// ---------------------------------------------------------------------------

const handleClick: FeatureClickHandler = (info) => {
  if (identifyActive) {
    addIdentifyResult(info);
  } else {
    const canvas = getMap().getCanvas();
    const rect = canvas.getBoundingClientRect();
    showPopup(info, lastMouseX - rect.left, lastMouseY - rect.top);
  }
};

// ---------------------------------------------------------------------------
// Layer loading with viewport bbox
// ---------------------------------------------------------------------------

/** Return the appropriate feature limit for a given geometry type and zoom.
 *  Polygon limits scale with zoom: simplified polygons at low zoom are cheap,
 *  so we allow more; full-resolution polygons at high zoom stay conservative. */
function getEffectiveLimit(gt?: GeomType, zoom?: number): number {
  switch (gt) {
    case "polygon": {
      const z = zoom ?? 12;
      if (z < 8) return 50_000;   // simplified — earcut is fast
      if (z < 12) return 20_000;  // moderate simplification
      return MAX_FEATURES_POLYGON; // full resolution
    }
    case "line":
      return MAX_FEATURES_LINE;
    case "point":
      return MAX_FEATURES_POINT;
    default:
      return MAX_FEATURES_POLYGON; // conservative for unknown
  }
}

/** Compute simplification tolerance for a given zoom level.
 *  Returns undefined above zoom 12 (full resolution). */
function getSimplifyTolerance(zoom: number): number | undefined {
  if (zoom >= 12) return undefined;
  return 360 / (Math.pow(2, zoom) * 256);
}

/** Scale earcut cooldown based on polygon count. */
function getEarcutCooldown(numRows: number): number {
  if (numRows < 1_000) return 2_000;
  if (numRows < 10_000) return 5_000;
  if (numRows < 50_000) return 10_000;
  return 20_000;
}

/** Load (or reload) a layer using the current viewport bbox.
 *  At low zoom levels, requests server-side grid aggregation instead of
 *  individual features to avoid WASM OOM on massive datasets. */
async function loadLayerWithViewport(
  ns: string,
  layer: string
): Promise<void> {
  const key = `${ns}/${layer}`;

  // Skip if this layer is already being loaded (prevents moveend race).
  if (loadingKeys.has(key)) return;
  loadingKeys.add(key);

  const viewportBbox = getViewportBbox();
  // Fetch 1.5x the viewport so small pans don't trigger refetches
  const fetchBbox = expandBbox(viewportBbox, 1.5);
  const knownType = geomTypes.get(key);
  const zoom = getMap().getZoom();
  const useAggregate = zoom < AGGREGATE_ZOOM_THRESHOLD;

  setTreeLayerLoading(ns, layer, true);
  try {
    if (useAggregate) {
      // --- Aggregated mode: server returns grid-binned centroids + counts ---
      const resolution = getAggregateResolution(zoom);
      const table = await loadLayer(
        ns, layer, fetchBbox, 50_000, undefined,
        { resolution }
      );
      tables.set(key, table);
      // Preserve original geom type if known; aggregated data is always points
      if (!knownType) geomTypes.set(key, "point");
      aggregatedKeys.add(key);
      loadedBbox.set(key, fetchBbox);
      loadedZoom.set(key, zoom);
      updateTreeLayerCount(ns, layer, table.numRows);
      debugLog(
        `loaded ${key} AGGREGATED: ${table.numRows} cells, resolution=${resolution}`
      );
    } else {
      // --- Full-resolution mode ---
      aggregatedKeys.delete(key);
      const limit = getEffectiveLimit(knownType, zoom);
      const simplify = knownType === "polygon" ? getSimplifyTolerance(zoom) : undefined;

      const table = await loadLayer(ns, layer, fetchBbox, limit, simplify);
      const gt = detectGeomType(table);
      tables.set(key, table);
      geomTypes.set(key, gt);
      loadedBbox.set(key, fetchBbox);
      loadedZoom.set(key, zoom);
      updateTreeLayerCount(ns, layer, table.numRows);

      debugLog(`loaded ${key}: ${table.numRows} rows, geomType=${gt}, batches=${table.batches.length}`);
      for (const f of table.schema.fields) {
        const ext = f.metadata.get("ARROW:extension:name") ?? "";
        debugLog(`  field: ${f.name}  type=${f.type}  ext=${ext}`);
      }

      if (gt === "polygon") {
        debugLogGeometry(table, key);
      }

      // If this was the first load and the type is point or line, we used a
      // conservative initial limit.  Re-fetch with the proper higher limit.
      if (!knownType && (gt === "point" || gt === "line")) {
        const higherLimit = getEffectiveLimit(gt, zoom);
        if (table.numRows >= limit && higherLimit > limit) {
          debugLog(`re-fetching ${key} with ${gt} limit (${higherLimit})`);
          const bigger = await loadLayer(ns, layer, fetchBbox, higherLimit);
          tables.set(key, bigger);
          geomTypes.set(key, gt);
          updateTreeLayerCount(ns, layer, bigger.numRows);
        }
      }

      // For polygon layers, set a dynamic cooldown that scales with feature count
      if (gt === "polygon") {
        const cooldown = getEarcutCooldown(table.numRows);
        earcutCooldownKeys.add(key);
        setTimeout(() => {
          earcutCooldownKeys.delete(key);
          debugLog(`earcut cooldown expired for ${key} (${cooldown}ms)`);
        }, cooldown);
      }
    }
  } catch (e) {
    console.error(`Failed to load ${key}:`, e);
    debugLog(`LOAD ERROR ${key}: ${(e as Error).message}`, "err");
    // If we already had cached data, keep the layer visible with stale data.
    // Only uncheck the layer if this was the first load (no cached data).
    if (!tables.has(key)) {
      visibleSet.delete(key);
      setTreeLayerChecked(ns, layer, false);
    }
    setStatus(`Failed to load ${key}: ${(e as Error).message}`);
  }
  setTreeLayerLoading(ns, layer, false);
  loadingKeys.delete(key);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  setStatus("Initializing...");

  // Load basemap configuration before map init
  const basemapConfigs = await loadBasemapConfig();
  const map = initMap(basemapConfigs[0].style);

  // Discover available namespaces from the catalog (tree endpoint preferred)
  let namespacePaths: string[][];
  try {
    namespacePaths = await fetchNamespaceTree();
  } catch {
    // Fallback: flat namespace list wrapped as single-segment paths
    const flatNs = await fetchNamespaces().catch(() => ["colorado"]);
    namespacePaths = flatNs.map((ns) => [ns]);
  }

  // Discover tables per namespace (in parallel)
  setStatus("Discovering tables...");
  const dottedPaths = namespacePaths.map((p) => p.join("."));
  const tablesPerNs = await Promise.all(
    dottedPaths.map(async (ns) => {
      try {
        const tblNames = await fetchTables(ns);
        return [ns, tblNames] as const;
      } catch {
        return [ns, []] as const;
      }
    })
  );
  const tablesMap = Object.fromEntries(tablesPerNs);

  // Build recursive catalog tree and render in sidebar
  const tree = buildCatalogTree(namespacePaths, tablesMap);
  initCatalogBrowser(tree, handleLayerToggle, handleZoom, handleRefresh);
  setStatus("No layers visible");

  // --- Sidebar toggle ---
  const sidebar = document.getElementById("sidebar");
  const sidebarToggle = document.getElementById("sidebar-toggle");
  if (sidebar && sidebarToggle) {
    sidebarToggle.addEventListener("click", () => {
      const collapsed = sidebar.classList.toggle("collapsed");
      sidebarToggle.classList.toggle("collapsed", collapsed);
      sidebarToggle.innerHTML = collapsed ? "&#x276F;" : "&#x276E;";
      sidebarToggle.title = collapsed ? "Expand sidebar" : "Collapse sidebar";
      // Let the CSS transition finish, then tell MapLibre the container changed
      setTimeout(() => map.resize(), 260);
    });
  }

  // --- Mouse tracking for popups ---
  map.getCanvas().addEventListener("mousemove", (e) => {
    lastMouseX = e.clientX;
    lastMouseY = e.clientY;
  });

  map.on("click", () => {
    if (!identifyActive) hidePopup();
  });

  // -----------------------------------------------------------------------
  // Viewport-aware reload on pan/zoom (debounced)
  // -----------------------------------------------------------------------

  let reloadTimer: ReturnType<typeof setTimeout> | null = null;

  onMoveEnd(() => {
    if (visibleSet.size === 0) return;
    if (reloadTimer) clearTimeout(reloadTimer);
    reloadTimer = setTimeout(reloadVisibleLayers, 400);
  });

  async function reloadVisibleLayers() {
    const bbox = getViewportBbox();
    const currentZoom = getMap().getZoom();
    const shouldAggregate = currentZoom < AGGREGATE_ZOOM_THRESHOLD;

    const reloadKeys = [...visibleSet].filter((key) => {
      if (loadingKeys.has(key)) return false;
      if (earcutCooldownKeys.has(key)) return false;

      // Always reload if we crossed the aggregate/full-res threshold
      const wasAggregated = aggregatedKeys.has(key);
      if (wasAggregated !== shouldAggregate) return true;

      // In aggregate mode, reload if the resolution bucket changed
      if (shouldAggregate) {
        const prevZoom = loadedZoom.get(key);
        if (prevZoom !== undefined) {
          if (getAggregateResolution(prevZoom) !== getAggregateResolution(currentZoom))
            return true;
        }
      }

      const prev = loadedBbox.get(key);
      if (!prev) return true;
      return !bboxContains(prev, bbox);
    });
    if (reloadKeys.length === 0) return;

    await Promise.all(
      reloadKeys.map((key) => {
        const [ns, layer] = splitKey(key);
        return loadLayerWithViewport(ns, layer);
      })
    );
    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Layer toggle handler (called from tree UI)
  // -----------------------------------------------------------------------

  async function handleLayerToggle(
    ns: string,
    layer: string,
    visible: boolean
  ) {
    const key = `${ns}/${layer}`;

    if (visible) {
      visibleSet.add(key);

      // Load on demand
      if (!tables.has(key)) {
        await loadLayerWithViewport(ns, layer);
      }
    } else {
      visibleSet.delete(key);
      aggregatedKeys.delete(key);
      loadedZoom.delete(key);
    }

    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Refresh handler
  // -----------------------------------------------------------------------

  async function handleRefresh() {
    debugLog("Manual refresh triggered");
    // Clear bbox + cooldown + aggregation caches but keep table data so a
    // failed reload doesn't lose the layer.
    for (const key of [...visibleSet]) {
      loadedBbox.delete(key);
      loadedZoom.delete(key);
      earcutCooldownKeys.delete(key);
      aggregatedKeys.delete(key);
    }
    setStatus("Refreshing...");
    await Promise.all(
      [...visibleSet].map((key) => {
        const [ns, layer] = splitKey(key);
        return loadLayerWithViewport(ns, layer);
      })
    );
    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Zoom-to-extent handler (called from tree zoom button)
  // -----------------------------------------------------------------------

  async function handleZoom(ns: string, layer: string) {
    setStatus(`Fetching extent for ${ns}/${layer}...`);
    try {
      const bbox = await fetchTableBbox(ns, layer);
      flyToBounds(bbox);

      // If the layer isn't visible yet, turn it on and load it
      const key = `${ns}/${layer}`;
      if (!visibleSet.has(key)) {
        visibleSet.add(key);
        setTreeLayerChecked(ns, layer, true);

        // Wait a moment for the fly animation to start, then load with the
        // new viewport (the moveend handler will also reload)
        setTimeout(async () => {
          await loadLayerWithViewport(ns, layer);
          rebuildLayers();
          updateStatusBar();
        }, 600);
      }
    } catch (e) {
      console.error(`Failed to get bbox for ${ns}/${layer}:`, e);
    }
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Identify mode
  // -----------------------------------------------------------------------

  const mapEl = document.getElementById("map")!;
  const selectBox = document.getElementById("select-box")!;
  let dragStart: { x: number; y: number } | null = null;

  function processBoxSelection(
    x: number,
    y: number,
    width: number,
    height: number
  ) {
    const results = pickObjectsInRect(x, y, width, height);
    for (const info of results) {
      const geoInfo = info as GeoArrowPickingInfo;
      if (!geoInfo.object) continue;
      const row = geoInfo.object.toJSON();

      const layerId = (info.layer?.id as string) ?? "";
      const slash = layerId.lastIndexOf("/");
      const nsName = slash >= 0 ? layerId.slice(0, slash) : "";
      const kind = slash >= 0 ? layerId.slice(slash + 1) : layerId;

      const props: Record<string, unknown> = {
        type: nsName ? `${nsName}/${kind}` : kind,
      };
      for (const [k, v] of Object.entries(row)) {
        if (k !== "geometry") props[k] = v;
      }
      addIdentifyResult(props);
    }
  }

  map.getCanvas().addEventListener("pointerdown", (e) => {
    if (!identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    dragStart = { x: e.clientX - rect.left, y: e.clientY - rect.top };
  });

  window.addEventListener("pointermove", (e) => {
    if (!dragStart || !identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const dx = Math.abs(cx - dragStart.x);
    const dy = Math.abs(cy - dragStart.y);
    if (dx + dy > 5) {
      map.dragPan.disable();
      selectBox.style.display = "block";
      selectBox.style.left = `${Math.min(dragStart.x, cx) + rect.left}px`;
      selectBox.style.top = `${Math.min(dragStart.y, cy) + rect.top}px`;
      selectBox.style.width = `${dx}px`;
      selectBox.style.height = `${dy}px`;
    }
  });

  window.addEventListener("pointerup", (e) => {
    if (!dragStart || !identifyActive) return;
    const rect = map.getCanvas().getBoundingClientRect();
    const cx = e.clientX - rect.left;
    const cy = e.clientY - rect.top;
    const dx = Math.abs(cx - dragStart.x);
    const dy = Math.abs(cy - dragStart.y);

    selectBox.style.display = "none";
    const wasDrag = dx + dy > 5;
    const start = dragStart;
    dragStart = null;
    map.dragPan.enable();

    if (wasDrag) {
      const bx = Math.min(start.x, cx);
      const by = Math.min(start.y, cy);
      processBoxSelection(bx, by, dx, dy);
    }
  });

  initIdentifyToggle((active) => {
    identifyActive = active;
    if (active) {
      hidePopup();
      mapEl.classList.add("identify-cursor");
      showIdentifyPanel(() => clearIdentifyResults());
    } else {
      mapEl.classList.remove("identify-cursor");
      hideIdentifyPanel();
      dragStart = null;
      selectBox.style.display = "none";
    }
  });

  // Basemap picker (only shows when multiple basemaps configured)
  initBasemapPicker(getBasemaps(), getCurrentBasemapIndex(), (index) => {
    switchBasemap(index);
  });
}

// ---------------------------------------------------------------------------
// Diagnostics — geometry data inspection
// ---------------------------------------------------------------------------

/** Log raw coordinate values from the Arrow table to verify data integrity */
function debugLogGeometry(table: Table, key: string): void {
  try {
    const geomCol = table.getChild("geometry") as Vector | null;
    if (!geomCol || geomCol.data.length === 0) {
      debugLog(`${key}: no geometry column or empty data`, "warn");
      return;
    }

    const batch = geomCol.data[0];
    debugLog(`${key}: geom batch0: length=${batch.length}, type=${batch.type}, children=${batch.children.length}`);

    // Navigate nested List<List<FixedSizeList[2]<Float64>>> to raw coordinates
    let d = batch;
    let depth = 0;
    const typePath: string[] = [String(d.type)];
    while (d.children?.length > 0 && depth < 6) {
      d = d.children[0];
      depth++;
      typePath.push(String(d.type));
    }
    debugLog(`${key}: nested path (depth=${depth}): ${typePath.join(" → ")}`);

    // Log offsets at each level for the first few polygons
    const b0 = batch;
    if (b0.valueOffsets) {
      const offs = Array.from(b0.valueOffsets.slice(0, 6));
      debugLog(`${key}: polygon offsets[0..5]: ${offs.join(", ")}`);
    }
    if (b0.children[0]?.valueOffsets) {
      const offs = Array.from(b0.children[0].valueOffsets.slice(0, 10));
      debugLog(`${key}: ring offsets[0..9]: ${offs.join(", ")}`);
    }

    // Extract raw Float64 coordinate values
    if (d.values instanceof Float64Array) {
      const v = d.values;
      debugLog(`${key}: raw Float64Array length=${v.length}`);
      const n = Math.min(20, v.length);
      const sample = Array.from(v.slice(0, n)).map((x) => x.toFixed(6));
      debugLog(`${key}: first ${n} coord values: [${sample.join(", ")}]`);
    } else {
      debugLog(`${key}: no Float64Array found at depth ${depth}`, "warn");
    }
  } catch (e) {
    debugLog(`${key}: geometry debug error: ${(e as Error).message}`, "err");
  }
}

/** Check the deck overlay for sub-layer state after earcut should have completed */
function debugCheckDeckLayers(key: string): void {
  setTimeout(() => {
    try {
      const overlay = getOverlay() as any;
      if (!overlay) {
        debugLog(`${key}: overlay not available`, "warn");
        return;
      }
      // Access the internal deck instance
      const deck = overlay._deck;
      if (!deck) {
        debugLog(`${key}: deck not available on overlay`, "warn");
        return;
      }
      const allLayers = deck.props?.layers ?? [];
      debugLog(`${key}: deck.props.layers count=${allLayers.length}`);

      // Check the layer manager for active layers
      const layerManager = deck.layerManager;
      if (layerManager) {
        const layers = layerManager.getLayers();
        debugLog(`${key}: layerManager has ${layers.length} total layers`);
        for (const layer of layers) {
          const id = layer.id ?? "?";
          const vis = layer.props?.visible ?? "?";
          const numInstances = layer.getNumInstances?.() ?? "?";
          const subLayers = layer.isComposite
            ? layer.getSubLayers?.()?.length ?? 0
            : "n/a";
          debugLog(
            `${key}: layer ${id}: visible=${vis}, instances=${numInstances}, subLayers=${subLayers}`
          );

          // For GeoArrow polygon layers, check state.triangles
          if (layer.state?.triangles !== undefined) {
            const tri = layer.state.triangles;
            if (tri === null) {
              debugLog(`${key}: ${id} state.triangles = NULL (earcut not done!)`, "warn");
            } else if (Array.isArray(tri)) {
              const sizes = tri.map((t: any) =>
                t instanceof Uint32Array ? t.length : String(typeof t)
              );
              debugLog(`${key}: ${id} state.triangles: [${sizes.join(", ")}] indices`);
            }
          }
        }
      }
    } catch (e) {
      debugLog(`${key}: deck inspection error: ${(e as Error).message}`, "err");
    }
  }, 5000); // Check 5 seconds after layer build
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function splitKey(key: string): [string, string] {
  const slash = key.indexOf("/");
  return [key.slice(0, slash), key.slice(slash + 1)];
}

/** Check if bounding box `outer` fully contains `inner`. */
function bboxContains(outer: Bbox, inner: Bbox): boolean {
  return (
    outer[0] <= inner[0] &&
    outer[1] <= inner[1] &&
    outer[2] >= inner[2] &&
    outer[3] >= inner[3]
  );
}

// Catch unhandled promise rejections — deck.gl's GeoArrowSolidPolygonLayer
// calls updateData() without awaiting it, so earcut errors become unhandled
// rejections that silently prevent rendering.
window.addEventListener("unhandledrejection", (event) => {
  const msg =
    event.reason instanceof Error
      ? event.reason.message
      : String(event.reason);
  const stack =
    event.reason instanceof Error ? event.reason.stack ?? "" : "";
  debugLog(`UNHANDLED REJECTION: ${msg}`, "err");
  if (stack) debugLog(`  stack: ${stack.split("\n").slice(0, 3).join(" | ")}`, "err");
  console.error("[webmap] Unhandled rejection:", event.reason);
  setStatus(`⚠ ${msg}`);
});

window.addEventListener("error", (event) => {
  debugLog(`GLOBAL ERROR: ${event.message} at ${event.filename}:${event.lineno}`, "err");
});

main().catch((err) => {
  console.error("Webmap initialization failed:", err);
  setStatus(`Error: ${err.message}`);
});

// ---------------------------------------------------------------------------
// Tier 3 Agent Integration (feature-flagged — no-op when VITE_AGENT_ENABLED != "true")
// ---------------------------------------------------------------------------

if (import.meta.env.VITE_AGENT_ENABLED === "true") {
  import("./agent-ws").then(({ AgentWebSocket }) => {
    import("./chat-panel").then(({ ChatPanel }) => {
      const sessionId = crypto.randomUUID();
      const panel = new ChatPanel(sessionId);
      let wsClient: InstanceType<typeof AgentWebSocket> | null = null;
      let panelOpen = false;

      // Debounced layer_ready batching — collect rapid-fire events
      type LREvent = import("./agent-ws").LayerReadyEvent;
      let pendingEvents: LREvent[] = [];
      let debounceTimer: ReturnType<typeof setTimeout> | null = null;

      function queueLayerReady(event: LREvent) {
        pendingEvents.push(event);
        if (debounceTimer) clearTimeout(debounceTimer);
        debounceTimer = setTimeout(flushPendingLayers, 300);
      }

      async function flushPendingLayers() {
        const batch = pendingEvents.splice(0);
        debounceTimer = null;
        if (batch.length === 0) return;

        // Load all queued layers in parallel
        const results = await Promise.allSettled(
          batch.map(async (event) => {
            const key = `${event.namespace}/${event.table}`;
            const arrowTable = await loadLayer(event.namespace, event.table);
            if (!arrowTable) return null;
            const geomType = detectGeomType(arrowTable);
            if (!geomType) return null;

            tables.set(key, arrowTable);
            geomTypes.set(key, geomType);
            visibleSet.add(key);
            updateTreeLayerCount(event.namespace, event.table, event.row_count);
            return event;
          }),
        );

        rebuildLayers();
        updateStatusBar();

        // Fly to union bbox of all successful loads
        const bboxes = results
          .filter(
            (r): r is PromiseFulfilledResult<LREvent | null> =>
              r.status === "fulfilled" && r.value?.bbox != null,
          )
          .map((r) => r.value!.bbox!);
        if (bboxes.length > 0) {
          const union: [number, number, number, number] = [
            Math.min(...bboxes.map((b) => b[0])),
            Math.min(...bboxes.map((b) => b[1])),
            Math.max(...bboxes.map((b) => b[2])),
            Math.max(...bboxes.map((b) => b[3])),
          ];
          flyToBounds(union);
        }
      }

      // Create toggle button
      const toggleBtn = document.createElement("button");
      toggleBtn.id = "agent-toggle-btn";
      toggleBtn.textContent = "\u{1F4AC} Agent";
      document.body.appendChild(toggleBtn);

      toggleBtn.addEventListener("click", () => {
        panelOpen = !panelOpen;
        if (panelOpen) {
          panel.mount(document.body);
          wsClient = new AgentWebSocket(
            sessionId,
            queueLayerReady,
            (connected) => panel.setAgentStatus(connected),
          );
          wsClient.connect();
          toggleBtn.textContent = "\u2715 Close";
        } else {
          panel.unmount();
          wsClient?.disconnect();
          wsClient = null;
          // Clear pending events on panel close
          pendingEvents = [];
          if (debounceTimer) {
            clearTimeout(debounceTimer);
            debounceTimer = null;
          }
          toggleBtn.textContent = "\u{1F4AC} Agent";
        }
      });
    });
  });
}
