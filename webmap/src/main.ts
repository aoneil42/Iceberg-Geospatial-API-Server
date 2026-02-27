import "./style.css";
import {
  initMap,
  setLayers,
  getMap,
  getOverlay,
  pickObjectsInRect,
  flyToBounds,
  getViewportBbox,
  onMoveEnd,
} from "./map";
import {
  loadLayer,
  fetchNamespaces,
  fetchTables,
  fetchTableBbox,
  MAX_FEATURES_POINT,
  MAX_FEATURES_LINE,
  MAX_FEATURES_POLYGON,
} from "./queries";
import type { Bbox } from "./queries";
import { buildAutoLayer, detectGeomType } from "./layers";
import type { FeatureClickHandler, GeomType } from "./layers";
import type { GeoArrowPickingInfo } from "@geoarrow/deck.gl-layers";
import type { Table, Vector } from "apache-arrow";
import {
  initLayerTree,
  updateTreeLayerCount,
  setTreeLayerLoading,
  setTreeLayerChecked,
  setStatus,
  showPopup,
  hidePopup,
  initIdentifyToggle,
  showIdentifyPanel,
  hideIdentifyPanel,
  addIdentifyResult,
  clearIdentifyResults,
  deactivateIdentifyButton,
  debugLog,
} from "./ui";
import type { NsTableMap } from "./ui";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** Loaded Arrow tables keyed by "namespace/layer" */
const tables = new Map<string, Table>();
/** Detected geometry type per loaded table key */
const geomTypes = new Map<string, GeomType>();
/** Set of currently visible layer keys ("namespace/layer") */
const visibleSet = new Set<string>();
/** Per-namespace table name lists (from the catalog) */
let nsTables: NsTableMap = {};
/** Viewport bbox used for the most recent load, per layer key */
const loadedBbox = new Map<string, Bbox | undefined>();
/** Keys currently being loaded — prevents moveend from re-triggering loads. */
const loadingKeys = new Set<string>();
/** Cooldown guard for polygon layers — prevents moveend from reloading a layer
 *  while earcut triangulation is likely still running.  If we reload the data
 *  during earcut, deck.gl receives a new table reference → restarts earcut
 *  from scratch → the layer never finishes rendering. */
const earcutCooldownKeys = new Set<string>();

/** User-configurable feature limit override; null = use defaults, 0 = unlimited */
let userLimit: number | null = null;

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
      debugLog(`  building ${key}: ${t.numRows} rows, ${t.batches.length} batches`);
      const l = buildAutoLayer(t, true, handleClick, key);
      debugLog(`  → ${key}: ${l.constructor.name} created OK`);
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
    const gt = geomTypes.get(key) ?? "unknown";
    parts.push(`${key}: ${n.toLocaleString()} ${GEOM_ABBREV[gt]}`);
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

/** Return the appropriate feature limit for a given geometry type.
 *  Respects the user-configurable override if set. */
function getEffectiveLimit(gt?: GeomType): number {
  // userLimit === null → use defaults; userLimit === 0 → unlimited (very high)
  if (userLimit !== null) return userLimit === 0 ? 10_000_000 : userLimit;
  switch (gt) {
    case "polygon":
      return MAX_FEATURES_POLYGON;
    case "line":
      return MAX_FEATURES_LINE;
    case "point":
      return MAX_FEATURES_POINT;
    default:
      return MAX_FEATURES_POLYGON; // conservative for unknown
  }
}

/** Load (or reload) a layer using the current viewport bbox. */
async function loadLayerWithViewport(
  ns: string,
  layer: string
): Promise<void> {
  const key = `${ns}/${layer}`;

  // Skip if this layer is already being loaded (prevents moveend race).
  if (loadingKeys.has(key)) return;
  loadingKeys.add(key);

  const bbox = getViewportBbox();
  const knownType = geomTypes.get(key);
  const limit = getEffectiveLimit(knownType);

  setTreeLayerLoading(ns, layer, true);
  try {
    const table = await loadLayer(ns, layer, bbox, limit);
    const gt = detectGeomType(table);
    tables.set(key, table);
    geomTypes.set(key, gt);
    loadedBbox.set(key, bbox);
    updateTreeLayerCount(ns, layer, table.numRows);

    // Debug: log schema info for troubleshooting rendering issues
    debugLog(`loaded ${key}: ${table.numRows} rows, geomType=${gt}, batches=${table.batches.length}`);
    for (const f of table.schema.fields) {
      const ext = f.metadata.get("ARROW:extension:name") ?? "";
      debugLog(`  field: ${f.name}  type=${f.type}  ext=${ext}`);
    }

    // Deep geometry diagnostics — log raw coordinate values + offsets
    if (gt === "polygon") {
      debugLogGeometry(table, key);
    }

    // If this was the first load and the type is point or line, we used a
    // conservative initial limit.  Re-fetch with the proper higher limit.
    if (!knownType && (gt === "point" || gt === "line")) {
      const higherLimit = getEffectiveLimit(gt);
      if (table.numRows >= limit && higherLimit > limit) {
        debugLog(`re-fetching ${key} with ${gt} limit (${higherLimit})`);
        const bigger = await loadLayer(ns, layer, bbox, higherLimit);
        tables.set(key, bigger);
        geomTypes.set(key, gt);
        updateTreeLayerCount(ns, layer, bigger.numRows);
      }
    }

    // For polygon layers, set a cooldown that prevents moveend from
    // reloading while earcut triangulation runs inside deck.gl.
    // Earcut for 10K polygons ≈ 2–5s; we give it 15s of breathing room.
    if (gt === "polygon") {
      earcutCooldownKeys.add(key);
      setTimeout(() => {
        earcutCooldownKeys.delete(key);
        debugLog(`earcut cooldown expired for ${key}`);
      }, 15_000);
    }
  } catch (e) {
    console.error(`Failed to load ${key}:`, e);
    visibleSet.delete(key);
    setTreeLayerChecked(ns, layer, false);
  }
  setTreeLayerLoading(ns, layer, false);
  loadingKeys.delete(key);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  setStatus("Initializing...");
  const map = initMap();

  // Discover available namespaces from the catalog
  const namespaces = await fetchNamespaces().catch(() => ["colorado"]);

  // Discover tables per namespace (in parallel)
  setStatus("Discovering tables...");
  const tablesPerNs = await Promise.all(
    namespaces.map(async (ns) => {
      try {
        const tblNames = await fetchTables(ns);
        return [ns, tblNames] as const;
      } catch {
        return [ns, []] as const;
      }
    })
  );
  nsTables = Object.fromEntries(tablesPerNs);

  // Build the layer tree UI — no layers enabled by default
  initLayerTree(nsTables, handleLayerToggle, handleZoom, handleRefresh, handleLimitChange);
  setStatus("No layers visible");

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
    const reloadKeys = [...visibleSet].filter((key) => {
      // Skip layers currently being loaded — prevents moveend from
      // restarting a load and causing earcut to restart from scratch.
      if (loadingKeys.has(key)) return false;
      // Skip polygon layers in earcut cooldown — reloading would create a
      // new table reference, making deck.gl restart earcut from scratch.
      if (earcutCooldownKeys.has(key)) return false;
      const prev = loadedBbox.get(key);
      if (!prev) return true;
      // Reload if the new viewport extends significantly beyond the loaded bbox
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
    }

    rebuildLayers();
    updateStatusBar();
  }

  // -----------------------------------------------------------------------
  // Refresh + Limit handlers
  // -----------------------------------------------------------------------

  async function handleRefresh() {
    debugLog("Manual refresh triggered");
    // Clear cached data and reload all visible layers
    for (const key of [...visibleSet]) {
      tables.delete(key);
      loadedBbox.delete(key);
      earcutCooldownKeys.delete(key);
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

  function handleLimitChange(limit: number) {
    userLimit = limit === 0 ? 0 : limit; // 0 = unlimited
    debugLog(`Feature limit changed to: ${limit === 0 ? "unlimited" : limit}`);
    // Auto-refresh after limit change
    handleRefresh();
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
