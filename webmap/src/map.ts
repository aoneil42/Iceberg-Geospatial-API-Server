import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { MapboxOverlay } from "@deck.gl/mapbox";
import type { Layer, PickingInfo } from "@deck.gl/core";
import type { Bbox } from "./queries";

let map: maplibregl.Map;
let deckOverlay: MapboxOverlay | null = null;
let pendingLayers: Layer[] | null = null;

// Track current layers so they survive basemap switches
let currentLayers: Layer[] = [];

// Global initial view (no data loaded yet)
const INITIAL_CENTER: [number, number] = [0, 20];
const INITIAL_ZOOM = 2;

// ---------------------------------------------------------------------------
// Basemap configuration
// ---------------------------------------------------------------------------

export interface BasemapConfig {
  name: string;
  style: string;
  token?: string;
}

let basemaps: BasemapConfig[] = [];
let currentBasemapIndex = 0;

/**
 * Load basemap configuration.
 * Priority: VITE_BASEMAP_STYLE env var > /basemaps.json > hardcoded fallback.
 */
export async function loadBasemapConfig(): Promise<BasemapConfig[]> {
  const envStyle = import.meta.env.VITE_BASEMAP_STYLE;
  if (envStyle) {
    basemaps = [{ name: "Default", style: envStyle }];
    return basemaps;
  }

  try {
    const resp = await fetch("/basemaps.json");
    if (resp.ok) {
      const parsed = await resp.json();
      if (Array.isArray(parsed) && parsed.length > 0) {
        basemaps = parsed;
        return basemaps;
      }
    }
  } catch {
    // fall through to default
  }

  basemaps = [
    { name: "Liberty", style: "https://tiles.openfreemap.org/styles/liberty" },
  ];
  return basemaps;
}

export function getBasemaps(): BasemapConfig[] {
  return basemaps;
}

export function getCurrentBasemapIndex(): number {
  return currentBasemapIndex;
}

// ---------------------------------------------------------------------------
// Deck.gl canvas sync
// ---------------------------------------------------------------------------

/**
 * Sync the deck.gl canvas to match the map canvas dimensions.
 * In non-interleaved mode, the Deck constructor measures its container
 * before MapLibre adds it to the DOM, resulting in a 300x150 default.
 * We poll via rAF until the canvas is correct, then stop.
 */
function syncDeckCanvas() {
  const deckCanvas = document.querySelector(
    ".deck-widget-container canvas"
  ) as HTMLCanvasElement | null;
  if (!deckCanvas) return false;

  const mapCanvas = map.getCanvas();
  const dpr = window.devicePixelRatio || 1;
  const w = Math.round(mapCanvas.clientWidth * dpr);
  const h = Math.round(mapCanvas.clientHeight * dpr);

  if (w === 0 || h === 0) return false; // map not laid out yet

  if (deckCanvas.width !== w || deckCanvas.height !== h) {
    deckCanvas.width = w;
    deckCanvas.height = h;
    deckCanvas.style.width = `${mapCanvas.clientWidth}px`;
    deckCanvas.style.height = `${mapCanvas.clientHeight}px`;
    return false; // changed — check again next frame to confirm it stuck
  }
  return true; // already correct
}

function attachOverlay() {
  if (deckOverlay) return; // already attached
  deckOverlay = new MapboxOverlay({
    interleaved: false,
    layers: [],
  });
  map.addControl(deckOverlay);

  // Poll until the deck canvas matches the map canvas, then apply layers.
  // A single rAF isn't reliable across browsers/build modes.
  let attempts = 0;
  const maxAttempts = 30; // ~500ms at 60fps
  const pollCanvasSize = () => {
    attempts++;
    const ok = syncDeckCanvas();
    if (ok || attempts >= maxAttempts) {
      // Canvas is sized (or we gave up) — apply pending layers
      if (pendingLayers) {
        deckOverlay!.setProps({ layers: pendingLayers });
        pendingLayers = null;
      }
    } else {
      requestAnimationFrame(pollCanvasSize);
    }
  };
  requestAnimationFrame(pollCanvasSize);

  // Also sync on window resize
  window.addEventListener("resize", () => syncDeckCanvas());
}

// ---------------------------------------------------------------------------
// Map init + basemap switching
// ---------------------------------------------------------------------------

export function initMap(style: string): maplibregl.Map {
  map = new maplibregl.Map({
    container: "map",
    style,
    center: INITIAL_CENTER,
    zoom: INITIAL_ZOOM,
    transformRequest: (url: string) => {
      // Append token for basemaps that require authentication
      const current = basemaps[currentBasemapIndex];
      if (current?.token) {
        try {
          const styleOrigin = new URL(current.style).origin;
          if (url.startsWith(styleOrigin)) {
            const sep = url.includes("?") ? "&" : "?";
            return { url: `${url}${sep}token=${current.token}` };
          }
        } catch {
          // invalid style URL — skip transform
        }
      }
      return { url };
    },
  });

  map.addControl(new maplibregl.NavigationControl(), "top-right");

  // Use "style.load" which fires as soon as the style JSON is parsed and
  // the GL context is ready — does NOT wait for all tile sources to finish.
  // The "load" event waits for ALL sources (including slow raster tiles)
  // which can block indefinitely with some basemap styles.
  map.once("style.load", () => {
    attachOverlay();
  });

  return map;
}

/**
 * Switch to a different basemap by index.
 * Re-attaches the deck.gl overlay and preserves current data layers.
 */
export function switchBasemap(index: number): void {
  if (index < 0 || index >= basemaps.length) return;
  currentBasemapIndex = index;
  const config = basemaps[index];

  map.setStyle(config.style);

  map.once("style.load", () => {
    // Remove old overlay and re-attach with current layers
    if (deckOverlay) {
      map.removeControl(deckOverlay);
      deckOverlay = null;
    }
    pendingLayers = currentLayers;
    attachOverlay();
  });
}

// ---------------------------------------------------------------------------
// Layer management
// ---------------------------------------------------------------------------

export function setLayers(newLayers: Layer[]): void {
  currentLayers = newLayers;
  if (deckOverlay) {
    deckOverlay.setProps({ layers: newLayers });
  } else {
    pendingLayers = newLayers;
  }
}

export function pickObjectsInRect(
  x: number,
  y: number,
  width: number,
  height: number
): PickingInfo[] {
  if (!deckOverlay) return [];
  return deckOverlay.pickObjects({ x, y, width, height });
}

export function flyToBounds(bbox: Bbox): void {
  const [minx, miny, maxx, maxy] = bbox;
  map.fitBounds(
    [
      [minx, miny],
      [maxx, maxy],
    ],
    { padding: 40, duration: 1500 }
  );
}

/** Return the current map viewport as [minx, miny, maxx, maxy]. */
export function getViewportBbox(): Bbox {
  const bounds = map.getBounds();
  return [
    bounds.getWest(),
    bounds.getSouth(),
    bounds.getEast(),
    bounds.getNorth(),
  ];
}

export function getMap(): maplibregl.Map {
  return map;
}
export function getOverlay(): MapboxOverlay | null {
  return deckOverlay;
}

/** Register a callback that fires after the map stops moving (debounced). */
export function onMoveEnd(callback: () => void): void {
  map.on("moveend", callback);
}
