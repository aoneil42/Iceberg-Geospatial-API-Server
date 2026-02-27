import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { MapboxOverlay } from "@deck.gl/mapbox";
import type { Layer, PickingInfo } from "@deck.gl/core";
import type { Bbox } from "./queries";

let map: maplibregl.Map;
let deckOverlay: MapboxOverlay | null = null;
let pendingLayers: Layer[] | null = null;

// Global initial view (no data loaded yet)
const INITIAL_CENTER: [number, number] = [0, 20];
const INITIAL_ZOOM = 2;

// OpenFreeMap provides free vector tiles with no API key required.
const BASEMAP_STYLE = "https://tiles.openfreemap.org/styles/liberty";

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

export function initMap(): maplibregl.Map {
  map = new maplibregl.Map({
    container: "map",
    style: BASEMAP_STYLE,
    center: INITIAL_CENTER,
    zoom: INITIAL_ZOOM,
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

export function setLayers(newLayers: Layer[]): void {
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
