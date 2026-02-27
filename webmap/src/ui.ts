export type LayerToggleCallback = (
  ns: string,
  layer: string,
  visible: boolean
) => void;

export type LayerZoomCallback = (ns: string, layer: string) => void;

export type RefreshCallback = () => void;
export type LimitChangeCallback = (limit: number) => void;

/** Per-namespace table lists: { colorado: ["points","lines","polygons"], ukraine: ["buildings"] } */
export type NsTableMap = Record<string, string[]>;

// Consistent colour palette for layer dots — cycles for arbitrary table names
const DOT_PALETTE = [
  "#1e90ff",
  "#dc143c",
  "#4169e1",
  "#ff8c00",
  "#2e8b57",
  "#9370db",
  "#20b2aa",
  "#cd5c5c",
];

function dotColor(index: number): string {
  return DOT_PALETTE[index % DOT_PALETTE.length];
}

function titleCase(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// ---------------------------------------------------------------------------
// Layer Tree
// ---------------------------------------------------------------------------

export function initLayerTree(
  nsTables: NsTableMap,
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback,
  onRefresh?: RefreshCallback,
  onLimitChange?: LimitChangeCallback
): void {
  const container = document.getElementById("controls")!;
  const namespaces = Object.keys(nsTables);

  const nsHtml = namespaces
    .map((ns) => {
      const tables = nsTables[ns] ?? [];
      return `
        <div class="tree-ns" data-ns="${ns}">
          <div class="tree-ns-header">
            <span class="tree-caret collapsed" data-caret="${ns}">\u25B8</span>
            <label class="tree-ns-check">
              <input type="checkbox" data-ns-check="${ns}" />
              <span class="tree-ns-name">${ns}</span>
            </label>
          </div>
          <div class="tree-ns-layers collapsed" data-ns-layers="${ns}">
            ${tables
              .map(
                (layer, i) => `
              <div class="layer-row">
                <label class="layer-toggle">
                  <input type="checkbox" data-ns="${ns}" data-layer="${layer}" />
                  <span class="layer-dot" style="background:${dotColor(i)}"></span>
                  <span class="layer-label">${titleCase(layer)}</span>
                  <span class="layer-count" data-lcount="${ns}/${layer}">\u2014</span>
                </label>
                <button class="layer-zoom-btn" data-zoom-ns="${ns}" data-zoom-layer="${layer}" title="Zoom to extent">\u2316</button>
              </div>`
              )
              .join("")}
          </div>
        </div>`;
    })
    .join("");

  container.innerHTML = `
    <div class="controls-inner">
      <div class="controls-title">Layers</div>
      ${nsHtml}
      <div class="toolbar-section">
        <div class="toolbar-row">
          <label class="toolbar-label">Limit</label>
          <select id="feature-limit" class="toolbar-select">
            <option value="1000">1K</option>
            <option value="5000">5K</option>
            <option value="10000" selected>10K</option>
            <option value="50000">50K</option>
            <option value="200000">200K</option>
            <option value="all">All</option>
          </select>
          <button id="refresh-btn" class="toolbar-btn" title="Reload visible layers">&#8635; Refresh</button>
        </div>
      </div>
    </div>
  `;

  // --- Event: individual layer checkbox ---
  container.addEventListener("change", (e) => {
    const input = e.target as HTMLInputElement;
    const ns = input.dataset.ns;
    const layer = input.dataset.layer;
    const nsCheck = input.dataset.nsCheck;

    if (ns && layer) {
      onToggle(ns, layer, input.checked);
      syncNsCheckbox(ns);
    } else if (nsCheck) {
      // Namespace-level toggle → check/uncheck all children
      const checked = input.checked;
      const layerInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns="${nsCheck}"][data-layer]`
      );
      for (const li of layerInputs) {
        if (li.checked !== checked) {
          li.checked = checked;
          onToggle(nsCheck, li.dataset.layer!, checked);
        }
      }
      // Auto-expand when enabling a namespace
      if (checked) expandNamespace(nsCheck);
    }
  });

  // --- Event: caret click to expand / collapse ---
  container.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;

    // Caret toggle
    const caret = target.closest("[data-caret]") as HTMLElement | null;
    if (caret) {
      const ns = caret.dataset.caret!;
      const layers = document.querySelector(`[data-ns-layers="${ns}"]`)!;
      const isCollapsed = layers.classList.toggle("collapsed");
      caret.classList.toggle("collapsed", isCollapsed);
      caret.textContent = isCollapsed ? "\u25B8" : "\u25BE";
      return;
    }

    // Zoom button
    const zoomBtn = target.closest("[data-zoom-ns]") as HTMLElement | null;
    if (zoomBtn) {
      const ns = zoomBtn.dataset.zoomNs!;
      const layer = zoomBtn.dataset.zoomLayer!;
      onZoom(ns, layer);
    }
  });

  // --- Event: refresh button ---
  document.getElementById("refresh-btn")?.addEventListener("click", () => {
    onRefresh?.();
  });

  // --- Event: feature limit change ---
  document.getElementById("feature-limit")?.addEventListener("change", (e) => {
    const val = (e.target as HTMLSelectElement).value;
    const limit = val === "all" ? 0 : parseInt(val, 10);
    onLimitChange?.(limit);
  });
}

function expandNamespace(ns: string): void {
  const layers = document.querySelector(`[data-ns-layers="${ns}"]`);
  const caret = document.querySelector(`[data-caret="${ns}"]`);
  if (layers && caret) {
    layers.classList.remove("collapsed");
    caret.classList.remove("collapsed");
    caret.textContent = "\u25BE";
  }
}

function syncNsCheckbox(ns: string): void {
  const layerInputs = document.querySelectorAll<HTMLInputElement>(
    `input[data-ns="${ns}"][data-layer]`
  );
  const total = layerInputs.length;
  const checked = Array.from(layerInputs).filter((i) => i.checked).length;
  const nsInput = document.querySelector<HTMLInputElement>(
    `input[data-ns-check="${ns}"]`
  );
  if (!nsInput) return;
  nsInput.checked = checked > 0;
  nsInput.indeterminate = checked > 0 && checked < total;
}

export function updateTreeLayerCount(
  ns: string,
  layer: string,
  count: number
): void {
  const el = document.querySelector(`[data-lcount="${ns}/${layer}"]`);
  if (el) el.textContent = count.toLocaleString();
}

export function setTreeLayerLoading(
  ns: string,
  layer: string,
  loading: boolean
): void {
  const el = document.querySelector(`[data-lcount="${ns}/${layer}"]`);
  if (el && loading) el.textContent = "\u2026";
}

export function setTreeLayerChecked(
  ns: string,
  layer: string,
  checked: boolean
): void {
  const input = document.querySelector<HTMLInputElement>(
    `input[data-ns="${ns}"][data-layer="${layer}"]`
  );
  if (input) {
    input.checked = checked;
    syncNsCheckbox(ns);
  }
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

export function setStatus(message: string): void {
  const el = document.getElementById("status")!;
  el.textContent = message;
}

// ---------------------------------------------------------------------------
// Debug log overlay (visible on-screen, toggled with ?debug=1)
// ---------------------------------------------------------------------------

const DEBUG_ENABLED =
  typeof window !== "undefined" &&
  new URLSearchParams(window.location.search).has("debug");

let debugPanel: HTMLElement | null = null;

function ensureDebugPanel(): HTMLElement {
  if (debugPanel) return debugPanel;
  debugPanel = document.createElement("div");
  debugPanel.id = "debug-log";
  Object.assign(debugPanel.style, {
    position: "fixed",
    bottom: "28px",
    left: "0",
    right: "0",
    maxHeight: "200px",
    overflowY: "auto",
    background: "rgba(0,0,0,0.85)",
    color: "#0f0",
    fontFamily: "monospace",
    fontSize: "11px",
    padding: "4px 8px",
    zIndex: "9999",
    pointerEvents: "auto",
  });
  document.body.appendChild(debugPanel);
  return debugPanel;
}

/** Append a debug message to the on-screen log (only when ?debug=1). */
export function debugLog(msg: string, level: "info" | "warn" | "err" = "info"): void {
  console.log(`[webmap-debug] ${msg}`);
  if (!DEBUG_ENABLED) return;
  const panel = ensureDebugPanel();
  const line = document.createElement("div");
  line.textContent = `${new Date().toISOString().slice(11, 23)} ${msg}`;
  if (level === "err") line.style.color = "#f44";
  if (level === "warn") line.style.color = "#ff4";
  panel.appendChild(line);
  panel.scrollTop = panel.scrollHeight;
}

// ---------------------------------------------------------------------------
// Popup (single-click)
// ---------------------------------------------------------------------------

export function showPopup(
  props: Record<string, unknown>,
  x: number,
  y: number
): void {
  const popup = document.getElementById("popup")!;
  const entries = Object.entries(props)
    .map(
      ([k, v]) =>
        `<div class="popup-row"><span class="popup-key">${k}</span> ${v}</div>`
    )
    .join("");
  popup.innerHTML = entries;
  popup.style.left = `${x + 12}px`;
  popup.style.top = `${y - 12}px`;
  popup.classList.remove("hidden");
}

export function hidePopup(): void {
  document.getElementById("popup")!.classList.add("hidden");
}

// ---------------------------------------------------------------------------
// Identify Mode
// ---------------------------------------------------------------------------

export type IdentifyToggleCallback = (active: boolean) => void;

export function initIdentifyToggle(
  onToggle: IdentifyToggleCallback
): void {
  const inner = document.querySelector(".controls-inner")!;
  const section = document.createElement("div");
  section.className = "identify-section";
  section.innerHTML = `
    <button id="identify-btn" class="identify-btn" title="Identify features">
      <span class="identify-icon">&#9432;</span>
      <span class="identify-label">Identify</span>
    </button>
  `;
  inner.appendChild(section);

  document.getElementById("identify-btn")!.addEventListener("click", () => {
    const isActive = document
      .getElementById("identify-btn")!
      .classList.toggle("active");
    onToggle(isActive);
  });
}

export function showIdentifyPanel(onClear: () => void): void {
  const panel = document.getElementById("identify-panel")!;
  panel.innerHTML = `
    <div class="identify-header">
      <span class="identify-title">Identify Results</span>
      <button id="identify-clear-btn" class="identify-clear-btn">Clear</button>
    </div>
    <div id="identify-results" class="identify-results"></div>
  `;
  panel.classList.remove("hidden");
  document
    .getElementById("identify-clear-btn")!
    .addEventListener("click", onClear);
}

export function hideIdentifyPanel(): void {
  const panel = document.getElementById("identify-panel")!;
  panel.classList.add("hidden");
  panel.innerHTML = "";
}

export function addIdentifyResult(props: Record<string, unknown>): void {
  const container = document.getElementById("identify-results");
  if (!container) return;

  const card = document.createElement("div");
  card.className = "identify-card";

  const featureType = props.type ?? "Feature";
  const entries = Object.entries(props)
    .filter(([k]) => k !== "type")
    .map(
      ([k, v]) =>
        `<div class="popup-row"><span class="popup-key">${k}</span> ${v}</div>`
    )
    .join("");

  card.innerHTML = `
    <div class="identify-card-header">${featureType}</div>
    ${entries}
  `;
  container.appendChild(card);
  container.scrollTop = container.scrollHeight;
}

export function clearIdentifyResults(): void {
  const container = document.getElementById("identify-results");
  if (container) container.innerHTML = "";
}

export function deactivateIdentifyButton(): void {
  document.getElementById("identify-btn")?.classList.remove("active");
}
