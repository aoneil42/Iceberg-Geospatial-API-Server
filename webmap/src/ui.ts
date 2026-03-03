export type LayerToggleCallback = (
  ns: string,
  layer: string,
  visible: boolean
) => void;

export type LayerZoomCallback = (ns: string, layer: string) => void;

export type RefreshCallback = () => void;

/** Per-namespace table lists: { colorado: ["points","lines","polygons"] } */
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
// Catalog Tree Data Model
// ---------------------------------------------------------------------------

export interface CatalogNode {
  name: string; // display name (last segment)
  fullPath: string; // dotted full path, e.g. "colorado.water"
  type: "namespace" | "table";
  children: CatalogNode[];
}

/**
 * Build a recursive catalog tree from flat namespace paths + table lists.
 *
 * @param namespacePaths  e.g. [["colorado"], ["colorado", "water"]]
 * @param tablesPerNs     dotted path → table names
 */
export function buildCatalogTree(
  namespacePaths: string[][],
  tablesPerNs: Record<string, string[]>
): CatalogNode[] {
  const root: CatalogNode[] = [];

  // Sort so parents come before children
  const sorted = [...namespacePaths].sort(
    (a, b) => a.length - b.length || a.join(".").localeCompare(b.join("."))
  );

  for (const parts of sorted) {
    let level = root;
    for (let i = 0; i < parts.length; i++) {
      const segment = parts[i];
      const fullPath = parts.slice(0, i + 1).join(".");
      let existing = level.find(
        (n) => n.fullPath === fullPath && n.type === "namespace"
      );
      if (!existing) {
        existing = {
          name: segment,
          fullPath,
          type: "namespace",
          children: [],
        };
        level.push(existing);
      }
      level = existing.children;
    }
  }

  // Add table leaf nodes under each namespace
  function addTables(nodes: CatalogNode[]): void {
    for (const node of nodes) {
      if (node.type !== "namespace") continue;
      const tables = tablesPerNs[node.fullPath] ?? [];
      for (const tableName of tables) {
        node.children.push({
          name: tableName,
          fullPath: `${node.fullPath}/${tableName}`,
          type: "table",
          children: [],
        });
      }
      addTables(node.children.filter((c) => c.type === "namespace"));
    }
  }
  addTables(root);

  return root;
}

// ---------------------------------------------------------------------------
// Layer Tree (legacy — flat namespace list, renders in #controls)
// ---------------------------------------------------------------------------

export function initLayerTree(
  nsTables: NsTableMap,
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback,
  onRefresh?: RefreshCallback,
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
          <button id="refresh-btn" class="toolbar-btn" title="Reload visible layers">&#8635; Refresh</button>
        </div>
      </div>
    </div>
  `;

  wireLayerTreeEvents(container, onToggle, onZoom);

  document.getElementById("refresh-btn")?.addEventListener("click", () => {
    onRefresh?.();
  });
}

// ---------------------------------------------------------------------------
// Catalog Browser (nested namespace tree, renders in #sidebar)
// ---------------------------------------------------------------------------

let globalColorIndex = 0;

function renderTreeNodes(nodes: CatalogNode[], depth: number): string {
  return nodes
    .map((node) => {
      if (node.type === "namespace") {
        const collapsed = depth > 0 ? " collapsed" : "";
        const caretChar = depth > 0 ? "\u25B8" : "\u25BE";
        return `
        <div class="cat-node cat-ns" data-ns="${node.fullPath}">
          <div class="cat-node-header" style="padding-left:${depth * 16 + 4}px">
            <span class="tree-caret${collapsed}" data-caret="${node.fullPath}">${caretChar}</span>
            <span class="cat-folder-icon">\uD83D\uDCC1</span>
            <label class="cat-ns-check">
              <input type="checkbox" data-ns-check="${node.fullPath}" />
              <span class="cat-ns-name">${node.name}</span>
            </label>
          </div>
          <div class="cat-ns-children${collapsed}" data-ns-children="${node.fullPath}">
            ${renderTreeNodes(node.children, depth + 1)}
          </div>
        </div>`;
      } else {
        // Table leaf node — ns is everything before the last /
        const slashIdx = node.fullPath.lastIndexOf("/");
        const ns = node.fullPath.slice(0, slashIdx);
        const table = node.fullPath.slice(slashIdx + 1);
        const color = dotColor(globalColorIndex++);
        return `
        <div class="cat-node cat-table" style="padding-left:${depth * 16 + 4}px">
          <label class="layer-toggle">
            <input type="checkbox" data-ns="${ns}" data-layer="${table}" />
            <span class="cat-table-icon">\uD83D\uDCC4</span>
            <span class="layer-dot" style="background:${color}"></span>
            <span class="layer-label">${titleCase(table)}</span>
            <span class="layer-count" data-lcount="${ns}/${table}">\u2014</span>
          </label>
        </div>`;
      }
    })
    .join("");
}

export function initCatalogBrowser(
  tree: CatalogNode[],
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback,
  onRefresh?: RefreshCallback,
): void {
  globalColorIndex = 0;

  const sidebarNav = document.querySelector(".sidebar-nav")!;

  // Insert catalog browser as the first section
  const browserSection = document.createElement("div");
  browserSection.id = "catalog-browser";
  browserSection.className = "sidebar-section";
  browserSection.innerHTML = `
    <h2 class="sidebar-section-title">Catalog</h2>
    <div id="catalog-tree" class="catalog-tree">${renderTreeNodes(tree, 0)}</div>
    <div class="catalog-toolbar">
      <div class="toolbar-row">
        <label class="toolbar-label">Basemap</label>
        <select id="basemap-select" class="toolbar-select"></select>
      </div>
      <div class="toolbar-row">
        <button id="refresh-btn" class="toolbar-btn" title="Reload visible layers">&#8635; Refresh</button>
      </div>
    </div>
  `;
  sidebarNav.insertBefore(browserSection, sidebarNav.firstChild);

  const treeContainer = document.getElementById("catalog-tree")!;
  wireLayerTreeEvents(treeContainer, onToggle, onZoom);
  initContextMenu(treeContainer, onZoom);

  document.getElementById("refresh-btn")?.addEventListener("click", () => {
    onRefresh?.();
  });
}

// ---------------------------------------------------------------------------
// Shared event wiring (used by both initLayerTree and initCatalogBrowser)
// ---------------------------------------------------------------------------

function wireLayerTreeEvents(
  container: HTMLElement,
  onToggle: LayerToggleCallback,
  onZoom: LayerZoomCallback
): void {
  // Checkbox changes (layer + namespace-level)
  container.addEventListener("change", (e) => {
    const input = e.target as HTMLInputElement;
    const ns = input.dataset.ns;
    const layer = input.dataset.layer;
    const nsCheck = input.dataset.nsCheck;

    if (ns && layer) {
      onToggle(ns, layer, input.checked);
      syncNsCheckbox(ns);
    } else if (nsCheck) {
      const checked = input.checked;
      // Toggle all descendant table checkboxes (exact match + nested)
      const directInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns="${nsCheck}"][data-layer]`
      );
      const nestedInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns^="${nsCheck}."][data-layer]`
      );
      for (const li of [...directInputs, ...nestedInputs]) {
        if (li.checked !== checked) {
          li.checked = checked;
          onToggle(li.dataset.ns!, li.dataset.layer!, checked);
        }
      }
      // Also sync child namespace checkboxes
      const childNsInputs = container.querySelectorAll<HTMLInputElement>(
        `input[data-ns-check^="${nsCheck}."]`
      );
      for (const ci of childNsInputs) {
        ci.checked = checked;
        ci.indeterminate = false;
      }
      if (checked) expandNamespace(nsCheck);
    }
  });

  // Caret click to toggle namespace collapse
  container.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;

    const caret = target.closest("[data-caret]") as HTMLElement | null;
    if (caret) {
      const ns = caret.dataset.caret!;
      const children =
        document.querySelector(`[data-ns-children="${ns}"]`) ??
        document.querySelector(`[data-ns-layers="${ns}"]`);
      if (children) {
        const isCollapsed = children.classList.toggle("collapsed");
        caret.classList.toggle("collapsed", isCollapsed);
        caret.textContent = isCollapsed ? "\u25B8" : "\u25BE";
      }
    }
  });
}

function expandNamespace(ns: string): void {
  const children =
    document.querySelector(`[data-ns-children="${ns}"]`) ??
    document.querySelector(`[data-ns-layers="${ns}"]`);
  const caret = document.querySelector(`[data-caret="${ns}"]`);
  if (children && caret) {
    children.classList.remove("collapsed");
    caret.classList.remove("collapsed");
    caret.textContent = "\u25BE";
  }
}

/**
 * Sync namespace checkbox state based on descendant table checkboxes.
 * Bubbles up to parent namespaces for nested hierarchies.
 */
function syncNsCheckbox(nsPath: string): void {
  const directInputs = document.querySelectorAll<HTMLInputElement>(
    `input[data-ns="${nsPath}"][data-layer]`
  );
  const nestedInputs = document.querySelectorAll<HTMLInputElement>(
    `input[data-ns^="${nsPath}."][data-layer]`
  );
  const allInputs = [...directInputs, ...nestedInputs];
  const total = allInputs.length;
  const checked = allInputs.filter((i) => i.checked).length;
  const nsInput = document.querySelector<HTMLInputElement>(
    `input[data-ns-check="${nsPath}"]`
  );
  if (nsInput) {
    nsInput.checked = checked > 0;
    nsInput.indeterminate = checked > 0 && checked < total;
  }

  // Bubble up to parent namespace
  const lastDot = nsPath.lastIndexOf(".");
  if (lastDot > 0) {
    syncNsCheckbox(nsPath.slice(0, lastDot));
  }
}

// ---------------------------------------------------------------------------
// Layer count / loading / checked helpers
// ---------------------------------------------------------------------------

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
export function debugLog(
  msg: string,
  level: "info" | "warn" | "err" = "info"
): void {
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
  // Append to catalog toolbar if it exists, otherwise fall back to controls-inner
  const toolbar =
    document.querySelector(".catalog-toolbar") ??
    document.querySelector(".controls-inner");
  if (!toolbar) return;

  const section = document.createElement("div");
  section.className = "identify-section";
  section.innerHTML = `
    <button id="identify-btn" class="identify-btn" title="Identify features">
      <span class="identify-icon">&#9432;</span>
      <span class="identify-label">Identify</span>
    </button>
  `;
  toolbar.appendChild(section);

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

// ---------------------------------------------------------------------------
// Basemap Picker
// ---------------------------------------------------------------------------

export type BasemapChangeCallback = (index: number) => void;

export function initBasemapPicker(
  basemaps: Array<{ name: string; style: string }>,
  currentIndex: number,
  onChange: BasemapChangeCallback
): void {
  const select = document.getElementById("basemap-select") as HTMLSelectElement | null;
  if (!select) return;

  select.innerHTML = basemaps
    .map(
      (b, i) =>
        `<option value="${i}" ${i === currentIndex ? "selected" : ""}>${b.name}</option>`
    )
    .join("");

  select.addEventListener("change", (e) => {
    const index = parseInt((e.target as HTMLSelectElement).value, 10);
    onChange(index);
  });
}

// ---------------------------------------------------------------------------
// Layer Context Menu (right-click on table nodes)
// ---------------------------------------------------------------------------

let activeContextMenu: HTMLElement | null = null;

function dismissContextMenu(): void {
  if (activeContextMenu) {
    activeContextMenu.remove();
    activeContextMenu = null;
  }
}

function initContextMenu(container: HTMLElement, onZoom: (ns: string, layer: string) => void): void {
  container.addEventListener("contextmenu", (e) => {
    const tableNode = (e.target as HTMLElement).closest(".cat-table");
    if (!tableNode) return;

    // Prevent default browser context menu
    e.preventDefault();
    e.stopPropagation();

    // Right-clicking a <label> causes browsers to fire a synthetic click
    // on the associated checkbox. Intercept and cancel that one-time click.
    const cb = tableNode.querySelector<HTMLInputElement>("input[type=checkbox]");
    if (cb) {
      cb.addEventListener("click", (ev) => { ev.preventDefault(); }, { once: true });
    }

    dismissContextMenu();

    const input = tableNode.querySelector<HTMLInputElement>("input[data-layer]");
    if (!input) return;

    const ns = input.dataset.ns!;
    const table = input.dataset.layer!;

    // Compute Esri layer index: position of this table among sibling .cat-table nodes
    const parent = tableNode.parentElement;
    let layerIndex = 0;
    if (parent) {
      const siblings = parent.querySelectorAll(":scope > .cat-table");
      for (let i = 0; i < siblings.length; i++) {
        if (siblings[i] === tableNode) { layerIndex = i; break; }
      }
    }

    // OGC collection name: dots → dashes
    const ogcCollection = `${ns.replace(/\./g, "-")}-${table}`;

    const menu = document.createElement("div");
    menu.className = "layer-context-menu";
    menu.style.left = `${e.clientX}px`;
    menu.style.top = `${e.clientY}px`;
    menu.innerHTML = `
      <a href="#" class="ctx-zoom">Zoom to extent</a>
      <div class="ctx-divider"></div>
      <div class="ctx-label">API Endpoints</div>
      <a href="/api/features/${ns}/${table}?limit=10" target="_blank">Feature API</a>
      <a href="/ogc/collections/${ogcCollection}/items" target="_blank">OGC API Features</a>
      <a href="/esri/rest/services/${ns}/FeatureServer/${layerIndex}" target="_blank">Esri GeoServices</a>
    `;
    menu.querySelector(".ctx-zoom")!.addEventListener("click", (ev) => {
      ev.preventDefault();
      dismissContextMenu();
      onZoom(ns, table);
    });
    document.body.appendChild(menu);
    activeContextMenu = menu;

    // Keep menu in viewport
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) {
      menu.style.left = `${e.clientX - rect.width}px`;
    }
    if (rect.bottom > window.innerHeight) {
      menu.style.top = `${e.clientY - rect.height}px`;
    }
  });

  // Dismiss only on clicks outside the menu
  document.addEventListener("mousedown", (e) => {
    if (activeContextMenu && !activeContextMenu.contains(e.target as Node)) {
      dismissContextMenu();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") dismissContextMenu();
  });
}
