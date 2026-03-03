import initWasm, {
  readGeoParquet,
  set_panic_hook,
} from "@geoarrow/geoparquet-wasm/esm/index.js";
import { tableFromIPC } from "apache-arrow";
import type { Table } from "apache-arrow";

/** Responses larger than this (in bytes) are rejected before being passed to
 *  WASM to avoid unrecoverable OOM panics inside the Rust allocator.
 *  geoparquet-wasm needs ~3× the file size in WASM linear memory
 *  (input buffer + decoded Arrow + IPC serialisation), and WASM linear memory
 *  is capped at 1–4 GB depending on the browser. 256 MB raw Parquet is a
 *  conservative ceiling that leaves headroom for ~768 MB peak WASM usage. */
const MAX_RESPONSE_BYTES = 256 * 1024 * 1024; // 256 MB

let wasmReady: Promise<void> | null = null;

function ensureWasm(): Promise<void> {
  if (!wasmReady) {
    wasmReady = initWasm().then(() => {
      // Enable Rust's console_error_panic_hook so WASM panics print a Rust
      // backtrace to console.error instead of the cryptic "unreachable" msg.
      set_panic_hook();
    });
  }
  return wasmReady;
}

export async function loadGeoParquet(url: string): Promise<Table> {
  await ensureWasm();
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  const buffer = await resp.arrayBuffer();

  if (buffer.byteLength > MAX_RESPONSE_BYTES) {
    const mb = (buffer.byteLength / (1024 * 1024)).toFixed(1);
    throw new Error(
      `Response too large (${mb} MB). Reduce the feature limit or zoom in to load fewer features.`
    );
  }

  try {
    const wasmTable = readGeoParquet(new Uint8Array(buffer));
    return tableFromIPC(wasmTable.intoIPCStream());
  } catch (e) {
    const msg = (e as Error).message ?? String(e);
    // WASM OOM panics surface as "unreachable" RuntimeErrors
    if (msg.includes("unreachable") || msg.includes("RuntimeError")) {
      const mb = (buffer.byteLength / (1024 * 1024)).toFixed(1);
      throw new Error(
        `WASM out of memory parsing ${mb} MB GeoParquet. ` +
          `Reduce the feature limit or zoom in to load fewer features.`
      );
    }
    throw e;
  }
}
