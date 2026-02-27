import initWasm, {
  readGeoParquet,
} from "@geoarrow/geoparquet-wasm/esm/index.js";
import { tableFromIPC } from "apache-arrow";
import type { Table } from "apache-arrow";

let wasmReady: Promise<void> | null = null;

function ensureWasm(): Promise<void> {
  if (!wasmReady) {
    wasmReady = initWasm().then(() => {});
  }
  return wasmReady;
}

export async function loadGeoParquet(url: string): Promise<Table> {
  await ensureWasm();
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  const buffer = await resp.arrayBuffer();
  const wasmTable = readGeoParquet(new Uint8Array(buffer));
  return tableFromIPC(wasmTable.intoIPCStream());
}
