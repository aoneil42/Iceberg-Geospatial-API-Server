#!/bin/sh
set -e

# PYGEOAPI_SERVER_URL controls link generation in OGC API responses.
#
# Default: http://localhost:3000/ogc — the nginx proxy rewrites this
# in all JSON/HTML responses to match the actual request host, so links
# work from any client (localhost, LAN IP, EC2 public IP) automatically.
#
# Override: set PYGEOAPI_SERVER_URL in .env for direct access (no nginx):
#   PYGEOAPI_SERVER_URL=http://my-server:5050
#
if [ -z "$PYGEOAPI_SERVER_URL" ] || [ "$PYGEOAPI_SERVER_URL" = "auto" ]; then
    PORT="${PUBLIC_PORT:-3000}"
    export PYGEOAPI_SERVER_URL="http://localhost:${PORT}/ogc"
fi
echo "[pygeoapi] PYGEOAPI_SERVER_URL=${PYGEOAPI_SERVER_URL}"

# Substitute env vars in the base pygeoapi config (creates the template)
python3 -c "
import os, re, shutil
config_path = os.environ['PYGEOAPI_CONFIG']
# Keep the original as a template for re-discovery
template = config_path + '.template'
if not os.path.exists(template):
    shutil.copy2(config_path, template)
config = open(template).read()
def replace_env(m):
    return os.environ.get(m.group(1), m.group(0))
config = re.sub(r'\\\$\{(\w+)\}', replace_env, config)
open(config_path, 'w').write(config)
"

# ---------------------------------------------------------------------------
# Supervisor: discover catalog tables, start pygeoapi, watch for changes.
# Polls LakeKeeper every 30s. If the table list changes, regenerates
# config + OpenAPI spec and restarts pygeoapi automatically.
# ---------------------------------------------------------------------------

exec python3 << 'PYEOF'
import json, os, signal, subprocess, sys, time, urllib.request, yaml

CATALOG_URL = "http://lakekeeper:8181/catalog"
TOKEN = "dummy"
WAREHOUSE = "lakehouse"
CONFIG_PATH = os.environ["PYGEOAPI_CONFIG"]
OPENAPI_PATH = os.environ["PYGEOAPI_OPENAPI"]
POLL_INTERVAL = int(os.environ.get("CATALOG_POLL_INTERVAL", "30"))

pygeoapi_proc = None


def api_get(url):
    """GET request to LakeKeeper with auth header."""
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def discover_tables():
    """Query LakeKeeper and return a sorted list of (namespace, table) tuples."""
    data = api_get(f"{CATALOG_URL}/v1/config?warehouse={WAREHOUSE}")
    prefix = data["defaults"]["prefix"]

    ns_data = api_get(f"{CATALOG_URL}/v1/{prefix}/namespaces")
    namespaces = [ns[0] if isinstance(ns, list) else ns for ns in ns_data.get("namespaces", [])]

    tables = []
    for ns in namespaces:
        try:
            tbl_data = api_get(f"{CATALOG_URL}/v1/{prefix}/namespaces/{ns}/tables")
            for ident in tbl_data.get("identifiers", []):
                name = ident.get("name", ident[-1] if isinstance(ident, list) else str(ident))
                tables.append((ns, name))
        except Exception as e:
            print(f"[pygeoapi-watcher] Warning: failed to list tables for {ns}: {e}", flush=True)

    return sorted(tables)


def build_config(tables):
    """Write pygeoapi config with discovered collections."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    resources = {}
    for ns, tbl in tables:
        resource_id = f"{ns}-{tbl}"
        title = f"{ns.title()} {tbl.replace('_', ' ').title()}"
        resources[resource_id] = {
            "type": "collection",
            "title": title,
            "description": f"{title} from the {ns} namespace",
            "keywords": [tbl, ns],
            "extents": {
                "spatial": {
                    "bbox": [-180.0, -90.0, 180.0, 90.0],
                    "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                }
            },
            "providers": [
                {
                    "type": "feature",
                    "name": "iceberg_geo.pygeoapi_provider.iceberg.IcebergProvider",
                    "data": f"{ns}.{tbl}",
                    "id_field": "id",
                    "options": {"geometry_column": "geometry"},
                }
            ],
        }

    config["resources"] = resources
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    return len(resources)


def generate_openapi():
    """Generate the OpenAPI spec from the current config."""
    subprocess.run(
        ["pygeoapi", "openapi", "generate", CONFIG_PATH, "--output-file", OPENAPI_PATH],
        check=True,
    )


def start_pygeoapi():
    """Start pygeoapi as a subprocess."""
    global pygeoapi_proc
    pygeoapi_proc = subprocess.Popen(
        ["pygeoapi", "serve"],
        env=os.environ.copy(),
    )
    print(f"[pygeoapi-watcher] Started pygeoapi (PID {pygeoapi_proc.pid})", flush=True)


def stop_pygeoapi():
    """Gracefully stop pygeoapi."""
    global pygeoapi_proc
    if pygeoapi_proc and pygeoapi_proc.poll() is None:
        print("[pygeoapi-watcher] Stopping pygeoapi...", flush=True)
        pygeoapi_proc.terminate()
        try:
            pygeoapi_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pygeoapi_proc.kill()
            pygeoapi_proc.wait()
        print("[pygeoapi-watcher] pygeoapi stopped", flush=True)


def handle_signal(signum, frame):
    """Forward signals to pygeoapi and exit."""
    stop_pygeoapi()
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# --- Initial discovery and startup ---
print("[pygeoapi-watcher] Initial catalog discovery...", flush=True)
try:
    current_tables = discover_tables()
    ns_groups = {}
    for ns, tbl in current_tables:
        ns_groups.setdefault(ns, []).append(tbl)
    for ns, tbls in ns_groups.items():
        print(f"[pygeoapi-watcher]   {ns}: {tbls}", flush=True)

    count = build_config(current_tables)
    print(f"[pygeoapi-watcher] Registered {count} collections", flush=True)
except Exception as e:
    print(f"[pygeoapi-watcher] Initial discovery failed ({e}), using static config", flush=True)
    current_tables = None

generate_openapi()
start_pygeoapi()

# --- Watch loop ---
print(f"[pygeoapi-watcher] Watching catalog every {POLL_INTERVAL}s for changes...", flush=True)
while True:
    # Check if pygeoapi is still running
    if pygeoapi_proc.poll() is not None:
        print(f"[pygeoapi-watcher] pygeoapi exited with code {pygeoapi_proc.returncode}", flush=True)
        sys.exit(pygeoapi_proc.returncode)

    time.sleep(POLL_INTERVAL)

    try:
        new_tables = discover_tables()
    except Exception as e:
        print(f"[pygeoapi-watcher] Catalog poll failed: {e}", flush=True)
        continue

    if new_tables != current_tables:
        added = set(new_tables) - set(current_tables or [])
        removed = set(current_tables or []) - set(new_tables)
        if added:
            print(f"[pygeoapi-watcher] Tables added: {[f'{ns}.{t}' for ns, t in added]}", flush=True)
        if removed:
            print(f"[pygeoapi-watcher] Tables removed: {[f'{ns}.{t}' for ns, t in removed]}", flush=True)

        current_tables = new_tables
        count = build_config(current_tables)
        print(f"[pygeoapi-watcher] Regenerating config ({count} collections)...", flush=True)

        generate_openapi()
        stop_pygeoapi()
        start_pygeoapi()
        print(f"[pygeoapi-watcher] Reload complete", flush=True)
PYEOF
