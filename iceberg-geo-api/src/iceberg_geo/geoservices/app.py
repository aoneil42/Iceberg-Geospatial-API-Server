"""
FastAPI application implementing a minimal Esri GeoServices REST API.

Endpoints implemented (covers ~90% of ArcGIS map visualization needs):
- /rest/info
- /rest/services
- /rest/services/{service_id}/FeatureServer
- /rest/services/{service_id}/FeatureServer/{layer_id}
- /rest/services/{service_id}/FeatureServer/{layer_id}/query

The service_id maps to an Iceberg namespace, and layer_id maps to
a table within that namespace (0-indexed from the list of tables).
"""

import logging
import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse

from iceberg_geo.query.catalog import list_namespaces

from .html import render_rest_info, render_services_directory
from .routes import feature_server

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(
    title="Iceberg GeoServices",
    description="Esri GeoServices REST API backed by Apache Iceberg",
    root_path=os.environ.get("ROOT_PATH", ""),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def log_request_timing(request: Request, call_next):
    """Log request timing for performance monitoring."""
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    # Only log query requests (the slow path) at INFO level
    path = request.url.path
    if "/query" in path or elapsed > 1.0:
        logger.info(
            "%s %s → %d (%.2fs, %s bytes)",
            request.method,
            request.url,
            response.status_code,
            elapsed,
            response.headers.get("content-length", "?"),
        )
    return response

app.include_router(feature_server.router, prefix="/rest/services")


def _base_url(request: Request) -> str:
    """Derive the public base URL from the request.

    Behind a reverse proxy (nginx), use X-Forwarded-Host and
    X-Forwarded-Proto to reconstruct the correct external URL.
    The ROOT_PATH env var provides the proxy prefix (e.g. /esri).
    """
    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "localhost")
    proto = request.headers.get("x-forwarded-proto", "http")
    root = request.scope.get("root_path", "")
    return f"{proto}://{host}{root}"


def _wants_html(request: Request, f: str | None) -> bool:
    """Check if the client wants HTML (explicit f=html or browser Accept)."""
    if f == "html":
        return True
    if f and f != "html":
        return False
    accept = request.headers.get("accept", "")
    return "text/html" in accept


@app.get("/rest/info")
@app.post("/rest/info")
async def rest_info(request: Request, f: str = None):
    """ArcGIS REST service directory info."""
    namespaces = list_namespaces()
    services = [{"name": ns, "type": "FeatureServer"} for ns in namespaces]

    if _wants_html(request, f):
        return HTMLResponse(render_rest_info(_base_url(request), services))

    return {
        "currentVersion": 11.0,
        "fullVersion": "11.0.0",
        "owningSystemUrl": "",
        "authInfo": {"isTokenBasedSecurity": False},
        "services": services,
    }


@app.get("/rest/services")
@app.post("/rest/services")
async def services_directory(request: Request, f: str = None):
    """Services directory — lists all available FeatureServer services."""
    namespaces = list_namespaces()
    services = [{"name": ns, "type": "FeatureServer"} for ns in namespaces]

    if _wants_html(request, f):
        return HTMLResponse(
            render_services_directory(_base_url(request), services)
        )

    return {
        "currentVersion": 11.0,
        "services": services,
    }
