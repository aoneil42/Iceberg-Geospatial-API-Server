"""
FastAPI application implementing a minimal Esri GeoServices REST API.

Endpoints implemented (covers ~90% of ArcGIS map visualization needs):
- /rest/info
- /rest/services/{service_id}/FeatureServer
- /rest/services/{service_id}/FeatureServer/{layer_id}
- /rest/services/{service_id}/FeatureServer/{layer_id}/query

The service_id maps to an Iceberg namespace, and layer_id maps to
a table within that namespace (0-indexed from the list of tables).
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .routes import feature_server

app = FastAPI(
    title="Iceberg GeoServices",
    description="Esri GeoServices REST API backed by Apache Iceberg",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(feature_server.router, prefix="/rest/services")


@app.get("/rest/info")
async def rest_info():
    """ArcGIS REST service directory info."""
    return {
        "currentVersion": 11.0,
        "fullVersion": "11.0.0",
        "owningSystemUrl": "",
        "authInfo": {"isTokenBasedSecurity": False},
    }
