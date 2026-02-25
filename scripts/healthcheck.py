#!/usr/bin/env python3
"""
Health check script for container health probes.

Usage:
    python scripts/healthcheck.py [pygeoapi|geoservices]

Exits 0 on success, 1 on failure.
"""

import sys
import urllib.request
import urllib.error


def check_pygeoapi(host="localhost", port=5000):
    """Check pygeoapi is responding."""
    try:
        url = f"http://{host}:{port}/"
        req = urllib.request.urlopen(url, timeout=5)
        return req.status == 200
    except (urllib.error.URLError, OSError):
        return False


def check_geoservices(host="localhost", port=8001):
    """Check GeoServices endpoint is responding."""
    try:
        url = f"http://{host}:{port}/rest/info"
        req = urllib.request.urlopen(url, timeout=5)
        return req.status == 200
    except (urllib.error.URLError, OSError):
        return False


def main():
    service = sys.argv[1] if len(sys.argv) > 1 else "geoservices"

    if service == "pygeoapi":
        ok = check_pygeoapi()
    elif service == "geoservices":
        ok = check_geoservices()
    else:
        print(f"Unknown service: {service}", file=sys.stderr)
        sys.exit(1)

    if ok:
        print(f"{service}: healthy")
        sys.exit(0)
    else:
        print(f"{service}: unhealthy", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
