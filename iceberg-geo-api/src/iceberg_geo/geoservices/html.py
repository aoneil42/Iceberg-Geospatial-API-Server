"""
HTML rendering for the Esri GeoServices REST directory.

Produces pages that match the standard ArcGIS REST Services Directory
look and feel, with breadcrumb navigation and property listings.
"""

from html import escape

_CSS = """\
body { font-family: "Lucida Sans Unicode","Lucida Grande",Verdana,Arial,Helvetica,sans-serif;
       font-size: 0.8em; color: #333; margin: 0; padding: 0; background: #fff; }
#header { background: #e8e8e8; border-bottom: 1px solid #aaa; padding: 6px 12px; }
#header h1 { font-size: 1.1em; margin: 0; font-weight: bold; color: #333; }
#breadcrumbs { padding: 6px 12px; font-size: 0.9em; color: #555; border-bottom: 1px solid #ddd;
               background: #f5f5f5; }
#breadcrumbs a { color: #0066cc; text-decoration: none; }
#breadcrumbs a:hover { text-decoration: underline; }
#content { padding: 12px; }
h2 { font-size: 1.05em; margin: 16px 0 8px 0; color: #333; border-bottom: 1px solid #ddd;
     padding-bottom: 4px; }
.prop { margin: 4px 0; }
.prop-name { font-weight: bold; color: #555; }
.prop-value { color: #333; }
a { color: #0066cc; text-decoration: none; }
a:hover { text-decoration: underline; }
ul { margin: 4px 0 4px 20px; padding: 0; }
li { margin: 2px 0; }
table.fields { border-collapse: collapse; margin: 8px 0; }
table.fields th { background: #e8e8e8; border: 1px solid #ccc; padding: 4px 8px;
                  text-align: left; font-size: 0.95em; }
table.fields td { border: 1px solid #ccc; padding: 4px 8px; font-size: 0.95em; }
.json-link { font-size: 0.85em; color: #888; margin-left: 8px; }
"""


def _page(title: str, breadcrumbs: list[tuple[str, str]], body: str) -> str:
    """Wrap body HTML in the standard ArcGIS REST directory page shell."""
    crumbs_html = ""
    for i, (label, href) in enumerate(breadcrumbs):
        if i > 0:
            crumbs_html += " &gt; "
        if href:
            crumbs_html += f'<a href="{escape(href)}">{escape(label)}</a>'
        else:
            crumbs_html += f"<b>{escape(label)}</b>"

    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{escape(title)}</title>
<style>{_CSS}</style></head>
<body>
<div id="header"><h1>ArcGIS REST Services Directory</h1></div>
<div id="breadcrumbs">{crumbs_html}</div>
<div id="content">{body}</div>
</body></html>"""


def _prop(name: str, value) -> str:
    return (
        f'<div class="prop"><span class="prop-name">{escape(name)}:</span> '
        f'<span class="prop-value">{escape(str(value))}</span></div>'
    )


def _link_prop(name: str, label: str, href: str) -> str:
    return (
        f'<div class="prop"><span class="prop-name">{escape(name)}:</span> '
        f'<a href="{escape(href)}">{escape(label)}</a></div>'
    )


def render_rest_info(base_url: str, services: list[dict]) -> str:
    """Render /rest/info as HTML."""
    body = _prop("Current Version", "11.0")
    body += '<h2>Services</h2><ul>'
    for svc in services:
        name = svc["name"]
        stype = svc["type"]
        href = f"{base_url}/rest/services/{name}/FeatureServer"
        body += f'<li><a href="{escape(href)}">{escape(name)}</a> ({escape(stype)})</li>'
    body += "</ul>"

    return _page(
        "ArcGIS REST Services Directory",
        [("Home", None)],
        body,
    )


def render_services_directory(base_url: str, services: list[dict]) -> str:
    """Render /rest/services as HTML."""
    body = '<h2>Services</h2><ul>'
    for svc in services:
        name = svc["name"]
        stype = svc["type"]
        href = f"{base_url}/rest/services/{name}/FeatureServer"
        body += f'<li><a href="{escape(href)}">{escape(name)}</a> ({escape(stype)})</li>'
    body += "</ul>"

    return _page(
        "Folder: /",
        [("Home", f"{base_url}/rest/services"), ("Services", None)],
        body,
    )


def render_feature_server(
    base_url: str, service_id: str, metadata: dict
) -> str:
    """Render /rest/services/{service_id}/FeatureServer as HTML."""
    svc_url = f"{base_url}/rest/services/{service_id}/FeatureServer"

    body = _prop("Service Description", metadata.get("serviceDescription", ""))
    body += _prop("Max Record Count", metadata.get("maxRecordCount", ""))
    body += _prop("Supported Query Formats", metadata.get("supportedQueryFormats", ""))
    body += _prop("Capabilities", metadata.get("capabilities", ""))

    sr = metadata.get("spatialReference", {})
    body += _prop("Spatial Reference", f'{sr.get("wkid", "")} ({sr.get("latestWkid", "")})')

    body += "<h2>Layers</h2><ul>"
    for layer in metadata.get("layers", []):
        lid = layer["id"]
        name = layer["name"]
        href = f"{svc_url}/{lid}"
        body += f'<li><a href="{escape(href)}">{escape(name)}</a> ({lid})</li>'
    body += "</ul>"

    json_href = f"{svc_url}?f=json"
    body += f'<h2>Supported Interfaces</h2>'
    body += f'<a href="{escape(json_href)}">JSON</a>'

    return _page(
        f"{service_id} (FeatureServer)",
        [
            ("Home", f"{base_url}/rest/services"),
            (service_id, None),
            ("FeatureServer", None),
        ],
        body,
    )


def render_layer(
    base_url: str, service_id: str, layer_id: int, metadata: dict
) -> str:
    """Render /rest/services/{service_id}/FeatureServer/{layer_id} as HTML."""
    svc_url = f"{base_url}/rest/services/{service_id}/FeatureServer"
    layer_url = f"{svc_url}/{layer_id}"
    layer_name = metadata.get("name", str(layer_id))

    body = _prop("Name", layer_name)
    body += _prop("Type", metadata.get("type", "Feature Layer"))
    body += _prop("Geometry Type", metadata.get("geometryType", ""))
    body += _prop("Object ID Field", metadata.get("objectIdField", ""))
    body += _prop("Max Record Count", metadata.get("maxRecordCount", ""))
    body += _prop("Supported Query Formats", metadata.get("supportedQueryFormats", ""))
    body += _prop("Capabilities", metadata.get("capabilities", ""))

    # Extent
    ext = metadata.get("extent", {})
    if ext:
        sr = ext.get("spatialReference", {})
        body += _prop(
            "Extent",
            f'[{ext.get("xmin")}, {ext.get("ymin")}, '
            f'{ext.get("xmax")}, {ext.get("ymax")}] '
            f'(WKID: {sr.get("wkid", "")})',
        )

    # Fields table
    fields = metadata.get("fields", [])
    if fields:
        body += "<h2>Fields</h2>"
        body += '<table class="fields"><tr><th>Name</th><th>Type</th><th>Alias</th></tr>'
        for fld in fields:
            body += (
                f"<tr><td>{escape(fld['name'])}</td>"
                f"<td>{escape(fld['type'])}</td>"
                f"<td>{escape(fld.get('alias', ''))}</td></tr>"
            )
        body += "</table>"

    # Query link
    query_href = f"{layer_url}/query?where=1%3D1&outFields=*&f=html"
    json_href = f"{layer_url}?f=json"
    body += "<h2>Supported Operations</h2>"
    body += f'<a href="{escape(query_href)}">Query</a>'
    body += f'<h2>Supported Interfaces</h2>'
    body += f'<a href="{escape(json_href)}">JSON</a>'

    return _page(
        f"{layer_name} ({service_id}/FeatureServer/{layer_id})",
        [
            ("Home", f"{base_url}/rest/services"),
            (service_id, f"{svc_url}"),
            ("FeatureServer", f"{svc_url}"),
            (layer_name, None),
        ],
        body,
    )


def render_query_form(
    base_url: str, service_id: str, layer_id: int, layer_name: str
) -> str:
    """Render the query form page for a layer."""
    svc_url = f"{base_url}/rest/services/{service_id}/FeatureServer"
    layer_url = f"{svc_url}/{layer_id}"
    query_url = f"{layer_url}/query"

    body = f"""\
<form action="{escape(query_url)}" method="get">
<h2>Query: {escape(layer_name)}</h2>
<table>
<tr><td><b>Where</b></td><td><input name="where" value="1=1" size="60"></td></tr>
<tr><td><b>Out Fields</b></td><td><input name="outFields" value="*" size="60"></td></tr>
<tr><td><b>Return Geometry</b></td>
    <td><select name="returnGeometry"><option value="true" selected>true</option>
    <option value="false">false</option></select></td></tr>
<tr><td><b>Geometry (bbox)</b></td><td><input name="geometry" value="" size="60"
    placeholder="xmin,ymin,xmax,ymax"></td></tr>
<tr><td><b>Spatial Rel</b></td>
    <td><select name="spatialRel">
    <option value="esriSpatialRelIntersects" selected>esriSpatialRelIntersects</option>
    <option value="esriSpatialRelContains">esriSpatialRelContains</option>
    <option value="esriSpatialRelWithin">esriSpatialRelWithin</option>
    </select></td></tr>
<tr><td><b>Result Record Count</b></td><td><input name="resultRecordCount" value="10" size="10"></td></tr>
<tr><td><b>Result Offset</b></td><td><input name="resultOffset" value="0" size="10"></td></tr>
<tr><td><b>Order By Fields</b></td><td><input name="orderByFields" value="" size="60"></td></tr>
<tr><td><b>Format</b></td>
    <td><select name="f"><option value="html" selected>HTML</option>
    <option value="json">JSON</option>
    <option value="geojson">GeoJSON</option>
    <option value="pbf">PBF</option>
    </select></td></tr>
<tr><td></td><td><input type="submit" value="Query (GET)"></td></tr>
</table>
</form>"""

    return _page(
        f"Query: {layer_name}",
        [
            ("Home", f"{base_url}/rest/services"),
            (service_id, f"{svc_url}"),
            ("FeatureServer", f"{svc_url}"),
            (layer_name, f"{layer_url}"),
            ("Query", None),
        ],
        body,
    )


def render_query_results(
    base_url: str,
    service_id: str,
    layer_id: int,
    layer_name: str,
    result_json: dict,
) -> str:
    """Render query results as an HTML table."""
    import json

    svc_url = f"{base_url}/rest/services/{service_id}/FeatureServer"
    layer_url = f"{svc_url}/{layer_id}"

    features = result_json.get("features", [])
    fields = result_json.get("fields", [])
    field_names = [f["name"] for f in fields] if fields else []

    # If no field metadata, infer from first feature attributes
    if not field_names and features:
        attrs = features[0].get("attributes", {})
        field_names = list(attrs.keys())

    body = f"<b>Results: {len(features)} features</b>"

    if features:
        body += '<table class="fields"><tr>'
        for fn in field_names:
            body += f"<th>{escape(fn)}</th>"
        body += "</tr>"
        for feat in features:
            attrs = feat.get("attributes", {})
            body += "<tr>"
            for fn in field_names:
                val = attrs.get(fn, "")
                body += f"<td>{escape(str(val))}</td>"
            body += "</tr>"
        body += "</table>"
    else:
        body += "<p>No features returned.</p>"

    return _page(
        f"Query Results: {layer_name}",
        [
            ("Home", f"{base_url}/rest/services"),
            (service_id, f"{svc_url}"),
            ("FeatureServer", f"{svc_url}"),
            (layer_name, f"{layer_url}"),
            ("Query", None),
        ],
        body,
    )
