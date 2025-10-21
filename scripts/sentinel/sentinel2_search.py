import json, os, sys, requests, pandas as pd
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from datetime import datetime

# --- INPUTS ---
AOI_GEOJSON = r"C:\EGM704\data_sets\egm704_project\qgis\AOI\desborough_aoi.geojson"
DATE_FROM   = "2025-05-01T00:00:00Z"
DATE_TO     = "2025-10-11T23:59:59Z"
CLOUD_MAX   = 20

# --- OUTPUTS ---
META_DIR = r"C:\EGM704\data_sets\egm704_project\data\sentinel2\metadata"
os.makedirs(META_DIR, exist_ok=True)
CSV_OUT = os.path.join(META_DIR, f"s2_cdse_search_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")

# --- CDSE OData endpoint (search) ---
ODATA = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

def fail(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

# Load AOI geometry
if not os.path.exists(AOI_GEOJSON):
    fail(f"AOI file not found: {AOI_GEOJSON}")

with open(AOI_GEOJSON, "r", encoding="utf-8") as f:
    gj = json.load(f)

geoms = []
for feat in gj.get("features", []):
    geoms.append(shape(feat["geometry"]))
if not geoms:
    fail("No features found in AOI GeoJSON.")

aoi = unary_union(geoms).buffer(0)
# Build POLYGON WKT (closed ring) from the AOI bbox (safe & simple)
minx, miny, maxx, maxy = aoi.bounds
poly_wkt = f"POLYGON(({minx} {miny},{maxx} {miny},{maxx} {maxy},{minx} {maxy},{minx} {miny}))"
geog = f"geography'SRID=4326;{poly_wkt}'"
footprint_filter = f"OData.CSC.Intersects(area={geog})"  # <-- removed geometry=Footprint

# Ensure CDSE-friendly timestamp literals (with milliseconds)
DATE_FROM = "2025-05-01T00:00:00.000Z"
DATE_TO   = "2025-10-11T23:59:59.999Z"

# Build the $filter
flt = (
    "Collection/Name eq 'SENTINEL-2' "
    "and Attributes/processingLevel eq 'L2A' "
    f"and ContentDate/Start ge {DATE_FROM} and ContentDate/Start le {DATE_TO} "
    f"and Attributes/cloudCoverPercentage le {CLOUD_MAX} "
    f"and {footprint_filter}"
)

params = {
    "$filter": flt,
    "$select": "Id,Name,ContentDate,Attributes,GeoFootprint",
    "$orderby": "ContentDate/Start desc",
    "$top": 100
}

print("[INFO] Querying CDSE OData…")
print("[DEBUG] Params:", params)  # helpful if it 400s again
r = requests.get(ODATA, params=params, timeout=90)
try:
    r.raise_for_status()
except Exception as e:
    print("[DEBUG] Final URL:", r.url)
    print("[DEBUG] Body:", r.text[:800])
    fail(f"OData request failed: {e}")

items = r.json().get("value", [])
print(f"[INFO] Found {len(items)} item(s).")

if not items:
    sys.exit(0)

# Flatten to DataFrame
rows = []
for it in items:
    attrs = it.get("Attributes", {}) or {}
    rows.append({
        "id": it.get("Id"),
        "name": it.get("Name"),
        "begin": it.get("ContentDate", {}).get("Start"),
        "end": it.get("ContentDate", {}).get("End"),
        "cloudcover": attrs.get("cloudCoverPercentage"),
        "mgrs": attrs.get("tileId") or attrs.get("name"),  # may vary
    })

df = pd.DataFrame(rows).sort_values("begin")
df.to_csv(CSV_OUT, index=False)
print(f"[OK] Saved CSV → {CSV_OUT}")

# Tip: to download later you will need a CDSE access token and use:
# https://download.dataspace.copernicus.eu/odata/v1/Products(<Id>)/$value
