from sentinelsat import SentinelAPI, geojson_to_wkt, read_geojson
from datetime import datetime
import pandas as pd
import os
import sys

# --- USER SETTINGS ---
# You can keep these hard-coded or (safer) set env vars COPERNICUS_USERNAME / COPERNICUS_PASSWORD.
USERNAME = os.getenv("COPERNICUS_USERNAME"
PASSWORD = os.getenv("COPERNICUS_PASSWORD")  # set in env for safety, or fill in temporarily
HUB_URL  = "https://apihub.copernicus.eu/apihub"  # Open Access Hub

# Use your actual AOI path (as you provided)
AOI_GEOJSON = r"C:\EGM704\data_sets\egm704_project\qgis\AOI\desborough_aoi.geojson"

# Date window (inclusive)
DATE_FROM = "2025-05-01"
DATE_TO   = "2025-10-11"  # today

CLOUD_MAX = 20  # percent

# --- OUTPUTS ---
METADATA_DIR = r"C:\EGM704\data_sets\egm704_project\data\sentinel2\metadata"
os.makedirs(METADATA_DIR, exist_ok=True)
CSV_OUT = os.path.join(METADATA_DIR, f"s2_search_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")

def fail(msg: str):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

# --- SANITY CHECKS ---
if not USERNAME or not (PASSWORD or os.getenv("COPERNICUS_PASSWORD")):
    print("[WARN] No Copernicus password detected. Set COPERNICUS_PASSWORD env var or fill PASSWORD in the script.")

if not os.path.exists(AOI_GEOJSON):
    fail(f"AOI file not found: {AOI_GEOJSON}")

# Load GeoJSON and convert to WKT (this fixes the TypeError you saw)
try:
    aoi_fc = read_geojson(AOI_GEOJSON)         # dict FeatureCollection
    footprint = geojson_to_wkt(aoi_fc)         # WKT polygon
except Exception as e:
    fail(f"Failed to read/convert AOI GeoJSON: {e}")

# Convert dates to YYYYMMDD (sentinelsat accepts strings or datetimes; this is explicit)
def yyyymmdd(s: str) -> str:
    return s.replace("-", "")

DATE_FROM_FMT = yyyymmdd(DATE_FROM)
DATE_TO_FMT   = yyyymmdd(DATE_TO)

# --- RUN QUERY ---
try:
    api = SentinelAPI(USERNAME, PASSWORD, HUB_URL)
except Exception as e:
    fail(f"Login/API init failed: {e}")

print(f"Querying S2 L2A for AOI={os.path.basename(AOI_GEOJSON)}, "
      f"{DATE_FROM}..{DATE_TO}, cloud ≤ {CLOUD_MAX}%")

try:
    products = api.query(
        area=footprint,
        date=(DATE_FROM_FMT, DATE_TO_FMT),
        platformname="Sentinel-2",
        processinglevel="Level-2A",
        cloudcoverpercentage=(0, CLOUD_MAX)
    )
except Exception as e:
    fail(f"Query failed: {e}")

count = len(products) if products else 0
print(f"Found {count} Sentinel-2 L2A item(s).")

if count == 0:
    sys.exit(0)

# Convert to DataFrame, keep handy columns, and save CSV
df = api.to_dataframe(products)

keep = [
    "title", "size", "beginposition", "endposition",
    "cloudcoverpercentage", "uuid", "link", "link_alternative"
]
for col in keep:
    if col not in df.columns:
        df[col] = None

df = df[keep].sort_values("beginposition")
df.to_csv(CSV_OUT, index=False)
print(f"Saved CSV → {CSV_OUT}")

# --- OPTIONAL: Download all (uncomment when ready) ---
# RAW_DIR = r"C:\EGM704\data_sets\egm704_project\data\sentinel2\raw"
# os.makedirs(RAW_DIR, exist_ok=True)
# api.download_all(products, directory_path=RAW_DIR)
# print(f"Downloaded products to: {RAW_DIR}")
