from sentinelsat import SentinelAPI, geojson_to_wkt
from datetime import datetime
import pandas as pd
import os

# ---- USER SETTINGS ----
USERNAME = "charmsn"
PASSWORD = "AbiJoel42"
HUB_URL  = "https://apihub.copernicus.eu/apihub"  # Copernicus Open Access Hub
AOI_GEOJSON = r"C:\EGM704\data_sets\egm704_project\qgis\AOI\desborough_aoi.geojson"
DATE_FROM = "2025-05-01"
DATE_TO   = "2025-10-11"  # today
CLOUD_MAX = 20

# ---- OUTPUTS ----
METADATA_DIR = r"C:\EGM704\data_sets\egm704_project\data\sentinel2\metadata"
os.makedirs(METADATA_DIR, exist_ok=True)
CSV_OUT = os.path.join(METADATA_DIR, f"s2_search_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")

# ---- RUN QUERY ----
api = SentinelAPI(USERNAME, PASSWORD, HUB_URL)
footprint = geojson_to_wkt(AOI_GEOJSON)

products = api.query(
    area=footprint,
    date=(DATE_FROM.replace("-", ""), DATE_TO.replace("-", "")),
    platformname="Sentinel-2",
    processinglevel="Level-2A",
    cloudcoverpercentage=(0, CLOUD_MAX)
)

print(f"Found {len(products)} Sentinel-2 L2A items with cloud ≤ {CLOUD_MAX}%")

if products:
    df = api.to_dataframe(products)
    # common, handy columns
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
else:
    print("No items matched your criteria.")
