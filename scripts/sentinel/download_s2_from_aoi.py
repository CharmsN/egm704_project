# download_s2_from_aoi.py
#
# Query and download Sentinel-2 L2A products for a named AOI
# Uses .netrc for credentials (no passwords in code)

from sentinelsat import SentinelAPI
import geopandas as gpd
from datetime import date

# ---------------- USER SETTINGS ----------------
AOI_PATH = r"../data/aoi/egm704_aoi_wgs84.gpkg"   # relative to /scripts
LAYER = "aoi_sites"
FEATURE = "desborough_operational"               # change to your AOI name
OUTPUT_DIR = r"../data/sentinel2_raw"

# date range – adjust as needed
DATE_FROM = "2024-01-01"
DATE_TO = date.today().isoformat()

# max cloud cover in percent (0..100)
MAX_CLOUD = 20
# ------------------------------------------------

# 1. Load AOI
gdf = gpd.read_file(AOI_PATH, layer=LAYER)
aoi = gdf[gdf["name"] == FEATURE]

if aoi.empty:
    raise ValueError(f"AOI named '{FEATURE}' not found in {AOI_PATH}")

geom = aoi.geometry.iloc[0].__geo_interface__

# 2. Connect to Copernicus (reads ~/.netrc)
api = SentinelAPI(None, None, "https://scihub.copernicus.eu/dhus")

# 3. Query Sentinel-2 L2A
products = api.query(
    area=geom,
    date=(DATE_FROM, DATE_TO),
    platformname="Sentinel-2",
    producttype="S2MSI2A",              # Level-2A
    cloudcoverpercentage=(0, MAX_CLOUD)
)

print(f"Found {len(products)} Sentinel-2 products for {FEATURE}")

# 4. Download (all)
if products:
    api.download_all(products, directory_path=OUTPUT_DIR)
else:
    print("No products matched the query.")
