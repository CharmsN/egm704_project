# clip_raster_to_aoi.py
#
# Clip a single raster (e.g. Sentinel-2 band) to an AOI in a GeoPackage

import rasterio
import geopandas as gpd
from rasterio.mask import mask
from pathlib import Path

# ------------ USER SETTINGS ------------
AOI_PATH = r"../data/aoi/egm704_aoi_wgs84.gpkg"
LAYER = "aoi_sites"
FEATURE = "desborough_operational"

INPUT_RASTER = r"../data/sentinel2_raw/example_band.jp2"   # change this
OUTPUT_RASTER = r"../data/sentinel2_raw/desborough_B04.tif"
# ---------------------------------------

# 1. read AOI
gdf = gpd.read_file(AOI_PATH, layer=LAYER)
aoi = gdf[gdf["name"] == FEATURE]

if aoi.empty:
    raise ValueError(f"AOI '{FEATURE}' not found in {AOI_PATH}")

# rasterio wants GeoJSON-like geometry
geoms = [aoi.geometry.iloc[0].__geo_interface__]

# 2. open raster and clip
with rasterio.open(INPUT_RASTER) as src:
    out_img, out_transform = mask(src, geoms, crop=True)
    out_meta = src.meta.copy()

# 3. update metadata
out_meta.update({
    "driver": "GTiff",
    "height": out_img.shape[1],
    "width": out_img.shape[2],
    "transform": out_transform
})

# 4. write out
Path(OUTPUT_RASTER).parent.mkdir(parents=True, exist_ok=True)
with rasterio.open(OUTPUT_RASTER, "w", **out_meta) as dst:
    dst.write(out_img)

print(f"✅ Clipped raster written to: {OUTPUT_RASTER}")
