# clip_all_s2_bands_to_aoi.py
#
# Batch-clip all Sentinel-2 JP2 bands in a folder to a single AOI
# Output: GeoTIFFs clipped to your study area

import rasterio
import geopandas as gpd
from rasterio.mask import mask
from pathlib import Path

# ------------- USER SETTINGS -------------
AOI_PATH = r"../data/aoi/egm704_aoi_wgs84.gpkg"
LAYER = "aoi_sites"
FEATURE = "desborough_operational"

INPUT_DIR = Path(r"../data/sentinel2_raw")          # where your JP2s are
OUTPUT_DIR = Path(r"../data/sentinel2_clipped")     # where to write clipped tifs
# -----------------------------------------

# 1. load AOI
gdf = gpd.read_file(AOI_PATH, layer=LAYER)
aoi = gdf[gdf["name"] == FEATURE]

if aoi.empty:
    raise ValueError(f"AOI '{FEATURE}' not found in {AOI_PATH}")

geom = [aoi.geometry.iloc[0].__geo_interface__]

# 2. make output dir
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 3. find JP2 files (recursively, in case of SAFE structure)
jp2_files = list(INPUT_DIR.rglob("*.jp2"))

if not jp2_files:
    print(f"No .jp2 files found under {INPUT_DIR}")
else:
    print(f"Found {len(jp2_files)} JP2 files to clip")

# 4. loop and clip
for jp2_path in jp2_files:
    rel = jp2_path.name.replace(".jp2", "")  # just filename
    out_path = OUTPUT_DIR / f"{rel}_{FEATURE}.tif"

    print(f"Clipping {jp2_path} → {out_path}")

    with rasterio.open(jp2_path) as src:
        try:
            out_img, out_transform = mask(src, geom, crop=True)
        except ValueError:
            # geometry and raster don't overlap
            print(f"  ⚠️  Skipping {jp2_path} (no overlap)")
            continue

        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_img.shape[1],
            "width": out_img.shape[2],
            "transform": out_transform
        })

        with rasterio.open(out_path, "w", **out_meta) as dst:
            dst.write(out_img)

print("✅ Done.")
