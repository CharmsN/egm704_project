# stack_s2_clipped_bands.py
#
# Build a single multiband GeoTIFF from previously clipped Sentinel-2 bands.
# Assumes filenames still contain the band name, e.g. ..._B04_10m_desborough_operational.tif

from pathlib import Path
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np

# ------------- USER SETTINGS -------------
INPUT_DIR = Path(r"../data/sentinel2_clipped")
OUTPUT_PATH = Path(r"../data/sentinel2_clipped/desborough_s2_stack.tif")
AOI_NAME = "desborough_operational"   # to help filter, set to '' to take all

# bands we want, in this order
BANDS = [
    ("B02", 10),
    ("B03", 10),
    ("B04", 10),
    ("B08", 10),
    ("B11", 20),
    ("B12", 20),
]
# -----------------------------------------


def find_band_file(base_dir: Path, band_code: str, aoi_name: str):
    """
    Look for a file in base_dir that contains _B0X_ and the AOI name.
    Returns the first match.
    """
    candidates = list(base_dir.glob(f"*{band_code}*{aoi_name}*.tif"))
    if not candidates:
        # try looser
        candidates = list(base_dir.glob(f"*{band_code}*.tif"))
    return candidates[0] if candidates else None


def resample_to(ref, src_path, dst_shape, dst_transform):
    """Resample src_path to match ref (10 m)"""
    with rasterio.open(src_path) as src:
        data = src.read(1)
        dst_data = np.empty(dst_shape, dtype=data.dtype)

        reproject(
            source=data,
            destination=dst_data,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=dst_transform,
            dst_crs=ref.crs,
            resampling=Resampling.bilinear,
        )

    return dst_data


def main():
    # 1) open a 10 m reference band (use B04 if available)
    ref_file = find_band_file(INPUT_DIR, "B04", AOI_NAME) or \
               find_band_file(INPUT_DIR, "B02", AOI_NAME)

    if ref_file is None:
        raise FileNotFoundError("Could not find a 10 m reference band in clipped folder.")

    with rasterio.open(ref_file) as ref:
        profile = ref.meta.copy()
        ref_height = ref.height
        ref_width = ref.width
        ref_transform = ref.transform
        ref_crs = ref.crs

    stacked = []

    # 2) loop over desired bands
    for band_code, band_res in BANDS:
        band_file = find_band_file(INPUT_DIR, band_code, AOI_NAME)
        if band_file is None:
            print(f"⚠️  Band {band_code} not found, will fill with zeros.")
            stacked.append(np.zeros((ref_height, ref_width), dtype=profile["dtype"]))
            continue

        if band_res == 10:
            # open and read directly
            with rasterio.open(band_file) as src:
                data = src.read(1)
            stacked.append(data)
        else:
            # 20 m → resample to 10 m
            print(f"Resampling {band_file.name} (20 m) to 10 m to match stack...")
            data_20m = resample_to(
                ref=rasterio.open(ref_file),
                src_path=band_file,
                dst_shape=(ref_height, ref_width),
                dst_transform=ref_transform,
            )
            stacked.append(data_20m)

    # 3) write out
    profile.update({
        "driver": "GTiff",
        "count": len(stacked),
        "height": ref_height,
        "width": ref_width,
        "transform": ref_transform,
        "crs": ref_crs,
    })

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(OUTPUT_PATH, "w", **profile) as dst:
        for idx, band_array in enumerate(stacked, start=1):
            dst.write(band_array, idx)

    print(f"✅ Stack written to {OUTPUT_PATH}")
    print("Band order in stack:")
    for i, (bc, _) in enumerate(BANDS, start=1):
        print(f"  {i}: {bc}")


if __name__ == "__main__":
    main()
