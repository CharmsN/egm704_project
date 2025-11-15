import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.enums import Resampling

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

PROJECT_ROOT = Path(r"C:\EGM704\data_sets\egm704_project")

RAW_LIDAR_DIR = PROJECT_ROOT / "data" / "raw" / "lidar_2022"
RAW_AOI = PROJECT_ROOT / "data" / "raw" / "aoi" / "aoi_sites.gpkg"
OUT_DIR = PROJECT_ROOT / "data" / "processed" / "lidar"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Each tile code must have a file RAW_LIDAR_DIR / f"{tile_code}.tif"
SITE_TILE_MAP = {
    "desborough_bng": ["SP88sw", "SP78se"],
    "wicksteed_bng": ["SP87ne"],
    "rewild_harb_bng": ["SP78nw", "SP79sw", "SP79se", "SP78ne"],
}

# ----------------------------------------------------------------------
# FUNCTIONS
# ----------------------------------------------------------------------


def load_aoi_for_site(site_name: str, aoi_path: Path = RAW_AOI) -> gpd.GeoDataFrame:
    """
    Load AOI geometry for a site from a Geopackage layer.

    site_name must match a layer name in the GPKG, e.g. 'desborough_bng'.
    Returns a 1-row GeoDataFrame.
    """
    if not aoi_path.exists():
        raise FileNotFoundError(f"AOI geopackage not found: {aoi_path}")

    gdf = gpd.read_file(aoi_path, layer=site_name)

    if len(gdf) == 0:
        raise ValueError(f"Layer '{site_name}' in {aoi_path} has no features.")

    # If multiple polygons exist for a site, dissolve into one
    if len(gdf) > 1:
        gdf = gdf.dissolve().reset_index(drop=True)

    # Keep as 1-row GeoDataFrame
    return gdf.iloc[[0]]


def clip_raster_to_aoi(
    raster_path: Path, aoi_gdf: gpd.GeoDataFrame, out_path: Path
) -> None:
    """Clip a raster to the AOI and save to out_path."""
    with rasterio.open(raster_path) as src:
        # Reproject AOI to raster CRS if needed
        if aoi_gdf.crs != src.crs:
            aoi_gdf = aoi_gdf.to_crs(src.crs)

        geoms = [aoi_gdf.geometry.values[0]]

        out_image, out_transform = mask(src, geoms, crop=True)
        out_meta = src.meta.copy()
        out_meta.update(
            {
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
            }
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **out_meta) as dst:
            dst.write(out_image)

    print(f"Saved clipped raster: {out_path}")


def compute_hillshade(dem_path: Path, out_path: Path, azimuth=315, altitude=45):
    """Simple hillshade from DTM using numpy (per tile)."""
    with rasterio.open(dem_path) as src:
        dem = src.read(1).astype("float32")
        dem[dem == src.nodata] = np.nan

        # Cellsize (assume square pixels)
        x_res = src.transform.a
        y_res = -src.transform.e  # usually negative in transform

        # Gradients
        dzdx, dzdy = np.gradient(dem, x_res, y_res)

        az = np.deg2rad(azimuth)
        alt = np.deg2rad(altitude)

        slope = np.arctan(np.hypot(dzdx, dzdy))
        aspect = np.arctan2(-dzdx, dzdy)

        # Hillshade formula
        hs = (
            np.sin(alt) * np.cos(slope)
            + np.cos(alt) * np.sin(slope) * np.cos(az - aspect)
        )

        # Normalise to 0–255, guarding against all-NaN / constant cases
        hs_min = np.nanmin(hs)
        hs_max = np.nanmax(hs)
        if np.isfinite(hs_min) and np.isfinite(hs_max) and hs_max != hs_min:
            hs_norm = (hs - hs_min) / (hs_max - hs_min)
        else:
            hs_norm = np.zeros_like(hs)

        hs_byte = (hs_norm * 255).astype("uint8")
        hs_byte[np.isnan(hs)] = 0  # nodata as 0 (black)

        profile = src.profile
        profile.update(
            dtype=rasterio.uint8,
            count=1,
            nodata=0,
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(hs_byte, 1)

    print(f"Saved hillshade: {out_path}")


def resample_to_10m(in_path: Path, out_path: Path, target_res: float = 10) -> None:
    """Resample 1 m DTM to 10 m (mean), matching Sentinel-2 scale."""
    with rasterio.open(in_path) as src:
        # Assume square pixels; src.res[0] is pixel size
        scale = target_res / src.res[0]
        new_height = int(src.height / scale)
        new_width = int(src.width / scale)

        transform = src.transform * rasterio.Affine.scale(scale, scale)

        kwargs = src.meta.copy()
        kwargs.update(
            {
                "height": new_height,
                "width": new_width,
                "transform": transform,
            }
        )

        data = src.read(
            out_shape=(src.count, new_height, new_width),
            resampling=Resampling.average,
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **kwargs) as dst:
            dst.write(data)

    print(f"Saved resampled DTM (10 m): {out_path}")


# ----------------------------------------------------------------------
# MAIN WORKFLOW
# ----------------------------------------------------------------------


def process_site(site_name: str, tile_codes) -> None:
    """
    Process one site that may span multiple LiDAR tiles.

    For each tile:
    - Clip tile to AOI  ->  *_DTM_1m_clipped.tif
    - Generate hillshade (1 m) -> *_hillshade_1m.tif
    - Resample DTM to 10 m -> *_DTM_10m.tif

    No mosaicking – outputs are per-tile.
    """
    print(f"\n=== Processing site '{site_name}' ===")
    aoi = load_aoi_for_site(site_name)

    site_out_dir = OUT_DIR / site_name
    site_out_dir.mkdir(parents=True, exist_ok=True)

    for tile_code in tile_codes:
        dtm_path = RAW_LIDAR_DIR / f"{tile_code}.tif"
        if not dtm_path.exists():
            raise FileNotFoundError(f"Expected LiDAR file not found: {dtm_path}")

        print(f"Using DTM for tile_code='{tile_code}': {dtm_path}")

        # 1. Clip to 1 m DTM
        clipped_path = site_out_dir / f"{site_name}_{tile_code}_DTM_1m_clipped.tif"
        clip_raster_to_aoi(dtm_path, aoi, clipped_path)

        # 2. Hillshade at 1 m
        hillshade_path = site_out_dir / f"{site_name}_{tile_code}_hillshade_1m.tif"
        compute_hillshade(clipped_path, hillshade_path)

        # 3. Resample to 10 m
        dtm_10m_path = site_out_dir / f"{site_name}_{tile_code}_DTM_10m.tif"
        resample_to_10m(clipped_path, dtm_10m_path)


if __name__ == "__main__":
    for site_name, tile_codes in SITE_TILE_MAP.items():
        process_site(site_name, tile_codes)
