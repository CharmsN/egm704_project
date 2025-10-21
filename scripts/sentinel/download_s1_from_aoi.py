#!/usr/bin/env python3
# download_s1_from_aoi.py
import os
import argparse
import geopandas as gpd
from shapely.geometry import shape
from shapely.ops import unary_union
from sentinelsat import SentinelAPI
from pathlib import Path

def load_aoi(aoi_path: str, layer: str | None = None, buffer_m: float | None = None):
    """
    Load AOI from a GeoPackage/GeoJSON/Shapefile. Optionally buffer by meters.
    Returns:
      aoi_wgs84 (shapely geometry in EPSG:4326),
      aoi_webm (same geometry in EPSG:3857) for area/overlap calcs.
    """
    gdf = gpd.read_file(aoi_path, layer=layer) if layer else gpd.read_file(aoi_path)
    if gdf.empty:
        raise ValueError(f"No features found in {aoi_path} (layer={layer})")
    # dissolve to single geometry
    geom = unary_union(gdf.geometry)
    # buffer (in meters) if asked: do in Web Mercator for simplicity
    gdf_3857 = gpd.GeoDataFrame(geometry=[geom], crs=gdf.crs or "EPSG:4326").to_crs(3857)
    geom_3857 = gdf_3857.geometry.iloc[0]
    if buffer_m:
        geom_3857 = geom_3857.buffer(buffer_m)
    # keep both 3857 and 4326 versions
    aoi_webm = geom_3857
    aoi_wgs84 = gpd.GeoSeries([geom_3857], crs=3857).to_crs(4326).iloc[0]
    return aoi_wgs84, aoi_webm

def find_best_s1(aoi_wgs84, aoi_webm, date_start: str, date_end: str,
                 product_type="GRD", mode="IW", polarisation="VV VH",
                 orbit_direction=None, limit=None):
    """
    Query SciHub for Sentinel-1 over the AOI and compute overlap. Returns (gdf, best_id).
    date_* format: 'YYYYMMDD'
    """
    api = SentinelAPI(None, None, api_url="https://scihub.copernicus.eu/dhus")

    query_kwargs = dict(
        platformname="Sentinel-1",
        producttype=product_type,              # 'GRD' or 'SLC'
        sensoroperationalmode=mode,            # 'IW' common over land
        polarisationmode=polarisation          # e.g. 'VV VH'
    )
    if orbit_direction:
        query_kwargs["orbitdirection"] = orbit_direction  # 'ASCENDING'/'DESCENDING'

    products = api.query(
        aoi_wgs84.wkt,        # WKT in EPSG:4326
        date=(date_start, date_end),
        **query_kwargs
    )
    n = len(products)
    print(f"Found {n} Sentinel-1 {product_type} product(s).")
    if n == 0:
        return None, None, None

    gdf = SentinelAPI.to_geodataframe(products)
    # project product footprints to 3857 for robust area/overlap calcs
    gdf = gdf.set_crs(4326, allow_override=True).to_crs(3857)

    aoi_area = aoi_webm.area
    gdf["overlap_area"] = gdf.geometry.intersection(aoi_webm).area
    gdf["overlap_frac"] = (gdf["overlap_area"] / aoi_area).fillna(0)

    # sort by overlap descending
    gdf = gdf.sort_values("overlap_frac", ascending=False)

    if limit is not None:
        gdf = gdf.head(int(limit))

    best_id = gdf.index[0]
    return gdf, best_id, api

def main():
    p = argparse.ArgumentParser(description="Search & download Sentinel-1 over an AOI (GeoPackage, etc.) via SciHub.")
    p.add_argument("--aoi", required=True, help="Path to AOI file (e.g., desborough_aoi.gpkg)")
    p.add_argument("--layer", default=None, help="Layer name in AOI (if GeoPackage has multiple)")
    p.add_argument("--buffer-m", type=float, default=None, help="Optional buffer around AOI in meters")
    p.add_argument("--start", required=True, help="Start date YYYYMMDD")
    p.add_argument("--end", required=True, help="End date YYYYMMDD")
    p.add_argument("--product-type", default="GRD", choices=["GRD", "SLC"])
    p.add_argument("--mode", default="IW", help="Sensor operational mode (e.g., IW, EW, SM)")
    p.add_argument("--pol", default="VV VH", help="Polarisation mode (e.g., 'VV VH', 'VV', 'HH HV')")
    p.add_argument("--orbit", default=None, choices=["ASCENDING", "DESCENDING"], help="Optional orbit direction")
    p.add_argument("--outdir", default="s1_downloads", help="Download directory")
    p.add_argument("--download-all", action="store_true", help="Download all results (sorted by overlap)")
    p.add_argument("--limit", type=int, default=None, help="Limit number of results considered/downloaded")
    args = p.parse_args()

    # Load AOI
    print(f"Loading AOI: {args.aoi} (layer={args.layer})")
    aoi_wgs84, aoi_webm = load_aoi(args.aoi, layer=args.layer, buffer_m=args.buffer_m)

    # Query & rank by overlap
    gdf, best_id, api = find_best_s1(
        aoi_wgs84, aoi_webm,
        date_start=args.start, date_end=args.end,
        product_type=args.product_type, mode=args.mode,
        polarisation=args.pol, orbit_direction=args.orbit,
        limit=args.limit
    )
    if gdf is None:
        print("No products found.")
        return

    # Show top hits
    show_cols = ["title", "beginposition", "endposition", "overlap_frac"]
    print("\nTop results (sorted by overlap):")
    try:
        print(gdf[show_cols].head(10).to_string(index=True))
    except Exception:
        print(gdf.head(10))

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    if args.download_all:
        # Download each (descending overlap). Use api.download for clarity.
        print(f"\nDownloading up to {len(gdf)} product(s) to: {os.path.abspath(args.outdir)}")
        for pid in gdf.index:
            print(f"Downloading: {pid}")
            api.download(pid, directory_path=args.outdir)
        print("Done.")
    else:
        print(f"\nBest overlap product id: {best_id}")
        api.download(best_id, directory_path=args.outdir)
        print("Done.")

if __name__ == "__main__":
    main()
