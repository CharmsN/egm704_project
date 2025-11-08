"""
Download Sentinel-1 products for your AOI using Copernicus Data Space (CDSE).

- Auth via Keycloak token (CDSE_USER / CDSE_PASS env vars)
- AOI from aoi_combined.gpkg
- Downloads ZIPs to data/raw/sentinel1

Adjust:
    AOI_PATH, START_DATE, END_DATE, MAX_RESULTS
as needed.
"""

import os
from pathlib import Path

import geopandas as gpd
import requests

# CDSE endpoints
TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
    "protocol/openid-connect/token"
)
CATALOGUE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_BASE_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"

# Paths (adjust if needed)
AOI_PATH = r"C:\EGM704\data_sets\egm704_project\data\raw\aoi_combined.gpkg"
OUT_DIR = Path(r"C:\EGM704\data_sets\egm704_project\data\raw\sentinel1")

# Date range and query limits
START_DATE = "2024-01-01"   # YYYY-MM-DD
END_DATE = "2024-12-31"
MAX_RESULTS = 5             # keep small while testing


def get_cdse_token() -> str:
    """Get an access token from Copernicus Data Space using env vars."""
    username = os.environ.get("CDSE_USER")
    password = os.environ.get("CDSE_PASS")

    if not username or not password:
        raise RuntimeError(
            "CDSE_USER or CDSE_PASS environment variables not set. "
            "Set them with `setx` and restart your terminal."
        )

    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    resp = requests.post(TOKEN_URL, data=data)
    resp.raise_for_status()
    token_info = resp.json()
    return token_info["access_token"]


def load_aoi_geometry(aoi_path: str):
    """Load AOI from GeoPackage and return a single WGS84 geometry."""
    print(f"Loading AOI from: {aoi_path}")
    aoi = gpd.read_file(aoi_path)

    if aoi.crs is None:
        raise ValueError("AOI has no CRS set. Please define a CRS first.")

    aoi_4326 = aoi.to_crs(4326)

    # Handle unary_union deprecation cleanly
    try:
        geom = aoi_4326.union_all()
    except AttributeError:
        geom = aoi_4326.unary_union

    print("AOI geometry loaded and converted to EPSG:4326")
    return geom


def search_s1_for_aoi(geom, start_date: str, end_date: str,
                      token: str, max_results: int = 10):
    """
    Search Sentinel-1 products intersecting the AOI.

    - Uses CDSE OData catalogue
    - Prints raw product names
    - Returns products after a light filter:
      keep IW GRD variants only, drop SLC/AUX/RAW/ETA.
    """
    minx, miny, maxx, maxy = geom.bounds

    # WKT polygon from bbox (CDSE expects SRID=4326)
    wkt = (
        f"POLYGON(({minx} {miny}, {minx} {maxy}, "
        f"{maxx} {maxy}, {maxx} {miny}, {minx} {miny}))"
    )

    headers = {"Authorization": f"Bearer {token}"}

    # OData filter: Sentinel-1 + intersects AOI + time range
    filter_expr = (
        "Collection/Name eq 'SENTINEL-1' and "
        f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}') and "
        f"ContentDate/Start ge {start_date}T00:00:00Z and "
        f"ContentDate/Start le {end_date}T23:59:59Z"
    )

    params = {
        "$top": max_results,
        "$filter": filter_expr,
        "$orderby": "ContentDate/Start desc",
    }

    print("Querying CDSE catalogue for Sentinel-1 products ...")
    r = requests.get(CATALOGUE_URL, headers=headers, params=params)
    r.raise_for_status()
    data = r.json()
    products = data.get("value", [])

    print(f"Raw products from CDSE: {len(products)}")
    for p in products:
        print("  -", p.get("Name", "(no name)"))

    # Filter:
    #  - Keep only S1A/S1B IW GRD* products
    #  - Exclude SLC, AUX, RAW, ETA
    filtered = []
    for p in products:
        name = p.get("Name", "")

        if not name:
            continue

        if ("S1A" not in name) and ("S1B" not in name):
            continue
        if "IW" not in name:
            continue
        if "GRD" not in name:
            continue

        # Exclude obvious non-standard or massive products
        if "SLC" in name:
            continue
        if "AUX_" in name:
            continue
        if "RAW__" in name:
            continue
        if "ETA__" in name:
            continue

        filtered.append(p)

    print(f"Products after filtering to IW GRD only: {len(filtered)}")
    return filtered


def download_product(product: dict, out_dir: Path, token: str):
    """Download a single Sentinel-1 product by product dict."""
    product_id = product["Id"]
    name = product["Name"]

    safe_name = name.replace(".SAFE", "")
    out_path = out_dir / f"{safe_name}.zip"

    if out_path.exists():
        print(f"Already downloaded: {out_path.name}")
        return

    headers = {"Authorization": f"Bearer {token}"}
    url = f"{DOWNLOAD_BASE_URL}({product_id})/$value"

    print(f"Downloading {name} to {out_path}")
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
    except Exception as e:
        # If something goes wrong, remove partial file
        if out_path.exists():
            try:
                out_path.unlink()
            except Exception:
                pass
        raise e

    print(f"Saved: {out_path}")


def main():
    print("Starting Sentinel-1 download with CDSE")
    token = get_cdse_token()
    print("Token acquired")

    geom = load_aoi_geometry(AOI_PATH)

    products = search_s1_for_aoi(
        geom=geom,
        start_date=START_DATE,
        end_date=END_DATE,
        token=token,
        max_results=MAX_RESULTS,
    )

    if not products:
        print("No Sentinel-1 IW GRD products found for this AOI/date range.")
        return

    print("These products will be downloaded:")
    for p in products:
        print("  -", p["Name"])

    for p in products:
        try:
            download_product(p, OUT_DIR, token)
        except Exception as e:
            print(f"Failed to download {p['Name']}: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
