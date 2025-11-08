#!/usr/bin/env python3
# download_s2_from_aoi.py
#
# Download Sentinel-2 scenes over AOI using:
# - STAC for search (AOI + date window)
# - zipper.dataspace.copernicus.eu for download (OData, which we know works)
#
# Uses existing CDSE_USER and CDSE_PASS environment variables for authentication.

import os
import requests
from pathlib import Path
import geopandas as gpd

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
STAC_SEARCH_URL = "https://catalogue.dataspace.copernicus.eu/stac/search"
ZIPPER_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({id})/$value"

# --- EDIT THESE AS NEEDED ---
BASE = Path(r"C:\EGM704\data_sets\egm704_project\data")
AOI_PATH = BASE / "raw" / "aoi_combined.gpkg"
OUT_DIR = BASE / "raw" / "sentinel2"

START_DATE = "2025-09-01T00:00:00Z"
END_DATE   = "2025-09-30T23:59:59Z"
MAX_ITEMS  = 4  # number of products to request from STAC


def get_cdse_token():
    """
    Get an access token from Copernicus Data Space using env vars:
    CDSE_USER and CDSE_PASS.
    """
    username = os.environ.get("CDSE_USER")
    password = os.environ.get("CDSE_PASS")
    if not username or not password:
        raise RuntimeError("CDSE_USER or CDSE_PASS not set in environment.")

    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    resp = requests.post(TOKEN_URL, data=data)
    resp.raise_for_status()
    j = resp.json()
    return j["access_token"], j.get("expires_in")


def get_aoi_bbox():
    """
    Read AOI from GeoPackage and return bbox [minLon, minLat, maxLon, maxLat] in EPSG:4326.
    """
    gdf = gpd.read_file(AOI_PATH)
    gdf = gdf.to_crs("EPSG:4326")
    minx, miny, maxx, maxy = gdf.total_bounds
    return [float(minx), float(miny), float(maxx), float(maxy)]


def stac_search_s2(token, bbox):
    """
    Query STAC for Sentinel-2 products over the AOI bbox and date window.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    body = {
        "collections": ["SENTINEL-2"],
        "bbox": bbox,  # [minLon, minLat, maxLon, maxLat]
        "datetime": f"{START_DATE}/{END_DATE}",
        "limit": MAX_ITEMS
    }

    resp = requests.post(STAC_SEARCH_URL, headers=headers, json=body, timeout=60)
    print("STAC status:", resp.status_code)
    print("STAC raw text (first 200 chars):", resp.text[:200])
    resp.raise_for_status()

    data = resp.json()
    return data.get("features", [])


def get_odata_id_from_feature(feature):
    """
    Extract the OData product ID (GUID) from STAC assets, if present.
    We look for an asset href containing 'Products(' and parse the ID inside parentheses.
    """
    assets = feature.get("assets", {}) or {}
    for a in assets.values():
        href = a.get("href", "")
        if "Products(" in href:
            try:
                part = href.split("Products(")[1]
                oid = part.split(")")[0]
                return oid
            except Exception:
                continue
    return None


def download_via_zipper(token, product_id, title):
    """
    Download product using zipper.dataspace.copernicus.eu and known ID.
    This is the same pattern that already works in your other script.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    base_name = title
    if base_name.endswith(".SAFE"):
        base_name = base_name[:-5]

    out_path = OUT_DIR / f"{base_name}.SAFE.zip"
    if out_path.exists():
        print(f"➡ Already exists: {out_path.name}")
        return

    url = ZIPPER_URL.format(id=product_id)
    headers = {"Authorization": f"Bearer {token}"}
    print(f"⬇ Downloading {title} via zipper ...")

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        downloaded = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(
                        f"  {downloaded/1e6:6.1f} MB / {total/1e6:6.1f} MB ({pct:4.1f}%)",
                        end="\r"
                    )

    print(f"\n✅ Saved: {out_path}")


def main():
    # 1) Token
    token, exp = get_cdse_token()
    print("✅ Got token, expires in:", exp, "seconds")

    # 2) AOI bbox
    bbox = get_aoi_bbox()
    print("📦 AOI bbox (minLon, minLat, maxLon, maxLat):", bbox)

    # 3) STAC search
    features = stac_search_s2(token, bbox)
    print(f"📦 Found {len(features)} Sentinel-2 product(s) over AOI in date range.")

    if not features:
        print("No products found. Try expanding START_DATE/END_DATE.")
        return

    # 4) List and download L2A only
    for i, feat in enumerate(features, start=1):
        props = feat.get("properties", {})
        pid = feat.get("id")
        title = props.get("title", pid)
        dt = props.get("datetime") or props.get("start_datetime")
        print(f"[{i}] {title} | {pid} | {dt}")

    for feat in features:
        props = feat.get("properties", {})
        title = props.get("title", feat.get("id", "UNKNOWN"))

        # Only download Level-2A (MSIL2A) products
        product_type = props.get("productType") or ""
        if ("MSIL2A" not in title) and (product_type != "S2MSI2A"):
            print(f"Skipping non-L2A product: {title}")
            continue

        odata_id = get_odata_id_from_feature(feat)
        if not odata_id:
            print(f"⚠ No OData ID found in assets for {title}, skipping.")
            continue

        download_via_zipper(token, odata_id, title)


if __name__ == "__main__":
    main()
