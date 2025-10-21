#!/usr/bin/env python3
# fetch_s1_cdse.py
import os
import sys
import json
import time
import math
import argparse
from urllib.parse import urljoin
import requests
from tqdm import tqdm
from dateutil.parser import isoparse

STAC_SEARCH = "https://catalogue.dataspace.copernicus.eu/stac/search"
# For downloads, we’ll try hrefs from STAC assets first.
# If needed, we can also fall back to the OData “zipper” endpoint using the product ID.
ODATA_DOWNLOAD_BASE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({id})/$value"

def bearer_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

def stac_search(token, bbox, start, end, product_type="GRD", polarizations=None,
                orbit_direction=None, limit=10, max_items=50):
    """
    Query the CDSE STAC API for Sentinel-1 products.
    Returns a list of STAC items (dicts).
    """
    # Build STAC POST body
    body = {
        "collections": ["SENTINEL-1"],
        "bbox": bbox,  # [minLon, minLat, maxLon, maxLat]
        "datetime": f"{start}/{end}",
        "limit": min(limit, max_items),
        # STAC 'query' filters for S1:
        # s1:productType (e.g., GRD, SLC, OCN)
        "query": {
            "s1:productType": {"eq": product_type}
        },
        # You can constrain instrument mode, polarisations, etc., if desired
    }
    if polarizations:
        body["query"]["sar:polarizations"] = {"in": polarizations}
    if orbit_direction:
        body["query"]["sat:orbit_state"] = {"eq": orbit_direction}  # 'ascending'/'descending'

    headers = bearer_headers(token)
    items = []

    # Basic pagination loop via 'next' link
    url = STAC_SEARCH
    while True:
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        items.extend(features)

        if len(items) >= max_items:
            items = items[:max_items]
            break

        next_link = None
        for lk in data.get("links", []):
            if lk.get("rel") == "next" and lk.get("href"):
                next_link = lk["href"]
                break

        if not next_link:
            break

        # For the next page, switch to GET (per many STAC impls) with the provided next href.
        url = next_link
        # Clear body so we follow server’s pagination token
        body = None

    return items

def pick_download_href(item: dict) -> str | None:
    """
    Try to find the best asset to download from a STAC item.
    Preference order:
      1) Any asset with role 'data'
      2) Asset key 'data' or 'product'
      3) First asset that looks like a product ZIP
    """
    assets = item.get("assets", {}) or {}

    # 1) Look for role == 'data'
    for k, a in assets.items():
        roles = a.get("roles", []) or []
        if "data" in roles and a.get("href"):
            return a["href"]

    # 2) Common keys
    for key in ("data", "product"):
        if key in assets and assets[key].get("href"):
            return assets[key]["href"]

    # 3) Fallback: first asset that looks like a ZIP/SAFE
    for k, a in assets.items():
        href = a.get("href", "")
        if href.endswith(".zip") or href.endswith(".SAFE") or "odata/v1/Products(" in href:
            return href

    return None

def download_with_progress(url, token, out_path, chunk=1024 * 1024):
    headers = bearer_headers(token)
    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        desc = os.path.basename(out_path)
        with open(out_path, "wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B", unit_scale=True, unit_divisor=1024,
            desc=desc
        ) as pbar:
            for chunk_bytes in r.iter_content(chunk_size=chunk):
                if chunk_bytes:
                    f.write(chunk_bytes)
                    if total > 0:
                        pbar.update(len(chunk_bytes))

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def main():
    ap = argparse.ArgumentParser(description="Fetch Sentinel-1 from Copernicus Data Space (CDSE)")
    ap.add_argument("--token", default=os.environ.get("CDSE_TOKEN"), help="CDSE API Bearer token (or set CDSE_TOKEN env var)")
    ap.add_argument("--bbox", type=float, nargs=4, required=True,
                    help="minLon minLat maxLon maxLat (EPSG:4326)")
    ap.add_argument("--start", required=True, help="Start datetime (ISO8601, e.g. 2025-09-01T00:00:00Z)")
    ap.add_argument("--end", required=True, help="End datetime (ISO8601, e.g. 2025-10-11T23:59:59Z)")
    ap.add_argument("--product-type", default="GRD", choices=["GRD", "SLC", "OCN"], help="Sentinel-1 product type")
    ap.add_argument("--orbit", choices=["ascending", "descending"], help="Optional orbit direction filter")
    ap.add_argument("--polarizations", nargs="*", help="Optional polarisations, e.g. VH VV or HH HV")
    ap.add_argument("--limit", type=int, default=10, help="Page size (server hint)")
    ap.add_argument("--max-items", type=int, default=10, help="Max number of items to return")
    ap.add_argument("--outdir", default="downloads", help="Directory to save downloads")
    ap.add_argument("--dry-run", action="store_true", help="Search only; do not download")
    args = ap.parse_args()

    if not args.token:
        sys.exit("ERROR: Provide a token via --token or CDSE_TOKEN env var.")

    # Basic validation on dates
    try:
        isoparse(args.start)
        isoparse(args.end)
    except Exception:
        sys.exit("ERROR: --start/--end must be ISO8601 datetimes (e.g. 2025-09-01T00:00:00Z)")

    print("Searching STAC…")
    items = stac_search(
        token=args.token,
        bbox=args.bbox,
        start=args.start,
        end=args.end,
        product_type=args.product_type,
        polarizations=args.polarizations,
        orbit_direction=args.orbit,
        limit=max(1, min(args.limit, 100)),
        max_items=args.max_items
    )

    if not items:
        print("No results.")
        return

    print(f"Found {len(items)} item(s).")
    for i, it in enumerate(items, 1):
        props = it.get("properties", {})
        sid = it.get("id") or it.get("properties", {}).get("id")
        dt = props.get("datetime") or props.get("start_datetime") or props.get("end_datetime")
        ptype = props.get("s1:productType")
        mode = props.get("sar:instrument_mode")
        pols = props.get("sar:polarizations")
        orbit = props.get("sat:orbit_state")
        print(f"[{i}] ID: {sid}")
        print(f"     datetime: {dt} | productType: {ptype} | mode: {mode} | pol: {pols} | orbit: {orbit}")

    if args.dry_run:
        return

    ensure_dir(args.outdir)

    print("\nDownloading…")
    for it in items:
        sid = it.get("id") or it.get("properties", {}).get("id")
        href = pick_download_href(it)

        # If STAC asset didn’t give us a direct product URL, fall back to OData zipper by ID (if present)
        if not href:
            # Try extract an OData id from links or assets
            # Otherwise, assume STAC 'id' works as OData id (often does)
            odata_id = sid
            if odata_id:
                href = ODATA_DOWNLOAD_BASE.format(id=odata_id)

        if not href:
            print(f"!! Skipping {sid}: no download href found.")
            continue

        # Derive filename
        filename = sid
        # Prefer meaningful name from STAC 'title' or href basename
        title = it.get("properties", {}).get("title") or it.get("assets", {}).get("data", {}).get("title")
        if title:
            filename = title
        elif href:
            filename = os.path.basename(href.split("?")[0])

        if not filename.lower().endswith((".zip", ".safe")):
            filename += ".zip"

        out_path = os.path.join(args.outdir, filename)

        # Download
        try:
            download_with_progress(href, args.token, out_path)
            print(f"✓ Saved: {out_path}")
        except requests.HTTPError as e:
            # If the direct asset 403s, try OData zipper as a fallback
            print(f"Warning: direct download failed for {sid}: {e}")
            try:
                href2 = ODATA_DOWNLOAD_BASE.format(id=sid)
                print(f"…trying OData zipper: {href2}")
                download_with_progress(href2, args.token, out_path)
                print(f"✓ Saved via OData: {out_path}")
            except Exception as e2:
                print(f"!! Failed to download {sid}: {e2}")

if __name__ == "__main__":
    main()
