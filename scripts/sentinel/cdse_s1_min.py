#!/usr/bin/env python3
# cdse_s1_min.py
import os, re, sys, json
from pathlib import Path
import requests
from tqdm import tqdm

STAC_SEARCH = "https://catalogue.dataspace.copernicus.eu/stac/search"
ODATA_DOWNLOAD_BASE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({id})/$value"

ILLEGAL = r'[<>:"/\\|?*]'
def safe_filename(name: str) -> str:
    base = os.path.basename(name)
    base = re.sub(ILLEGAL, "_", base).rstrip(" .")
    return base

def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def search_s1(token: str, bbox, start, end, product_type="GRD", limit=1):
    body = {
        "collections": ["SENTINEL-1"],
        "bbox": bbox,  # [minLon, minLat, maxLon, maxLat] (WGS84)
        "datetime": f"{start}/{end}",
        "limit": limit,
        "query": {"s1:productType": {"eq": product_type}},
    }
    r = requests.post(STAC_SEARCH, headers=auth_headers(token), json=body, timeout=60)
    r.raise_for_status()
    return (r.json() or {}).get("features", [])

def download_product_by_id(token: str, product_id: str, outdir: str):
    url = ODATA_DOWNLOAD_BASE.format(id=product_id)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    # Always use a very safe name: <id>.zip
    fname = safe_filename(product_id + ".zip")
    out_path = outdir / fname
    with requests.get(url, headers=auth_headers(token), stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        with open(out_path, "wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B", unit_scale=True, unit_divisor=1024,
            desc=fname
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    if total > 0:
                        bar.update(len(chunk))
    print(f"Saved -> {out_path}")

def main():
    # Simple arg parsing without external libs
    if len(sys.argv) < 6:
        print("Usage:")
        print("  python cdse_s1_min.py <minLon> <minLat> <maxLon> <maxLat> <startISO> <endISO> [GRD|SLC] [OUTDIR]")
        print("Example:")
        print("  python cdse_s1_min.py -0.90 52.35 -0.70 52.45 2025-09-01T00:00:00Z 2025-10-11T23:59:59Z GRD C:\\s1_out")
        sys.exit(1)

    minLon, minLat, maxLon, maxLat = map(float, sys.argv[1:5])
    startISO, endISO = sys.argv[5], sys.argv[6]
    prod = sys.argv[7] if len(sys.argv) > 7 else "GRD"
    outdir = sys.argv[8] if len(sys.argv) > 8 else "downloads"

    token = os.environ.get("CDSE_TOKEN")
    if not token:
        sys.exit("ERROR: Set CDSE_TOKEN environment variable with your Bearer token.")

    print("Searching STAC…")
    items = search_s1(token, [minLon, minLat, maxLon, maxLat], startISO, endISO, product_type=prod, limit=1)
    if not items:
        print("No Sentinel-1 items found with those parameters.")
        return

    it = items[0]
    pid = it.get("id") or it.get("properties", {}).get("id")
    dt = (it.get("properties", {}) or {}).get("datetime")
    print(f"Found 1 item:\n  id: {pid}\n  datetime: {dt}")

    print("Downloading via OData zipper (stable)…")
    download_product_by_id(token, pid, outdir)

if __name__ == "__main__":
    main()
