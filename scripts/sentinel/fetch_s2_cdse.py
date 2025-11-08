import os
import requests
from pathlib import Path

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOGUE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
ZIPPER_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({id})/$value"

# === EDIT THESE FOR YOUR PROJECT ===
OUT_DIR = Path(r"C:\EGM704\data_sets\egm704_project\data\raw\sentinel2")
START_DATE = "2025-09-01T00:00:00Z"
END_DATE   = "2025-09-30T23:59:59Z"
MAX_PRODUCTS = 2  # how many S2 scenes to download (for now)


def get_cdse_token():
    """
    Get an access token from Copernicus Data Space using env vars:
    CDSE_USER and CDSE_PASS.
    """
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
    return token_info["access_token"], token_info.get("expires_in")


def query_s2_products(token):
    """
    Query OData for Sentinel-2 Level-2A products within a date window.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # Filter:
    # - Sentinel-2 collection
    # - productType == S2MSI2A (Level-2A)
    # - between START_DATE and END_DATE
    filter_str = (
        "Collection/Name eq 'SENTINEL-2' "
        "and Attributes/OData.CSC.StringAttribute/any("
        "a:a/Name eq 'productType' and a/Value eq 'S2MSI2A') "
        f"and ContentDate/Start ge {START_DATE} "
        f"and ContentDate/End le {END_DATE}"
    )

    params = {
        "$top": MAX_PRODUCTS,
        "$filter": filter_str
    }

    r = requests.get(CATALOGUE_URL, headers=headers, params=params)
    r.raise_for_status()

    data = r.json()
    products = data.get("value", [])
    return products


def download_product(token, product):
    """
    Download a single product via zipper endpoint.
    """
    prod_id = product["Id"]
    name = product["Name"]  # this is the SAFE/zip name

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{name}.zip"

    if out_path.exists():
        print(f"➡ Already downloaded: {out_path.name}")
        return

    url = ZIPPER_URL.format(id=prod_id)
    headers = {"Authorization": f"Bearer {token}"}

    print(f"⬇ Downloading {name} …")
    with requests.get(url, headers=headers, stream=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"  {downloaded/1e6:6.1f} MB / {total/1e6:6.1f} MB ({pct:4.1f}%)", end="\r")

    print(f"\n✅ Saved: {out_path}")


def main():
    # 1) Get a token
    token, expires_in = get_cdse_token()
    print("✅ Got token (first 60 chars):", token[:60] + "...")
    print("⏳ Expires in (seconds):", expires_in)

    # 2) Query Sentinel-2 products
    products = query_s2_products(token)
    print(f"📦 Found {len(products)} product(s) matching filters.")

    if not products:
        print("No products found. Try widening dates or removing filters.")
        return

    # Show a summary
    for i, p in enumerate(products, start=1):
        print(f"[{i}] {p['Name']}  |  Id: {p['Id']}  |  "
              f"{p['ContentDate']['Start']} -> {p['ContentDate']['End']}")

    # 3) Download each product
    for p in products:
        download_product(token, p)


if __name__ == "__main__":
    main()
