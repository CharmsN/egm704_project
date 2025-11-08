import os
import requests

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOGUE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


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


def main():
    # 1) Get a token
    token, expires_in = get_cdse_token()
    print("✅ Got token (first 60 chars):", token[:60] + "...")
    print("⏳ Expires in (seconds):", expires_in)

    # 2) Do a tiny Sentinel-1 catalogue query as a sanity check
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "$top": 1,
        "$filter": "Collection/Name eq 'SENTINEL-2'"
    }

    r = requests.get(CATALOGUE_URL, headers=headers, params=params)
    r.raise_for_status()

    data = r.json()
    products = data.get("value", [])
    print(f"📦 Sample product count: {len(products)}")
    if products:
        print("🆔 First product ID:", products[0]["Id"])
        print("🛰️ First product Name:", products[0]["Name"])


if __name__ == "__main__":
    main()
