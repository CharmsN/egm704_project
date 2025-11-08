import os
from pathlib import Path

from esa_snappy import ProductIO, GPF, HashMap

# Ensure all SNAP operators are registered
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

RAW_DIR = Path(r"C:\EGM704\data_sets\egm704_project\data\raw\sentinel1")
OUT_DIR = Path(r"C:\EGM704\data_sets\egm704_project\data\processed\sentinel1_preprocessed")

# Choose your output projection: EPSG:32630 (UTM Zone 30N) or EPSG:27700 (British National Grid)
TARGET_EPSG = "EPSG:32630"


def apply_orbit(product):
    params = HashMap()
    params.put("orbitType", "Sentinel Precise (Auto Download)")
    params.put("continueOnFail", True)
    print("  - Apply-Orbit-File")
    return GPF.createProduct("Apply-Orbit-File", params, product)


def remove_thermal_noise(product):
    params = HashMap()
    params.put("removeThermalNoise", True)
    print("  - ThermalNoiseRemoval")
    return GPF.createProduct("ThermalNoiseRemoval", params, product)


def calibrate(product):
    params = HashMap()
    # Your bands include Intensity_VV and Intensity_VH – we calibrate those
    params.put("outputSigmaBand", True)
    params.put("sourceBands", "Intensity_VV,Intensity_VH")
    params.put("selectedPolarisations", "VV,VH")
    params.put("outputImageScaleInDb", False)
    print("  - Calibration (sigma0 VV/VH)")
    return GPF.createProduct("Calibration", params, product)


def terrain_correct(product):
    params = HashMap()
    params.put("demName", "SRTM 3Sec")
    params.put("demResamplingMethod", "BILINEAR_INTERPOLATION")
    params.put("imgResamplingMethod", "BILINEAR_INTERPOLATION")
    params.put("pixelSpacingInMeter", 10.0)
    params.put("mapProjection", TARGET_EPSG)
    params.put("saveSelectedSourceBand", True)
    print("  - Terrain-Correction")
    return GPF.createProduct("Terrain-Correction", params, product)


def preprocess_single_product(input_path: Path, output_path: Path):
    print(f"Processing: {input_path.name}")
    product = ProductIO.readProduct(str(input_path))
    if product is None:
        print("  ! Failed to read product")
        return

    print(f"  Name:   {product.getName()}")
    print(f"  Size:   {product.getSceneRasterWidth()} x {product.getSceneRasterHeight()}")
    print(f"  Bands:  {[b.getName() for b in product.getBands()]}")

    p_orbit = apply_orbit(product)
    p_tn = remove_thermal_noise(p_orbit)
    p_cal = calibrate(p_tn)
    p_tc = terrain_correct(p_cal)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"  - Writing output to: {output_path}")
    ProductIO.writeProduct(p_tc, str(output_path), "GeoTIFF-BigTIFF")
    print("  Done.\n")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process all S1A GRD zips in the raw directory
    for zip_path in sorted(RAW_DIR.glob("S1A_IW_GRDH_*.zip")):
        out_name = zip_path.stem.replace(".SAFE", "") + "_TC.tif"
        out_path = OUT_DIR / out_name

        if out_path.exists():
            print(f"Skipping existing: {out_path.name}")
            continue

        preprocess_single_product(zip_path, out_path)

    print("All products processed.")


if __name__ == "__main__":
    main()
