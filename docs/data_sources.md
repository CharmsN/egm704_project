# Data Sources

This document describes the datasets used in the EGM704 Remote Sensing Project, including their provenance, licensing, and processing notes.

---

## 1. Sentinel-1 SAR Data
- **Provider:** Copernicus Open Access Hub (ESA)
- **Data Type:** C-band Synthetic Aperture Radar (VV, VH polarisation)
- **Product:** IW GRD (Interferometric Wide swath, Ground Range Detected)
- **Acquisition Dates:** [Insert specific date range]
- **Spatial Resolution:** 10 m
- **CRS:** EPSG:4326 (WGS84), reprojected to EPSG:27700 (British National Grid)
- **Processing:** Terrain correction, speckle filtering, conversion to sigma nought (dB)
- **Usage:** Backscatter metrics and texture features for habitat classification

---

## 2. Sentinel-2 Optical Data
- **Provider:** Copernicus Open Access Hub (ESA)
- **Data Type:** Multispectral optical imagery (L1C and L2A)
- **Bands Used:** B2 (Blue), B3 (Green), B4 (Red), B8 (NIR), B11/B12 (SWIR)
- **Spatial Resolution:** 10–20 m
- **Acquisition Dates:** [Insert date range, e.g., Summer 2024 composite]
- **Processing:** Atmospheric correction via Sen2Cor, cloud masking, and NDVI/NDWI/NDMI index generation
- **Usage:** Spectral indices and vegetation metrics for classification and change detection

---

## 3. UAV Imagery
- **Provider:** Ulster University / Field Data Collection
- **Sensor Type:** RGB and Multispectral (Parrot Sequoia / DJI Mavic 3 Multispectral)
- **Acquisition Dates:** [Insert survey dates]
- **Ground Resolution:** 5–10 cm
- **Processing:** Orthorectification, mosaicking, radiometric calibration
- **Usage:** High-resolution training and validation data for supervised classification

---

## 4. LiDAR Composite DTM
- **Provider:** Environment Agency (EA)
- **Dataset:** National LIDAR Composite DTM 2022 (1 m)
- **Access:** [https://environment.data.gov.uk/dataset/lidar-composite-dtm-2022](https://environment.data.gov.uk/dataset/lidar-composite-dtm-2022)
- **CRS:** EPSG:27700 (British National Grid)
- **Usage:** Terrain normalisation and structural metrics (height, slope, roughness)

---

## 5. UKHab Habitat Mapping
- **Provider:** UKHab Ltd. (2023)
- **Dataset:** UK Habitat Classification v2
- **Access:** [https://ukhab.org/](https://ukhab.org/)
- **Usage:** Target classification scheme and crosswalk for training/validation data
- **Notes:** All classified outputs are aligned to UKHab v2 codes.

---

## 6. Ancillary / Reference Data
| Dataset | Source | Purpose |
|----------|---------|----------|
| Administrative Boundaries | OS OpenData | Study area delineation |
| Rewilding sites / NCA polygons | Natural England | Spatial planning context |
| Natural Capital Register (Defra, 2022) | GOV.UK | Policy linkage and reporting framework |

---

## 7. Licensing
All datasets are used under their respective open licenses:
- Copernicus data: *Free and open access* under ESA Data Policy  
- EA LiDAR: *Open Government Licence (OGL)*  
- UKHab: *Academic/research use licence*  
- OS OpenData: *OGL v3*

