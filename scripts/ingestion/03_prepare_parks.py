# %%
"""
03_prepare_parks.py (VS Code cell-based version)

Goal:
- Download parks from OSM
- Keep polygon geometries
- Clean columns
- Standardize CRS
- Save GeoJSON for PostGIS
"""

# %%
# Imports
from pathlib import Path

import geopandas as gpd
import osmnx as ox
import pandas as pd

# %%
# Configuration
PLACE_NAME = "Dane County, Wisconsin, USA"

OSM_TAGS = {
    "leisure": ["park", "recreation_ground"],
}

OUTPUT_FILENAME = "dane_county_parks_osm.geojson"

# %%
# Helper: project root
def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]

# %%
# Step 1 — Download parks from OSM
print(f"Downloading OSM park features for: {PLACE_NAME}")
print(f"Using tags: {OSM_TAGS}")

parks_raw = ox.features_from_place(PLACE_NAME, OSM_TAGS)

print(f"Downloaded {len(parks_raw)} raw features")
parks_raw.head()

# %%
# Step 2 — Inspect geometry types
print("Geometry types:")
print(parks_raw.geometry.geom_type.value_counts())

# %%
# Step 3 — Keep only polygons
parks_poly = parks_raw[
    parks_raw.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
].copy()

print(f"Polygon features retained: {len(parks_poly)}")

# %%
# Step 4 — Flatten index (OSMnx returns multi-index)
parks_poly = parks_poly.reset_index()

print("Columns after reset_index:")
print(parks_poly.columns.tolist())

parks_poly.head()

# %%
# Step 5 — Keep useful columns
preferred_columns = [
    "element",
    "id",
    "name",
    "leisure",
    "access",
    "operator",
    "landuse",
    "geometry",
]

cols = [c for c in preferred_columns if c in parks_poly.columns]

parks_clean = parks_poly[cols].copy()

print("Columns kept:")
print(parks_clean.columns.tolist())

parks_clean.head()

# %%
# Step 6 — Rename columns + add park_id
parks_clean = parks_clean.rename(
    columns={
        "element": "osm_element",
        "id": "osm_id",
    }
)

parks_clean["park_id"] = range(1, len(parks_clean) + 1)

# Reorder so park_id is first
parks_clean = parks_clean[
    ["park_id"] + [c for c in parks_clean.columns if c != "park_id"]
]

parks_clean.head()

# %%
# Step 7 — Ensure CRS is EPSG:4326
print("CRS before:", parks_clean.crs)

if parks_clean.crs is None:
    raise ValueError("Parks GeoDataFrame has no CRS")

if parks_clean.crs.to_string() != "EPSG:4326":
    parks_clean = parks_clean.to_crs("EPSG:4326")

print("CRS after:", parks_clean.crs)

# %%
# Step 8 — Quick map check (VERY IMPORTANT)
parks_clean.plot(figsize=(8, 8))

# %%
# Step 9 — Save to GeoJSON
project_root = get_project_root()

output_dir = project_root / "data" / "raw" / "parks"
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / OUTPUT_FILENAME

parks_clean.to_file(output_path, driver="GeoJSON")

print(f"Saved to: {output_path}")
# %%
# %%
len(parks_poly)
# %%
# %%
parks_poly["leisure"].value_counts(dropna=False)
# %%
# %%
parks_poly[["name", "leisure"]].sample(20, random_state=42)
# %%
# %%
parks_poly["name"].isna().sum()
# %%
