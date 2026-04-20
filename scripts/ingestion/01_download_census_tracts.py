"""
Download Dane County census tract boundaries from Census TIGERweb.

This script:
1. Queries the Census TIGERweb tract layer for Dane County, Wisconsin.
2. Converts the GeoJSON response into a GeoDataFrame.
3. Keeps a small set of useful columns.
4. Saves the result to data/raw/census_tracts/.

Notes
-----
- This script does NOT require a Census API key.
- It uses the TIGERweb ArcGIS REST service, not the ACS tabular API.
- Output CRS is EPSG:4326.
"""

from __future__ import annotations

from pathlib import Path
import json

import geopandas as gpd
import requests


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

STATE_FIPS = "55"     # Wisconsin
COUNTY_FIPS = "025"   # Dane County

TIGER_TRACTS_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/Tracts_Blocks/MapServer/0/query"
)

REQUEST_TIMEOUT = 60


def get_project_root() -> Path:
    """
    Resolve the project root from this script's location.

    Assumes this file lives in:
        <repo_root>/scripts/01_download_census_tracts.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


def fetch_dane_county_tracts() -> gpd.GeoDataFrame:
    """
    Fetch census tract polygons for Dane County from TIGERweb.

    Returns
    -------
    gpd.GeoDataFrame
        Census tract geometries and selected attributes in EPSG:4326.

    Raises
    ------
    requests.HTTPError
        If the web request fails.
    KeyError
        If the expected GeoJSON structure is missing.
    """
    # The "where" clause filters the national tract layer down to Dane County.
    params = {
        "where": f"STATE='{STATE_FIPS}' AND COUNTY='{COUNTY_FIPS}'",
        "outFields": "GEOID,STATE,COUNTY,TRACT,BASENAME,NAME,AREALAND,AREAWATER",
        "outSR": "4326",
        "f": "geojson",
    }

    # A Session is optional here, but it is a good habit in ETL scripts.
    with requests.Session() as session:
        response = session.get(TIGER_TRACTS_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        geojson = response.json()

    # Convert GeoJSON features into a GeoDataFrame.
    gdf = gpd.GeoDataFrame.from_features(geojson["features"], crs="EPSG:4326")

    # Keep the output tidy and predictable.
    keep_columns = [
        col
        for col in [
            "GEOID",
            "STATE",
            "COUNTY",
            "TRACT",
            "BASENAME",
            "NAME",
            "AREALAND",
            "AREAWATER",
            "geometry",
        ]
        if col in gdf.columns
    ]
    gdf = gdf[keep_columns].copy()

    # Rename fields to friendlier Python/PostGIS-style names.
    gdf = gdf.rename(
        columns={
            "GEOID": "geoid",
            "STATE": "state_fips",
            "COUNTY": "county_fips",
            "TRACT": "tract_code",
            "BASENAME": "tract_basename",
            "NAME": "tract_name",
            "AREALAND": "area_land_m2_raw",
            "AREAWATER": "area_water_m2_raw",
        }
    )

    # Preserve leading zeros by forcing identifiers to string.
    for col in ["geoid", "state_fips", "county_fips", "tract_code"]:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str)

    return gdf


def save_tracts(gdf: gpd.GeoDataFrame) -> Path:
    """
    Save tract boundaries to the raw census_tracts directory.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Tract GeoDataFrame to save.

    Returns
    -------
    Path
        Path to the saved GeoJSON file.
    """
    project_root = get_project_root()
    output_dir = project_root / "data" / "raw" / "census_tracts"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "dane_county_census_tracts.geojson"
    gdf.to_file(output_path, driver="GeoJSON")

    return output_path


def main() -> None:
    """
    Run the tract download workflow.
    """
    print("Downloading Dane County census tract boundaries from TIGERweb...")
    tracts_gdf = fetch_dane_county_tracts()

    print(f"Downloaded {len(tracts_gdf)} tract features.")
    print("Saving GeoJSON...")

    output_path = save_tracts(tracts_gdf)
    print(f"Saved tract boundaries to: {output_path}")


if __name__ == "__main__":
    main()