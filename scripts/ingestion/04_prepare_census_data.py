# %%
"""
04_prepare_census_data.py

Prepare census tract geometry + ACS attributes for PostGIS loading.

This script:
1. Reads downloaded Dane County census tract boundaries.
2. Reads downloaded ACS 5-year tract-level data.
3. Merges tract geometry with ACS attributes using GEOID.
4. Adds simple analysis flags.
5. Saves one enriched GeoJSON to data/processed/census/.

Input files:
- data/raw/census_tracts/dane_county_census_tracts.geojson
- data/raw/acs/dane_county_acs5_2024.csv

Output file:
- data/processed/census/dane_county_tracts_acs_2024.geojson

----------------------------------------------------------------------
⚠️ IMPORTANT: PROJ / GDAL ENVIRONMENT SETUP (WSL / micromamba)
----------------------------------------------------------------------

If you see an error like:

    PROJ: proj_create_from_database: Open of .../share/proj failed

This means the PROJ data directory is not set correctly.

Before running this script, run:

    micromamba activate postgis-park-access

    export PROJ_DATA=/home/chris/micromamba/envs/postgis-park-access/share/proj

Then run:

    python scripts/ingestion/04_prepare_census_data.py

This ensures CRS transformations and GeoJSON writing work correctly.





"""

# %%
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd


# %%
# Configuration

ACS_YEAR = "2024"

TRACTS_FILENAME = "dane_county_census_tracts.geojson"
ACS_FILENAME = f"dane_county_acs5_{ACS_YEAR}.csv"
OUTPUT_FILENAME = f"dane_county_tracts_acs_{ACS_YEAR}.geojson"


# %%
def get_project_root() -> Path:
    """
    Return the repository root.

    Assumes this file lives in:
        <repo_root>/scripts/ingestion/04_prepare_census_data.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


# %%
def load_tracts(project_root: Path) -> gpd.GeoDataFrame:
    """
    Load census tract boundaries.

    Parameters
    ----------
    project_root : Path
        Repository root.

    Returns
    -------
    gpd.GeoDataFrame
        Census tract geometries.
    """
    tracts_path = (
        project_root
        / "data"
        / "raw"
        / "census_tracts"
        / TRACTS_FILENAME
    )

    print(f"Loading tracts from: {tracts_path}")

    tracts_gdf = gpd.read_file(tracts_path)

    # GEOID should always be treated as a string, not a number.
    tracts_gdf["geoid"] = tracts_gdf["geoid"].astype(str)

    return tracts_gdf


# %%
def load_acs(project_root: Path) -> pd.DataFrame:
    """
    Load ACS tract-level attributes.

    Parameters
    ----------
    project_root : Path
        Repository root.

    Returns
    -------
    pd.DataFrame
        ACS attributes.
    """
    acs_path = (
        project_root
        / "data"
        / "raw"
        / "acs"
        / ACS_FILENAME
    )

    print(f"Loading ACS data from: {acs_path}")

    acs_df = pd.read_csv(
        acs_path,
        dtype={
            "geoid": str,
            "state": str,
            "county": str,
            "tract": str,
        },
    )

    return acs_df


# %%
def merge_tracts_and_acs(
    tracts_gdf: gpd.GeoDataFrame,
    acs_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    Merge tract geometries with ACS attributes.

    Parameters
    ----------
    tracts_gdf : gpd.GeoDataFrame
        Census tract geometries.
    acs_df : pd.DataFrame
        ACS attributes.

    Returns
    -------
    gpd.GeoDataFrame
        Enriched census tract GeoDataFrame.
    """
    print("Merging tracts and ACS data on geoid...")

    merged_gdf = tracts_gdf.merge(
        acs_df,
        on="geoid",
        how="left",
        validate="1:1",
        indicator=True,
    )

    print("\nMerge result:")
    print(merged_gdf["_merge"].value_counts())

    unmatched_count = (merged_gdf["_merge"] != "both").sum()

    if unmatched_count > 0:
        print("\nWarning: Some tracts did not match ACS data.")
        print(
            merged_gdf.loc[
                merged_gdf["_merge"] != "both",
                ["geoid", "_merge"],
            ].head(10)
        )

    # Drop the merge indicator before saving.
    merged_gdf = merged_gdf.drop(columns=["_merge"])

    return merged_gdf


# %%
def add_analysis_flags(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add simple fields useful for downstream analysis.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Enriched tract GeoDataFrame.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with additional analysis helper fields.
    """
    gdf = gdf.copy()

    # True/False flag for tracts with population.
    # This helps later when excluding water-only or zero-population tracts.
    gdf["has_population"] = gdf["total_pop"] > 0

    # True/False flag for tracts with usable income data.
    gdf["has_income_data"] = gdf["med_income"].notna()

    # Optional readable category for later plotting or filtering.
    gdf["tract_data_status"] = "usable"
    gdf.loc[~gdf["has_population"], "tract_data_status"] = "zero_population"
    gdf.loc[
        gdf["has_population"] & ~gdf["has_income_data"],
        "tract_data_status",
    ] = "missing_income"

    return gdf


# %%
def validate_prepared_census(gdf: gpd.GeoDataFrame) -> None:
    """
    Print quick checks for prepared census data.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Prepared census GeoDataFrame.
    """
    print("\n--- PREPARED CENSUS CHECK ---")
    print(f"Rows: {len(gdf)}")
    print(f"CRS: {gdf.crs}")
    print(f"Duplicate GEOIDs: {gdf['geoid'].duplicated().sum()}")
    print(f"Null geometries: {gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~gdf.geometry.is_valid).sum()}")

    print("\nData status counts:")
    print(gdf["tract_data_status"].value_counts(dropna=False))

    print("\nMissing values:")
    print(
        gdf[
            [
                "total_pop",
                "med_income",
                "housing_total",
                "renter_units",
                "pct_renter",
            ]
        ].isna().sum()
    )


# %%
def save_prepared_census(gdf: gpd.GeoDataFrame, project_root: Path) -> Path:
    """
    Save prepared census data as GeoJSON.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Prepared census GeoDataFrame.
    project_root : Path
        Repository root.

    Returns
    -------
    Path
        Output file path.
    """
    output_dir = project_root / "data" / "processed" / "census"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / OUTPUT_FILENAME

    print(f"\nSaving prepared census data to: {output_path}")
    gdf.to_file(output_path, driver="GeoJSON")

    return output_path


# %%
def main() -> None:
    """
    Run the census preparation workflow.
    """
    project_root = get_project_root()

    tracts_gdf = load_tracts(project_root)
    acs_df = load_acs(project_root)

    prepared_gdf = merge_tracts_and_acs(tracts_gdf, acs_df)
    prepared_gdf = add_analysis_flags(prepared_gdf)

    validate_prepared_census(prepared_gdf)

    output_path = save_prepared_census(prepared_gdf, project_root)

    print("\nDone.")
    print(f"Prepared census file saved at: {output_path}")
    print("\nSample of prepared census data:")
    print(prepared_gdf.head())


# %%
if __name__ == "__main__":
    main()
# %%
