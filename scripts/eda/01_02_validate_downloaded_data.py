"""
Validate downloaded census tract geometry and ACS data before loading to PostGIS.

What this script checks
-----------------------
1. Census tract geometry file exists and loads successfully.
2. ACS CSV file exists and loads successfully.
3. Tract geometry quality:
   - row count
   - CRS
   - geometry types
   - null geometries
   - invalid geometries
   - duplicate GEOIDs
4. ACS table quality:
   - row count
   - duplicate GEOIDs
   - missing values in key columns
   - summary statistics for numeric columns
   - smallest and largest values for numeric columns
5. Join quality between tract geometries and ACS data:
   - match counts
   - unmatched tract GEOIDs
6. Bounding box sanity check for tract geometry.

Why this script is useful
-------------------------
This gives you a fast checkpoint before uploading data to PostGIS. It helps you
separate:
- data problems
from
- database loading problems
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import geopandas as gpd


def get_project_root() -> Path:
    """
    Return the repository root.

    Assumes this file lives in:
        <repo_root>/scripts/ingestion/01_02_validate_downloaded_data.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


def print_header(title: str) -> None:
    """
    Print a simple section header.

    Parameters
    ----------
    title : str
        Header text to print.
    """
    print(f"\n--- {title} ---")


def validate_tracts(tracts_gdf: gpd.GeoDataFrame) -> None:
    """
    Print validation checks for census tract geometry.

    Parameters
    ----------
    tracts_gdf : gpd.GeoDataFrame
        Census tract GeoDataFrame.
    """
    print_header("TRACTS CHECK")

    print(f"Rows: {len(tracts_gdf)}")
    print(f"CRS: {tracts_gdf.crs}")

    print("\nGeometry types:")
    print(tracts_gdf.geometry.geom_type.value_counts())

    print(f"\nNull geometries: {tracts_gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~tracts_gdf.geometry.is_valid).sum()}")
    print(f"Duplicate GEOIDs: {tracts_gdf['geoid'].duplicated().sum()}")

    print("\nSample tract GEOIDs:")
    print(tracts_gdf["geoid"].head())

    print_header("TRACT BOUNDS CHECK")
    print("Total bounds [minx, miny, maxx, maxy]:")
    print(tracts_gdf.total_bounds)


def validate_acs_structure(acs_df: pd.DataFrame, numeric_columns: list[str]) -> None:
    """
    Print structural validation checks for ACS data.

    Parameters
    ----------
    acs_df : pd.DataFrame
        ACS DataFrame.
    numeric_columns : list[str]
        Numeric ACS columns to inspect.
    """
    print_header("ACS STRUCTURE CHECK")

    print(f"Rows: {len(acs_df)}")
    print(f"Duplicate GEOIDs: {acs_df['geoid'].duplicated().sum()}")

    print("\nMissing values by key column:")
    for col in ["geoid"] + numeric_columns:
        if col in acs_df.columns:
            print(f"{col}: {acs_df[col].isna().sum()}")

    print("\nSample ACS GEOIDs:")
    print(acs_df["geoid"].head())


def validate_acs_numeric_columns(acs_df: pd.DataFrame, numeric_columns: list[str]) -> None:
    """
    Print numeric validation checks for ACS columns.

    For each numeric column, this prints:
    - summary statistics via describe()
    - smallest values
    - largest values

    Parameters
    ----------
    acs_df : pd.DataFrame
        ACS DataFrame.
    numeric_columns : list[str]
        Numeric ACS columns to inspect.
    """
    print_header("ACS NUMERIC COLUMN CHECKS")

    for col in numeric_columns:
        if col not in acs_df.columns:
            print(f"\nColumn '{col}' not found. Skipping.")
            continue

        print(f"\nColumn: {col}")
        print("Summary statistics:")
        print(acs_df[col].describe())

        # Show the smallest values with GEOID for context.
        print("\nSmallest values:")
        print(
            acs_df[["geoid", col]]
            .sort_values(by=col, ascending=True)
            .head(5)
        )

        # Show the largest values with GEOID for context.
        print("\nLargest values:")
        print(
            acs_df[["geoid", col]]
            .sort_values(by=col, ascending=True)
            .tail(5)
        )


def validate_merge(tracts_gdf: gpd.GeoDataFrame, acs_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Merge tract geometry with ACS data and print join-quality checks.

    Parameters
    ----------
    tracts_gdf : gpd.GeoDataFrame
        Census tract GeoDataFrame.
    acs_df : pd.DataFrame
        ACS DataFrame.

    Returns
    -------
    gpd.GeoDataFrame
        Merged GeoDataFrame with merge indicator column.
    """
    print_header("MERGE CHECK")

    merged = tracts_gdf.merge(acs_df, on="geoid", how="left", indicator=True)

    print("Merge result counts:")
    print(merged["_merge"].value_counts())

    missing_acs = merged[merged["_merge"] != "both"]
    print(f"\nTracts missing ACS match: {len(missing_acs)}")

    if not missing_acs.empty:
        print("\nSample unmatched tract GEOIDs:")
        print(missing_acs["geoid"].head(10))

    return merged


def print_optional_interpretation_hints(merged: gpd.GeoDataFrame) -> None:
    """
    Print a few optional interpretation checks that are useful for this project.

    Parameters
    ----------
    merged : gpd.GeoDataFrame
        Merged tract + ACS GeoDataFrame.
    """
    print_header("OPTIONAL INTERPRETATION CHECKS")

    # Check for tracts with no population.
    if "total_pop" in merged.columns:
        zero_pop_count = (merged["total_pop"] == 0).sum()
        print(f"Tracts with total_pop == 0: {zero_pop_count}")

        if zero_pop_count > 0:
            print("\nSample zero-population tracts:")
            print(
                merged.loc[merged["total_pop"] == 0, ["geoid", "total_pop"]]
                .head(10)
            )

    # Check for tracts missing income data.
    if "med_income" in merged.columns:
        missing_income_count = merged["med_income"].isna().sum()
        print(f"\nTracts with missing med_income: {missing_income_count}")

        if missing_income_count > 0:
            print("\nSample tracts missing med_income:")
            cols_to_show = [col for col in ["geoid", "total_pop", "med_income"] if col in merged.columns]
            print(
                merged.loc[merged["med_income"].isna(), cols_to_show]
                .head(10)
            )


def main() -> None:
    """
    Run validation checks for downloaded tract geometry and ACS data.
    """
    project_root = get_project_root()

    tracts_path = project_root / "data" / "raw" / "census_tracts" / "dane_county_census_tracts.geojson"
    acs_path = project_root / "data" / "raw" / "acs" / "dane_county_acs5_2024.csv"

    print("Loading files...")
    print(f"Tracts path: {tracts_path}")
    print(f"ACS path:    {acs_path}")

    # Read tract geometry.
    tracts_gdf = gpd.read_file(tracts_path)

    # Read ACS data.
    # Force identifiers to string so leading zeros are preserved.
    acs_df = pd.read_csv(
        acs_path,
        dtype={
            "geoid": str,
            "state": str,
            "county": str,
            "tract": str,
        },
    )

    # Define numeric ACS columns we care about for validation.
    numeric_columns = [
        "total_pop",
        "med_income",
        "housing_total",
        "renter_units",
        "pct_renter",
    ]

    validate_tracts(tracts_gdf)
    validate_acs_structure(acs_df, numeric_columns)
    validate_acs_numeric_columns(acs_df, numeric_columns)

    merged = validate_merge(tracts_gdf, acs_df)
    print_optional_interpretation_hints(merged)

    print("\nDone.")


if __name__ == "__main__":
    main()