from __future__ import annotations

from pathlib import Path
import pandas as pd
import geopandas as gpd


def get_project_root() -> Path:
    """Return repo root assuming this file lives in scripts/ingestion/."""
    return Path(__file__).resolve().parents[2]


def main() -> None:
    """Quick validation of census tract geometry and ACS data."""
    project_root = get_project_root()

    tracts_path = project_root / "data" / "raw" / "census_tracts" / "dane_county_census_tracts.geojson"
    acs_path = project_root / "data" / "raw" / "acs" / "dane_county_acs5_2024.csv"

    print("\nLoading files...")
    tracts_gdf = gpd.read_file(tracts_path)
    acs_df = pd.read_csv(acs_path, dtype={"geoid": str, "state": str, "county": str, "tract": str})

    print("\n--- TRACTS CHECK ---")
    print(f"Rows: {len(tracts_gdf)}")
    print(f"CRS: {tracts_gdf.crs}")
    print("Geometry types:")
    print(tracts_gdf.geometry.geom_type.value_counts())
    print(f"Null geometries: {tracts_gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~tracts_gdf.geometry.is_valid).sum()}")
    print(f"Duplicate GEOIDs: {tracts_gdf['geoid'].duplicated().sum()}")

    print("\nSample tract GEOIDs:")
    print(tracts_gdf["geoid"].head())

    print("\n--- ACS CHECK ---")
    print(f"Rows: {len(acs_df)}")
    print(f"Duplicate GEOIDs: {acs_df['geoid'].duplicated().sum()}")
    print(f"Missing total_pop: {acs_df['total_pop'].isna().sum()}")
    print(f"Missing med_income: {acs_df['med_income'].isna().sum()}")

    print("\nSample ACS GEOIDs:")
    print(acs_df["geoid"].head())

    print("\n--- MERGE CHECK ---")
    merged = tracts_gdf.merge(acs_df, on="geoid", how="left", indicator=True)

    print("Merge result counts:")
    print(merged["_merge"].value_counts())

    missing_acs = merged[merged["_merge"] != "both"]
    print(f"Tracts missing ACS match: {len(missing_acs)}")

    if not missing_acs.empty:
        print("\nSample unmatched tract GEOIDs:")
        print(missing_acs["geoid"].head(10))

    print("\n--- BOUNDS CHECK ---")
    print("Total bounds:")
    print(tracts_gdf.total_bounds)

    print("\nDone.")


if __name__ == "__main__":
    main()