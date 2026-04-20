"""
Download ACS 5-year demographic data for Dane County census tracts.

This script:
1. Calls the Census ACS 5-year API for all tracts in Dane County, Wisconsin.
2. Pulls a small set of variables useful for park access / equity analysis.
3. Renames ACS variable codes to cleaner names.
4. Converts numeric fields from strings to numbers.
5. Creates a tract GEOID for joining with tract boundaries.
6. Saves the result to data/raw/acs/.

Notes
-----
- This script can work without a Census API key for light use, but you should
  still wire the key in from the start.
- Set the key in your shell like this:

    export CENSUS_API_KEY="your_key_here"

- If the environment variable is not present, the script still tries the request.
"""

from __future__ import annotations

from pathlib import Path
import os

import pandas as pd
import requests


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

STATE_FIPS = "55"     # Wisconsin
COUNTY_FIPS = "025"   # Dane County

# You can switch this to 2023 if you want a more conservative first pass.
ACS_YEAR = "2024"

ACS_URL = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
REQUEST_TIMEOUT = 60

# Read the user's Census API key from an environment variable.
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Mapping from ACS variable code -> cleaner output column name.
ACS_VARIABLES: dict[str, str] = {
    "B01003_001E": "total_pop",      # Total population
    "B19013_001E": "med_income",     # Median household income
    "B25003_001E": "housing_total",  # Total occupied housing units
    "B25003_003E": "renter_units",   # Renter-occupied housing units
}

# Optional metadata fields returned by the API that we use to build GEOID.
GEOGRAPHY_FIELDS = ["state", "county", "tract"]


def get_project_root() -> Path:
    """
    Resolve the project root from this script's location.

    Assumes this file lives in:
        <repo_root>/scripts/02_download_acs_data.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


def build_request_params() -> dict[str, str]:
    """
    Build request parameters for the ACS API call.

    Returns
    -------
    dict[str, str]
        Query string parameters for the Census API.
    """
    variable_string = ",".join(ACS_VARIABLES.keys())

    params = {
        "get": variable_string,
        "for": "tract:*",
        "in": f"state:{STATE_FIPS} county:{COUNTY_FIPS}",
    }

    # Only include the key if the user has set one.
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    return params


def fetch_acs_data() -> pd.DataFrame:
    """
    Fetch ACS 5-year tract-level data for Dane County.

    Returns
    -------
    pd.DataFrame
        Raw ACS data as a DataFrame, with header row applied.

    Raises
    ------
    requests.HTTPError
        If the web request fails.
    ValueError
        If the API returns an unexpected payload.
    """
    params = build_request_params()

    with requests.Session() as session:
        response = session.get(ACS_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

    if not data or len(data) < 2:
        raise ValueError("ACS API returned no usable data.")

    # Census returns a list of rows:
    # - row 0 = column names
    # - remaining rows = data values
    df = pd.DataFrame(data[1:], columns=data[0])

    return df


def clean_acs_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and reshape ACS data for downstream joins and analysis.

    Parameters
    ----------
    df : pd.DataFrame
        Raw ACS DataFrame from the API.

    Returns
    -------
    pd.DataFrame
        Cleaned ACS DataFrame with friendly column names and numeric types.
    """
    # Build the full tract GEOID:
    # 2-digit state + 3-digit county + 6-digit tract
    df["geoid"] = df["state"] + df["county"] + df["tract"]

    # Rename ACS variable codes to friendlier names.
    df = df.rename(columns=ACS_VARIABLES)

    # Convert selected ACS estimate fields from strings to numbers.
    numeric_columns = list(ACS_VARIABLES.values())
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived field: percent renter occupied.
    # We guard against divide-by-zero.
    df["pct_renter"] = (
        (df["renter_units"] / df["housing_total"]) * 100
    ).where(df["housing_total"] > 0)

    # Keep the cleaned output focused and easy to inspect.
    keep_columns = [
        "geoid",
        "state",
        "county",
        "tract",
        "total_pop",
        "med_income",
        "housing_total",
        "renter_units",
        "pct_renter",
    ]
    df = df[keep_columns].copy()

    return df


def save_acs_data(df: pd.DataFrame) -> Path:
    """
    Save cleaned ACS data to the raw ACS directory.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned ACS DataFrame.

    Returns
    -------
    Path
        Path to the saved CSV file.
    """
    project_root = get_project_root()
    output_dir = project_root / "data" / "raw" / "acs"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"dane_county_acs5_{ACS_YEAR}.csv"
    df.to_csv(output_path, index=False)

    return output_path


def main() -> None:
    """
    Run the ACS download workflow.
    """
    print(f"Downloading ACS 5-year tract data for Dane County ({ACS_YEAR})...")

    if CENSUS_API_KEY:
        print("Census API key found in environment.")
    else:
        print("No Census API key found. Proceeding without one.")

    raw_df = fetch_acs_data()
    print(f"Downloaded {len(raw_df)} ACS rows.")

    clean_df = clean_acs_data(raw_df)
    print("Cleaned ACS data.")

    output_path = save_acs_data(clean_df)
    print(f"Saved ACS data to: {output_path}")


if __name__ == "__main__":
    main()