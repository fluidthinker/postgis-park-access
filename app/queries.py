"""
DuckDB query functions for the Streamlit app.

This module keeps SQL out of the Streamlit UI code.
"""

from pathlib import Path

import duckdb
import pandas as pd


def load_access_data(parquet_path: Path) -> pd.DataFrame:
    """
    Load the full tract park access dataset.

    Parameters
    ----------
    parquet_path : Path
        Path to tract_park_access.parquet.

    Returns
    -------
    pd.DataFrame
        Full analysis dataset.
    """
    return duckdb.query(
        f"""
        SELECT *
        FROM '{parquet_path}'
        """
    ).to_df()


def get_access_summary(parquet_path: Path) -> pd.DataFrame:
    """
    Summarize income, renters, and access metrics by access tier.

    Parameters
    ----------
    parquet_path : Path
        Path to tract_park_access.parquet.

    Returns
    -------
    pd.DataFrame
        Summary table grouped by access tier.
    """
    return duckdb.query(
        f"""
        SELECT
            access_tier,
            COUNT(*) AS tract_count,
            ROUND(AVG(med_income), 0) AS avg_med_income,
            ROUND(AVG(pct_renter), 1) AS avg_pct_renter,
            ROUND(AVG(park_sqm_per_capita), 1) AS avg_park_sqm_per_capita,
            ROUND(AVG(nearest_park_distance_m), 1) AS avg_nearest_park_distance_m
        FROM '{parquet_path}'
        GROUP BY access_tier
        ORDER BY access_tier
        """
    ).to_df()


def get_access_tier_counts(parquet_path: Path) -> pd.DataFrame:
    """
    Count census tracts by access tier.

    Parameters
    ----------
    parquet_path : Path
        Path to tract_park_access.parquet.

    Returns
    -------
    pd.DataFrame
        Access tier counts.
    """
    return duckdb.query(
        f"""
        SELECT
            access_tier,
            COUNT(*) AS tract_count
        FROM '{parquet_path}'
        GROUP BY access_tier
        ORDER BY access_tier
        """
    ).to_df()


def get_chart_data(parquet_path: Path) -> pd.DataFrame:
    """
    Return fields needed for charts.

    Parameters
    ----------
    parquet_path : Path
        Path to tract_park_access.parquet.

    Returns
    -------
    pd.DataFrame
        Chart-ready dataset.
    """
    return duckdb.query(
        f"""
        SELECT
            geoid,
            med_income,
            pct_renter,
            park_sqm_per_capita,
            nearest_park_distance_m,
            access_tier
        FROM '{parquet_path}'
        WHERE med_income IS NOT NULL
        """
    ).to_df()


def get_project_root() -> Path:
    """
    Return project root for module-level testing.

    Assumes this file lives in:
        <repo_root>/app/queries.py

    Returns
    -------
    Path
        Absolute path to project root.
    """
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    project_root = get_project_root()
    test_parquet_path = (
        project_root
        / "data"
        / "processed"
        / "analysis"
        / "tract_park_access.parquet"
    )

    print(f"Testing queries.py with: {test_parquet_path}")
    print(f"File exists: {test_parquet_path.exists()}")

    if not test_parquet_path.exists():
        raise FileNotFoundError(test_parquet_path)

    access_data = load_access_data(test_parquet_path)
    access_summary = get_access_summary(test_parquet_path)
    tier_counts = get_access_tier_counts(test_parquet_path)
    chart_data = get_chart_data(test_parquet_path)

    print("\nFull data shape:")
    print(access_data.shape)

    print("\nAccess summary:")
    print(access_summary)

    print("\nTier counts:")
    print(tier_counts)

    print("\nChart data sample:")
    print(chart_data.head())