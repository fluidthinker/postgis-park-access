# %%
from pathlib import Path

import duckdb


# %%
def get_project_root() -> Path:
    """
    Return repo root.

    Assumes this file lives in:
        <repo_root>/scripts/eda/04_duckdb_exploration.py
    """
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = get_project_root()

PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "analysis" / "tract_park_access.parquet"

print(PARQUET_PATH)
print("Exists:", PARQUET_PATH.exists())


# %%
df = duckdb.query(f"""
    SELECT *
    FROM '{PARQUET_PATH}'
""").to_df()

print("Rows:", len(df))
print("\nColumns:")
print(df.columns.tolist())


# %%
df_access = duckdb.query(f"""
    SELECT
        access_tier,
        COUNT(*) AS tract_count,
        AVG(med_income) AS avg_income,
        AVG(pct_renter) AS avg_renter
    FROM '{PARQUET_PATH}'
    GROUP BY access_tier
    ORDER BY access_tier
""").to_df()

print(df_access)
# %%
