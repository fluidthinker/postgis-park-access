# %%
import duckdb

PARQUET_PATH = "data/processed/analysis/tract_park_access.parquet"

# %%
# Basic load
df = duckdb.query(f"""
    SELECT *
    FROM '{PARQUET_PATH}'
""").to_df()

print("Rows:", len(df))
print("\nColumns:")
print(df.columns.tolist())

# %%
# Access tier summary
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

print("\nAccess Tier Summary:")
print(df_access)

# %%
# Income vs park access
df_income = duckdb.query(f"""
    SELECT
        med_income,
        park_sqm_per_capita,
        nearest_park_distance_m
    FROM '{PARQUET_PATH}'
""").to_df()

print("\nSample income vs access:")
print(df_income.head())