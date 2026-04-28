# %%
"""
06_create_analysis_tables.py

Create tract-level park access analysis tables in PostGIS.

Run from repo root:

    micromamba activate postgis-park-access
    python scripts/ingestion/06_create_analysis_tables.py

This script:
1. Uses the loaded census tract and OSM park tables.
2. Calculates park area within each tract.
3. Calculates park area per capita.
4. Calculates distance from each tract centroid to the nearest park.
5. Creates an access tier using both:
   - availability: park square meters per capita
   - proximity: distance to nearest park
6. Validates the output table.

Important concept
-----------------
For this project:

    accessibility = availability + proximity

Where:
- availability = park_sqm_per_capita
- proximity = nearest_park_distance_m

Access tier combines both measures.
"""

# %%
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# %%
# ---------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------

DB_NAME = "park_access"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

DB_SCHEMA = "public"

CENSUS_TABLE = "census_tracts_enriched"
PARKS_TABLE = "parks_osm"
ANALYSIS_TABLE = "tract_park_access"


# %%
# ---------------------------------------------------------------------
# Analysis thresholds
# ---------------------------------------------------------------------
# These are first-pass, interpretable thresholds.
# You can tune them later after inspecting the distribution.

LOW_PARK_SQM_PER_CAPITA = 10
HIGH_PARK_SQM_PER_CAPITA = 30

NEAR_PARK_DISTANCE_M = 800
FAR_PARK_DISTANCE_M = 1600


# %%
def build_database_url(
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: str,
    db_name: str,
) -> str:
    """
    Build a SQLAlchemy database URL for PostgreSQL/PostGIS.

    Parameters
    ----------
    db_user : str
        PostgreSQL username.
    db_password : str
        PostgreSQL password.
    db_host : str
        Database host.
    db_port : str
        Database port.
    db_name : str
        Database name.

    Returns
    -------
    str
        SQLAlchemy connection URL.
    """
    return (
        f"postgresql+psycopg://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )


# %%
def create_analysis_table(
    engine: Engine,
    schema: str,
    census_table: str,
    parks_table: str,
    analysis_table: str,
    low_park_sqm_per_capita: float,
    high_park_sqm_per_capita: float,
    near_park_distance_m: float,
    far_park_distance_m: float,
) -> None:
    """
    Create tract-level park access analysis table.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    schema : str
        Database schema.
    census_table : str
        Census tract table name.
    parks_table : str
        Parks table name.
    analysis_table : str
        Output analysis table name.
    low_park_sqm_per_capita : float
        Lower availability threshold.
    high_park_sqm_per_capita : float
        Higher availability threshold.
    near_park_distance_m : float
        Near proximity threshold.
    far_park_distance_m : float
        Far proximity threshold.
    """
    print(f"Creating analysis table: {schema}.{analysis_table}")

    sql = f"""
    DROP TABLE IF EXISTS {schema}.{analysis_table};

    CREATE TABLE {schema}.{analysis_table} AS

    WITH usable_tracts AS (
        SELECT
            geoid,
            tract_name,
            total_pop,
            med_income,
            housing_total,
            renter_units,
            pct_renter,
            has_population,
            has_income_data,
            tract_data_status,
            geometry
        FROM {schema}.{census_table}
        WHERE has_population = true
    ),
    park_intersections AS (
        SELECT
            geoid,
            park_id,
            ST_Area(intersection_geom::geography) AS park_area_sqm
        FROM (
            SELECT
                t.geoid,
                p.park_id,
                ST_Intersection(t.geometry, p.geometry) AS intersection_geom
            FROM usable_tracts t
            JOIN public.parks_osm p
                ON ST_Intersects(t.geometry, p.geometry)
        ) intersections
        WHERE NOT ST_IsEmpty(intersection_geom)
        AND ST_Area(intersection_geom::geography) > 0
    ),

    park_area_by_tract AS (
        SELECT
            geoid,
            COUNT(DISTINCT park_id) AS intersecting_park_count,
            SUM(park_area_sqm) AS park_area_sqm
        FROM park_intersections
        GROUP BY geoid
    ),
    nearest_park AS (
        SELECT
            t.geoid,
            nearest.park_id AS nearest_park_id,
            nearest.name AS nearest_park_name,
            ST_Distance(
                ST_Centroid(t.geometry)::geography,
                nearest.geometry::geography
            ) AS nearest_park_distance_m
            FROM usable_tracts t
            LEFT JOIN LATERAL (
                SELECT
                p.park_id,
                p.name,
                p.geometry
            FROM public.parks_osm p
            ORDER BY ST_Centroid(t.geometry) <-> p.geometry
            LIMIT 1
            ) nearest ON true
    ), 
    metrics AS (
        SELECT
            t.geoid,
            t.tract_name,
            t.total_pop,
            t.med_income,
            t.housing_total,
            t.renter_units,
            t.pct_renter,
            t.tract_data_status,

            COALESCE(pa.intersecting_park_count, 0) AS intersecting_park_count,
            COALESCE(pa.park_area_sqm, 0) AS park_area_sqm,

            CASE
                WHEN t.total_pop > 0
                THEN COALESCE(pa.park_area_sqm, 0) / t.total_pop
                ELSE NULL
            END AS park_sqm_per_capita,

            np.nearest_park_id,
            np.nearest_park_name,
            np.nearest_park_distance_m,

            t.geometry
        FROM usable_tracts t
        LEFT JOIN park_area_by_tract pa
            ON t.geoid = pa.geoid
        LEFT JOIN nearest_park np
            ON t.geoid = np.geoid
    )

    SELECT
        *,
        CASE
            WHEN park_sqm_per_capita >= {high_park_sqm_per_capita}
                 AND nearest_park_distance_m <= {near_park_distance_m}
                THEN 'High Access'

            WHEN park_sqm_per_capita < {low_park_sqm_per_capita}
                 AND nearest_park_distance_m > {far_park_distance_m}
                THEN 'Very Low Access'

            WHEN park_sqm_per_capita < {low_park_sqm_per_capita}
                 OR nearest_park_distance_m > {far_park_distance_m}
                THEN 'Low Access'

            ELSE 'Moderate Access'
        END AS access_tier
    FROM metrics;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"Created analysis table: {schema}.{analysis_table}")


# %%
def create_analysis_indexes(
    engine: Engine,
    schema: str,
    analysis_table: str,
) -> None:
    """
    Create indexes on the analysis table.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    schema : str
        Database schema.
    analysis_table : str
        Analysis table name.
    """
    print(f"Creating indexes on: {schema}.{analysis_table}")

    sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{analysis_table}_geometry
    ON {schema}.{analysis_table}
    USING GIST (geometry);

    CREATE INDEX IF NOT EXISTS idx_{analysis_table}_geoid
    ON {schema}.{analysis_table} (geoid);

    CREATE INDEX IF NOT EXISTS idx_{analysis_table}_access_tier
    ON {schema}.{analysis_table} (access_tier);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Analysis indexes created.")


# %%
def validate_analysis_table(
    engine: Engine,
    schema: str,
    analysis_table: str,
) -> None:
    """
    Run validation checks on the analysis table.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    schema : str
        Database schema.
    analysis_table : str
        Analysis table name.
    """
    print("\n--- ANALYSIS TABLE CHECKS ---")

    queries = {
        "row count": f"""
            SELECT COUNT(*)
            FROM {schema}.{analysis_table};
        """,
        "access tier counts": f"""
            SELECT access_tier, COUNT(*)
            FROM {schema}.{analysis_table}
            GROUP BY access_tier
            ORDER BY access_tier;
        """,
        "park area per capita summary": f"""
            SELECT
                MIN(park_sqm_per_capita),
                AVG(park_sqm_per_capita),
                MAX(park_sqm_per_capita)
            FROM {schema}.{analysis_table};
        """,
        "nearest park distance summary": f"""
            SELECT
                MIN(nearest_park_distance_m),
                AVG(nearest_park_distance_m),
                MAX(nearest_park_distance_m)
            FROM {schema}.{analysis_table};
        """,
        "income by access tier": f"""
            SELECT
                access_tier,
                COUNT(*) AS tract_count,
                AVG(med_income) AS avg_med_income,
                AVG(pct_renter) AS avg_pct_renter,
                AVG(park_sqm_per_capita) AS avg_park_sqm_per_capita,
                AVG(nearest_park_distance_m) AS avg_nearest_park_distance_m
            FROM {schema}.{analysis_table}
            GROUP BY access_tier
            ORDER BY access_tier;
        """,
    }

    with engine.connect() as conn:
        for label, query in queries.items():
            print(f"\n{label}:")
            result = conn.execute(text(query)).fetchall()
            for row in result:
                print(row)


# %%
def main() -> None:
    """
    Run the analysis table creation workflow.
    """
    database_url = build_database_url(
        db_user=DB_USER,
        db_password=DB_PASSWORD,
        db_host=DB_HOST,
        db_port=DB_PORT,
        db_name=DB_NAME,
    )

    engine = create_engine(database_url)

    create_analysis_table(
        engine=engine,
        schema=DB_SCHEMA,
        census_table=CENSUS_TABLE,
        parks_table=PARKS_TABLE,
        analysis_table=ANALYSIS_TABLE,
        low_park_sqm_per_capita=LOW_PARK_SQM_PER_CAPITA,
        high_park_sqm_per_capita=HIGH_PARK_SQM_PER_CAPITA,
        near_park_distance_m=NEAR_PARK_DISTANCE_M,
        far_park_distance_m=FAR_PARK_DISTANCE_M,
    )

    create_analysis_indexes(
        engine=engine,
        schema=DB_SCHEMA,
        analysis_table=ANALYSIS_TABLE,
    )

    validate_analysis_table(
        engine=engine,
        schema=DB_SCHEMA,
        analysis_table=ANALYSIS_TABLE,
    )

    print("\nDone creating analysis table.")


# %%
if __name__ == "__main__":
    main()
# %%
