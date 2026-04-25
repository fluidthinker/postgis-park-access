# %%
"""
05_load_to_postgis.py

Load prepared census and parks data into PostGIS.

Run from repo root:

    micromamba activate postgis-park-access
    export PROJ_DATA=$(python -c "import pyproj; print(pyproj.datadir.get_data_dir())")
    python scripts/ingestion/05_load_to_postgis.py

This script:
1. Reads prepared census tract + ACS GeoJSON.
2. Reads prepared OSM parks GeoJSON.
3. Connects to PostgreSQL/PostGIS using SQLAlchemy.
4. Replaces existing PostGIS tables with fresh loaded data.
5. Creates spatial and attribute indexes.
6. Runs basic PostGIS validation checks.
"""

# %%
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# %%
# ---------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------

def get_project_root() -> Path:
    """
    Return the repository root.

    Assumes this file lives in:
        <repo_root>/scripts/ingestion/05_load_to_postgis.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = get_project_root()

ACS_YEAR = "2024"

CENSUS_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "census"
    / f"dane_county_tracts_acs_{ACS_YEAR}.geojson"
)

PARKS_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "parks"
    / "dane_county_parks_osm.geojson"
)


# %%
# ---------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------

DB_NAME = "park_access"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

CENSUS_TABLE = "census_tracts_enriched"
PARKS_TABLE = "parks_osm"
DB_SCHEMA = "public"


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
def load_geodata(project_path: Path, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON file as a GeoDataFrame and ensure EPSG:4326.

    Parameters
    ----------
    project_path : Path
        Path to the GeoJSON file.
    dataset_name : str
        Human-readable dataset name for logging.

    Returns
    -------
    gpd.GeoDataFrame
        Loaded GeoDataFrame in EPSG:4326.
    """
    print(f"Loading {dataset_name} data from: {project_path}")

    gdf = gpd.read_file(project_path)

    if gdf.crs is None:
        raise ValueError(f"{dataset_name} GeoDataFrame has no CRS.")

    if gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


# %%
def validate_before_load(
    census_gdf: gpd.GeoDataFrame,
    parks_gdf: gpd.GeoDataFrame,
) -> None:
    """
    Print pre-load validation checks.

    Parameters
    ----------
    census_gdf : gpd.GeoDataFrame
        Prepared census tract data.
    parks_gdf : gpd.GeoDataFrame
        Prepared OSM parks data.
    """
    print("\n--- PRE-LOAD CHECKS ---")

    print("\nCensus:")
    print(f"Rows: {len(census_gdf)}")
    print(f"CRS: {census_gdf.crs}")
    print(f"Duplicate GEOIDs: {census_gdf['geoid'].duplicated().sum()}")
    print(f"Null geometries: {census_gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~census_gdf.geometry.is_valid).sum()}")

    print("\nParks:")
    print(f"Rows: {len(parks_gdf)}")
    print(f"CRS: {parks_gdf.crs}")

    if "park_id" not in parks_gdf.columns:
        raise KeyError("parks_gdf is missing required column: park_id")

    print(f"Duplicate park_id: {parks_gdf['park_id'].duplicated().sum()}")
    print(f"Null geometries: {parks_gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~parks_gdf.geometry.is_valid).sum()}")

    print("\nPark geometry types:")
    print(parks_gdf.geometry.geom_type.value_counts())


# %%
def create_postgis_extension(engine: Engine) -> None:
    """
    Ensure PostGIS extension exists.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    """
    print("\nEnsuring PostGIS extension exists...")

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))


# %%
def load_geodataframe_to_postgis(
    gdf: gpd.GeoDataFrame,
    table_name: str,
    engine: Engine,
    schema: str,
) -> None:
    """
    Load a GeoDataFrame to PostGIS.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to load.
    table_name : str
        Destination table name.
    engine : Engine
        SQLAlchemy database engine.
    schema : str
        Destination schema.

    Notes
    -----
    if_exists="replace" means rerunning this script replaces the table.
    It does not append duplicate rows.
    """
    print(f"\nLoading table: {schema}.{table_name}")

    gdf.to_postgis(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )

    print(f"Loaded table: {schema}.{table_name}")


# %%
def create_spatial_indexes(
    engine: Engine,
    census_table: str,
    parks_table: str,
    schema: str,
) -> None:
    """
    Create spatial and attribute indexes.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    census_table : str
        Census table name.
    parks_table : str
        Parks table name.
    schema : str
        Database schema.
    """
    print("\nCreating indexes...")

    sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{census_table}_geometry
    ON {schema}.{census_table}
    USING GIST (geometry);

    CREATE INDEX IF NOT EXISTS idx_{parks_table}_geometry
    ON {schema}.{parks_table}
    USING GIST (geometry);

    CREATE INDEX IF NOT EXISTS idx_{census_table}_geoid
    ON {schema}.{census_table} (geoid);

    CREATE INDEX IF NOT EXISTS idx_{parks_table}_park_id
    ON {schema}.{parks_table} (park_id);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Indexes created.")


# %%
def validate_postgis_load(
    engine: Engine,
    census_table: str,
    parks_table: str,
    schema: str,
) -> None:
    """
    Run basic validation checks inside PostGIS.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy database engine.
    census_table : str
        Census table name.
    parks_table : str
        Parks table name.
    schema : str
        Database schema.
    """
    print("\n--- POSTGIS LOAD CHECKS ---")

    queries = {
        "census row count": f"SELECT COUNT(*) FROM {schema}.{census_table};",
        "parks row count": f"SELECT COUNT(*) FROM {schema}.{parks_table};",
        "census SRID": f"SELECT DISTINCT ST_SRID(geometry) FROM {schema}.{census_table};",
        "parks SRID": f"SELECT DISTINCT ST_SRID(geometry) FROM {schema}.{parks_table};",
        "usable census tracts": (
            f"SELECT COUNT(*) FROM {schema}.{census_table} "
            f"WHERE tract_data_status = 'usable';"
        ),
        "zero population tracts": (
            f"SELECT COUNT(*) FROM {schema}.{census_table} "
            f"WHERE tract_data_status = 'zero_population';"
        ),
    }

    with engine.connect() as conn:
        for label, query in queries.items():
            result = conn.execute(text(query)).fetchall()
            print(f"{label}: {result}")


# %%
def main() -> None:
    """
    Run the PostGIS loading workflow.
    """
    census_gdf = load_geodata(CENSUS_PATH, dataset_name="census")
    parks_gdf = load_geodata(PARKS_PATH, dataset_name="parks")

    validate_before_load(census_gdf, parks_gdf)

    database_url = build_database_url(
        db_user=DB_USER,
        db_password=DB_PASSWORD,
        db_host=DB_HOST,
        db_port=DB_PORT,
        db_name=DB_NAME,
    )

    engine = create_engine(database_url)

    create_postgis_extension(engine)

    load_geodataframe_to_postgis(
        gdf=census_gdf,
        table_name=CENSUS_TABLE,
        engine=engine,
        schema=DB_SCHEMA,
    )

    load_geodataframe_to_postgis(
        gdf=parks_gdf,
        table_name=PARKS_TABLE,
        engine=engine,
        schema=DB_SCHEMA,
    )

    create_spatial_indexes(
        engine=engine,
        census_table=CENSUS_TABLE,
        parks_table=PARKS_TABLE,
        schema=DB_SCHEMA,
    )

    validate_postgis_load(
        engine=engine,
        census_table=CENSUS_TABLE,
        parks_table=PARKS_TABLE,
        schema=DB_SCHEMA,
    )

    print("\nDone loading data to PostGIS.")


# %%
if __name__ == "__main__":
    main()