# %%
"""
05_load_to_postgis.py

Load prepared census and parks data into PostGIS.

This script:
1. Reads processed census tract + ACS GeoJSON.
2. Reads prepared OSM parks GeoJSON.
3. Connects to PostgreSQL/PostGIS using SQLAlchemy.
4. Writes both GeoDataFrames to PostGIS tables.
5. Runs basic SQL validation checks.

----------------------------------------------------------------------
IMPORTANT: PROJ / GDAL ENVIRONMENT SETUP
----------------------------------------------------------------------

If you see an error like:

    PROJ: proj_create_from_database: Open of .../share/proj failed

Run this before executing the script:

    micromamba activate postgis-park-access
    export PROJ_DATA=$(python -c "import pyproj; print(pyproj.datadir.get_data_dir())")

Then run:

    python scripts/analysis/05_load_to_postgis.py

----------------------------------------------------------------------
Expected input files:
- data/processed/census/dane_county_tracts_acs_2024.geojson
- data/raw/parks/dane_county_parks_osm.geojson

PostGIS output tables:
- public.census_tracts_enriched
- public.parks_osm
----------------------------------------------------------------------
"""

# %%
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from sqlalchemy import create_engine, text


# %%
# Configuration

ACS_YEAR = "2024"

CENSUS_FILENAME = f"dane_county_tracts_acs_{ACS_YEAR}.geojson"
PARKS_FILENAME = "dane_county_parks_osm.geojson"

CENSUS_TABLE = "census_tracts_enriched"
PARKS_TABLE = "parks_osm"

DB_NAME = "park_access"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"


# %%
def get_project_root() -> Path:
    """
    Return the repository root.

    Assumes this file lives in:
        <repo_root>/scripts/analysis/05_load_to_postgis.py

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return Path(__file__).resolve().parents[2]


# %%
def build_database_url() -> str:
    """
    Build a SQLAlchemy database URL for PostgreSQL.

    Returns
    -------
    str
        SQLAlchemy connection URL.
    """
    return (
        f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


# %%
def load_census_data(project_root: Path) -> gpd.GeoDataFrame:
    """
    Load prepared census tract + ACS data.

    Parameters
    ----------
    project_root : Path
        Repository root.

    Returns
    -------
    gpd.GeoDataFrame
        Prepared census GeoDataFrame.
    """
    census_path = (
        project_root
        / "data"
        / "processed"
        / "census"
        / CENSUS_FILENAME
    )

    print(f"Loading census data from: {census_path}")

    gdf = gpd.read_file(census_path)

    # Ensure CRS is defined and standardized.
    if gdf.crs is None:
        raise ValueError("Census GeoDataFrame has no CRS.")

    if gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


# %%
def load_parks_data(project_root: Path) -> gpd.GeoDataFrame:
    """
    Load prepared OSM parks data.

    Parameters
    ----------
    project_root : Path
        Repository root.

    Returns
    -------
    gpd.GeoDataFrame
        Prepared parks GeoDataFrame.
    """
    parks_path = (
        project_root
        / "data"
        / "raw"
        / "parks"
        / PARKS_FILENAME
    )

    print(f"Loading parks data from: {parks_path}")

    gdf = gpd.read_file(parks_path)

    # Ensure CRS is defined and standardized.
    if gdf.crs is None:
        raise ValueError("Parks GeoDataFrame has no CRS.")

    if gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


# %%
def validate_before_load(census_gdf: gpd.GeoDataFrame, parks_gdf: gpd.GeoDataFrame) -> None:
    """
    Print quick checks before loading data to PostGIS.

    Parameters
    ----------
    census_gdf : gpd.GeoDataFrame
        Prepared census data.
    parks_gdf : gpd.GeoDataFrame
        Prepared parks data.
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
    print(f"Duplicate park_id: {parks_gdf['park_id'].duplicated().sum()}")
    print(f"Null geometries: {parks_gdf.geometry.isna().sum()}")
    print(f"Invalid geometries: {(~parks_gdf.geometry.is_valid).sum()}")

    print("\nPark geometry types:")
    print(parks_gdf.geometry.geom_type.value_counts())


# %%
def create_postgis_extension(engine) -> None:
    """
    Ensure the PostGIS extension exists in the target database.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    """
    print("\nEnsuring PostGIS extension exists...")

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))


# %%
def load_geodataframe_to_postgis(
    gdf: gpd.GeoDataFrame,
    table_name: str,
    engine,
) -> None:
    """
    Load a GeoDataFrame to PostGIS.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to load.
    table_name : str
        Destination PostGIS table name.
    engine
        SQLAlchemy engine.
    """
    print(f"\nLoading table: {table_name}")

    gdf.to_postgis(
        name=table_name,
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
    )

    print(f"Loaded table: {table_name}")


# %%
def create_spatial_indexes(engine) -> None:
    """
    Create spatial indexes and useful attribute indexes.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    """
    print("\nCreating indexes...")

    sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{CENSUS_TABLE}_geometry
    ON public.{CENSUS_TABLE}
    USING GIST (geometry);

    CREATE INDEX IF NOT EXISTS idx_{PARKS_TABLE}_geometry
    ON public.{PARKS_TABLE}
    USING GIST (geometry);

    CREATE INDEX IF NOT EXISTS idx_{CENSUS_TABLE}_geoid
    ON public.{CENSUS_TABLE} (geoid);

    CREATE INDEX IF NOT EXISTS idx_{PARKS_TABLE}_park_id
    ON public.{PARKS_TABLE} (park_id);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("Indexes created.")


# %%
def validate_postgis_load(engine) -> None:
    """
    Run basic validation checks inside PostGIS.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    """
    print("\n--- POSTGIS LOAD CHECKS ---")

    queries = {
        "census row count": f"SELECT COUNT(*) FROM public.{CENSUS_TABLE};",
        "parks row count": f"SELECT COUNT(*) FROM public.{PARKS_TABLE};",
        "census SRID": f"SELECT DISTINCT ST_SRID(geometry) FROM public.{CENSUS_TABLE};",
        "parks SRID": f"SELECT DISTINCT ST_SRID(geometry) FROM public.{PARKS_TABLE};",
        "usable census tracts": (
            f"SELECT COUNT(*) FROM public.{CENSUS_TABLE} "
            f"WHERE tract_data_status = 'usable';"
        ),
        "zero population tracts": (
            f"SELECT COUNT(*) FROM public.{CENSUS_TABLE} "
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
    project_root = get_project_root()

    census_gdf = load_census_data(project_root)
    parks_gdf = load_parks_data(project_root)

    parks_gdf = load_parks_data(get_project_root())     
   





    validate_before_load(census_gdf, parks_gdf)

    database_url = build_database_url()
    engine = create_engine(database_url)

    create_postgis_extension(engine)

    load_geodataframe_to_postgis(
        gdf=census_gdf,
        table_name=CENSUS_TABLE,
        engine=engine,
    )

    load_geodataframe_to_postgis(
        gdf=parks_gdf,
        table_name=PARKS_TABLE,
        engine=engine,
    )

    create_spatial_indexes(engine)
    validate_postgis_load(engine)

    print("\nDone loading data to PostGIS.")


# %%
if __name__ == "__main__":
    main()
# %%
