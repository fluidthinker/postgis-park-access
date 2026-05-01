# %%
"""
07_export_results.py

Export PostGIS analysis results to portable formats (GeoParquet + GeoJSON).

Run:

    micromamba activate postgis-park-access
    export PROJ_DATA=$(python -c "import pyproj; print(pyproj.datadir.get_data_dir())")
    python scripts/ingestion/07_export_results.py

Overview
--------
This script:
1. Connects to PostGIS
2. Loads the tract_park_access analysis table
3. Exports results to:
   - GeoParquet (for DuckDB / Streamlit)
   - GeoJSON (for debugging / GIS tools)

Why this matters
----------------
- PostGIS performs heavy spatial analysis
- Parquet provides a lightweight, fast, portable format
- Enables deployment without a running database
"""

# %%
from pathlib import Path
import geopandas as gpd
from sqlalchemy import create_engine

# %%
# ---------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------

DB_NAME = "park_access"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

SCHEMA = "public"
TABLE = "tract_park_access"

# %%
# ---------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------

OUTPUT_DIR = Path("data/processed/analysis")

PARQUET_FILENAME = "tract_park_access.parquet"
GEOJSON_FILENAME = "tract_park_access.geojson"


# %%
def build_database_url() -> str:
    """
    Construct SQLAlchemy database connection string.

    Returns
    -------
    str
        PostgreSQL/PostGIS connection URL.
    """
    return (
        f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


# %%
def get_project_root() -> Path:
    """
    Get repository root directory based on current file location.

    Returns
    -------
    Path
        Absolute path to project root.
    """
    return Path(__file__).resolve().parents[2]


# %%
def ensure_output_dir(path: Path) -> None:
    """
    Create output directory if it does not exist.

    Parameters
    ----------
    path : Path
        Directory path to create.
    """
    path.mkdir(parents=True, exist_ok=True)


# %%
def load_analysis_table(engine) -> gpd.GeoDataFrame:
    """
    Load PostGIS analysis table into a GeoDataFrame.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy database engine.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing tract-level park access metrics.
    """
    print("Loading analysis table from PostGIS...")

    query = f"""
        SELECT *
        FROM {SCHEMA}.{TABLE}
    """

    gdf = gpd.read_postgis(
        query,
        engine,
        geom_col="geometry"
    )

    print(f"Loaded {len(gdf)} rows.")
    return gdf


# %%
def export_to_parquet(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    """
    Export GeoDataFrame to GeoParquet format.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Data to export.
    output_path : Path
        Output file path.
    """
    print(f"Exporting to Parquet: {output_path}")

    gdf.to_parquet(output_path)

    print("Parquet export complete.")


# %%
def export_to_geojson(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    """
    Export GeoDataFrame to GeoJSON format.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Data to export.
    output_path : Path
        Output file path.
    """
    print(f"Exporting to GeoJSON: {output_path}")

    gdf.to_file(output_path, driver="GeoJSON")

    print("GeoJSON export complete.")


# %%
def main() -> None:
    """
    Execute export pipeline.

    Workflow
    --------
    1. Resolve project paths
    2. Connect to PostGIS
    3. Load analysis table
    4. Export to Parquet and GeoJSON
    """
    project_root = get_project_root()
    output_dir = project_root / OUTPUT_DIR

    ensure_output_dir(output_dir)

    engine = create_engine(build_database_url())

    gdf = load_analysis_table(engine)

    # Quick sanity checks (useful for debugging / verification)
    print("\nColumns:")
    print(gdf.columns.tolist())

    print("\nSample rows:")
    print(gdf.head())

    export_to_parquet(gdf, output_dir / PARQUET_FILENAME)

    export_to_geojson(gdf, output_dir / GEOJSON_FILENAME)

    print("\nDone exporting results.")


# %%
if __name__ == "__main__":
    main()
# %%
