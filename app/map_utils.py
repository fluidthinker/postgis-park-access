"""
Map utilities for the Streamlit app.

This module creates an interactive Folium map from exported GeoParquet data.
"""

from pathlib import Path

import folium
import geopandas as gpd


ACCESS_TIER_COLORS = {
    "High Access": "#2ca25f",
    "Moderate Access": "#fee08b",
    "Low Access": "#f46d43",
    "Very Low Access": "#a50026",
}
# Map defaults
MAP_CENTER = [43.07, -89.40]
MAP_ZOOM_START = 10
MAP_TILES = "cartodbpositron"

DEFAULT_TRACT_STYLE = {
    "color": "black",
    "weight": 0.5,
    "fillOpacity": 0.65,
}

DEFAULT_FILL_COLOR = "#cccccc"

TOOLTIP_FIELDS = [
    "geoid",
    "access_tier",
    "med_income",
    "pct_renter",
    "park_sqm_per_capita",
    "nearest_park_distance_m",
]

TOOLTIP_ALIASES = [
    "GEOID:",
    "Access Tier:",
    "Median Income:",
    "% Renters:",
    "Park sqm / Capita:",
    "Nearest Park Distance (m):",
]




def load_access_geodata(parquet_path: Path) -> gpd.GeoDataFrame:
    """
    Load GeoParquet as a GeoDataFrame.

    Parameters
    ----------
    parquet_path : Path
        Path to tract_park_access.parquet.

    Returns
    -------
    gpd.GeoDataFrame
        Tract-level access data with geometry.
    """
    gdf = gpd.read_parquet(parquet_path)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    if gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf

def style_by_access_tier(feature: dict) -> dict:
    """
    Style a GeoJSON feature based on access tier.
    """
    access_tier = feature["properties"].get("access_tier")
    fill_color = ACCESS_TIER_COLORS.get(access_tier, DEFAULT_FILL_COLOR)

    return {
        **DEFAULT_TRACT_STYLE,
        "fillColor": fill_color,
    }


def create_access_map(gdf: gpd.GeoDataFrame) -> folium.Map:
    """
    Create interactive Folium map of park access tiers.
    """
    m = folium.Map(
        location=MAP_CENTER,
        zoom_start=MAP_ZOOM_START,
        tiles=MAP_TILES,
    )

    tooltip = folium.GeoJsonTooltip(
        fields=TOOLTIP_FIELDS,
        aliases=TOOLTIP_ALIASES,
        localize=True,
        sticky=True,
    )

    folium.GeoJson(
        gdf,
        name="Park Access by Census Tract",
        style_function=style_by_access_tier,
        tooltip=tooltip,
    ).add_to(m)

    folium.LayerControl().add_to(m)

    return m




def get_project_root() -> Path:
    """
    Return project root for module-level testing.

    Assumes this file lives in:
        <repo_root>/app/map_utils.py

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

    output_path = project_root / "outputs" / "maps" / "test_access_map.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Testing map_utils.py with: {test_parquet_path}")
    print(f"File exists: {test_parquet_path.exists()}")

    if not test_parquet_path.exists():
        raise FileNotFoundError(test_parquet_path)

    gdf = load_access_geodata(test_parquet_path)

    print("\nLoaded GeoDataFrame:")
    print(f"Rows: {len(gdf)}")
    print(f"CRS: {gdf.crs}")
    print("Access tier counts:")
    print(gdf["access_tier"].value_counts())

    access_map = create_access_map(gdf)
    access_map.save(output_path)

    print(f"\nSaved test map to: {output_path}")