# %%
from pathlib import Path

import geopandas as gpd
import contextily as cx
import matplotlib.pyplot as plt


# %%
def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


project_root = get_project_root()

parks_path = project_root / "data" / "raw" / "parks" / "dane_county_parks_osm.geojson"
tracts_path = project_root / "data" / "raw" / "census_tracts" / "dane_county_census_tracts.geojson"

output_dir = project_root / "outputs" / "maps"
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / "osm_parks_over_imagery_with_tracts.png"


# %%
parks = gpd.read_file(parks_path)
tracts = gpd.read_file(tracts_path)

parks_web = parks.to_crs(epsg=3857)
tracts_web = tracts.to_crs(epsg=3857)

# %% 
parks.head()


# %%
fig, ax = plt.subplots(figsize=(12, 10))

# Census tracts: thin outlines
tracts_web.plot(
    ax=ax,
    facecolor="none",
    edgecolor="deepskyblue",
    linewidth=0.35,
    alpha=0.8,
)

# Parks: red outlines, transparent fill
parks_web.plot(
    ax=ax,
    facecolor="none",
    edgecolor="red",
    linewidth=0.5,
    alpha=0.9,
)

# Add ESRI imagery basemap
cx.add_basemap(
    ax,
    source=cx.providers.Esri.WorldImagery,
)

ax.set_title("OSM Park Polygons and Census Tracts over ESRI Imagery")
ax.set_axis_off()

plt.tight_layout()

plt.savefig(
    output_path,
    dpi=300,
    bbox_inches="tight",
)

plt.show()

print(f"Saved map to: {output_path}")
# %%
