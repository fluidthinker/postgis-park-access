# %%
from pathlib import Path
import geopandas as gpd
import contextily as cx

# %%
# Load parks data
project_root = Path(__file__).resolve().parents[2]

parks_path = project_root / "data" / "raw" / "parks" / "dane_county_parks_osm.geojson"

parks = gpd.read_file(parks_path)

print(len(parks))
parks.head()

# %%
# Reproject for basemap
parks_web = parks.to_crs(epsg=3857)


# Plot with ESRI imagery
# %%
ax = parks_web.plot(
    figsize=(10, 10),
    facecolor="none",     # 👈 KEY CHANGE
    edgecolor="red",
    linewidth=0.5
)

import contextily as cx
cx.add_basemap(ax, source=cx.providers.Esri.WorldImagery)

ax.set_title("OSM Parks (Outline) over Imagery")
ax.set_axis_off()

