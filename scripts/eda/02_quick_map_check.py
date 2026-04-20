from pathlib import Path
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main():
    root = get_project_root()

    tracts_path = root / "data/raw/census_tracts/dane_county_census_tracts.geojson"
    acs_path = root / "data/raw/acs/dane_county_acs5_2024.csv"

    tracts = gpd.read_file(tracts_path)
    acs = pd.read_csv(acs_path, dtype={"geoid": str})

    merged = tracts.merge(acs, on="geoid")
   
    # --- simple plot ---
    fig, ax = plt.subplots(figsize=(8, 8))

    merged.plot(
        column="med_income",
        cmap="viridis",
        legend=True,
        ax=ax,
        edgecolor="black",
        linewidth=0.2,
        missing_kwds = { 
        "color": "lightgrey",
        "label": "No population / no data"
        }
    )

    ax.set_title("Dane County Median Income by Census Tract")
    ax.axis("off")

    plt.show()


if __name__ == "__main__":
    main()