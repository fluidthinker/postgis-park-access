"""
Streamlit app for Dane County park access analysis.

Run from project root:

    streamlit run app/app.py
"""

from pathlib import Path

import altair as alt
import streamlit as st
from streamlit_folium import st_folium

from queries import (
    get_access_summary,
    get_chart_data,
    load_access_data,
)
from map_utils import create_access_map, load_access_geodata


def get_project_root() -> Path:
    """Return project root."""
    return Path(__file__).resolve().parents[1]


@st.cache_data
def cached_load_access_data(parquet_path: Path):
    """Load main tract-level access table."""
    return load_access_data(parquet_path)


@st.cache_data
def cached_get_access_summary(parquet_path: Path):
    """Load access-tier summary table."""
    return get_access_summary(parquet_path)


@st.cache_data
def cached_get_chart_data(parquet_path: Path):
    """Load chart-ready data."""
    return get_chart_data(parquet_path)


@st.cache_data
def cached_load_access_geodata(parquet_path: Path):
    """Load GeoDataFrame for Folium map."""
    return load_access_geodata(parquet_path)


def create_access_tier_count_chart(filtered_df):
    """Create bar chart showing number of census tracts by access tier."""
    tier_counts = (
        filtered_df["access_tier"]
        .value_counts()
        .rename_axis("access_tier")
        .reset_index(name="tract_count")
    )

    return (
        alt.Chart(tier_counts)
        .mark_bar()
        .encode(
            x=alt.X("access_tier:N", title="Access Tier"),
            y=alt.Y("tract_count:Q", title="Number of Census Tracts"),
            tooltip=[
                alt.Tooltip("access_tier:N", title="Access Tier"),
                alt.Tooltip("tract_count:Q", title="Census Tracts"),
            ],
        )
        .properties(height=350)
    )


def create_income_boxplot(filtered_chart_df):
    """Create boxplot comparing median household income by access tier."""
    return (
        alt.Chart(filtered_chart_df)
        .mark_boxplot()
        .encode(
            x=alt.X("access_tier:N", title="Access Tier"),
            y=alt.Y("med_income:Q", title="Median Household Income ($)"),
            tooltip=[
                alt.Tooltip("access_tier:N", title="Access Tier"),
                alt.Tooltip(
                    "med_income:Q",
                    title="Median Household Income",
                    format="$,.0f",
                ),
            ],
        )
        .properties(height=350)
    )


def create_income_distance_scatter(filtered_chart_df):
    """Create scatter plot comparing income and distance to nearest park."""
    return (
        alt.Chart(filtered_chart_df)
        .mark_circle(size=80, opacity=0.75)
        .encode(
            x=alt.X("med_income:Q", title="Median Household Income ($)"),
            y=alt.Y(
                "nearest_park_distance_m:Q",
                title="Distance to Nearest Park (m)",
            ),
            color=alt.Color("access_tier:N", title="Access Tier"),
            tooltip=[
                alt.Tooltip("access_tier:N", title="Access Tier"),
                alt.Tooltip(
                    "med_income:Q",
                    title="Median Household Income",
                    format="$,.0f",
                ),
                alt.Tooltip(
                    "nearest_park_distance_m:Q",
                    title="Distance to Nearest Park (m)",
                    format=",.0f",
                ),
                alt.Tooltip(
                    "park_sqm_per_capita:Q",
                    title="Park Area per Resident (sq m)",
                    format=",.1f",
                ),
            ],
        )
        .properties(height=425)
    )


def main() -> None:
    """Render Streamlit dashboard."""

    st.set_page_config(
        page_title="Dane County Park Access",
        layout="wide",
    )

    project_root = get_project_root()
    parquet_path = (
        project_root
        / "data"
        / "processed"
        / "analysis"
        / "tract_park_access.parquet"
    )

    df = cached_load_access_data(parquet_path)
    summary_df = cached_get_access_summary(parquet_path)
    chart_df = cached_get_chart_data(parquet_path)
    map_gdf = cached_load_access_geodata(parquet_path)

    # -------------------------
    # Sidebar filters
    # -------------------------

    st.sidebar.header("Dashboard Filters")

    st.sidebar.markdown(
        """
        Use the filter below to focus on one or more park access tiers.

        Click the **x** to remove a tier, or use the dropdown to add it back.
        The map and charts update to match your selection.
        """
    )

    access_tiers = sorted(df["access_tier"].dropna().unique())

    selected_tiers = st.sidebar.multiselect(
        "Access tiers",
        options=access_tiers,
        default=access_tiers,
    )

    st.sidebar.markdown(
        """
        **What the tiers mean**

        Each tract's access tier reflects how much park space it has and how far it is
        from the nearest park.

        - **High Access** — plenty of park space, close by
        - **Moderate Access** — reasonable space or distance, not both
        - **Low Access** — limited space and/or farther from a park
        - **Very Low Access** — little nearby park space, farthest from a park
        """
    )

    filtered_df = df[df["access_tier"].isin(selected_tiers)].copy()
    filtered_chart_df = chart_df[chart_df["access_tier"].isin(selected_tiers)].copy()
    filtered_map_gdf = map_gdf[map_gdf["access_tier"].isin(selected_tiers)].copy()

    # -------------------------
    # Title and intro
    # -------------------------

    st.title("Dane County Park Access Analysis")

    st.markdown(
        """
        **An interactive dashboard exploring how park access varies across Dane County,
        Wisconsin, and how that access relates to median household income.**
        """
    )

    st.markdown(
        """
        Access to parks supports recreation, public health, and quality of life. This project
        looks at whether some neighborhoods have much better park access than others —
        and whether that gap lines up with income.

        **Access** here means two things: how much park space is nearby, and how far away
        the closest park is. Details on how these were calculated are below.
        """
    )

    # -------------------------
    # Key findings
    # -------------------------

    st.subheader("Key Findings")

    st.markdown(
        """
        - Park access varies a lot from one census tract to another — some neighborhoods
          have plenty of park space nearby, others have very little.
        - Lower-access neighborhoods tend to have lower median incomes, but it's not a
          hard rule — there are higher-income tracts with poor access and lower-income
          tracts with good access.
        - Income only tells part of the story. Where a neighborhood sits on the map matters
          just as much as how much money people there make.
        """
    )

    # -------------------------
    # Map
    # -------------------------

    st.subheader("Park Access by Census Tract")

    st.markdown(
        """
        Each census tract is colored by its park access tier. Hover over a tract to view
        median household income, renter share, park area per resident, and distance to
        the nearest park.
        """
    )

    access_map = create_access_map(filtered_map_gdf)

    st_folium(
        access_map,
        width=None,
        height=650,
    )

    # -------------------------
    # Supporting evidence
    # -------------------------

    st.subheader("Supporting Evidence")

    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("### Census Tracts by Access Tier")
        st.markdown(
            """
            This chart shows how many census tracts fall into each park access category.
            """
        )
        st.altair_chart(
            create_access_tier_count_chart(filtered_df),
            width="stretch",
        )

    with right_col:
        st.markdown("### Median Income by Access Tier")
        st.markdown(
            """
            This chart compares median household income across access tiers.
            """
        )
        st.altair_chart(
            create_income_boxplot(filtered_chart_df),
            width="stretch",
        )

    # -------------------------
    # Scatter plot
    # -------------------------

    st.subheader("Income vs. Distance to Nearest Park")

    st.markdown(
        """
        This scatter plot explores whether census tracts with higher or lower median
        household incomes tend to be closer to parks.
        """
    )

    st.altair_chart(
        create_income_distance_scatter(filtered_chart_df),
        width="stretch",
    )

    st.markdown(
        """
        **Interpretation:** Income doesn't cleanly predict distance to a park. A few
        lower-income tracts are far from parks, but plenty of tracts at every income level
        are close to one — the two just aren't tightly linked.
        """
    )

    # -------------------------
    # Methods
    # -------------------------

    with st.expander("How were access tiers calculated?"):
        st.markdown(
            """
            Access tiers are based on tract-level park access metrics created during the
            spatial analysis phase.

            The project uses two core measures:

            - **Park area per resident:** total park area associated with a census tract
              divided by population.
            - **Distance to nearest park:** distance from each census tract to the nearest park.

            These measures were calculated using PostGIS spatial analysis and exported
            to GeoParquet. The Streamlit dashboard reads the finished GeoParquet file
            through DuckDB and GeoPandas.
            """
        )

        st.dataframe(summary_df, width="stretch")

    # -------------------------
    # Limitations
    # -------------------------

    with st.expander("Limitations"):
        st.markdown(
            """
            This is an exploratory look at the data, not a final policy conclusion.

            Important limitations:

            - Park data comes from OpenStreetMap and may be incomplete or inconsistently classified.
            - Census tracts are useful for regional comparison but may hide neighborhood-scale variation.
            - Distance to the nearest park does not necessarily represent safe or walkable access.
            - Median household income is only one socioeconomic indicator and does not imply causation.
            - Park quality, amenities, entrances, sidewalks, transit access, and barriers were not included.
            """
        )

    # -------------------------
    # Data preview
    # -------------------------

    with st.expander("View underlying data"):
        st.dataframe(filtered_df, width="stretch")


if __name__ == "__main__":
    main()