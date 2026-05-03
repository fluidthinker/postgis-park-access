"""
Streamlit app for Dane County park access analysis.

Run from project root:

    streamlit run app/app.py
"""

from pathlib import Path

import streamlit as st
from streamlit_folium import st_folium

from queries import (
    get_access_summary,
    get_access_tier_counts,
    get_chart_data,
    load_access_data,
)
from map_utils import create_access_map, load_access_geodata


def get_project_root() -> Path:
    """
    Return project root.

    Assumes this file lives in:
        <repo_root>/app/app.py

    Returns
    -------
    Path
        Absolute path to project root.
    """
    return Path(__file__).resolve().parents[1]


@st.cache_data
def cached_load_access_data(parquet_path: Path):
    return load_access_data(parquet_path)


@st.cache_data
def cached_get_access_summary(parquet_path: Path):
    return get_access_summary(parquet_path)


@st.cache_data
def cached_get_access_tier_counts(parquet_path: Path):
    return get_access_tier_counts(parquet_path)


@st.cache_data
def cached_get_chart_data(parquet_path: Path):
    return get_chart_data(parquet_path)


@st.cache_data
def cached_load_access_geodata(parquet_path: Path):
    return load_access_geodata(parquet_path)


def main() -> None:
    """
    Render Streamlit dashboard.
    """
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

    st.title("🌳 Dane County Park Access Analysis")

    st.markdown(
        """
        This dashboard explores park accessibility across Dane County census tracts.

        **Accessibility** is defined using two complementary measures:

        - **Availability:** park square meters per capita
        - **Proximity:** distance to nearest park
        """
    )

    df = cached_load_access_data(parquet_path)
    summary_df = cached_get_access_summary(parquet_path)
    tier_counts_df = cached_get_access_tier_counts(parquet_path)
    chart_df = cached_get_chart_data(parquet_path)
    map_gdf = cached_load_access_geodata(parquet_path)

    # Sidebar filters
    st.sidebar.header("Filters")

    selected_tiers = st.sidebar.multiselect(
        "Access tiers",
        options=sorted(df["access_tier"].unique()),
        default=sorted(df["access_tier"].unique()),
    )

    filtered_df = df[df["access_tier"].isin(selected_tiers)].copy()
    filtered_map_gdf = map_gdf[map_gdf["access_tier"].isin(selected_tiers)].copy()

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Tracts", len(filtered_df))

    with col2:
        st.metric(
            "Avg Park sqm / Capita",
            round(filtered_df["park_sqm_per_capita"].mean(), 1),
        )

    with col3:
        st.metric(
            "Avg Distance to Park (m)",
            round(filtered_df["nearest_park_distance_m"].mean(), 1),
        )

    with col4:
        st.metric(
            "Avg Median Income",
            f"${filtered_df['med_income'].mean():,.0f}",
        )

    # Map
    st.subheader("Park Access Map")

    access_map = create_access_map(filtered_map_gdf)

    st_folium(
        access_map,
        width=None,
        height=600,
    )

    # Summary table
    st.subheader("Access Tier Summary")
    st.dataframe(summary_df, use_container_width=True)

    # Charts
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Access Tier Counts")
        st.bar_chart(
            tier_counts_df,
            x="access_tier",
            y="tract_count",
        )

    with right_col:
        st.subheader("Income vs Park Area per Capita")
        st.scatter_chart(
            chart_df,
            x="med_income",
            y="park_sqm_per_capita",
            color="access_tier",
        )

    st.subheader("Income vs Distance to Nearest Park")
    st.scatter_chart(
        chart_df,
        x="med_income",
        y="nearest_park_distance_m",
        color="access_tier",
    )

    # Data preview
    with st.expander("View data"):
        st.dataframe(filtered_df, use_container_width=True)


if __name__ == "__main__":
    main()