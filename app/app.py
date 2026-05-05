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
    get_access_tier_counts,
    get_chart_data,
    load_access_data,
)
from map_utils import create_access_map, load_access_geodata


# =========================
# Project Paths
# =========================

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


# =========================
# Cached Data Access
# =========================

@st.cache_data
def cached_load_access_data(parquet_path: Path):
    """Load main tract-level access table."""
    return load_access_data(parquet_path)


@st.cache_data
def cached_get_access_summary(parquet_path: Path):
    """Load access-tier summary table."""
    return get_access_summary(parquet_path)


@st.cache_data
def cached_get_access_tier_counts(parquet_path: Path):
    """Load access-tier tract counts."""
    return get_access_tier_counts(parquet_path)


@st.cache_data
def cached_get_chart_data(parquet_path: Path):
    """Load chart-ready data."""
    return get_chart_data(parquet_path)


@st.cache_data
def cached_load_access_geodata(parquet_path: Path):
    """Load GeoDataFrame for Folium map."""
    return load_access_geodata(parquet_path)


# =========================
# Main Streamlit App
# =========================

def main() -> None:
    """
    Render Streamlit dashboard.
    """

    # -------------------------
    # Page setup
    # -------------------------

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

    # -------------------------
    # Page title / intro
    # -------------------------

    st.title("🌳 Dane County Park Access Analysis")

    st.markdown(
        """
        This dashboard explores park accessibility across Dane County census tracts.

        **Accessibility** is defined using two complementary measures:

        - **Availability:** park square meters per capita
        - **Proximity:** distance to nearest park
        """
    )

    # -------------------------
    # Load data
    # -------------------------

    df = cached_load_access_data(parquet_path)
    summary_df = cached_get_access_summary(parquet_path)
    tier_counts_df = cached_get_access_tier_counts(parquet_path)
    chart_df = cached_get_chart_data(parquet_path)
    map_gdf = cached_load_access_geodata(parquet_path)

    # -------------------------
    # Sidebar filters
    # -------------------------

    st.sidebar.header("Filters")

    selected_tiers = st.sidebar.multiselect(
        "Access tiers",
        options=sorted(df["access_tier"].dropna().unique()),
        default=sorted(df["access_tier"].dropna().unique()),
    )

    filtered_df = df[df["access_tier"].isin(selected_tiers)].copy()
    filtered_chart_df = chart_df[chart_df["access_tier"].isin(selected_tiers)].copy()
    filtered_map_gdf = map_gdf[map_gdf["access_tier"].isin(selected_tiers)].copy()

    filtered_tier_counts_df = (
        filtered_df["access_tier"]
        .value_counts()
        .rename_axis("access_tier")
        .reset_index(name="tract_count")
    )

    # -------------------------
    # Metrics row
    # -------------------------

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Tracts", f"{len(filtered_df):,}")

    with col2:
        st.metric(
            "Avg Park sqm / Capita",
            f"{filtered_df['park_sqm_per_capita'].mean():,.1f}",
        )

    with col3:
        st.metric(
            "Avg Distance to Park (m)",
            f"{filtered_df['nearest_park_distance_m'].mean():,.1f}",
        )

    with col4:
        st.metric(
            "Avg Median Income",
            f"${filtered_df['med_income'].mean():,.0f}",
        )

    # -------------------------
    # Map section
    # -------------------------

    st.subheader("Park Access Map")

    access_map = create_access_map(filtered_map_gdf)

    st_folium(
        access_map,
        width=None,
        height=600,
    )

    # -------------------------
    # Summary table
    # -------------------------

    st.subheader("Access Tier Summary")
    st.dataframe(summary_df, use_container_width=True)

    # -------------------------
    # Charts section
    # -------------------------

    st.subheader("Charts")

    left_col, right_col = st.columns(2)

    # Chart 1: Access tier counts
    with left_col:
        st.subheader("Access Tier Counts")
        st.bar_chart(
            filtered_tier_counts_df,
            x="access_tier",
            y="tract_count",
        )

    # Chart 2: Median income by access tier
    income_boxplot = (
        alt.Chart(filtered_chart_df)
        .mark_boxplot()
        .encode(
            x=alt.X("access_tier:N", title="Access Tier"),
            y=alt.Y("med_income:Q", title="Median Income"),
            tooltip=["access_tier", "med_income"],
        )
        .properties(height=350)
    )

    with right_col:
        st.subheader("Median Income by Access Tier")
        st.altair_chart(income_boxplot, use_container_width=True)

    # Chart 3: Nearest park distance by access tier
    distance_boxplot = (
        alt.Chart(filtered_chart_df)
        .mark_boxplot()
        .encode(
            x=alt.X("access_tier:N", title="Access Tier"),
            y=alt.Y(
                "nearest_park_distance_m:Q",
                title="Nearest Park Distance (m)",
            ),
            tooltip=["access_tier", "nearest_park_distance_m"],
        )
        .properties(height=350)
    )

    st.subheader("Nearest Park Distance by Access Tier")
    st.altair_chart(distance_boxplot, use_container_width=True)

    # Extra exploratory charts
    st.subheader("Exploratory Relationships")

    scatter_left, scatter_right = st.columns(2)

    with scatter_left:
        st.subheader("Income vs Park Area per Capita")
        st.scatter_chart(
            filtered_chart_df,
            x="med_income",
            y="park_sqm_per_capita",
            color="access_tier",
        )

    with scatter_right:
        st.subheader("Income vs Distance to Nearest Park")
        st.scatter_chart(
            filtered_chart_df,
            x="med_income",
            y="nearest_park_distance_m",
            color="access_tier",
        )

    # -------------------------
    # Data preview
    # -------------------------

    with st.expander("View data"):
        st.dataframe(filtered_df, use_container_width=True)


if __name__ == "__main__":
    main()