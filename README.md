# Dane County Park Access Analysis

<p align="center">
  <img src="docs/images/top_hero.jpg" width="700">
  <br>
  <em>Interactive Streamlit dashboard exploring park accessibility across Dane County, Wisconsin.</em>
</p>

---

## Project Highlights

- Built an end-to-end geospatial data pipeline from raw spatial and demographic data to an interactive dashboard.
- Combined U.S. Census ACS data with OpenStreetMap park polygons.
- Used PostGIS to calculate tract-level park accessibility metrics.
- Exported final spatial analysis results to GeoParquet for portability.
- Used DuckDB to query the exported analytical dataset without requiring a running database.
- Built a Streamlit dashboard with an interactive Folium map, access tier filtering, and supporting charts.
- Organized the application into separate modules for data access, mapping, and dashboard layout.

---

## Project Overview

Access to parks supports recreation, public health, and quality of life. This project explores whether some neighborhoods in Dane County, Wisconsin have much better park access than others — and whether those differences line up with median household income.

Park access is measured using two ideas:

- **Availability** — how much park space is available per resident.
- **Proximity** — how far each census tract is from the nearest park.

The project combines U.S. Census American Community Survey data with park polygons from OpenStreetMap. Spatial analysis was performed in PostGIS, results were exported to GeoParquet, and an interactive Streamlit dashboard was built to communicate the findings.

The goal was not only to make a map, but to build a small geospatial data application with a clear pipeline, reusable outputs, and an interface that explains the results.

---

## Research Questions

This project explores three main questions:

- How does park accessibility vary across Dane County census tracts?
- Do lower-income neighborhoods tend to have lower park accessibility?
- What spatial patterns appear when park access is mapped across the county?

This is an exploratory spatial analysis project rather than a predictive modeling project. The dashboard is meant to help users see patterns, compare access tiers, and understand where park access appears stronger or weaker.

---

## Interactive Dashboard

The final Streamlit dashboard allows users to:

- View park access by census tract on an interactive map.
- Filter the map and charts by access tier.
- Hover over census tracts to inspect local access and demographic values.
- Compare median household income across park access tiers.
- Review the methodology, limitations, and future enhancement ideas.

<p align="center">
  <img src="docs/images/charts.jpg" width="700">
  <br>
  <em>Supporting charts compare access tier counts and median household income by access tier.</em>
</p>

---

## Key Findings

- Park access varies substantially across Dane County census tracts.
- Lower-access neighborhoods tend to have lower median household incomes, but this is not a hard rule.
- Income only tells part of the story; location and spatial context matter.
- The relationship between income and distance to the nearest park is not strongly linear.

These findings suggest that park access is spatially uneven, but not explained by income alone.

---

## System Architecture

One of the main design goals was to separate spatial computation from dashboard presentation.

PostGIS was used for the heavier spatial analysis. Once the tract-level park access metrics were calculated, the results were exported to GeoParquet. The Streamlit app then reads the finished analytical dataset using DuckDB and GeoPandas.

```text
Raw Data
    │
    ▼
PostGIS Spatial Analysis
    │
    ▼
GeoParquet Export
    │
    ▼
DuckDB Analytical Queries
    │
    ▼
Streamlit Dashboard