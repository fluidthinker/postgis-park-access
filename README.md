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

---

# Data Pipeline

The project follows a simple but effective geospatial data engineering workflow.

```text
ACS Census Data          OpenStreetMap Parks
        │                       │
        └──────────────┬────────┘
                       ▼
                PostGIS Database
                       │
             Spatial SQL Analysis
                       │
        Area-weighted park metrics
        Nearest park calculations
        Access tier classification
                       │
                       ▼
         GeoParquet Export (Portable Dataset)
                       │
                       ▼
                  DuckDB Query Layer
                       │
                       ▼
             Streamlit Interactive Dashboard
```

By separating data preparation from visualization, the dashboard remains lightweight and responsive while the computationally intensive spatial analysis only needs to be performed once.

---

# Repository Structure

```text
postgis-park-access/
│
├── app/
│   ├── app.py
│   ├── queries.py
│   └── map_utils.py
│
├── scripts/
│   ├── ingestion/
│   └── eda/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docs/
│   └── images/
│
├── outputs/
│
├── environment.yml
└── README.md
```

### Folder Overview

| Folder | Purpose |
|---------|---------|
| **app/** | Streamlit application and supporting modules |
| **scripts/** | Data ingestion, spatial analysis, and exploratory analysis |
| **data/** | Raw and processed datasets |
| **docs/images/** | Images used by the README |
| **outputs/** | Generated maps and exported outputs |

---

# Running the Project

## 1. Clone the repository

```bash
git clone <repository-url>

cd postgis-park-access
```

---

## 2. Create the environment

```bash
micromamba create -f environment.yml

micromamba activate postgis-park-access
```

---

## 3. Launch the dashboard

```bash
streamlit run app/app.py
```

The dashboard opens automatically in your browser.

---

# Dashboard Components

The application consists of three primary modules.

| Module | Responsibility |
|----------|----------------|
| **app.py** | Streamlit user interface and dashboard layout |
| **queries.py** | DuckDB queries and analytical data access |
| **map_utils.py** | Folium map creation, styling, and legends |

Separating these responsibilities keeps the code easier to maintain and test.

---

# Methodology

<p align="center">
  <img src="docs/images/methodology.jpg" width="700">
  <br>
  <em>The dashboard documents the methodology used to calculate park accessibility and summarizes results by access tier.</em>
</p>

Park accessibility was evaluated using two complementary measures:

- **Availability** – total park area available per resident.
- **Proximity** – distance from each census tract to its nearest park.

These measures were combined to classify census tracts into four access tiers:

- High Access
- Moderate Access
- Low Access
- Very Low Access

The dashboard also documents project limitations and provides transparency about the analytical approach.

---

# Future Enhancements

Potential future improvements include:

- Add park polygons as an optional map layer.
- Add a median household income choropleth.
- Explore spatial clustering using Moran's I or Getis-Ord Gi* statistics.
- Investigate additional socioeconomic variables.
- Compare straight-line distance with network-based walking distance.
- Deploy the dashboard using Google Cloud Run.

---

# Lessons Learned

This project reinforced several important software engineering and geospatial analysis principles.

### Separate computation from presentation

Performing the spatial analysis once in PostGIS and exporting the results to GeoParquet simplified the dashboard and improved performance.

### Build modular applications

Separating SQL queries, mapping logic, and the Streamlit interface made the project easier to understand and maintain.

### Design around the user

The dashboard became much stronger after shifting the focus from displaying maps and charts to communicating a clear analytical story.

### Documentation is part of software engineering

Returning to the project after several months reinforced the value of good documentation, architectural diagrams, and leaving clear "breadcrumbs" for future maintenance.

---

# Technologies Used

### Geospatial

- PostGIS
- GeoPandas
- OpenStreetMap
- OSMnx
- GeoParquet

### Data Engineering

- DuckDB
- PostgreSQL
- Pandas

### Dashboard

- Streamlit
- Folium
- Altair

### Development

- Python
- Micromamba
- Docker
- Git
- Visual Studio Code

---

# Acknowledgements

This project uses publicly available data from:

- U.S. Census Bureau — American Community Survey (ACS)
- OpenStreetMap contributors

---

## Author

**Chris Randle**

Master of Environmental Science

Geospatial Data Engineer | GIS Developer | Spatial Data Science

Portfolio: https://fluidthinker.github.io/

LinkedIn: [[[LinkedIn](https://www.linkedin.com/in/chrisr-letsmakechange/)]]

---