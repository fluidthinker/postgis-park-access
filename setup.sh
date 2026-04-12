#!/usr/bin/env bash

set -euo pipefail

# Use provided path or current directory
PROJECT_ROOT="${1:-$(pwd)}"

echo "Creating project at: ${PROJECT_ROOT}"

# -----------------------
# DATA DIRECTORIES
# -----------------------
mkdir -p "${PROJECT_ROOT}/data/raw/parks"
mkdir -p "${PROJECT_ROOT}/data/raw/census_tracts"
mkdir -p "${PROJECT_ROOT}/data/raw/acs"

mkdir -p "${PROJECT_ROOT}/data/interim"
mkdir -p "${PROJECT_ROOT}/data/processed"

# -----------------------
# SQL DIRECTORIES
# -----------------------
mkdir -p "${PROJECT_ROOT}/sql/schema"
mkdir -p "${PROJECT_ROOT}/sql/analysis"
mkdir -p "${PROJECT_ROOT}/sql/qa"

# -----------------------
# SCRIPT DIRECTORIES
# -----------------------
mkdir -p "${PROJECT_ROOT}/scripts/ingestion"
mkdir -p "${PROJECT_ROOT}/scripts/analysis"
mkdir -p "${PROJECT_ROOT}/scripts/utils"

# -----------------------
# APP + DOCS + OUTPUTS
# -----------------------
mkdir -p "${PROJECT_ROOT}/app"
mkdir -p "${PROJECT_ROOT}/docs"
mkdir -p "${PROJECT_ROOT}/outputs/maps"
mkdir -p "${PROJECT_ROOT}/outputs/tables"
mkdir -p "${PROJECT_ROOT}/outputs/figures"

# -----------------------
# TESTS
# -----------------------
mkdir -p "${PROJECT_ROOT}/tests"

# -----------------------
# ROOT FILES
# -----------------------
touch "${PROJECT_ROOT}/README.md"
touch "${PROJECT_ROOT}/.gitignore"
touch "${PROJECT_ROOT}/docker-compose.yml"
touch "${PROJECT_ROOT}/requirements.txt"
touch "${PROJECT_ROOT}/environment.yml"

# -----------------------
# OPTIONAL STARTER FILES
# -----------------------
touch "${PROJECT_ROOT}/app/streamlit_app.py"

touch "${PROJECT_ROOT}/docs/project_notes.md"
touch "${PROJECT_ROOT}/docs/data_sources.md"

touch "${PROJECT_ROOT}/scripts/ingestion/01_download_census_tracts.py"
touch "${PROJECT_ROOT}/scripts/ingestion/02_download_acs_data.py"
touch "${PROJECT_ROOT}/scripts/ingestion/03_prepare_parks.py"

touch "${PROJECT_ROOT}/scripts/analysis/04_load_to_postgis.py"
touch "${PROJECT_ROOT}/scripts/analysis/05_create_analysis_tables.py"
touch "${PROJECT_ROOT}/scripts/analysis/06_export_results.py"

touch "${PROJECT_ROOT}/sql/schema/01_create_schema.sql"
touch "${PROJECT_ROOT}/sql/schema/02_create_parks_table.sql"
touch "${PROJECT_ROOT}/sql/schema/03_create_census_tracts_table.sql"

touch "${PROJECT_ROOT}/sql/analysis/01_park_access_analysis.sql"

touch "${PROJECT_ROOT}/sql/qa/01_validation_queries.sql"

echo "----------------------------------------"
echo "Project scaffold created successfully."
echo "----------------------------------------"
echo "Next steps:"
echo "  cd ${PROJECT_ROOT}"
echo "  code ."