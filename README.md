# Sales-analytics-data-pipeline
## Project Overview

An end-to-end data engineering project that ingests, validates,
transforms, and analyzes sales data using Python, SQL, and
Apache Spark.

## Objectives

- Build a reliable data ingestion pipeline.
- Validate and clean raw sales datasets.
- Integrate orders, customers, and products.
- Generate analytics-ready datasets and quality reports.
- Perform SQL-based sales analysis.
- Extend processing with Apache Spark.
- Prepare the pipeline for future Azure deployment.

## Technology Stack

- Python
- SQL
- Google Colab
- Google Drive
- Git and GitHub
- Apache Spark (planned)

## Initial Data Sources

- orders.csv
- customers.csv
- products.csv

## Repository Structure

- src/sales_pipeline/: pipeline implementation
- tests/: automated tests
- notebooks/: development notebooks
- docs/: architecture and project documentation
- config/: non-secret configuration

## Project Status

Phase 0: Project setup and architecture.

## Planned Pipeline

Raw CSV files -> Ingestion -> Data Quality Checks ->
Transformation -> Analytics-Ready Data -> Sales Insights

## Data and Security

Generated datasets and runtime outputs are stored separately
from source code. Credentials and access tokens must never
be committed to this repository.
