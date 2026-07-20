# Flight-Redemption-Dashboard-Pipeline

The Flight Redemption Analytics Platform is an end-to-end data engineering project that ingests cash flight prices and award redemption data, processes them through a medallion-style pipeline, and surfaces high-value redemption opportunities using cents-per-point (CPP) analysis.

## What this project does

This repository now includes a working local ETL workflow for:

- ingesting cash flight pricing data,
- generating synthetic award redemption data,
- transforming that data into bronze, silver, and gold layers,
- and producing gold-layer analytics for identifying attractive award redemptions.

## Current pipeline status

The project has progressed from initial ingestion scaffolding to a functional analytics pipeline:

- Bronze layer: raw/normalized flight data is written to Parquet.
- Silver layer: cash data is cleaned and enriched with timestamps and duration fields.
- Bronze award layer: award redemption records are created and stored in Parquet.
- Gold layer: cash and award data are joined to compute CPP for comparison and sorting.
- Quality layer: PySpark checks required fields, schema drift, duplicate flight IDs, invalid flight times, and null-rate thresholds.

## Core components

- Python and PySpark for ETL processing
- Parquet-based storage for the medallion layers
- Synthetic award generation logic for reward point estimation
- Regression test coverage for the gold analytics transformation
- Parquet quality outputs for record-level flags and run-level null-rate metrics
- A simple dashboard entry point for future visualization work

## Project structure

- etl/bronze/: bronze ingestion scripts
- etl/silver/: silver transformation scripts
- etl/gold/: gold analytics logic
- utils/: shared helpers such as the synthetic award generator
- tests/: regression tests for the pipeline logic
- dashboard/: starter dashboard application

## How to run locally

1. Create and activate a Python virtual environment.
2. Install the dependencies from requirements.txt.
3. Run the end-to-end pipeline from the project root:
   - `python etl/run_pipeline.py`
4. Use the generated Parquet outputs in data folder for downstream analysis.

You can also run the ETL steps individually if you want to inspect each stage:
- bronze cash ingestion
- silver cash transformation
- bronze award ingestion
- gold analytics build

## Recent progress

The project now includes a working gold layer that joins cash and award data, calculates CPP, and marks likely good redemption opportunities. The core transformation is verified by an automated regression test.

## Next steps

Future work will focus on:

- wiring the pipeline into Airflow orchestration,
- expanding the dashboard with real analytics views,
- and adding more data quality checks and monitoring.
