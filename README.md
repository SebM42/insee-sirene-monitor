# insee-sirene-monitor

End-to-end data pipeline on French business registry data (SIRENE/INSEE) 
for economic activity monitoring in the Auvergne-Rhône-Alpes region.

## Overview

This pipeline ingests monthly updates from the SIRENE API, historizes 
establishment data using a SCD Type 2 pattern, and produces business-ready 
aggregations for trend analysis (sector dynamics, regional activity, 
creation/closure rates).

## Stack

- **Platform** : Databricks (Serverless)
- **Storage** : Cloudflare R2 (S3-compatible) + Delta Lake
- **Transformations** : Python (Bronze → Silver) + dbt (Silver → Gold)
- **Orchestration** : Databricks Workflows + GitHub Actions

## Architecture

Medallion architecture : Bronze → Silver → Gold

- **Bronze** : transit layer — raw API batches, deleted once transformed
- **Silver** : historical source of truth — SCD Type 2, monthly snapshots
- **Gold** : business aggregations — dbt models, sector/region trends

Architecture decisions are documented in [DECISIONS.md](DECISIONS.md).

## Status

🚧 In development