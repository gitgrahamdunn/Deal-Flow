# Deal Flow Ingestion Backend

## Install

```bash
pip install -e deal_flow_ingest
```

## Run

Dry-run (offline, bundled CSVs, sqlite fallback):

```bash
python -m deal_flow_ingest run --dry-run
```

Live-run (network attempts + resilient partial ingestion):

```bash
python -m deal_flow_ingest run --start YYYY-MM-DD --end YYYY-MM-DD
```

Optional flags:

- `--refresh`
- `--config deal_flow_ingest/deal_flow_ingest/configs/sources.yaml`

## Reset development database

```bash
python -m deal_flow_ingest reset
```

This deletes the local SQLite database and recreates it using migrations.

## Environment

- `DATABASE_URL` optional.
- Default if unset: `sqlite:///./data/deal_flow.db`.

## Migrations

CLI automatically runs Alembic `upgrade head` before loading data.
Manual command:

```bash
cd deal_flow_ingest && alembic upgrade head
```

## Canonical Schema Overview

Implemented tables:

- `dim_operator`
- `dim_well`
- `dim_business_associate`
- `dim_facility`
- `bridge_well_facility`
- `bridge_operator_business_associate`
- `fact_facility_production_monthly`
- `fact_well_production_monthly`
- `fact_operator_production_monthly`
- `fact_interest_ownership`
- `fact_operator_liability`
- `fact_operator_metrics`
- `fact_well_status`
- `fact_well_restart_score`
- `ingestion_run`


`dim_business_associate` stores Petrinex BA identifiers and names separately from `dim_operator`, and `bridge_operator_business_associate` provides a lightweight mapping layer between the two. This is a schema-first preparation step for future Petrinex BA-based ingestion and later WI/non-op/royalty ownership mapping.

## Dry-run samples

`deal_flow_ingest/deal_flow_ingest/sample_data/` contains tiny realistic samples including suspended/shut-in wells and monthly facility production. Dry-run generates operator metrics plus restart candidates without network.


## Opportunity scanner (stripper / restart screen)

Use the export command to build a ranked CSV of marginal/suspended well opportunities from existing ingested tables (`dim_well`, `fact_well_production_monthly`, `fact_well_restart_score`, `fact_operator_metrics`).

```bash
python -m deal_flow_ingest export-opportunities --min-score 30 --limit 250 --output data/exports/well_opportunities.csv
```

The scanner applies transparent rule-based weighting (restart score, production recency, historical oil presence, and operator distress) and emits explainable `screening_notes` rule flags for each well.

Caveat: this is a first-pass screening tool for review prioritization, not an engineering/economic forecast.

## Caveats

- Facility-to-well estimated production uses equal allocation across bridged wells and is labeled `is_estimated=true`.
- Open Alberta source is a connector skeleton placeholder.
- Distress and restart scores are screening formulas, not engineering/economic forecasts.

## Next enhancements

- True WI/royalty ingestion into `fact_interest_ownership` from authoritative ownership sources.
- PostGIS geospatial indexing and map-ready geometry dimensions.
- Better production allocation (test separators, stream factors, runtime weighting).
- Decline-curve and probabilistic restart models.
- Economics layer (price decks, LOE, payout/risked NPV).

## Live Public Sources

The live run mode now includes resilient connectors for Alberta public datasets:

- **AER ST37 List of Wells in Alberta**: live mode resolves in this order: `local_live_file` (if configured and present), then `dataset_url` (if configured), then automatic discovery from the official ST37 landing page, then configured alternate landing pages. Once a live artifact URL is discovered it is cached in source metadata and reused on subsequent non-refresh runs. The loader fetches, extracts (if ZIP), and parses well identifiers, status, licensee, and legal location fields. If discovery fails, the connector logs a warning and returns zero rows.
- **Petrinex Alberta Public Data**: currently remains a live connector skeleton/placeholder until stable artifact URLs are wired.
- **AER Spatial Data**: fetches configured shapefile ZIP artifacts and extracts them to `data/raw/aer_spatial/`; parsing is intentionally metadata-first with a clean TODO path for geometry enrichment.
- **AER Liability / Estimated Liability / LCA-style reporting**: currently remains a live connector skeleton/placeholder until stable artifact URLs are wired.

> Note: public non-op and royalty ownership is not yet available from one clean public source. A future inference layer will be added to estimate these interests using available proxy datasets.
