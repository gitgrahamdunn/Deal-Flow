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
- `dim_facility`
- `bridge_well_facility`
- `fact_facility_production_monthly`
- `fact_well_production_monthly`
- `fact_operator_production_monthly`
- `fact_interest_ownership`
- `fact_operator_liability`
- `fact_operator_metrics`
- `fact_well_status`
- `fact_well_restart_score`
- `ingestion_run`

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

- **AER ST37 List of Wells in Alberta**: pulls ST37 landing metadata and parses configured TXT/CSV artifacts into well identifiers, status, licensee, and legal location fields when available.
- **Petrinex Alberta Public Data**: supports configurable business associate/operator-like extracts, facility master, well-facility bridge, and monthly production feeds with defensive column validation.
- **AER Spatial Data**: fetches configured shapefile ZIP artifacts and extracts them to `data/raw/aer_spatial/`; parsing is intentionally metadata-first with a clean TODO path for geometry enrichment.
- **AER Liability / Estimated Liability / LCA-style reporting**: loads configured downloadable liability artifacts when present, or logs metadata-only status when only a landing page is configured.

> Note: public non-op and royalty ownership is not yet available from one clean public source. A future inference layer will be added to estimate these interests using available proxy datasets.
