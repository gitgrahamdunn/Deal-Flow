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
