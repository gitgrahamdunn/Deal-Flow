# Deal Flow Ingestion MVP

Ingestion-first pipeline for Alberta oil & gas public datasets. No UI/web app in this repo yet.

## Install

From repo root:

```bash
pip install -e deal_flow_ingest
```

## Run

Dry run (offline/sample data + sqlite):

```bash
python -m deal_flow_ingest run --dry-run
```

Live run (attempt public downloads; uses `DATABASE_URL` when set):

```bash
python -m deal_flow_ingest run --start YYYY-MM-DD --end YYYY-MM-DD
```

Optional flags:

- `--refresh` force redownload and bypass cache validation
- `--config PATH` override source config yaml

## Environment Variables

- `DATABASE_URL` (optional): Postgres or any SQLAlchemy-compatible URL. If unset, defaults to `sqlite:///./data/deal_flow.db`.

## Tables

- `dim_operator`: normalized operator dimension.
- `fact_production_monthly`: canonical monthly production facts by operator/source/basis.
- `fact_operator_metrics`: 30d/365d operator oil metrics as of run end date.
- `ingestion_run`: run-level audit log with source status and row counts.

## Data caveats

MVP currently computes rolling 30-day and 365-day values from monthly data. The 30-day value is an approximation because it uses month bucket totals (not daily meter-level data).

## Next enhancements

- Add robust field mapping per source and schema contracts.
- Add daily-level ingestion where public data allows.
- Add stronger data quality checks (null spikes, outlier detection, duplicate diagnostics).
- Add orchestration (scheduled runs) and alerting.
