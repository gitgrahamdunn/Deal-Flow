# Deal Flow Ingestion Backend

## Install

```bash
pip install -e deal_flow_ingest
```

## CLI commands

```bash
python -m deal_flow_ingest --help
python -m deal_flow_ingest run --dry-run
python -m deal_flow_ingest run --start YYYY-MM-DD --end YYYY-MM-DD
python -m deal_flow_ingest export-opportunities --min-score 30 --limit 250
python -m deal_flow_ingest apply_saved_sql
python -m deal_flow_ingest reset
python -m deal_flow_ingest reset --include-cache
```

## Refactored architecture overview

`cli.py` now only handles argument parsing, logging, command dispatch, and terminal output. Pipeline orchestration and dataframe preparation live in `services/pipeline.py` via stage helpers (`prepare_source_frames`, `prepare_wells_df`, `prepare_production_dfs`, `build_restart_scores`, `build_operator_metrics`, etc.).

Source dataset routing is composable: connectors now feed `dict[str, list[pd.DataFrame]]` and are merged explicitly by data kind, so future multi-source overlays no longer overwrite by `data_kind`.

## Source discovery flow

- **AER ST37**: local file override → configured artifact URL → discovered URL from configured landing pages; discovered URLs are cached in source metadata.
- **ST37 parsing**: parser mode is logged (`delimited` vs `fixed_width`), malformed-row counts are logged, and fixed-width licensee extraction now requires confidence (otherwise left blank).
- **Petrinex**: discovery now excludes PDFs for tabular loaders; discovery only accepts likely machine-readable artifacts (`csv/xls/xlsx/zip`) and supports alternate landing pages.

## SQLite vs Postgres guidance

- SQLite remains fully supported for local development and tests.
- Loader chunk sizing and update batching are dialect-aware.
- JSON-like fields are handled consistently via centralized DB-layer compatibility serialization for SQLite text-backed JSON columns.

## Local development flow

1. Run migrations implicitly via `python -m deal_flow_ingest run --dry-run`.
2. Validate a live pull with `python -m deal_flow_ingest run`.
3. Export screening output with `export-opportunities`.
4. Use `reset` to safely rebuild local SQLite state.

## Reset command behavior

- `reset` deletes only SQLite DB files by default, then reruns migrations.
- Non-SQLite reset requires explicit `--force`.
- `--include-cache` additionally clears `data/raw` cache artifacts for local dev re-ingestion.

## What is real vs placeholder today

- **Real/live today**: ST37 artifact discovery + parsing + ingestion, dry-run datasets, SQLite/Postgres-compatible loaders, metrics/restart/opportunity outputs.
- **Prepared but not fully wired for production**: Petrinex live datasets and liability source coverage remain partial and resilient-by-default (pipeline continues even when those feeds are unavailable).

## Known limitations

- Facility-to-well production remains equal-allocation estimation (`is_estimated=true`) until higher-fidelity allocation inputs are wired.
- Restart and distress metrics are screening heuristics, not engineering/economic forecasts.
- Open Alberta connector remains a placeholder.
