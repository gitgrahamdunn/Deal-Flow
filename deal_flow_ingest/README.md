# Deal Flow Ingestion Backend

## Install

```bash
pip install -e deal_flow_ingest
```

## CLI commands

```bash
python -m deal_flow_ingest --help
python -m deal_flow_ingest check-sources --dry-run
python -m deal_flow_ingest run --dry-run
python -m deal_flow_ingest run --start YYYY-MM-DD --end YYYY-MM-DD
python -m deal_flow_ingest export-opportunities --min-score 30 --limit 250
python -m deal_flow_ingest export-package-candidates --min-score 40 --limit 100
python -m deal_flow_ingest export-seller-theses --min-score 40 --limit 100
python -m deal_flow_ingest apply_saved_sql
python -m deal_flow_ingest reset
python -m deal_flow_ingest reset --include-cache
```

## Operator shortcut commands

After installing the package in your environment:

```bash
pip install -e deal_flow_ingest
```

you can use the higher-level operator CLI:

```bash
dealflow sources
dealflow doctor
dealflow ingest
dealflow refresh
dealflow ingest --start 2025-12-01 --end 2026-01-31
dealflow build-sql
dealflow ui
dealflow app
dealflow gui
dealflow top50
dealflow top100
dealflow lowprod --limit 100 --max-prod 15
dealflow theses --limit 100 --min-score 20
dealflow theses --sort-by avg_oil_bpd_30d --ascending --max-prod 15
dealflow packages --limit 100 --min-score 20
dealflow reset
dealflow reset --include-cache
```

Recommended day-to-day usage:

```bash
dealflow sources
dealflow refresh
dealflow app
dealflow top50
dealflow lowprod --limit 100 --max-prod 15
dealflow packages --limit 100
```

GUI usage:

```bash
dealflow app
dealflow app --port 8502
```

This launches a local Streamlit app with:

- seller theses
- low-production target screening
- package candidates
- operator drilldown
- in-app refresh controls

## Recommended live workflow

```bash
python -m deal_flow_ingest check-sources
python -m deal_flow_ingest run
python -m deal_flow_ingest apply_saved_sql
python -m deal_flow_ingest export-package-candidates --min-score 40 --limit 100
python -m deal_flow_ingest export-seller-theses --min-score 40 --limit 100
```

Outputs are written by default to:

- `data/exports/well_opportunities.csv`
- `data/exports/package_candidates.csv`
- `data/exports/seller_theses.csv`

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

1. Check source availability via `python -m deal_flow_ingest check-sources --dry-run` or `python -m deal_flow_ingest check-sources`.
2. Run migrations implicitly via `python -m deal_flow_ingest run --dry-run`.
3. Validate a live pull with `python -m deal_flow_ingest run`.
4. Apply curated SQL views with `python -m deal_flow_ingest apply_saved_sql`.
5. Export screening output with `export-opportunities`.
6. Export package-level asset groups with `export-package-candidates`.
7. Export operator-level acquisition targets with `export-seller-theses`.
8. Use `reset` to safely rebuild local SQLite state.

Current curated SQL views:
- `deal_flow_targets`: operator-level seller screening.
- `restart_well_candidates`: well-level restart screening with area keys.
- `asset_clusters`: grouped operator-area packages for concentration analysis.
- `package_candidates`: facility-linked operator-area packages with infrastructure context.
- `operator_area_footprints`: operator-level concentration summaries showing core vs scattered footprints.
- `deal_flow_opportunities`: operator opportunity ranking built on `deal_flow_targets`.
- `seller_theses`: analyst-facing combined seller ranking built from targets, opportunities, packages, and footprints.

## Reset command behavior

- `reset` deletes only SQLite DB files by default, then reruns migrations.
- Non-SQLite reset requires explicit `--force`.
- `--include-cache` additionally clears `data/raw` cache artifacts for local dev re-ingestion.

## What is real vs placeholder today

- **Real/live today**: ST37 artifact discovery + parsing + ingestion, Petrinex business associate/facility/bridge/monthly production public API ingestion, SQLite/Postgres-compatible loaders, metrics/restart/opportunity outputs.
- **Prepared but not fully wired for production**: liability and spatial source coverage remain disabled by default until direct public artifacts are wired.

## Known limitations

- Facility-to-well production remains equal-allocation estimation (`is_estimated=true`) until higher-fidelity allocation inputs are wired.
- Restart and distress metrics are screening heuristics, not engineering/economic forecasts.
- AER liability and spatial connectors are not yet enabled for default live runs because public artifact wiring is incomplete.
- Open Alberta connector remains a placeholder.
