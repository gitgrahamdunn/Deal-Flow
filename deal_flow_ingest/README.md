# Deal Flow Ingestion Backend

## Install

```bash
pip install -e deal_flow_ingest
```

For the long-term webapp path, run the warehouse on Postgres:

```bash
pip install -e deal_flow_ingest
docker-compose -f docker-compose.postgres.yml up -d
export DATABASE_URL=postgresql+psycopg://dealflow:dealflow@localhost:5432/dealflow
python -m deal_flow_ingest run --dry-run
python -m deal_flow_ingest apply_saved_sql
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
dealflow funnel --status
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
dealflow app --host 0.0.0.0 --port 8443
dealflow funnel --port 8443 --https-port 8443 --bg --yes
dealflow funnel --status
```

This launches a local Streamlit app with:

- seller theses
- low-production target screening
- package candidates
- operator drilldown
- thesis detail view
- in-app refresh controls

Remote access with Tailscale Funnel:

```bash
dealflow app
dealflow funnel --bg --yes
```

Then open the Funnel URL reported by `dealflow funnel --status`.

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

For frontend work, use the backend query contract in `services/registry_queries.py`. It provides filtered well/facility/pipeline map layers, candidate overlays, combined map frames, and filter-option lookups without exposing the frontend directly to raw SQL view wiring.

## Source discovery flow

- **AER ST37**: local file override → configured artifact URL → discovered URL from configured landing pages; discovered URLs are cached in source metadata.
- **AER general well / ST102 facility list**: landing-page discovery prefers tabular artifacts (`xlsx/csv/zip`) and enriches canonical well/facility registry rows with licence, naming, and lifecycle fields.
- **ST37 parsing**: parser mode is logged (`delimited` vs `fixed_width`), malformed-row counts are logged, and fixed-width licensee extraction now requires confidence (otherwise left blank).
- **Petrinex**: discovery now excludes PDFs for tabular loaders; discovery only accepts likely machine-readable artifacts (`csv/xls/xlsx/zip`) and supports alternate landing pages.
- **AMI Crown tenure**: current support is local-file-backed only. The repo can ingest user-supplied AMI CSV extracts for Crown dispositions, clients, land keys, and participants, but it does not claim anonymous live download support.

## SQLite vs Postgres guidance

- Postgres is the recommended target for the self-hosted webapp and any concurrent ingestion/query workload.
- SQLite remains supported for local development and tests.
- Loader chunk sizing and update batching are dialect-aware.
- JSON-like fields are handled consistently via centralized DB-layer compatibility serialization for SQLite text-backed JSON columns.
- `reset --force` now clears and recreates the `public` schema on Postgres before rerunning migrations.
- Alembic enables `postgis` automatically when running against PostgreSQL.

## Postgres bootstrap

1. Start the bundled database:
```bash
docker-compose -f docker-compose.postgres.yml up -d
```
2. Point the app at it:
```bash
export DATABASE_URL=postgresql+psycopg://dealflow:dealflow@localhost:5432/dealflow
```
3. Run migrations and a smoke ingest:
```bash
python -m deal_flow_ingest run --dry-run
python -m deal_flow_ingest apply_saved_sql
```
4. Rebuild the real warehouse in Postgres:
```bash
python -m deal_flow_ingest run
python -m deal_flow_ingest apply_saved_sql
```

Fresh reingestion is the recommended cutover path from SQLite. The warehouse is reproducible, so a dump/restore step is not required.

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
- `asset_registry_wells`: canonical well registry combining ST37, AER general well attributes, production rollup, and restart metadata.
- `asset_registry_facilities`: canonical facility registry combining Petrinex infrastructure, ST102 metadata, linked well counts, and production rollup.
- `asset_registry_pipelines`: canonical pipeline-segment registry combining AER spatial pipeline attributes with mapped operators.
- `asset_registry_crown_tenure`: asset-to-Crown-disposition matches for wells and facilities using exact ATS legal-location keys.

## Reset command behavior

- `reset` deletes only SQLite DB files by default, then reruns migrations.
- Non-SQLite reset requires explicit `--force`.
- `--include-cache` additionally clears `data/raw` cache artifacts for local dev re-ingestion.

## What is real vs placeholder today

- **Real/live today**: ST37 artifact discovery + parsing + ingestion, AER general well and ST102 facility-list enrichment, AER pipeline spatial ingestion, Petrinex business associate/facility/bridge/monthly production public API ingestion, SQLite/Postgres-compatible loaders, registry views, and metrics/restart/opportunity outputs.
- **Local-file working today**: AMI Crown disposition tenure ingestion for user-supplied extract files, including client/holder and ATS land-key joins into a curated tenure view.
- **Prepared but not fully wired for production**: liability and spatial source coverage remain disabled by default until direct public artifacts are wired.

## Known limitations

- Facility-to-well production remains equal-allocation estimation (`is_estimated=true`) until higher-fidelity allocation inputs are wired.
- Restart and distress metrics are screening heuristics, not engineering/economic forecasts.
- ST102 and general-well-data joins currently enrich canonical wells/facilities by shared identifiers only; broader licence-to-asset reconciliation is still a follow-on step.
- AMI tenure support is Crown-only context and is not working-interest ownership. Freehold mineral title and WI remain out of scope unless private source documents are supplied.
- AER liability and broader non-pipeline spatial connectors are not yet enabled for default live runs because public artifact wiring is incomplete.
- Open Alberta connector remains a placeholder.
