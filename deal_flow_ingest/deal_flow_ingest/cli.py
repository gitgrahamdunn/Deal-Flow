from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from deal_flow_ingest.config import get_database_url, iter_enabled_sources, load_config
from deal_flow_ingest.db.load import (
    new_run_id,
    record_ingestion_run,
    replace_fact_by_month_range,
    replace_fact_liability,
    replace_fact_metrics,
    replace_fact_operator_prod,
    replace_fact_restart,
    replace_fact_well_status,
    upsert_dim_facility,
    upsert_dim_operator,
    upsert_dim_well,
    load_bridge_well_facility,
)
from deal_flow_ingest.db.migrate import upgrade_to_head
from deal_flow_ingest.db.schema import (
    DimOperator,
    DimWell,
    FactFacilityProductionMonthly,
    FactOperatorLiability,
    FactOperatorMetrics,
    FactOperatorProductionMonthly,
    FactWellProductionMonthly,
    FactWellRestartScore,
    get_engine,
)
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources import load_dataset
from deal_flow_ingest.transform.metrics import compute_operator_metrics, compute_well_restart_scores
from deal_flow_ingest.transform.normalize import month_start, normalize_operator_name, normalize_uwi
from deal_flow_ingest.transform.opportunities import compute_well_opportunities


WELL_DIM_COLUMNS = [
    "well_id",
    "uwi_raw",
    "status",
    "licensee_operator_id",
    "lsd",
    "section",
    "township",
    "range",
    "meridian",
    "lat",
    "lon",
    "first_seen",
    "last_seen",
    "source",
]
WELL_STATUS_COLUMNS = ["well_id", "status"]
FACILITY_PROD_COLUMNS = ["month", "facility_id", "oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl", "source"]
BRIDGE_COLUMNS = ["well_id", "facility_id", "effective_from", "effective_to", "source"]
OPERATOR_PROD_COLUMNS = ["month", "operator_id", "oil_bbl", "gas_mcf", "water_bbl", "basis_level", "source"]
LIABILITY_COLUMNS = [
    "as_of_date",
    "operator_id",
    "inactive_wells",
    "active_wells",
    "deemed_assets",
    "deemed_liabilities",
    "ratio",
    "source",
]


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="deal_flow_ingest", description="Run Deal Flow ingestion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--start", type=str)
    run.add_argument("--end", type=str)
    run.add_argument("--refresh", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--config", default="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml")
    export = sub.add_parser("export-opportunities")
    export.add_argument("--min-score", type=float, default=30.0)
    export.add_argument("--limit", type=int, default=250)
    export.add_argument("--output", type=str, default="data/exports/well_opportunities.csv")
    return parser.parse_args()




def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _ensure_database_dir() -> None:
    db_url = get_database_url()
    if db_url.startswith("sqlite:///"):
        db_path = Path(db_url.removeprefix("sqlite:///"))
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)

def _parse_date(s: str | None, default: date) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date() if s else default


def _prep_wells(df: pd.DataFrame, op_map: dict[str, int], source: str, as_of: date) -> pd.DataFrame:
    if df.empty:
        return _empty_df(WELL_DIM_COLUMNS)
    out = pd.DataFrame()
    out["uwi_raw"] = df["uwi"].astype(str)
    out["well_id"] = out["uwi_raw"].map(normalize_uwi)
    out["status"] = df.get("status", "UNKNOWN").astype(str)
    out["licensee_operator_id"] = df.get("licensee", "").astype(str).map(normalize_operator_name).map(op_map)
    out["lsd"] = df.get("lsd")
    out["section"] = pd.to_numeric(df.get("section"), errors="coerce")
    out["township"] = pd.to_numeric(df.get("township"), errors="coerce")
    out["range"] = pd.to_numeric(df.get("range"), errors="coerce")
    out["meridian"] = pd.to_numeric(df.get("meridian"), errors="coerce")
    out["lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
    out["lon"] = pd.to_numeric(df.get("lon"), errors="coerce")
    out["first_seen"] = as_of
    out["last_seen"] = as_of
    out["source"] = source
    return out


def _dedupe_wells_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "well_id" not in df.columns:
        return df

    working = df.copy()
    working["_row_order"] = range(len(working))

    status_non_empty = (
        working["status"].fillna("").astype(str).str.strip().ne("") if "status" in working.columns else pd.Series(False, index=working.index)
    )
    licensee_present = (
        working["licensee_operator_id"].notna() if "licensee_operator_id" in working.columns else pd.Series(False, index=working.index)
    )

    score = status_non_empty.astype(int) + licensee_present.astype(int)
    for column in ["lsd", "section", "township", "range", "meridian"]:
        score += (working[column].notna().astype(int) if column in working.columns else 0)

    has_lat_lon = (
        working[[c for c in ["lat", "lon"] if c in working.columns]].notna().any(axis=1).astype(int)
        if any(c in working.columns for c in ["lat", "lon"])
        else 0
    )
    score += has_lat_lon

    working["_completeness_score"] = score

    return (
        working.sort_values(["_completeness_score", "_row_order"], ascending=[False, True])
        .drop_duplicates(subset=["well_id"], keep="first")
        .sort_values("_row_order")
        .drop(columns=["_completeness_score", "_row_order"])
    )


def _estimate_well_production(fac_prod: pd.DataFrame, bridge: pd.DataFrame) -> pd.DataFrame:
    if fac_prod.empty or bridge.empty:
        return _empty_df(["month", "well_id", "oil_bbl", "gas_mcf", "water_bbl", "source", "is_estimated"])
    merged = fac_prod.merge(bridge[["facility_id", "well_id"]], on="facility_id", how="inner")
    counts = merged.groupby(["month", "facility_id"])["well_id"].transform("count").replace(0, 1)
    merged["oil_bbl"] = merged["oil_bbl"] / counts
    merged["gas_mcf"] = merged["gas_mcf"] / counts
    merged["water_bbl"] = merged["water_bbl"] / counts
    out = (
        merged.groupby(["month", "well_id", "source"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
        .sum()
        .assign(is_estimated=True)
    )
    return out


def run_ingestion(args: argparse.Namespace) -> int:
    LOGGER.info("Starting ingestion run")
    _ensure_database_dir()
    end = _parse_date(args.end, date.today())
    start = _parse_date(args.start, end - timedelta(days=365))
    started_at = datetime.utcnow()
    run_id = new_run_id()

    LOGGER.info("Loading configuration from %s", args.config)
    cfg = load_config(args.config)
    source_entries = [s for s in iter_enabled_sources(cfg) if s.enabled]
    sample_dir = Path(__file__).resolve().parent / "sample_data"
    downloader = Downloader(Path("data/raw"))

    datasets: dict[str, pd.DataFrame] = {}
    sources_ok: list[str] = []
    sources_failed: dict[str, str] = {}
    row_counts: dict[str, int | dict] = {"source_rows": {}, "loaded": {}}
    source_summaries: list[dict[str, object]] = []

    LOGGER.info("Enabled sources: %s", [s.key for s in source_entries])

    for source in source_entries:
        try:
            df = load_dataset(downloader, sample_dir, source, args.dry_run, args.refresh)
            datasets[source.data_kind] = df
            row_counts["source_rows"][source.key] = int(len(df))
            sources_ok.append(source.key)
            last_result = downloader.last_result
            source_summaries.append({
                "source": source.key,
                "artifact_downloaded": bool(last_result.downloaded) if last_result else False,
                "rows_parsed": int(len(df)),
                "load_status": "success",
            })
            LOGGER.info("Loaded source %s (%s rows)", source.key, len(df))
        except Exception as exc:
            sources_failed[source.key] = str(exc)
            source_summaries.append({
                "source": source.key,
                "artifact_downloaded": False,
                "rows_parsed": 0,
                "load_status": f"failed: {exc}",
            })
            LOGGER.exception("Failed to load source %s", source.key)

    try:
        upgrade_to_head()
        database_url = get_database_url()
        LOGGER.info("Using database %s", database_url)
        engine = get_engine(database_url)
        with engine.begin() as conn:
            operators_raw = pd.concat(
                [
                    datasets.get("wells", pd.DataFrame()).get("licensee", pd.Series(dtype=str)).rename("name_raw"),
                    datasets.get("facility_master", pd.DataFrame()).get("facility_operator", pd.Series(dtype=str)).rename("name_raw"),
                    datasets.get("operators", pd.DataFrame()).get("name_raw", pd.Series(dtype=str)).rename("name_raw"),
                    datasets.get("liability", pd.DataFrame()).get("operator", pd.Series(dtype=str)).rename("name_raw"),
                ],
                ignore_index=True,
            )
            operator_map = upsert_dim_operator(
                conn,
                pd.DataFrame({"name_raw": operators_raw.fillna("").astype(str)}),
                source="multi_source",
            )

            wells_df = _prep_wells(datasets.get("wells", pd.DataFrame()), operator_map, "aer_st37", end)
            original_wells_count = len(wells_df)
            duplicate_counts = pd.Series(dtype=int)
            if not wells_df.empty and "well_id" in wells_df.columns:
                duplicate_counts = wells_df["well_id"].value_counts()
                duplicate_counts = duplicate_counts[duplicate_counts > 1]
                if not duplicate_counts.empty:
                    top_duplicates = duplicate_counts.head(20)
                    LOGGER.info("Top duplicate ST37 well_id counts (up to 20):")
                    for well_id, count in top_duplicates.items():
                        LOGGER.info("  %s: %s", well_id, count)

            wells_df = _dedupe_wells_df(wells_df)
            deduped_wells_count = len(wells_df)
            duplicate_removed_count = original_wells_count - deduped_wells_count
            LOGGER.info(
                "ST37 wells rows before dedupe=%s after dedupe=%s duplicates_removed=%s",
                original_wells_count,
                deduped_wells_count,
                duplicate_removed_count,
            )
            row_counts["loaded"]["dim_well"] = upsert_dim_well(conn, wells_df)

            fac_raw = datasets.get("facility_master", pd.DataFrame())
            fac_df = pd.DataFrame()
            if not fac_raw.empty:
                fac_df["facility_id"] = fac_raw["facility_id"].astype(str)
                fac_df["facility_type"] = fac_raw.get("facility_type")
                fac_df["facility_operator_id"] = fac_raw.get("facility_operator", "").astype(str).map(normalize_operator_name).map(operator_map)
                fac_df["lsd"] = fac_raw.get("lsd")
                fac_df["section"] = pd.to_numeric(fac_raw.get("section"), errors="coerce")
                fac_df["township"] = pd.to_numeric(fac_raw.get("township"), errors="coerce")
                fac_df["range"] = pd.to_numeric(fac_raw.get("range"), errors="coerce")
                fac_df["meridian"] = pd.to_numeric(fac_raw.get("meridian"), errors="coerce")
                fac_df["lat"] = pd.to_numeric(fac_raw.get("lat"), errors="coerce")
                fac_df["lon"] = pd.to_numeric(fac_raw.get("lon"), errors="coerce")
                fac_df["source"] = "petrinex_facility"
            row_counts["loaded"]["dim_facility"] = upsert_dim_facility(conn, fac_df)

            bridge_raw = datasets.get("well_facility_bridge", pd.DataFrame())
            bridge_df = _empty_df(BRIDGE_COLUMNS)
            if not bridge_raw.empty:
                bridge_df["well_id"] = bridge_raw["well_id"].astype(str).map(normalize_uwi)
                bridge_df["facility_id"] = bridge_raw["facility_id"].astype(str)
                bridge_df["effective_from"] = pd.to_datetime(bridge_raw.get("effective_from"), errors="coerce").dt.date
                bridge_df["effective_to"] = pd.to_datetime(bridge_raw.get("effective_to"), errors="coerce").dt.date
                bridge_df["effective_from"] = bridge_df["effective_from"].map(lambda v: None if pd.isna(v) else v)
                bridge_df["effective_to"] = bridge_df["effective_to"].map(lambda v: None if pd.isna(v) else v)
                bridge_df["source"] = "petrinex_bridge"
            row_counts["loaded"]["bridge_well_facility"] = load_bridge_well_facility(conn, bridge_df)

            fac_prod_raw = datasets.get("facility_production", pd.DataFrame())
            fac_prod = _empty_df(FACILITY_PROD_COLUMNS)
            if not fac_prod_raw.empty:
                fac_prod["month"] = month_start(fac_prod_raw["month"]).dt.date
                fac_prod["facility_id"] = fac_prod_raw["facility_id"].astype(str)
                fac_prod["oil_bbl"] = pd.to_numeric(fac_prod_raw.get("oil_bbl"), errors="coerce").fillna(0.0)
                fac_prod["gas_mcf"] = pd.to_numeric(fac_prod_raw.get("gas_mcf"), errors="coerce").fillna(0.0)
                fac_prod["water_bbl"] = pd.to_numeric(fac_prod_raw.get("water_bbl"), errors="coerce").fillna(0.0)
                fac_prod["condensate_bbl"] = pd.to_numeric(fac_prod_raw.get("condensate_bbl"), errors="coerce").fillna(0.0)
                fac_prod["source"] = "petrinex_public"
            row_counts["loaded"]["fact_facility_production_monthly"] = replace_fact_by_month_range(
                conn, FactFacilityProductionMonthly, fac_prod, "petrinex_public", start, end
            )

            well_prod_raw = datasets.get("well_production", pd.DataFrame())
            well_prod = _empty_df(["month", "well_id", "oil_bbl", "gas_mcf", "water_bbl", "source", "is_estimated"])
            if not well_prod_raw.empty:
                well_prod["month"] = month_start(well_prod_raw["month"]).dt.date
                well_prod["well_id"] = well_prod_raw["well_id"].astype(str).map(normalize_uwi)
                well_prod["oil_bbl"] = pd.to_numeric(well_prod_raw.get("oil_bbl"), errors="coerce").fillna(0.0)
                well_prod["gas_mcf"] = pd.to_numeric(well_prod_raw.get("gas_mcf"), errors="coerce").fillna(0.0)
                well_prod["water_bbl"] = pd.to_numeric(well_prod_raw.get("water_bbl"), errors="coerce").fillna(0.0)
                well_prod["source"] = "well_metered"
                well_prod["is_estimated"] = False
            else:
                well_prod = _estimate_well_production(fac_prod.assign(month=pd.to_datetime(fac_prod["month"])), bridge_df)
                if not well_prod.empty:
                    well_prod["month"] = pd.to_datetime(well_prod["month"]).dt.date
            row_counts["loaded"]["fact_well_production_monthly"] = replace_fact_by_month_range(
                conn, FactWellProductionMonthly, well_prod, well_prod["source"].iloc[0] if not well_prod.empty else "well_metered", start, end
            )

            status_df = wells_df[WELL_STATUS_COLUMNS].copy() if not wells_df.empty else _empty_df(WELL_STATUS_COLUMNS)
            if not status_df.empty:
                status_df["status_date"] = end
                status_df["source"] = "aer_st37"
            row_counts["loaded"]["fact_well_status"] = replace_fact_well_status(conn, status_df, "aer_st37")

            fac_to_op = (
                fac_df[["facility_id", "facility_operator_id"]].rename(columns={"facility_operator_id": "operator_id"})
                if not fac_df.empty
                else _empty_df(["facility_id", "operator_id"])
            )
            op_prod = fac_prod.merge(fac_to_op, on="facility_id", how="left") if not fac_prod.empty else _empty_df(OPERATOR_PROD_COLUMNS)
            if not op_prod.empty:
                op_prod = (
                    op_prod.groupby(["month", "operator_id"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
                    .sum()
                    .assign(basis_level="facility", source="petrinex_public")
                )
                op_prod = op_prod[op_prod["operator_id"].notna()]
                op_prod["operator_id"] = op_prod["operator_id"].astype(int)
            else:
                op_prod = _empty_df(OPERATOR_PROD_COLUMNS)
            row_counts["loaded"]["fact_operator_production_monthly"] = replace_fact_operator_prod(
                conn, op_prod, "petrinex_public", start, end
            )

            liability_raw = datasets.get("liability", pd.DataFrame())
            liability_df = _empty_df(LIABILITY_COLUMNS)
            if not liability_raw.empty:
                liability_df["as_of_date"] = pd.to_datetime(liability_raw["as_of_date"]).dt.date
                liability_df["operator_id"] = liability_raw["operator"].astype(str).map(normalize_operator_name).map(operator_map)
                liability_df["inactive_wells"] = pd.to_numeric(liability_raw.get("inactive_wells"), errors="coerce")
                liability_df["active_wells"] = pd.to_numeric(liability_raw.get("active_wells"), errors="coerce")
                liability_df["deemed_assets"] = pd.to_numeric(liability_raw.get("deemed_assets"), errors="coerce")
                liability_df["deemed_liabilities"] = pd.to_numeric(liability_raw.get("deemed_liabilities"), errors="coerce")
                liability_df["ratio"] = pd.to_numeric(liability_raw.get("ratio"), errors="coerce")
                liability_df["source"] = "aer_llr"
                liability_df = liability_df[liability_df["operator_id"].notna()]
                liability_df["operator_id"] = liability_df["operator_id"].astype(int)
            row_counts["loaded"]["fact_operator_liability"] = replace_fact_liability(conn, liability_df, "aer_llr")

            restart_input = wells_df[WELL_STATUS_COLUMNS].copy() if not wells_df.empty else _empty_df(WELL_STATUS_COLUMNS)
            restart_df = compute_well_restart_scores(restart_input, well_prod, end)
            if not restart_df.empty and "flags" in restart_df:
                restart_df["flags"] = restart_df["flags"].map(
                    lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
                )
            row_counts["loaded"]["fact_well_restart_score"] = replace_fact_restart(conn, restart_df, end)

            metrics_df = compute_operator_metrics(op_prod, liability_df, restart_df, wells_df, end)
            if not metrics_df.empty and "source_notes" in metrics_df:
                metrics_df["source_notes"] = metrics_df["source_notes"].map(
                    lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
                )
            row_counts["loaded"]["fact_operator_metrics"] = replace_fact_metrics(conn, metrics_df, end)

            if sum(int(v) for v in row_counts.get("source_rows", {}).values()) == 0:
                print("No live source rows were parsed; pipeline completed with empty data.")

            record_ingestion_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.utcnow(),
                status="success",
                sources_ok=sources_ok,
                sources_failed=sources_failed,
                row_counts=row_counts,
                notes="Resilient ingestion run completed",
            )

            top_prod = pd.DataFrame()
            top_restart = pd.DataFrame()
            if not metrics_df.empty:
                ops = pd.read_sql(select(DimOperator.operator_id, DimOperator.name_norm), conn)
                top_prod = metrics_df.merge(ops, on="operator_id", how="left").sort_values("avg_oil_bpd_30d", ascending=False).head(10)
                top_restart = metrics_df.merge(ops, on="operator_id", how="left").sort_values(
                    "restart_upside_bpd_est", ascending=False
                ).head(10)

        LOGGER.info("Run succeeded with run_id=%s", run_id)
        print(f"run status: success ({run_id})")
        print(f"sources ok: {sources_ok}")
        print(f"sources failed: {sources_failed}")
        print("top 10 operators by avg_oil_bpd_30d")
        print(top_prod[["name_norm", "avg_oil_bpd_30d"]].to_string(index=False) if not top_prod.empty else "none")
        print("top 10 operators by restart_upside_bpd_est")
        print(top_restart[["name_norm", "restart_upside_bpd_est"]].to_string(index=False) if not top_restart.empty else "none")
        print("source summary")
        for item in source_summaries:
            print(
                f"- {item['source']}: artifact_downloaded={item['artifact_downloaded']} rows={item['rows_parsed']} load={item['load_status']}"
            )
        return 0

    except Exception as exc:
        LOGGER.exception("Pipeline run failed")
        print(f"Pipeline run failed: {exc}")
        engine = get_engine(get_database_url())
        with engine.begin() as conn:
            record_ingestion_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.utcnow(),
                status="failed",
                sources_ok=sources_ok,
                sources_failed={**sources_failed, "pipeline": str(exc)},
                row_counts=row_counts,
                notes="Pipeline failed",
            )
        print(f"run status: failed ({run_id})")
        print("source summary")
        for item in source_summaries:
            print(
                f"- {item['source']}: artifact_downloaded={item['artifact_downloaded']} rows={item['rows_parsed']} load={item['load_status']}"
            )
        return 1


def export_opportunities(args: argparse.Namespace) -> int:
    _ensure_database_dir()
    engine = get_engine(get_database_url())
    with engine.connect() as conn:
        wells_df = pd.read_sql(select(DimWell), conn)
        well_prod_df = pd.read_sql(select(FactWellProductionMonthly), conn)
        restart_df = pd.read_sql(select(FactWellRestartScore), conn)
        operator_metrics_df = pd.read_sql(select(FactOperatorMetrics), conn)

    if not restart_df.empty and "as_of_date" in restart_df:
        as_of_date = pd.to_datetime(restart_df["as_of_date"], errors="coerce").max().date()
    elif not operator_metrics_df.empty and "as_of_date" in operator_metrics_df:
        as_of_date = pd.to_datetime(operator_metrics_df["as_of_date"], errors="coerce").max().date()
    else:
        as_of_date = date.today()

    opportunities = compute_well_opportunities(
        wells_df=wells_df,
        well_prod_df=well_prod_df,
        restart_df=restart_df,
        operator_metrics_df=operator_metrics_df,
        as_of_date=as_of_date,
    )
    opportunities = opportunities[opportunities["stripper_score"] >= args.min_score]
    opportunities = opportunities.sort_values("stripper_score", ascending=False).head(args.limit)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    opportunities.to_csv(output_path, index=False)

    cols = [
        "well_id",
        "operator_id",
        "current_status",
        "stripper_score",
        "opportunity_tier",
        "restart_score",
        "months_since_last_production",
    ]
    print(f"exported {len(opportunities)} opportunities to {output_path}")
    print("top 20 opportunities")
    print(opportunities[cols].head(20).to_string(index=False) if not opportunities.empty else "none")
    return 0


LOGGER = logging.getLogger(__name__)


def main() -> None:
    _configure_logging()
    args = parse_args()
    if args.command == "run":
        raise SystemExit(run_ingestion(args))
    if args.command == "export-opportunities":
        raise SystemExit(export_opportunities(args))
