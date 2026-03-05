from __future__ import annotations

import argparse
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
    get_engine,
)
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources import load_dataset
from deal_flow_ingest.transform.metrics import compute_operator_metrics, compute_well_restart_scores
from deal_flow_ingest.transform.normalize import month_start, normalize_operator_name, normalize_uwi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="deal_flow_ingest")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--start", type=str)
    run.add_argument("--end", type=str)
    run.add_argument("--refresh", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--config", default="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml")
    return parser.parse_args()


def _parse_date(s: str | None, default: date) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date() if s else default


def _prep_wells(df: pd.DataFrame, op_map: dict[str, int], source: str, as_of: date) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
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


def _estimate_well_production(fac_prod: pd.DataFrame, bridge: pd.DataFrame) -> pd.DataFrame:
    if fac_prod.empty or bridge.empty:
        return pd.DataFrame(columns=["month", "well_id", "oil_bbl", "gas_mcf", "water_bbl", "source", "is_estimated"])
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
    end = _parse_date(args.end, date.today())
    start = _parse_date(args.start, end - timedelta(days=365))
    started_at = datetime.utcnow()
    run_id = new_run_id()

    cfg = load_config(args.config)
    source_entries = [s for s in iter_enabled_sources(cfg) if s.enabled]
    sample_dir = Path("deal_flow_ingest/deal_flow_ingest/sample_data")
    downloader = Downloader(Path("data/raw"))

    datasets: dict[str, pd.DataFrame] = {}
    sources_ok: list[str] = []
    sources_failed: dict[str, str] = {}
    row_counts: dict[str, int | dict] = {"source_rows": {}, "loaded": {}}

    for source in source_entries:
        try:
            df = load_dataset(downloader, sample_dir, source, args.dry_run, args.refresh)
            datasets[source.data_kind] = df
            row_counts["source_rows"][source.key] = int(len(df))
            sources_ok.append(source.key)
        except Exception as exc:
            sources_failed[source.key] = str(exc)

    try:
        upgrade_to_head()
        engine = get_engine(get_database_url())
        with engine.begin() as conn:
            operators_raw = pd.concat(
                [
                    datasets.get("wells", pd.DataFrame()).get("licensee", pd.Series(dtype=str)).rename("name_raw"),
                    datasets.get("facility_master", pd.DataFrame()).get("facility_operator", pd.Series(dtype=str)).rename("name_raw"),
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
            bridge_df = pd.DataFrame()
            if not bridge_raw.empty:
                bridge_df["well_id"] = bridge_raw["well_id"].astype(str).map(normalize_uwi)
                bridge_df["facility_id"] = bridge_raw["facility_id"].astype(str)
                bridge_df["effective_from"] = pd.to_datetime(bridge_raw.get("effective_from")).dt.date
                bridge_df["effective_to"] = pd.to_datetime(bridge_raw.get("effective_to")).dt.date
                bridge_df["source"] = "petrinex_bridge"
            row_counts["loaded"]["bridge_well_facility"] = load_bridge_well_facility(conn, bridge_df)

            fac_prod_raw = datasets.get("facility_production", pd.DataFrame())
            fac_prod = pd.DataFrame()
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
            well_prod = pd.DataFrame()
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

            status_df = wells_df[["well_id", "status"]].copy() if not wells_df.empty else pd.DataFrame(columns=["well_id", "status"])
            if not status_df.empty:
                status_df["status_date"] = end
                status_df["source"] = "aer_st37"
            row_counts["loaded"]["fact_well_status"] = replace_fact_well_status(conn, status_df, "aer_st37")

            fac_to_op = fac_df[["facility_id", "facility_operator_id"]].rename(columns={"facility_operator_id": "operator_id"}) if not fac_df.empty else pd.DataFrame(columns=["facility_id", "operator_id"])
            op_prod = fac_prod.merge(fac_to_op, on="facility_id", how="left") if not fac_prod.empty else pd.DataFrame()
            if not op_prod.empty:
                op_prod = (
                    op_prod.groupby(["month", "operator_id"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
                    .sum()
                    .assign(basis_level="facility", source="petrinex_public")
                )
                op_prod = op_prod[op_prod["operator_id"].notna()]
                op_prod["operator_id"] = op_prod["operator_id"].astype(int)
            row_counts["loaded"]["fact_operator_production_monthly"] = replace_fact_operator_prod(
                conn, op_prod, "petrinex_public", start, end
            )

            liability_raw = datasets.get("liability", pd.DataFrame())
            liability_df = pd.DataFrame()
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

            restart_df = compute_well_restart_scores(wells_df[["well_id", "status"]], well_prod, end)
            row_counts["loaded"]["fact_well_restart_score"] = replace_fact_restart(conn, restart_df, end)

            metrics_df = compute_operator_metrics(op_prod, liability_df, restart_df, wells_df, end)
            row_counts["loaded"]["fact_operator_metrics"] = replace_fact_metrics(conn, metrics_df, end)

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

        print(f"run status: success ({run_id})")
        print(f"sources ok: {sources_ok}")
        print(f"sources failed: {sources_failed}")
        print("top 10 operators by avg_oil_bpd_30d")
        print(top_prod[["name_norm", "avg_oil_bpd_30d"]].to_string(index=False) if not top_prod.empty else "none")
        print("top 10 operators by restart_upside_bpd_est")
        print(top_restart[["name_norm", "restart_upside_bpd_est"]].to_string(index=False) if not top_restart.empty else "none")
        return 0

    except Exception as exc:
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
        return 1


def main() -> None:
    args = parse_args()
    if args.command == "run":
        raise SystemExit(run_ingestion(args))
