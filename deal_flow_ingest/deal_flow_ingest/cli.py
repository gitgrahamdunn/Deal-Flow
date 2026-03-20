from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import select, text

from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import DimWell, FactOperatorMetrics, FactWellProductionMonthly, FactWellRestartScore, get_engine
from deal_flow_ingest.services.pipeline import (
    ensure_database_dir,
    reset_database as reset_database_service,
    run_ingestion_pipeline,
    run_source_diagnostics,
)
from deal_flow_ingest.transform.opportunities import compute_well_opportunities


LOGGER = logging.getLogger(__name__)


def get_seller_theses_frame(args: argparse.Namespace) -> pd.DataFrame:
    ensure_database_dir()
    engine = get_engine(get_database_url())
    with engine.connect() as conn:
        theses = pd.read_sql(text("select * from seller_theses"), conn)

    if theses.empty:
        return theses

    theses["thesis_score"] = pd.to_numeric(theses["thesis_score"], errors="coerce").fillna(0.0)
    theses["avg_oil_bpd_30d"] = pd.to_numeric(theses.get("avg_oil_bpd_30d", 0), errors="coerce").fillna(0.0)
    theses["avg_oil_bpd_365d"] = pd.to_numeric(theses.get("avg_oil_bpd_365d", 0), errors="coerce").fillna(0.0)
    theses["seller_score"] = pd.to_numeric(theses.get("seller_score", 0), errors="coerce").fillna(0.0)
    theses["opportunity_score"] = pd.to_numeric(theses.get("opportunity_score", 0), errors="coerce").fillna(0.0)
    sort_by = getattr(args, "sort_by", "thesis_score")
    ascending = bool(getattr(args, "ascending", False))
    max_avg_oil_bpd_30d = getattr(args, "max_avg_oil_bpd_30d", None)

    theses = theses[theses["thesis_score"] >= args.min_score]
    if max_avg_oil_bpd_30d is not None:
        theses = theses[theses["avg_oil_bpd_30d"] <= float(max_avg_oil_bpd_30d)]

    sort_columns = [sort_by]
    ascending_values = [ascending]
    if sort_by != "thesis_score":
        sort_columns.append("thesis_score")
        ascending_values.append(False)
    if sort_by != "seller_score":
        sort_columns.append("seller_score")
        ascending_values.append(False)
    return theses.sort_values(sort_columns, ascending=ascending_values).head(args.limit)


def get_package_candidates_frame(args: argparse.Namespace) -> pd.DataFrame:
    ensure_database_dir()
    engine = get_engine(get_database_url())
    with engine.connect() as conn:
        packages = pd.read_sql(text("select * from package_candidates"), conn)

    if packages.empty:
        return packages

    packages["package_score"] = pd.to_numeric(packages["package_score"], errors="coerce").fillna(0.0)
    packages = packages[packages["package_score"] >= args.min_score]
    return packages.sort_values(
        ["package_score", "estimated_restart_upside_bpd", "high_priority_well_count"],
        ascending=False,
    ).head(args.limit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="deal_flow_ingest", description="Run Deal Flow ingestion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--start", type=str)
    run.add_argument("--end", type=str)
    run.add_argument("--refresh", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--config", default="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml")
    check = sub.add_parser("check-sources")
    check.add_argument("--refresh", action="store_true")
    check.add_argument("--dry-run", action="store_true")
    check.add_argument("--config", default="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml")
    export = sub.add_parser("export-opportunities")
    export.add_argument("--min-score", type=float, default=30.0)
    export.add_argument("--limit", type=int, default=250)
    export.add_argument("--output", type=str, default="data/exports/well_opportunities.csv")
    seller = sub.add_parser("export-seller-theses")
    seller.add_argument("--min-score", type=float, default=0.0)
    seller.add_argument("--limit", type=int, default=250)
    seller.add_argument("--output", type=str, default="data/exports/seller_theses.csv")
    packages = sub.add_parser("export-package-candidates")
    packages.add_argument("--min-score", type=float, default=0.0)
    packages.add_argument("--limit", type=int, default=250)
    packages.add_argument("--output", type=str, default="data/exports/package_candidates.csv")
    sub.add_parser("apply_saved_sql")
    reset = sub.add_parser("reset")
    reset.add_argument("--force", action="store_true")
    reset.add_argument("--include-cache", action="store_true")
    return parser.parse_args()


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def reset_database(args: argparse.Namespace) -> int:
    code, message = reset_database_service(force=bool(args.force), include_cache=bool(getattr(args, "include_cache", False)))
    print(message)
    return code


def run_ingestion(args: argparse.Namespace) -> int:
    status, result = run_ingestion_pipeline(args)
    if result is None:
        return 1

    if status == 0:
        print(f"run status: success ({result.run_id})")
    else:
        print(f"run status: failed ({result.run_id})")
    print(f"sources ok: {result.sources_ok}")
    print(f"sources failed: {result.sources_failed}")
    if sum(int(item.get("rows_parsed", 0)) for item in result.source_summaries) == 0:
        print("No live source rows were parsed; pipeline completed with empty data.")
    print("top 10 operators by avg_oil_bpd_30d")
    print(result.top_prod[["name_norm", "avg_oil_bpd_30d"]].to_string(index=False) if not result.top_prod.empty else "none")
    print("top 10 operators by restart_upside_bpd_est")
    print(
        result.top_restart[["name_norm", "restart_upside_bpd_est"]].to_string(index=False)
        if not result.top_restart.empty
        else "none"
    )
    print("source summary")
    for item in result.source_summaries:
        print(
            f"- {item['source']}: artifact_downloaded={item['artifact_downloaded']} rows={item['rows_parsed']} load={item['load_status']}"
        )
    return status


def check_sources(args: argparse.Namespace) -> int:
    status, report = run_source_diagnostics(args)
    print(f"source diagnostics status: {'ok' if status == 0 else 'failed'}")
    print(f"required failures: {report['required_failures']}")
    print(f"optional failures: {report['optional_failures']}")
    print("source summary")
    for item in report["source_summaries"]:
        location = item.get("artifact_url") or item.get("sample_file") or "n/a"
        print(
            f"- {item['source']}: maturity={item['maturity']} required_for_live={item['required_for_live']} "
            f"rows={item['rows_parsed']} status={item['load_status']} location={location}"
        )
    return status


def export_opportunities(args: argparse.Namespace) -> int:
    ensure_database_dir()
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


def export_seller_theses(args: argparse.Namespace) -> int:
    theses = get_seller_theses_frame(args)
    if theses.empty:
        print("no seller theses found; run apply_saved_sql after ingestion")
        return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    theses.to_csv(output_path, index=False)

    cols = [
        "operator",
        "avg_oil_bpd_30d",
        "avg_oil_bpd_365d",
        "thesis_priority",
        "thesis_score",
        "seller_score",
        "opportunity_score",
        "footprint_type",
        "core_area_key",
        "package_count",
    ]
    print(f"exported {len(theses)} seller theses to {output_path}")
    print("top 20 seller theses")
    print(theses[cols].head(20).to_string(index=False) if not theses.empty else "none")
    return 0


def export_package_candidates(args: argparse.Namespace) -> int:
    packages = get_package_candidates_frame(args)
    if packages.empty:
        print("no package candidates found; run apply_saved_sql after ingestion")
        return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    packages.to_csv(output_path, index=False)

    cols = [
        "operator",
        "area_key",
        "suspended_well_count",
        "high_priority_well_count",
        "linked_facility_count",
        "estimated_restart_upside_bpd",
        "package_score",
    ]
    print(f"exported {len(packages)} package candidates to {output_path}")
    print("top 20 package candidates")
    print(packages[cols].head(20).to_string(index=False) if not packages.empty else "none")
    return 0


def main() -> int:
    _configure_logging()
    args = parse_args()
    if args.command == "run":
        return run_ingestion(args)
    if args.command == "check-sources":
        return check_sources(args)
    if args.command == "export-opportunities":
        return export_opportunities(args)
    if args.command == "export-seller-theses":
        return export_seller_theses(args)
    if args.command == "export-package-candidates":
        return export_package_candidates(args)
    if args.command == "apply_saved_sql":
        from deal_flow_ingest.apply_saved_sql import apply_saved_sql

        apply_saved_sql()
        return 0
    if args.command == "reset":
        return reset_database(args)
    return 1
