from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import get_database_url, load_config
from deal_flow_ingest.db.load import load_metrics, load_monthly_facts, record_ingestion_run, upsert_operators
from deal_flow_ingest.db.schema import create_tables, dim_operator, fact_operator_metrics, get_engine
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources import aer, open_alberta, petrinex
from deal_flow_ingest.transform.metrics import compute_operator_metrics
from deal_flow_ingest.transform.normalize import normalize_production_df

SOURCE_LOADERS = {
    "open_alberta": open_alberta.load,
    "aer": aer.load,
    "petrinex": petrinex.load,
}


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


def run_ingestion(args: argparse.Namespace) -> int:
    end = _parse_date(args.end, date.today())
    start = _parse_date(args.start, end - timedelta(days=365))
    started_at = datetime.utcnow()

    cfg = load_config(args.config)
    downloader = Downloader(Path("data/raw"))
    sample_dir = Path("deal_flow_ingest/deal_flow_ingest/sample_data")

    sources_ok: list[str] = []
    sources_failed: dict[str, str] = {}
    normalized_chunks: list[pd.DataFrame] = []
    row_counts = {"raw_rows": {}, "normalized_rows": 0, "monthly_loaded": 0, "metrics_loaded": 0}

    for source_name, scfg in cfg.sources.items():
        if not scfg.enabled:
            continue
        try:
            loader = SOURCE_LOADERS[source_name]
            raw_df = loader(downloader, sample_dir, args.dry_run, args.refresh, scfg.url)
            row_counts["raw_rows"][source_name] = int(len(raw_df))
            norm = normalize_production_df(raw_df, source=scfg.source_name, basis_level=scfg.basis_level)
            norm = norm[(norm["month"].dt.date >= start) & (norm["month"].dt.date <= end)]
            normalized_chunks.append(norm)
            sources_ok.append(source_name)
        except Exception as exc:
            sources_failed[source_name] = str(exc)

    normalized = pd.concat(normalized_chunks, ignore_index=True) if normalized_chunks else pd.DataFrame(
        columns=["month", "operator_name_raw", "oil_bbl", "gas_mcf", "water_bbl", "source", "basis_level", "operator_name_norm"]
    )
    row_counts["normalized_rows"] = int(len(normalized))

    engine = get_engine(get_database_url())
    create_tables(engine)

    with engine.begin() as conn:
        op_map = upsert_operators(conn, normalized) if not normalized.empty else {}
        if not normalized.empty:
            normalized["operator_id"] = normalized["operator_name_norm"].map(op_map)
            row_counts["monthly_loaded"] = load_monthly_facts(conn, normalized, start, end)
        else:
            row_counts["monthly_loaded"] = 0

        metrics = compute_operator_metrics(normalized[["month", "operator_id", "oil_bbl"]], end) if not normalized.empty else pd.DataFrame()
        if not metrics.empty:
            row_counts["metrics_loaded"] = load_metrics(conn, metrics, end)
        run_id = record_ingestion_run(
            conn,
            started_at=started_at,
            finished_at=datetime.utcnow(),
            status="success" if not sources_failed else "success",
            sources_ok=sources_ok,
            sources_failed=sources_failed,
            row_counts=row_counts,
            notes=(
                "30d metric approximated from monthly records using month buckets. "
                "Use daily data in next iteration for higher fidelity."
            ),
        )

        # top 10 output
        query = (
            pd.read_sql(
                fact_operator_metrics.select()
                .with_only_columns(
                    fact_operator_metrics.c.operator_id,
                    fact_operator_metrics.c.avg_oil_bpd_30d,
                )
                .where(fact_operator_metrics.c.as_of_date == end),
                conn,
            )
            if row_counts["metrics_loaded"]
            else pd.DataFrame(columns=["operator_id", "avg_oil_bpd_30d"])
        )
        if not query.empty:
            dim_df = pd.read_sql(dim_operator.select(), conn)
            top = query.merge(dim_df[["operator_id", "operator_name_norm"]], on="operator_id", how="left")
            top = top.sort_values("avg_oil_bpd_30d", ascending=False).head(10)
            print("Top 10 operators by avg_oil_bpd_30d")
            print(top[["operator_name_norm", "avg_oil_bpd_30d"]].to_string(index=False))
        else:
            print("Top 10 operators by avg_oil_bpd_30d")
            print("No metrics available for selected window.")

    print(f"Ingestion run complete: {run_id}")
    return 0


def main() -> None:
    args = parse_args()
    if args.command == "run":
        raise SystemExit(run_ingestion(args))
