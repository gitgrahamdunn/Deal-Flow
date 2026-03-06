import os
import sqlite3
from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.cli import run_ingestion


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        conn.close()


def test_pipeline_idempotency(tmp_path: Path):
    db_path = tmp_path / "deal_flow.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml",
    )
    assert run_ingestion(args) == 0
    first_counts = {
        "ingestion_run": _count_rows(db_path, "ingestion_run"),
        "fact_operator_metrics": _count_rows(db_path, "fact_operator_metrics"),
        "fact_well_restart_score": _count_rows(db_path, "fact_well_restart_score"),
    }

    assert run_ingestion(args) == 0
    second_counts = {
        "ingestion_run": _count_rows(db_path, "ingestion_run"),
        "fact_operator_metrics": _count_rows(db_path, "fact_operator_metrics"),
        "fact_well_restart_score": _count_rows(db_path, "fact_well_restart_score"),
    }

    assert second_counts["ingestion_run"] == first_counts["ingestion_run"] + 1
    assert second_counts["fact_operator_metrics"] == first_counts["fact_operator_metrics"]
    assert second_counts["fact_well_restart_score"] == first_counts["fact_well_restart_score"]
