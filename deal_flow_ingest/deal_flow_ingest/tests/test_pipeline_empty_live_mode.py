import os
import sqlite3
from argparse import Namespace
from pathlib import Path

import pandas as pd

from deal_flow_ingest.cli import run_ingestion


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        conn.close()


def _latest_ingestion_status(db_path: Path) -> str:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("select status from ingestion_run order by started_at desc limit 1").fetchone()[0]
    finally:
        conn.close()


def test_pipeline_live_mode_all_sources_empty_succeeds(tmp_path: Path, monkeypatch, capsys):
    db_path = tmp_path / "deal_flow_empty.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

    def _empty_dataset(*_args, **_kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("deal_flow_ingest.deal_flow_ingest.services.pipeline.load_dataset", _empty_dataset)

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=False,
        config="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml",
    )

    assert run_ingestion(args) == 0
    assert _latest_ingestion_status(db_path) == "success"

    for table in [
        "fact_facility_production_monthly",
        "fact_well_production_monthly",
        "fact_operator_production_monthly",
        "fact_operator_liability",
        "fact_well_status",
        "fact_well_restart_score",
        "fact_operator_metrics",
    ]:
        assert _count_rows(db_path, table) == 0

    output = capsys.readouterr().out
    assert "No live source rows were parsed; pipeline completed with empty data." in output
