import os
import sqlite3
from argparse import Namespace
from pathlib import Path

import pandas as pd
import yaml

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


def _write_config(tmp_path: Path, sources: dict) -> Path:
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(yaml.safe_dump({"sources": sources}), encoding="utf-8")
    return config_path


def test_pipeline_live_mode_optional_sources_empty_succeeds(tmp_path: Path, monkeypatch, capsys):
    db_path = tmp_path / "deal_flow_empty.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    config_path = _write_config(
        tmp_path,
        {
            "open_alberta_placeholder": {
                "enabled": True,
                "source_name": "open_alberta",
                "maturity": "planned",
                "required_for_live": False,
                "parser_name": "open_alberta_placeholder",
                "dataset_url": "",
                "file_type": "csv",
                "refresh_frequency": "ad_hoc",
            }
        },
    )

    def _empty_dataset(*_args, **_kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("deal_flow_ingest.deal_flow_ingest.services.pipeline.load_dataset", _empty_dataset)

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=False,
        config=str(config_path),
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


def test_pipeline_live_mode_required_source_empty_fails(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "deal_flow_required_empty.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    config_path = _write_config(
        tmp_path,
        {
            "aer_st37": {
                "enabled": True,
                "source_name": "aer_st37",
                "maturity": "live_working",
                "required_for_live": True,
                "parser_name": "aer_st37",
                "dataset_url": "",
                "file_type": "txt",
                "refresh_frequency": "monthly",
            }
        },
    )

    def _empty_dataset(*_args, **_kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("deal_flow_ingest.deal_flow_ingest.services.pipeline.load_dataset", _empty_dataset)

    args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=False,
        config=str(config_path),
    )

    assert run_ingestion(args) == 1
    assert _latest_ingestion_status(db_path) == "failed"
