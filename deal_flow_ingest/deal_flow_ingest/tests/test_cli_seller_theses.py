from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import export_seller_theses, run_ingestion
from deal_flow_ingest.config import get_default_config_path


def test_export_seller_theses_writes_csv(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "deal_flow.db"
    output_path = tmp_path / "exports" / "seller_theses.csv"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    run_args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config=get_default_config_path(),
    )
    assert run_ingestion(run_args) == 0
    assert apply_saved_sql() == 0

    export_args = Namespace(min_score=0.0, limit=25, output=str(output_path))
    assert export_seller_theses(export_args) == 0

    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "operator,as_of_date" in content
    assert "thesis_score" in content


def test_export_seller_theses_builds_views_if_missing(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "deal_flow.db"
    output_path = tmp_path / "exports" / "seller_theses.csv"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    run_args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config=get_default_config_path(),
    )
    assert run_ingestion(run_args) == 0

    export_args = Namespace(min_score=0.0, limit=25, output=str(output_path))
    assert export_seller_theses(export_args) == 0

    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "operator,as_of_date" in content
    assert "thesis_score" in content
