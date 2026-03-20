from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.apply_saved_sql import apply_saved_sql
from deal_flow_ingest.cli import export_package_candidates, run_ingestion


def test_export_package_candidates_writes_csv(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "deal_flow.db"
    output_path = tmp_path / "exports" / "package_candidates.csv"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    run_args = Namespace(
        start=None,
        end="2025-03-31",
        refresh=False,
        dry_run=True,
        config="deal_flow_ingest/deal_flow_ingest/configs/sources.yaml",
    )
    assert run_ingestion(run_args) == 0
    assert apply_saved_sql() == 0

    export_args = Namespace(min_score=0.0, limit=25, output=str(output_path))
    assert export_package_candidates(export_args) == 0

    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "operator,area_key" in content
    assert "package_score" in content
