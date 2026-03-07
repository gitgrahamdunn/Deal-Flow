from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.cli import reset_database


def test_reset_database_sqlite(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "nested" / "deal_flow.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("stale")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    args = Namespace(force=False)
    assert reset_database(args) == 0

    assert db_path.exists()
    assert db_path.stat().st_size > 0


def test_reset_database_non_sqlite_requires_force(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/deal_flow")

    called = {"migrate": False}

    def _fake_upgrade() -> None:
        called["migrate"] = True

    monkeypatch.setattr("deal_flow_ingest.deal_flow_ingest.cli.upgrade_to_head", _fake_upgrade)

    args = Namespace(force=False)
    assert reset_database(args) == 1
    assert called["migrate"] is False
