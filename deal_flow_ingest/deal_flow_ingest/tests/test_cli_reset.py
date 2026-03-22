from argparse import Namespace
from pathlib import Path

from deal_flow_ingest.cli import reset_database
from deal_flow_ingest.services.pipeline import reset_database as reset_database_service


def test_reset_database_sqlite(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "nested" / "deal_flow.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("stale")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    args = Namespace(force=False, include_cache=False)
    assert reset_database(args) == 0

    assert db_path.exists()
    assert db_path.stat().st_size > 0


def test_reset_database_non_sqlite_requires_force(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/deal_flow")

    called = {"migrate": False}

    def _fake_upgrade() -> None:
        called["migrate"] = True

    monkeypatch.setattr("deal_flow_ingest.deal_flow_ingest.services.pipeline.upgrade_to_head", _fake_upgrade)

    args = Namespace(force=False, include_cache=False)
    assert reset_database(args) == 1
    assert called["migrate"] is False


def test_reset_database_postgres_force_recreates_public_schema(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/deal_flow")

    executed: list[str] = []
    called = {"migrate": False}

    class _Conn:
        dialect = type("Dialect", (), {"name": "postgresql"})()

        def execute(self, stmt):
            executed.append(str(stmt))

    class _BeginCtx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Engine:
        def begin(self):
            return _BeginCtx()

    monkeypatch.setattr("deal_flow_ingest.services.pipeline.get_engine", lambda _url: _Engine())
    monkeypatch.setattr("deal_flow_ingest.services.pipeline.upgrade_to_head", lambda: called.__setitem__("migrate", True))

    code, message = reset_database_service(force=True, include_cache=False)

    assert code == 0
    assert message == "Database reset complete"
    assert executed == ["DROP SCHEMA IF EXISTS public CASCADE", "CREATE SCHEMA public"]
    assert called["migrate"] is True
