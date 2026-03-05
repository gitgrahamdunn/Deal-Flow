from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config


def upgrade_to_head() -> None:
    base = Path(__file__).resolve().parents[2]
    alembic_ini = base / "alembic.ini"
    cfg = Config(str(alembic_ini))
    cfg.set_main_option("script_location", str(base / "alembic"))
    command.upgrade(cfg, "head")
