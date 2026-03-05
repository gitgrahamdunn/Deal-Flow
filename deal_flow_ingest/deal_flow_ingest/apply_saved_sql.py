from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import text

from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import get_engine

SQL_FILES = (
    Path("sql/views/deal_flow_targets.sql"),
    Path("sql/views/restart_well_candidates.sql"),
    Path("sql/views/deal_flow_opportunities.sql"),
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _extract_view_name(sql_text: str, fallback_name: str) -> str:
    match = re.search(r"CREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\"\.]+)", sql_text, flags=re.IGNORECASE)
    return match.group(1).strip('"') if match else fallback_name


def apply_saved_sql() -> int:
    engine = get_engine(get_database_url())
    repo_root = _repo_root()

    with engine.begin() as conn:
        for sql_file in SQL_FILES:
            sql_path = repo_root / sql_file
            if not sql_path.exists():
                continue

            sql_text = sql_path.read_text(encoding="utf-8")
            view_name = _extract_view_name(sql_text, sql_file.stem)
            conn.execute(text(f'DROP VIEW IF EXISTS "{view_name}"'))
            conn.execute(text(sql_text))
            print(f"Applied: {sql_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(apply_saved_sql())
