from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from deal_flow_ingest.config import get_database_url
from deal_flow_ingest.db.schema import get_engine

SQL_FILES = (
    Path("sql/views/deal_flow_targets.sql"),
    Path("sql/views/restart_well_candidates.sql"),
    Path("sql/views/deal_flow_opportunities.sql"),
)


def apply_saved_sql() -> int:
    engine = get_engine(get_database_url())

    with engine.begin() as conn:
        for sql_file in SQL_FILES:
            if not sql_file.exists():
                continue

            view_name = sql_file.stem
            conn.execute(text(f'DROP VIEW IF EXISTS "{view_name}"'))
            conn.execute(text(sql_file.read_text(encoding="utf-8")))
            print(f"Applied: {sql_file}")

    return 0


if __name__ == "__main__":
    raise SystemExit(apply_saved_sql())
