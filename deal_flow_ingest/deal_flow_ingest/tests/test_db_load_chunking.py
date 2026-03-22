from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, func, select

from deal_flow_ingest.db.load import replace_fact_well_status, upsert_dim_well
from deal_flow_ingest.db.schema import Base, DimWell, FactWellStatus


def test_upsert_dim_well_large_sqlite_batch_succeeds():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    row_count = 1500
    wells_df = pd.DataFrame(
        {
            "well_id": [f"WELL-{idx:06d}" for idx in range(row_count)],
            "uwi_raw": [f"00/{idx:06d}" for idx in range(row_count)],
            "licensee_operator_id": [None] * row_count,
            "status": ["ACTIVE"] * row_count,
            "lsd": [None] * row_count,
            "section": [None] * row_count,
            "township": [None] * row_count,
            "range": [None] * row_count,
            "meridian": [None] * row_count,
            "lat": [None] * row_count,
            "lon": [None] * row_count,
            "first_seen": [None] * row_count,
            "last_seen": [None] * row_count,
            "source": ["test"] * row_count,
        }
    )

    with engine.begin() as conn:
        affected = upsert_dim_well(conn, wells_df)
        assert affected == row_count

        total = conn.execute(select(func.count()).select_from(DimWell)).scalar_one()
        assert total == row_count


def test_upsert_dim_well_dedupes_by_completeness_score():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    wells_df = pd.DataFrame(
        {
            "well_id": ["100011100100W500", "100011100100W500", "200022200200W600"],
            "uwi_raw": ["00/11-11-001-01W5/0", "00/11-11-001-01W5/0", "00/22-22-002-02W6/0"],
            "licensee_operator_id": [None, 123, None],
            "status": ["", "ACTIVE", "SUSPENDED"],
            "lsd": [None, "11", None],
            "section": [None, 11, None],
            "township": [None, 1, None],
            "range": [None, 1, None],
            "meridian": [None, 5, None],
            "lat": [None, 51.0, None],
            "lon": [None, -114.0, None],
            "first_seen": [None, None, None],
            "last_seen": [None, None, None],
            "source": ["test", "test", "test"],
        }
    )

    with engine.begin() as conn:
        affected = upsert_dim_well(conn, wells_df)
        assert affected == 2

        rows = conn.execute(select(DimWell)).all()
        assert len(rows) == 2

        row_by_well_id = {row.well_id: row for row in rows}
        retained = row_by_well_id["100011100100W500"]
        assert retained.status == "ACTIVE"
        assert retained.licensee_operator_id == 123
        assert retained.section == 11
        assert retained.lat == 51.0


def test_replace_fact_well_status_dedupes_duplicate_unique_keys():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    status_df = pd.DataFrame(
        {
            "well_id": ["WELL-1", "WELL-1", "WELL-2"],
            "status": ["UNKNOWN", "UNKNOWN", "ACTIVE"],
            "status_date": [None, None, pd.Timestamp("2025-01-01").date()],
            "source": ["aer_st37", "aer_st37", "aer_st37"],
        }
    )

    with engine.begin() as conn:
        affected = replace_fact_well_status(conn, status_df, "aer_st37")
        assert affected == 2

        rows = conn.execute(select(FactWellStatus)).all()
        assert len(rows) == 2


def test_replace_fact_well_status_replaces_existing_rows_across_payload_sources():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    initial_df = pd.DataFrame(
        {
            "well_id": ["WELL-1", "WELL-2"],
            "status": ["UNKNOWN", "SHUT-IN"],
            "status_date": [None, pd.Timestamp("2025-02-01").date()],
            "source": ["aer_st37", "petrinex_monthly_activity"],
        }
    )
    replacement_df = pd.DataFrame(
        {
            "well_id": ["WELL-1", "WELL-2"],
            "status": ["UNKNOWN", "SHUT-IN"],
            "status_date": [None, pd.Timestamp("2025-02-01").date()],
            "source": ["aer_st37", "petrinex_monthly_activity"],
        }
    )

    with engine.begin() as conn:
        assert replace_fact_well_status(conn, initial_df, "aer_st37") == 2
        assert replace_fact_well_status(conn, replacement_df, "aer_st37") == 2

        rows = conn.execute(select(FactWellStatus)).all()
        assert len(rows) == 2
