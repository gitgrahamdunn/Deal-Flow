from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, func, select

from deal_flow_ingest.db.load import upsert_dim_well
from deal_flow_ingest.db.schema import Base, DimWell


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
