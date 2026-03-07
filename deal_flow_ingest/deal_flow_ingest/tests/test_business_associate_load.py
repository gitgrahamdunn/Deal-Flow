from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, func, select

from deal_flow_ingest.db.load import (
    upsert_bridge_operator_business_associate,
    upsert_dim_business_associate,
)
from deal_flow_ingest.db.schema import (
    Base,
    BridgeOperatorBusinessAssociate,
    DimBusinessAssociate,
    DimOperator,
)


def test_upsert_dim_business_associate_inserts_and_dedupes_ba_ids():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    df = pd.DataFrame(
        {
            "ba_id": ["BA-001", "BA-001", "BA-002", ""],
            "ba_name_raw": ["Alpha Energy Ltd", "Alpha Energy Limited", "Beta Oil Inc.", "Ignored"],
            "entity_type": ["operator", "operator", "wi_owner", "operator"],
        }
    )

    with engine.begin() as conn:
        ba_map = upsert_dim_business_associate(conn, df, source="petrinex_business_associate")
        assert ba_map == {"BA-001": "BA-001", "BA-002": "BA-002"}

        count = conn.execute(select(func.count()).select_from(DimBusinessAssociate)).scalar_one()
        assert count == 2

        ba_001 = conn.execute(
            select(DimBusinessAssociate.ba_name_raw, DimBusinessAssociate.ba_name_norm, DimBusinessAssociate.entity_type).where(
                DimBusinessAssociate.ba_id == "BA-001"
            )
        ).one()
        assert ba_001.ba_name_raw == "Alpha Energy Limited"
        assert ba_001.ba_name_norm == "ALPHA ENERGY LTD"
        assert ba_001.entity_type == "operator"


def test_upsert_bridge_operator_business_associate_dedupes_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            DimOperator.__table__.insert(),
            [{"name_raw": "Operator One", "name_norm": "OPERATOR ONE"}, {"name_raw": "Operator Two", "name_norm": "OPERATOR TWO"}],
        )
        upsert_dim_business_associate(
            conn,
            pd.DataFrame(
                {
                    "ba_id": ["BA-001", "BA-002"],
                    "ba_name_raw": ["Operator One BA", "Operator Two BA"],
                }
            ),
            source="seed",
        )

        bridge_df = pd.DataFrame(
            {
                "operator_id": [1, 1, 2],
                "ba_id": ["BA-001", "BA-001", "BA-002"],
                "match_method": ["exact_name", "manual", "normalized_name"],
                "confidence": [1.0, 0.95, 0.85],
            }
        )
        inserted = upsert_bridge_operator_business_associate(conn, bridge_df)
        assert inserted == 2

        rows = conn.execute(select(BridgeOperatorBusinessAssociate)).all()
        assert len(rows) == 2
        pair_map = {(row.operator_id, row.ba_id): row for row in rows}
        assert pair_map[(1, "BA-001")].match_method == "manual"
