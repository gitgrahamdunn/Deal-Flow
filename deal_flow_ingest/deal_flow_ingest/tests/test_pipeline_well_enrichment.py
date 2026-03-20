from __future__ import annotations

import pandas as pd

from deal_flow_ingest.services.pipeline import enrich_wells_with_facility_operator


def test_enrich_wells_with_facility_operator_backfills_missing_operator() -> None:
    wells_df = pd.DataFrame(
        {
            "well_id": ["1000111222333444", "1000555666777888"],
            "licensee_operator_id": [None, 9],
            "status": ["UNKNOWN", "UNKNOWN"],
        }
    )
    bridge_df = pd.DataFrame(
        {
            "well_id": ["1000111222333444", "1000555666777888"],
            "facility_id": ["FAC-1", "FAC-2"],
        }
    )
    fac_df = pd.DataFrame(
        {
            "facility_id": ["FAC-1", "FAC-2"],
            "facility_operator_id": [7, 8],
        }
    )

    enriched = enrich_wells_with_facility_operator(wells_df, bridge_df, fac_df)

    assert enriched.loc[enriched["well_id"] == "1000111222333444", "licensee_operator_id"].iloc[0] == 7
    assert enriched.loc[enriched["well_id"] == "1000555666777888", "licensee_operator_id"].iloc[0] == 9
