from __future__ import annotations

import pandas as pd

from deal_flow_ingest.services.pipeline import enrich_wells_with_facility_operator, prepare_wells_df


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


def test_prepare_wells_df_backfills_location_and_licence_from_petrinex_sources() -> None:
    frames_by_kind = {
        "wells": [
            pd.DataFrame(
                {
                    "uwi": ["100011100100W400"],
                    "licensee": ["Alpha Energy Ltd."],
                    "status": ["SUSPENDED"],
                }
            ),
            pd.DataFrame(
                {
                    "uwi": ["100011100100W400"],
                    "license_number": ["0482521"],
                    "well_name": ["ALPHA 01-01-050-08W4"],
                    "field_name": ["ALPHA FIELD"],
                    "pool_name": ["ALPHA POOL"],
                    "licensee": ["Alpha Energy Ltd."],
                    "status": ["SUSPENDED"],
                    "spud_date": ["2021-03-15"],
                    "lsd": ["10"],
                    "section": [1],
                    "township": [50],
                    "range": [8],
                    "meridian": [4],
                    "source": ["petrinex_public_well_infrastructure"],
                }
            ),
        ]
    }

    wells_df = prepare_wells_df(frames_by_kind, {"ALPHA ENERGY LTD": 7}, pd.Timestamp("2025-03-31").date())

    assert len(wells_df) == 1
    row = wells_df.iloc[0]
    assert row["well_id"] == "100011100100W400"
    assert row["license_number"] == "0482521"
    assert row["well_name"] == "ALPHA 01-01-050-08W4"
    assert row["field_name"] == "ALPHA FIELD"
    assert row["pool_name"] == "ALPHA POOL"
    assert row["licensee_operator_id"] == 7
    assert row["lsd"] == "10"
    assert int(row["section"]) == 1
    assert int(row["township"]) == 50


def test_prepare_wells_df_backfills_from_licence_only_rows_by_license_number() -> None:
    frames_by_kind = {
        "wells": [
            pd.DataFrame(
                {
                    "uwi": ["100033300300W400"],
                    "license_number": ["0482523"],
                    "licensee": [""],
                    "status": [None],
                }
            ),
            pd.DataFrame(
                {
                    "uwi": [""],
                    "license_number": ["0482523"],
                    "well_name": ["03-03-051-09W4"],
                    "pool_name": ["BETA POOL"],
                    "licensee": ["Beta Petroleum Inc"],
                    "status": ["SHUT-IN"],
                    "spud_date": ["2020-09-20"],
                    "lsd": [12],
                    "section": [3],
                    "township": [51],
                    "range": [9],
                    "meridian": [4],
                    "source": ["petrinex_public_well_licence"],
                }
            ),
        ]
    }

    wells_df = prepare_wells_df(frames_by_kind, {"BETA PETROLEUM INC": 9}, pd.Timestamp("2025-03-31").date())

    assert len(wells_df) == 1
    row = wells_df.iloc[0]
    assert row["well_id"] == "100033300300W400"
    assert row["license_number"] == "0482523"
    assert row["well_name"] == "03-03-051-09W4"
    assert row["pool_name"] == "BETA POOL"
    assert row["status"] == "SHUT-IN"
    assert row["licensee_operator_id"] == 9
    assert row["lsd"] == 12
    assert int(row["section"]) == 3
    assert int(row["township"]) == 51
