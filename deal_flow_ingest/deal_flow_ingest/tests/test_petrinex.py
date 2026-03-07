from pathlib import Path

import pandas as pd

from deal_flow_ingest.sources.petrinex import (
    _discover_petrinex_artifact_urls,
    load_business_associate,
    load_facility_master,
    load_monthly_production,
    load_well_facility_bridge,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "petrinex"


def test_discover_petrinex_artifact_urls() -> None:
    html = (FIXTURE_DIR / "discovery_page.html").read_text(encoding="utf-8")

    discovered = _discover_petrinex_artifact_urls(html, "https://www.petrinex.ca/PD/Pages/APD.aspx")

    assert discovered == {
        "facility_master": "https://www.petrinex.ca/files/Petrinex_Facility_Master_2024.csv",
        "monthly_production": "https://www.petrinex.ca/files/Petrinex_Facility_Production_2024.xlsx",
        "well_facility_bridge": "https://www.petrinex.ca/files/Petrinex_Well-Facility_Bridge_2024.csv",
        "business_associate": "https://www.petrinex.ca/files/Petrinex_Business_Associate_2024.csv",
    }


def test_parse_facility_master() -> None:
    source = pd.read_csv(FIXTURE_DIR / "facility_master.csv")

    parsed = load_facility_master(source)

    assert list(parsed.columns) == ["facility_id", "facility_operator"]
    assert parsed.to_dict("records") == [
        {"facility_id": "ABCF001", "facility_operator": "Operator One"},
        {"facility_id": "ABCF002", "facility_operator": "Operator Two"},
    ]


def test_parse_well_facility_bridge() -> None:
    source = pd.read_csv(FIXTURE_DIR / "well_facility_bridge.csv")

    parsed = load_well_facility_bridge(source)

    assert list(parsed.columns) == ["well_id", "facility_id"]
    assert parsed.iloc[0]["well_id"] == 1000111222333444
    assert parsed.iloc[0]["facility_id"] == "ABCF001"


def test_parse_monthly_production() -> None:
    source = pd.read_csv(FIXTURE_DIR / "monthly_production.csv")

    parsed = load_monthly_production(source)

    assert list(parsed.columns) == ["month", "facility_id", "oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl"]
    assert parsed.iloc[0].to_dict() == {
        "month": "2024-01-01",
        "facility_id": "ABCF001",
        "oil_bbl": 10,
        "gas_mcf": 200,
        "water_bbl": 30,
        "condensate_bbl": 4,
    }


def test_parse_business_associate() -> None:
    source = pd.read_csv(FIXTURE_DIR / "business_associate.csv")

    parsed = load_business_associate(source)

    assert list(parsed.columns) == ["ba_id", "ba_name_raw", "entity_type"]
    assert parsed["ba_id"].tolist() == ["", ""]
    assert parsed["ba_name_raw"].tolist() == ["Operator One", "Operator Two"]
