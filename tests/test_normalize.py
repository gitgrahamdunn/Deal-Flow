import pandas as pd

from deal_flow.transform.joins import attach_operator_from_wells
from deal_flow.transform.normalize import normalize_production


def test_normalize_production_types() -> None:
    df = pd.DataFrame({"month": ["2026-01-01"], "operator": [None], "oil_bbl": ["10"], "gas_mcf": ["5"], "water_bbl": [None]})
    out = normalize_production(df)
    assert out.loc[0, "operator"] == "UNKNOWN"
    assert out.loc[0, "oil_bbl"] == 10


def test_attach_operator_from_wells() -> None:
    prod = pd.DataFrame({"well_id": ["1"], "operator": [None]})
    wells = pd.DataFrame({"well_id": ["1"], "operator": ["Alpha"]})
    out = attach_operator_from_wells(prod, wells)
    assert out.loc[0, "operator"] == "Alpha"
