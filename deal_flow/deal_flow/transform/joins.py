from __future__ import annotations

import pandas as pd


def attach_operator_from_wells(prod: pd.DataFrame, wells: pd.DataFrame) -> pd.DataFrame:
    if wells.empty:
        return prod
    lookup = wells[["well_id", "operator"]].dropna().drop_duplicates(subset=["well_id"])
    out = prod.merge(lookup, on="well_id", how="left", suffixes=("", "_well"))
    out["operator"] = out["operator"].fillna(out["operator_well"])
    return out.drop(columns=["operator_well"])
