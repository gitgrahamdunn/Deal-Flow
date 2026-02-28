from __future__ import annotations

import pandas as pd


def _normalize_well_id(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    return out.str.replace(r"\.0+$", "", regex=True)


def attach_operator_from_wells(prod: pd.DataFrame, wells: pd.DataFrame) -> pd.DataFrame:
    if wells.empty:
        return prod
    prod_norm = prod.copy()
    prod_norm["well_id"] = _normalize_well_id(prod_norm["well_id"])

    lookup = wells[["well_id", "operator"]].dropna().drop_duplicates(subset=["well_id"]).copy()
    lookup["well_id"] = _normalize_well_id(lookup["well_id"])

    out = prod_norm.merge(lookup, on="well_id", how="left", suffixes=("", "_well"))
    out["operator"] = out["operator"].fillna(out["operator_well"])
    return out.drop(columns=["operator_well"])
