from __future__ import annotations

import pandas as pd


def normalize_production(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["month"] = pd.to_datetime(out["month"]) 
    for col in ["oil_bbl", "gas_mcf", "water_bbl"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["operator"] = out["operator"].fillna("UNKNOWN")
    return out


def normalize_wells(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["well_id"] = out["well_id"].astype(str)
    out["operator"] = out["operator"].fillna("UNKNOWN")
    return out
