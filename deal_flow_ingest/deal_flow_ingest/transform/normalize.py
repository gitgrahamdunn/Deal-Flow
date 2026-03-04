from __future__ import annotations

import re

import pandas as pd


def normalize_operator_name(name: str) -> str:
    val = (name or "").strip().upper()
    val = re.sub(r"[\.,'\-]", " ", val)
    val = re.sub(r"\bLIMITED\b", "LTD", val)
    val = re.sub(r"\bINCORPORATED\b", "INC", val)
    val = re.sub(r"\bCOMPANY\b", "CO", val)
    val = re.sub(r"\bCORPORATION\b", "CORP", val)
    val = re.sub(r"\s+", " ", val).strip()
    return val


def _pick_column(df: pd.DataFrame, options: list[str], default: str | None = None) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for opt in options:
        if opt in lower:
            return lower[opt]
    return default


def normalize_production_df(df: pd.DataFrame, source: str, basis_level: str) -> pd.DataFrame:
    month_col = _pick_column(df, ["month", "production_month", "date"])
    operator_col = _pick_column(df, ["operator", "operator_name", "licensee", "company"])
    oil_col = _pick_column(df, ["oil_bbl", "oil", "crude_oil_bbl", "oil_volume"])
    gas_col = _pick_column(df, ["gas_mcf", "gas", "gas_volume"])
    water_col = _pick_column(df, ["water_bbl", "water", "water_volume"])

    if not month_col or not operator_col or not oil_col:
        raise ValueError("Input dataset missing required month/operator/oil columns")

    out = pd.DataFrame(
        {
            "month": pd.to_datetime(df[month_col]).dt.to_period("M").dt.to_timestamp(),
            "operator_name_raw": df[operator_col].astype(str),
            "oil_bbl": pd.to_numeric(df[oil_col], errors="coerce").fillna(0.0),
            "gas_mcf": pd.to_numeric(df[gas_col], errors="coerce") if gas_col else None,
            "water_bbl": pd.to_numeric(df[water_col], errors="coerce") if water_col else None,
            "source": source,
            "basis_level": basis_level,
        }
    )
    out["operator_name_norm"] = out["operator_name_raw"].map(normalize_operator_name)
    out["gas_mcf"] = out["gas_mcf"].astype(float)
    out["water_bbl"] = out["water_bbl"].astype(float)
    return out
