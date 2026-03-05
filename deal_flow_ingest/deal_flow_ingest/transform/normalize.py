from __future__ import annotations

import re

import pandas as pd


def normalize_operator_name(name: str) -> str:
    val = (name or "").strip().upper()
    val = re.sub(r"[^A-Z0-9 ]", " ", val)
    val = re.sub(r"\bLIMITED\b", "LTD", val)
    val = re.sub(r"\bINCORPORATED\b", "INC", val)
    val = re.sub(r"\bCOMPANY\b", "CO", val)
    val = re.sub(r"\bCORPORATION\b", "CORP", val)
    return re.sub(r"\s+", " ", val).strip()


def normalize_uwi(value: str) -> str:
    v = (value or "").upper().strip()
    v = re.sub(r"[^A-Z0-9]", "", v)
    return v


def month_start(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series).dt.to_period("M").dt.to_timestamp()
