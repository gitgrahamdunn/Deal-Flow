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
    if re.match(r"^[A-Z]{4}\d", v):
        v = v[4:]
    return v


def month_start(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series).dt.to_period("M").dt.to_timestamp()


def normalize_lsd(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip().upper()
    if not text:
        return None
    text = re.sub(r"[^0-9A-Z]", "", text)
    if not text:
        return None
    if text.isdigit():
        return text.zfill(2)
    return text


def normalize_ats_location(
    *,
    lsd: object = None,
    section: object = None,
    township: object = None,
    range_: object = None,
    meridian: object = None,
) -> dict[str, int | str | None]:
    def _to_int(value: object) -> int | None:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip()
        if not text:
            return None
        match = re.search(r"\d+", text)
        if not match:
            return None
        return int(match.group(0))

    return {
        "lsd": normalize_lsd(lsd),
        "section": _to_int(section),
        "township": _to_int(township),
        "range": _to_int(range_),
        "meridian": _to_int(meridian),
    }
