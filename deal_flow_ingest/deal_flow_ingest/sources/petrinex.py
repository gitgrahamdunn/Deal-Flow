from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)


def load_public_data(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch Petrinex landing page: %s", exc)

    if not source.dataset_url:
        LOGGER.warning("Petrinex source %s missing dataset_url", source.key)
        return pd.DataFrame()

    result = downloader.fetch(source.key, source.dataset_url, refresh=refresh, file_type=source.file_type)
    df = _read_table(result.path)
    if df.empty:
        return df

    if source.data_kind == "facility_master":
        return _normalize_facility_master(df)
    if source.data_kind == "well_facility_bridge":
        return _normalize_well_facility_bridge(df)
    if source.data_kind == "facility_production":
        return _normalize_monthly_production(df)
    if source.data_kind == "operators":
        return _normalize_business_associate(df)
    return df


def _read_table(path: Path) -> pd.DataFrame:
    for sep in [",", "|", "\t", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, engine="python", on_bad_lines="skip")
            if df.shape[1] > 1:
                return df
        except Exception:  # noqa: BLE001
            continue
    return pd.DataFrame()


def _normalize_business_associate(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["name_raw"] = df.get(cols.get("business_associate_name"), df.get(cols.get("operator"), ""))
    if out["name_raw"].replace("", pd.NA).dropna().empty:
        LOGGER.warning("Petrinex operator-like dataset missing expected columns; found=%s", list(df.columns))
    return out


def _normalize_facility_master(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["facility_id"] = df.get(cols.get("facility_id"), df.get(cols.get("facility"), ""))
    out["facility_type"] = df.get(cols.get("facility_type"), None)
    out["facility_operator"] = df.get(cols.get("operator"), df.get(cols.get("business_associate_name"), ""))
    for field in ["lsd", "section", "township", "range", "meridian", "lat", "lon"]:
        out[field] = df.get(cols.get(field), None)
    return out


def _normalize_well_facility_bridge(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["well_id"] = df.get(cols.get("well_id"), df.get(cols.get("uwi"), ""))
    out["facility_id"] = df.get(cols.get("facility_id"), "")
    out["effective_from"] = df.get(cols.get("effective_from"), None)
    out["effective_to"] = df.get(cols.get("effective_to"), None)
    return out


def _normalize_monthly_production(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["month"] = df.get(cols.get("production_month"), df.get(cols.get("month"), None))
    out["facility_id"] = df.get(cols.get("facility_id"), "")
    out["oil_bbl"] = df.get(cols.get("oil_bbl"), df.get(cols.get("oil"), 0))
    out["gas_mcf"] = df.get(cols.get("gas_mcf"), df.get(cols.get("gas"), 0))
    out["water_bbl"] = df.get(cols.get("water_bbl"), df.get(cols.get("water"), 0))
    out["condensate_bbl"] = df.get(cols.get("condensate_bbl"), 0)
    return out
