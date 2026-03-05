from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)


def load_st37(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch ST37 landing page %s: %s", source.landing_page_url, exc)
    if not source.dataset_url:
        LOGGER.warning("ST37 source configured without direct dataset URL; skipping parse")
        return pd.DataFrame()

    result = downloader.fetch(source.key, source.dataset_url, refresh=refresh, file_type=source.file_type)
    return _parse_well_table(result.path)


def load_spatial(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch AER spatial landing page %s: %s", source.landing_page_url, exc)
    if not source.dataset_url:
        LOGGER.warning("AER spatial source has no dataset URL; metadata-only mode")
        return pd.DataFrame()

    result = downloader.fetch(
        source.key,
        source.dataset_url,
        refresh=refresh,
        file_type=source.file_type,
        extract_zip=source.file_type.lower() == "zip",
    )
    if result.extracted_dir:
        target = Path("data/raw/aer_spatial")
        target.mkdir(parents=True, exist_ok=True)
        for file in result.extracted_dir.rglob("*"):
            if file.is_file():
                destination = target / file.name
                destination.write_bytes(file.read_bytes())
    LOGGER.info("AER spatial artifact fetched; parser TODO for shape enrichment")
    return pd.DataFrame()


def load_liability(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch liability landing page %s: %s", source.landing_page_url, exc)

    if not source.dataset_url:
        LOGGER.info("Liability source configured without direct artifact; skipping rows")
        return pd.DataFrame()

    result = downloader.fetch(source.key, source.dataset_url, refresh=refresh, file_type=source.file_type)
    parsed = _read_tabular(result.path)
    if parsed.empty:
        return parsed

    column_map = {c.lower().strip(): c for c in parsed.columns}
    out = pd.DataFrame()
    out["as_of_date"] = parsed.get(column_map.get("as_of_date"), parsed.get(column_map.get("date")))
    out["operator"] = parsed.get(column_map.get("operator"), parsed.get(column_map.get("licensee"), ""))
    out["inactive_wells"] = parsed.get(column_map.get("inactive_wells"), 0)
    out["active_wells"] = parsed.get(column_map.get("active_wells"), 0)
    out["deemed_assets"] = parsed.get(column_map.get("deemed_assets"), 0)
    out["deemed_liabilities"] = parsed.get(column_map.get("deemed_liabilities"), 0)
    out["ratio"] = parsed.get(column_map.get("ratio"), parsed.get(column_map.get("lmr"), 0))
    return out


def _parse_well_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    else:
        frame = _read_delimited_fallback(path)

    if frame.empty:
        return frame

    cols = {c.lower().strip(): c for c in frame.columns}
    out = pd.DataFrame()
    out["uwi"] = frame.get(cols.get("uwi"), frame.get(cols.get("well_id"), ""))
    out["status"] = frame.get(cols.get("status"), frame.get(cols.get("well_status"), "UNKNOWN"))
    out["licensee"] = frame.get(cols.get("licensee"), frame.get(cols.get("operator"), ""))
    for field in ["township", "range", "section", "meridian", "lat", "lon", "lsd"]:
        out[field] = frame.get(cols.get(field), None)
    return out.dropna(subset=["uwi"], how="all")


def _read_delimited_fallback(path: Path) -> pd.DataFrame:
    for sep in ["|", "\t", ",", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, engine="python", on_bad_lines="skip")
            if df.shape[1] > 1:
                return df
        except Exception:  # noqa: BLE001
            continue
    return pd.DataFrame()


def _read_tabular(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt"}:
        return _read_delimited_fallback(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.DataFrame()
