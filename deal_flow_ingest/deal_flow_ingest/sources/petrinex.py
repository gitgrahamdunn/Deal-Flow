from __future__ import annotations

import logging
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)


_DATA_KIND_TO_DISCOVERY_KEY = {
    "facility_master": "facility_master",
    "well_facility_bridge": "well_facility_bridge",
    "facility_production": "monthly_production",
    "operators": "business_associate",
}


def load_public_data(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    artifact_url = (source.dataset_url or "").strip()
    discovery_key = _DATA_KIND_TO_DISCOVERY_KEY.get(source.data_kind)

    if not artifact_url and source.landing_page_url and discovery_key:
        try:
            landing_page = downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
            html = landing_page.path.read_text(encoding="utf-8", errors="replace")
            discovered_urls = _discover_petrinex_artifact_urls(html, source.landing_page_url)
            LOGGER.info("Discovered Petrinex artifact URLs: %s", discovered_urls)
            artifact_url = discovered_urls.get(discovery_key, "")
            if not artifact_url:
                LOGGER.warning("No Petrinex artifact discovered for %s on %s", discovery_key, source.landing_page_url)
                return pd.DataFrame()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed Petrinex artifact discovery for %s: %s", source.key, exc)
            return pd.DataFrame()

    if not artifact_url:
        LOGGER.warning("Petrinex source %s missing dataset_url and no discovered URL", source.key)
        return pd.DataFrame()

    try:
        result = downloader.fetch(source.key, artifact_url, refresh=refresh, file_type=_guess_file_type(artifact_url, source.file_type))
        df = _read_table(result.path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch/parse Petrinex artifact %s for %s: %s", artifact_url, source.key, exc)
        return pd.DataFrame()

    if df.empty:
        LOGGER.warning("Parsed 0 rows for Petrinex dataset %s from %s", source.key, artifact_url)
        return df

    if source.data_kind == "facility_master":
        parsed = load_facility_master(df)
    elif source.data_kind == "well_facility_bridge":
        parsed = load_well_facility_bridge(df)
    elif source.data_kind == "facility_production":
        parsed = load_monthly_production(df)
    elif source.data_kind == "operators":
        parsed = load_business_associate(df)
    else:
        parsed = df

    LOGGER.info("Parsed %s rows for Petrinex dataset %s", len(parsed.index), source.key)
    return parsed


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href:
            self._current_href = href
            self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        self.links.append((self._current_href, " ".join(self._current_text_parts).strip()))
        self._current_href = None
        self._current_text_parts = []


def _discover_petrinex_artifact_urls(html: str, base_url: str = "https://www.petrinex.ca/PD/Pages/APD.aspx") -> dict[str, str]:
    parser = _AnchorParser()
    parser.feed(html)

    wanted = {
        "facility_master": ["facility", "master"],
        "monthly_production": ["facility", "production"],
        "well_facility_bridge": ["well-facility"],
        "business_associate": ["business", "associate"],
    }

    candidates: dict[str, list[tuple[int, str]]] = {key: [] for key in wanted}
    for href, text in parser.links:
        raw_href = (href or "").strip()
        if not raw_href:
            continue
        lowered_href = raw_href.lower()
        if lowered_href.startswith("mailto:") or lowered_href.startswith("javascript:"):
            continue

        absolute_url = urljoin(base_url, raw_href)
        searchable = f"{text} {raw_href} {absolute_url}".lower().replace("_", " ")
        extension_score = 0
        if ".csv" in searchable:
            extension_score = 3
        elif ".xlsx" in searchable or ".xls" in searchable:
            extension_score = 2

        for key, required_tokens in wanted.items():
            if all(token in searchable for token in required_tokens):
                score = extension_score
                if "archive" not in searchable:
                    score += 1
                candidates[key].append((score, absolute_url))

    discovered: dict[str, str] = {}
    for key, options in candidates.items():
        if options:
            discovered[key] = max(options, key=lambda item: item[0])[1]
    return discovered


def _guess_file_type(url: str, default_file_type: str) -> str:
    lowered = url.lower()
    if lowered.endswith(".xlsx"):
        return "xlsx"
    if lowered.endswith(".xls"):
        return "xls"
    if lowered.endswith(".csv"):
        return "csv"
    return default_file_type


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    for sep in [",", "|", "\t", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, engine="python", on_bad_lines="skip")
            if df.shape[1] > 1:
                return df
        except Exception:  # noqa: BLE001
            continue
    return pd.DataFrame()


def _find_column_name(columns: dict[str, str], *aliases: str) -> str | None:
    for alias in aliases:
        if alias in columns:
            return columns[alias]
    return None


def _column_as_series(df: pd.DataFrame, columns: dict[str, str], aliases: tuple[str, ...], default_value: object = "") -> pd.Series:
    column = _find_column_name(columns, *aliases)
    if column:
        return df[column]
    return pd.Series([default_value] * len(df.index), index=df.index)


def load_business_associate(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["ba_id"] = _column_as_series(
        df,
        cols,
        (
            "business_associate_id",
            "business associate id",
            "ba_id",
            "ba id",
            "operator_ba_id",
            "operator ba id",
        ),
        "",
    )
    out["ba_name_raw"] = _column_as_series(
        df,
        cols,
        ("business_associate_name", "business associate name", "operator", "company_name", "company name"),
        "",
    )
    out["entity_type"] = _column_as_series(df, cols, ("entity_type", "entity type", "associate_type", "associate type"), None)
    return out


def load_facility_master(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["facility_id"] = _column_as_series(
        df,
        cols,
        ("facility_id", "facility id", "facility", "facility ba id", "facility ba identifier"),
        "",
    )
    out["facility_operator"] = _column_as_series(
        df,
        cols,
        ("operator", "operator_name", "operator name", "business_associate_name", "business associate name"),
        "",
    )
    return out


def load_well_facility_bridge(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["well_id"] = _column_as_series(df, cols, ("well_id", "well id", "uwi", "well", "well identifier"), "")
    out["facility_id"] = _column_as_series(df, cols, ("facility_id", "facility id", "facility", "facility ba id"), "")
    return out


def load_monthly_production(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["month"] = _column_as_series(
        df,
        cols,
        ("production_month", "production month", "month", "activity_month", "activity month"),
        "",
    )
    out["facility_id"] = _column_as_series(df, cols, ("facility_id", "facility id", "facility", "facility ba id"), "")
    out["oil_bbl"] = _column_as_series(df, cols, ("oil_bbl", "oil bbl", "oil", "oil production"), 0)
    out["gas_mcf"] = _column_as_series(df, cols, ("gas_mcf", "gas mcf", "gas", "gas production"), 0)
    out["water_bbl"] = _column_as_series(df, cols, ("water_bbl", "water bbl", "water", "water production"), 0)
    out["condensate_bbl"] = _column_as_series(df, cols, ("condensate_bbl", "condensate bbl", "condensate"), 0)
    return out
