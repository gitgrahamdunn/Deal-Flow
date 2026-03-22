from __future__ import annotations

import logging
import tempfile
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
from zipfile import ZipFile

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)

_PETRINEX_DIRECT_URLS = {
    "operators": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Business%20Associate/CSV",
    "well_facility_bridge": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20to%20Facility%20Link/CSV",
    "facility_master": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Infrastructure/CSV",
    "petrinex_public_well_infrastructure": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Infrastructure/CSV",
    "petrinex_public_well_licence": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Licence/CSV",
}


def load_public_data(
    downloader: Downloader,
    source: SourcePayload,
    refresh: bool,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    artifact_url = (
        source.dataset_url
        or _PETRINEX_DIRECT_URLS.get(source.key, "")
        or _PETRINEX_DIRECT_URLS.get(source.data_kind, "")
    ).strip()
    discovery_key = _source_to_discovery_key(source)

    if source.data_kind == "facility_production":
        df = _load_monthly_production_range(downloader, source, refresh, start_date, end_date)
    elif not artifact_url and source.landing_page_url and discovery_key:
        landing_pages = [source.landing_page_url, *(source.alternate_landing_page_urls or [])]
        for landing_url in [u for u in landing_pages if u]:
            try:
                landing_page = downloader.fetch(source.key, landing_url, refresh=refresh, file_type="html")
                html = landing_page.path.read_text(encoding="utf-8", errors="replace")
                discovered_urls = _discover_petrinex_artifact_urls(html, landing_url)
                LOGGER.info("Discovered Petrinex artifact URLs from %s: %s", landing_url, discovered_urls)
                artifact_url = discovered_urls.get(discovery_key, "")
                if artifact_url:
                    break
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed Petrinex artifact discovery for %s on %s: %s", source.key, landing_url, exc)
        if not artifact_url:
            LOGGER.warning("No Petrinex artifact discovered for %s from configured landing pages", discovery_key)
            return pd.DataFrame()
        df = _load_single_petrinex_artifact(downloader, source, artifact_url, refresh)
    else:
        if not artifact_url:
            LOGGER.warning("Petrinex source %s missing dataset_url and no discovered URL", source.key)
            return pd.DataFrame()
        df = _load_single_petrinex_artifact(downloader, source, artifact_url, refresh)

    if df.empty:
        LOGGER.warning("Parsed 0 rows for Petrinex dataset %s", source.key)
        return df

    if source.data_kind == "facility_master":
        parsed = load_facility_master(df)
    elif source.data_kind == "well_facility_bridge":
        parsed = load_well_facility_bridge(df)
    elif source.data_kind == "facility_production":
        parsed = load_monthly_production(df)
    elif source.data_kind == "operators":
        parsed = load_business_associate(df)
    elif source.key == "petrinex_public_well_infrastructure":
        parsed = load_well_infrastructure(df)
    elif source.key == "petrinex_public_well_licence":
        parsed = load_well_licence(df)
    else:
        parsed = df

    LOGGER.info("Parsed %s rows for Petrinex dataset %s", len(parsed.index), source.key)
    return parsed


def _load_single_petrinex_artifact(downloader: Downloader, source: SourcePayload, artifact_url: str, refresh: bool) -> pd.DataFrame:
    try:
        result = downloader.fetch(source.key, artifact_url, refresh=refresh, file_type="zip", extract_zip=True)
        target_path = _resolve_petrinex_artifact(result.path, result.extracted_dir)
        if target_path is None:
            LOGGER.warning("No tabular Petrinex artifact found in %s for %s", artifact_url, source.key)
            return pd.DataFrame()
        return _read_table(target_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to fetch/parse Petrinex artifact %s for %s: %s", artifact_url, source.key, exc)
        return pd.DataFrame()


def _load_monthly_production_range(
    downloader: Downloader,
    source: SourcePayload,
    refresh: bool,
    start_date: date | None,
    end_date: date | None,
) -> pd.DataFrame:
    if start_date is None or end_date is None:
        LOGGER.warning("Petrinex monthly production requested without a date range")
        return pd.DataFrame()

    months = pd.period_range(start=start_date, end=end_date, freq="M")
    frames: list[pd.DataFrame] = []
    for month in months:
        artifact_url = f"https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/{month.strftime('%Y-%m')}/CSV"
        try:
            df = _load_single_petrinex_artifact(downloader, source, artifact_url, refresh)
        except Exception:  # pragma: no cover - defensive; helper already swallows most errors
            df = pd.DataFrame()
        if df.empty:
            LOGGER.info("No Petrinex monthly production rows for %s", month.strftime("%Y-%m"))
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_petrinex_artifact(path: Path, extracted_dir: Path | None) -> Path | None:
    candidates: list[Path] = []
    if extracted_dir and extracted_dir.exists():
        candidates.extend(sorted(p for p in extracted_dir.rglob("*") if p.is_file()))
    else:
        candidates.append(path)

    for candidate in candidates:
        suffix = candidate.suffix.lower()
        if suffix in {".csv", ".xlsx", ".xls"}:
            return candidate
        if suffix == ".zip":
            with tempfile.TemporaryDirectory(prefix="petrinex_inner_") as tmp_dir:
                tmp_path = Path(tmp_dir)
                with ZipFile(candidate, "r") as archive:
                    archive.extractall(tmp_path)
                nested = _resolve_petrinex_artifact(candidate, tmp_path)
                if nested is not None:
                    final_path = candidate.parent / nested.name
                    final_path.write_bytes(nested.read_bytes())
                    return final_path
    return None


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
        "well_infrastructure": ["well", "infrastructure"],
        "well_licence": ["well", "licence"],
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
        if ".pdf" in searchable:
            LOGGER.debug("Skipping non-tabular Petrinex artifact candidate: %s", absolute_url)
            continue
        extension_score = 0
        if ".csv" in searchable:
            extension_score = 4
        elif ".xlsx" in searchable or ".xls" in searchable:
            extension_score = 3
        elif ".zip" in searchable:
            extension_score = 1
        else:
            continue

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


def _source_to_discovery_key(source: SourcePayload) -> str | None:
    if source.key == "petrinex_public_well_infrastructure":
        return "well_infrastructure"
    if source.key == "petrinex_public_well_licence":
        return "well_licence"
    if source.data_kind == "facility_master":
        return "facility_master"
    if source.data_kind == "well_facility_bridge":
        return "well_facility_bridge"
    if source.data_kind == "facility_production":
        return "monthly_production"
    if source.data_kind == "operators":
        return "business_associate"
    return None


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
            "baidentifier",
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
        ("balegalname", "business_associate_name", "business associate name", "operator", "company_name", "company name"),
        "",
    )
    out["entity_type"] = _column_as_series(
        df, cols, ("balicenceeligibilitytype", "entity_type", "entity type", "associate_type", "associate type"), None
    )
    return out


def load_facility_master(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["facility_id"] = _column_as_series(
        df,
        cols,
        ("facilityid", "facility_id", "facility id", "facility", "facility ba id", "facility ba identifier"),
        "",
    )
    out["facility_type"] = _column_as_series(df, cols, ("facilitytype", "facility_type", "facility type"), None)
    out["facility_operator"] = _column_as_series(
        df,
        cols,
        ("operatorname", "operator", "operator_name", "operator name", "business_associate_name", "business associate name"),
        "",
    )
    out["lsd"] = _column_as_series(df, cols, ("facilitylegalsubdivision", "facility legal subdivision"), None)
    out["section"] = _column_as_series(df, cols, ("facilitysection", "facility section"), None)
    out["township"] = _column_as_series(df, cols, ("facilitytownship", "facility township"), None)
    out["range"] = _column_as_series(df, cols, ("facilityrange", "facility range"), None)
    out["meridian"] = _column_as_series(df, cols, ("facilitymeridian", "facility meridian"), None)
    return out


def load_well_facility_bridge(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["well_id"] = _column_as_series(
        df, cols, ("wellid", "well_id", "well id", "uwi", "well", "well identifier"), ""
    )
    out["facility_id"] = _column_as_series(
        df, cols, ("linkedfacilityid", "facility_id", "facility id", "facility", "facility ba id"), ""
    )
    out["effective_from"] = _column_as_series(df, cols, ("linkedstartdate", "effective_from", "effective from"), None)
    return out


def load_well_infrastructure(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["uwi"] = _column_as_series(
        df,
        cols,
        ("wellidentifier", "well identifier", "wellid", "well_id", "well id", "uwi", "well"),
        "",
    )
    out["license_number"] = _column_as_series(
        df,
        cols,
        ("welllicencenumber", "well licence number", "licencenumber", "license number", "licence number", "licencenumber"),
        None,
    )
    out["well_name"] = _column_as_series(df, cols, ("wellname", "well name"), None)
    out["field_name"] = _column_as_series(df, cols, ("fieldname", "field name", "field"), None)
    out["pool_name"] = _column_as_series(df, cols, ("poolname", "pool name", "pooldepositname", "targetpool"), None)
    out["licensee"] = _column_as_series(
        df,
        cols,
        ("licenseename", "licensee name", "licensee", "operatorname", "operator name", "licenseename", "linkedfacilityoperatorlegalname"),
        "",
    )
    out["status"] = _column_as_series(
        df,
        cols,
        ("wellstatusmode", "well status mode", "wellstatus", "well status", "currentstatus", "current status", "wellstatustype", "well status type", "licencestatus", "licence status", "status"),
        None,
    )
    out["spud_date"] = _column_as_series(df, cols, ("spuddate", "spud date"), None)
    out["lsd"] = _column_as_series(df, cols, ("welllegalsubdivision", "well legal subdivision", "lsd"), None)
    out["section"] = _column_as_series(df, cols, ("wellsection", "well section", "section"), None)
    out["township"] = _column_as_series(df, cols, ("welltownship", "well township", "township"), None)
    out["range"] = _column_as_series(df, cols, ("wellrange", "well range", "range"), None)
    out["meridian"] = _column_as_series(df, cols, ("wellmeridian", "well meridian", "meridian"), None)
    out["licensee"] = out["licensee"].fillna("").astype(str)
    return out


def load_well_licence(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    out["uwi"] = _column_as_series(
        df,
        cols,
        ("wellidentifier", "well identifier", "wellid", "well_id", "well id", "uwi", "well"),
        "",
    )
    out["license_number"] = _column_as_series(
        df,
        cols,
        ("welllicencenumber", "well licence number", "licencenumber", "license number", "licence number", "licencenumber", "licencenumber"),
        None,
    )
    out["licensee"] = _column_as_series(
        df,
        cols,
        ("licenseename", "licensee name", "licenceename", "licencee name", "licensee", "operatorname", "operator name"),
        "",
    )
    out["status"] = _column_as_series(
        df,
        cols,
        ("licencestatus", "license status", "welllicencestatus", "well licence status", "status"),
        None,
    )
    out["spud_date"] = _column_as_series(df, cols, ("spuddate", "spud date", "licenceissuedate", "licence issue date"), None)
    out["well_name"] = _column_as_series(df, cols, ("wellname", "well name", "licencelocation", "licence location"), None)
    out["field_name"] = _column_as_series(df, cols, ("fieldname", "field name"), None)
    out["pool_name"] = _column_as_series(df, cols, ("poolname", "pool name", "targetpool"), None)
    out["lsd"] = _column_as_series(df, cols, ("licencelegalsubdivision", "licence legal subdivision", "lsd"), None)
    out["section"] = _column_as_series(df, cols, ("licencesection", "licence section", "section"), None)
    out["township"] = _column_as_series(df, cols, ("licencetownship", "licence township", "township"), None)
    out["range"] = _column_as_series(df, cols, ("licencerange", "licence range", "range"), None)
    out["meridian"] = _column_as_series(df, cols, ("licencemeridian", "licence meridian", "meridian"), None)
    out["licensee"] = out["licensee"].fillna("").astype(str)
    return out


def load_monthly_production(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    if any(alias in cols for alias in ("oil bbl", "oil_bbl", "gas mcf", "gas_mcf", "water bbl", "water_bbl")):
        out = pd.DataFrame(index=df.index)
        out["month"] = _column_as_series(
            df,
            cols,
            ("productionmonth", "production_month", "production month", "month", "activity_month", "activity month"),
            "",
        )
        out["facility_id"] = _column_as_series(
            df,
            cols,
            ("reportingfacilityid", "facility_id", "facility id", "facility", "facility ba id"),
            "",
        )
        out["oil_bbl"] = _column_as_series(df, cols, ("oil_bbl", "oil bbl", "oil", "oil production"), 0)
        out["gas_mcf"] = _column_as_series(df, cols, ("gas_mcf", "gas mcf", "gas", "gas production"), 0)
        out["water_bbl"] = _column_as_series(df, cols, ("water_bbl", "water bbl", "water", "water production"), 0)
        out["condensate_bbl"] = _column_as_series(df, cols, ("condensate_bbl", "condensate bbl", "condensate"), 0)
        return out

    working = pd.DataFrame(index=df.index)
    working["month"] = _column_as_series(
        df,
        cols,
        ("productionmonth", "production_month", "production month", "month", "activity_month", "activity month"),
        "",
    )
    working["facility_id"] = _column_as_series(
        df,
        cols,
        ("reportingfacilityid", "facility_id", "facility id", "facility", "facility ba id"),
        "",
    )
    working["well_id"] = _column_as_series(
        df,
        cols,
        ("fromtoid", "from_to_id", "from to id"),
        "",
    ).fillna("").astype(str)
    working["from_to_type"] = _column_as_series(
        df,
        cols,
        ("fromtoidtype", "from_to_id_type", "from to id type"),
        "",
    ).fillna("").astype(str)
    working["activity_id"] = _column_as_series(
        df,
        cols,
        ("activityid", "activity_id", "activity id"),
        "",
    ).fillna("").astype(str)
    proration_product = _column_as_series(
        df,
        cols,
        ("prorationproduct", "proration product"),
        "",
    ).fillna("").astype(str)
    product_id = _column_as_series(df, cols, ("productid", "product id", "product"), "").fillna("").astype(str)
    working["product"] = proration_product.where(proration_product.str.strip() != "", product_id)
    working["volume"] = pd.to_numeric(_column_as_series(df, cols, ("volume",), 0), errors="coerce").fillna(0.0)

    if working.empty:
        return working

    product = working["product"].str.upper().str.strip()
    oil_mask = product.eq("OIL") | product.str.contains("OIL")
    gas_mask = (
        product.eq("GAS")
        | product.eq("ACGAS")
        | product.eq("ENTGAS")
        | product.eq("C1-MX")
        | product.str.contains("GAS")
    )
    water_mask = (
        product.eq("WATER")
        | product.eq("FSHWTR")
        | product.eq("BRKWTR")
        | product.str.contains("WATER")
    )
    condensate_mask = (
        product.eq("COND")
        | product.eq("C5-SP")
        | product.eq("C4-SP")
        | product.str.contains("COND")
        | product.str.contains("C5")
    )
    working["oil_bbl"] = working["volume"].where(oil_mask, 0.0)
    working["gas_mcf"] = working["volume"].where(gas_mask, 0.0)
    working["water_bbl"] = working["volume"].where(water_mask, 0.0)
    working["condensate_bbl"] = working["volume"].where(condensate_mask, 0.0)

    well_id_is_wi = working["from_to_type"].str.upper().str.strip().eq("WI")
    well_id_looks_like_wi = working["well_id"].str.upper().str.startswith("ABWI")
    working["well_id"] = working["well_id"].where(well_id_is_wi | well_id_looks_like_wi, "")

    useful_mask = (
        working[["oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl"]].ne(0.0).any(axis=1)
        | working["activity_id"].str.strip().ne("")
    )
    return working.loc[
        useful_mask,
        ["month", "facility_id", "well_id", "activity_id", "oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl"],
    ].reset_index(drop=True)
