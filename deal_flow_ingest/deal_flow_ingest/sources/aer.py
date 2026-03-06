from __future__ import annotations

import logging
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
from zipfile import BadZipFile, ZipFile

import pandas as pd

from deal_flow_ingest.config import SourcePayload
from deal_flow_ingest.io.downloader import Downloader

LOGGER = logging.getLogger(__name__)


def load_st37(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    local_live_file = (source.local_live_file or "").strip()
    if local_live_file:
        local_path = Path(local_live_file).expanduser()
        if local_path.exists() and local_path.is_file():
            target_path = _resolve_st37_artifact(local_path, extracted_dir=None)
            if target_path is None:
                LOGGER.warning("ST37 local live file did not contain a parseable TXT/CSV file: %s", local_path)
                return pd.DataFrame()
            LOGGER.info("Using local live ST37 file: %s", local_path)
            return _parse_well_table(target_path)

    artifact_url = (source.dataset_url or "").strip()
    discovered_artifact = False

    if not artifact_url and not refresh:
        cached_url = _load_cached_discovered_artifact_url(downloader, source.key)
        if cached_url:
            LOGGER.info("Using cached discovered ST37 artifact URL: %s", cached_url)
            artifact_url = cached_url

    if not artifact_url:
        discovered_url = _discover_st37_from_landing_pages(downloader, source, refresh=refresh)
        if not discovered_url:
            LOGGER.warning("Unable to discover ST37 artifact URL from configured landing pages; returning empty dataframe.")
            return pd.DataFrame()
        artifact_url = discovered_url
        discovered_artifact = True

    result = downloader.fetch(
        source.key,
        artifact_url,
        refresh=refresh,
        file_type=_guess_artifact_file_type(artifact_url, source.file_type),
        extract_zip=artifact_url.lower().endswith(".zip") or (source.file_type or "").lower() == "zip",
    )

    target_path = _resolve_st37_artifact(result.path, result.extracted_dir)
    if target_path is None:
        LOGGER.warning("ST37 artifact did not contain a parseable TXT/CSV file: %s", result.path)
        return pd.DataFrame()

    parsed = _parse_well_table(target_path)
    if discovered_artifact:
        _cache_discovered_artifact_url(downloader, source.key, artifact_url)
        LOGGER.info("Parsed %s ST37 rows from discovered artifact", len(parsed.index))
    return parsed


def _discover_st37_from_landing_pages(downloader: Downloader, source: SourcePayload, refresh: bool) -> str | None:
    landing_urls = [u for u in [source.landing_page_url, *(source.alternate_landing_page_urls or [])] if u]
    for landing_url in landing_urls:
        LOGGER.info("Attempting ST37 discovery from landing page: %s", landing_url)
        try:
            landing_page = downloader.fetch(source.key, landing_url, refresh=refresh, file_type="html")
            landing_page_html = landing_page.path.read_text(encoding="utf-8", errors="replace")
            discovered_url = _discover_st37_artifact_url(landing_page_html, landing_url)
            if discovered_url:
                LOGGER.info("Discovered ST37 artifact URL from %s: %s", landing_url, discovered_url)
                return discovered_url
            LOGGER.warning("No ST37 artifact URL candidates found on landing page: %s", landing_url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch/parse ST37 landing page %s: %s", landing_url, exc)
    return None


def _load_cached_discovered_artifact_url(downloader: Downloader, source_key: str) -> str | None:
    meta = downloader._load_metadata(source_key)
    value = meta.get("discovered_artifact_url")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _cache_discovered_artifact_url(downloader: Downloader, source_key: str, artifact_url: str) -> None:
    meta = downloader._load_metadata(source_key)
    cached = meta.get("discovered_artifact_url")
    if cached == artifact_url:
        return
    meta["discovered_artifact_url"] = artifact_url
    downloader._save_metadata(source_key, meta)


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


def _discover_st37_artifact_url(landing_page_html: str, base_url: str) -> str | None:
    parser = _AnchorParser()
    parser.feed(landing_page_html)
    candidates: list[tuple[int, str]] = []
    for href, text in parser.links:
        raw_href = (href or "").strip()
        if not raw_href:
            continue
        lowered_href = raw_href.lower()
        if lowered_href.startswith("mailto:") or lowered_href.startswith("javascript:"):
            continue

        absolute_url = urljoin(base_url, raw_href)
        absolute_lower = absolute_url.lower()
        searchable = f"{text} {raw_href} {absolute_url}".lower()

        if "st37" not in searchable:
            continue
        if any(token in searchable for token in ["pdf", "shapefile", "shape file", ".shp", " shp "]):
            continue

        score = 0
        if "wells in alberta" in searchable:
            score += 8
        if "text format" in searchable or " text " in searchable:
            score += 6
        if absolute_lower.endswith(".zip") or ".zip" in searchable:
            score += 5
        if "txt" in searchable:
            score += 3
        if "st37" in searchable:
            score += 3

        candidates.append((score, absolute_url))

    if not candidates:
        return None

    best_score, best_url = max(candidates, key=lambda item: item[0])
    if best_score <= 0:
        return None
    return best_url


def _guess_artifact_file_type(url: str, default_file_type: str) -> str:
    lowered = url.lower()
    for token in ["zip", "txt", "csv", "html", "pdf"]:
        if lowered.endswith(f".{token}"):
            return token
    return default_file_type


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
    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path)
    elif suffix == ".txt":
        frame = _parse_st37_text(path)
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


def _resolve_st37_artifact(path: Path, extracted_dir: Path | None) -> Path | None:
    if path.suffix.lower() in {".txt", ".csv"}:
        return path

    if path.suffix.lower() == ".zip":
        if extracted_dir:
            candidate = _find_best_text_candidate(extracted_dir)
            if candidate:
                return candidate
        try:
            with ZipFile(path, "r") as archive:
                with archive.open(_pick_best_zip_member(archive.namelist())) as zipped_file:
                    extracted = path.parent / f"{path.stem}_st37.txt"
                    extracted.write_bytes(zipped_file.read())
                    return extracted
        except (BadZipFile, FileNotFoundError, KeyError) as exc:
            LOGGER.warning("Unable to extract ST37 ZIP artifact %s: %s", path, exc)
            return None

    return None


def _pick_best_zip_member(members: list[str]) -> str:
    text_like = [m for m in members if m.lower().endswith((".txt", ".csv"))]
    if not text_like:
        raise KeyError("No text-like member found in ZIP")
    preferred = sorted(text_like, key=lambda m: _st37_candidate_priority(Path(m)))
    return preferred[0]


def _find_best_text_candidate(extracted_dir: Path) -> Path | None:
    files = [p for p in extracted_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".txt", ".csv"}]
    if not files:
        return None
    return sorted(files, key=_st37_candidate_priority)[0]


def _st37_candidate_priority(path: Path) -> tuple[int, int, str]:
    name = path.name.lower()
    if path.suffix.lower() == ".txt" and "st37" in name:
        return (0, len(name), name)
    if path.suffix.lower() == ".txt":
        return (1, len(name), name)
    if path.suffix.lower() == ".csv":
        return (2, len(name), name)
    return (3, len(name), name)


def _parse_st37_text(path: Path) -> pd.DataFrame:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    non_empty = [line for line in lines if line.strip()]
    if not non_empty:
        return pd.DataFrame()

    header_idx, delimiter = _detect_header_and_delimiter(non_empty)
    if header_idx is not None and delimiter:
        parsed = _parse_delimited_st37(non_empty[header_idx:], delimiter)
    else:
        parsed = _parse_fixed_width_st37(non_empty)

    if parsed.empty:
        return parsed
    return parsed


def _detect_header_and_delimiter(lines: list[str]) -> tuple[int | None, str | None]:
    delimiters = ["|", "\t", ",", ";"]
    for idx, line in enumerate(lines[:25]):
        lower = line.lower()
        if "uwi" not in lower:
            continue
        for delimiter in delimiters:
            if delimiter in line:
                return idx, delimiter
    return None, None


def _parse_delimited_st37(lines: list[str], delimiter: str) -> pd.DataFrame:
    columns = [token.strip().lower() for token in lines[0].split(delimiter)]
    records: list[dict[str, str]] = []
    skipped = 0
    for line in lines[1:]:
        pieces = [token.strip() for token in line.split(delimiter)]
        if len(pieces) != len(columns):
            skipped += 1
            continue
        record = dict(zip(columns, pieces, strict=True))
        mapped = _map_st37_record(record)
        if not mapped["uwi"]:
            skipped += 1
            continue
        records.append(mapped)

    if skipped:
        LOGGER.info("ST37 parser skipped %s malformed delimited rows", skipped)
    return pd.DataFrame(records)


def _parse_fixed_width_st37(lines: list[str]) -> pd.DataFrame:
    records: list[dict[str, str]] = []
    skipped = 0
    for line in lines:
        uwi_match = re.search(r"\b\d{2}/\d{2}-\d{2}-\d{3}-\d{2}W\d\b", line)
        if not uwi_match:
            skipped += 1
            continue

        uwi = uwi_match.group(0)
        parts = [part.strip() for part in re.split(r"\s{2,}", line.strip()) if part.strip()]
        candidate_status = ""
        candidate_licensee = ""
        if len(parts) >= 2:
            if parts[0] == uwi and len(parts) > 1:
                candidate_status = parts[1]
            elif uwi in parts:
                uwi_idx = parts.index(uwi)
                candidate_status = parts[uwi_idx + 1] if len(parts) > uwi_idx + 1 else ""
        if len(parts) >= 3:
            candidate_licensee = parts[-1]

        lsd, section, township, rng, meridian = _parse_location_from_uwi(uwi)
        records.append(
            {
                "uwi": uwi,
                "status": candidate_status,
                "licensee": candidate_licensee,
                "lsd": lsd,
                "section": section,
                "township": township,
                "range": rng,
                "meridian": meridian,
            }
        )

    if skipped:
        LOGGER.info("ST37 parser skipped %s malformed fixed-width rows", skipped)
    return pd.DataFrame(records)


def _map_st37_record(record: dict[str, str]) -> dict[str, str]:
    uwi = _first_present(record, ["uwi", "well_id", "wellid", "well identifier"])
    status = _first_present(record, ["status", "well_status", "well status"])
    licensee = _first_present(record, ["licensee", "operator", "licensee/operator"])
    township = _first_present(record, ["township", "twp"])
    rng = _first_present(record, ["range", "rng"])
    section = _first_present(record, ["section", "sec"])
    meridian = _first_present(record, ["meridian", "mer"])

    lsd = _first_present(record, ["lsd", "legal_subdivision", "legal subdivision"])
    if uwi and (not township or not rng or not section or not meridian):
        parsed_lsd, parsed_section, parsed_township, parsed_range, parsed_meridian = _parse_location_from_uwi(uwi)
        township = township or parsed_township
        rng = rng or parsed_range
        section = section or parsed_section
        meridian = meridian or parsed_meridian
        lsd = lsd or parsed_lsd

    return {
        "uwi": uwi,
        "status": status,
        "licensee": licensee,
        "township": township,
        "range": rng,
        "section": section,
        "meridian": meridian,
        "lsd": lsd,
    }


def _first_present(record: dict[str, str], keys: list[str]) -> str:
    lowered = {k.lower().strip(): str(v).strip() for k, v in record.items()}
    for key in keys:
        value = lowered.get(key)
        if value and value.lower() != "nan":
            return value
    return ""


def _parse_location_from_uwi(uwi: str) -> tuple[str, str, str, str, str]:
    match = re.search(r"(?P<lsd>\d{2})/(?P<section>\d{2})-(?P<township>\d{2,3})-(?P<range>\d{2,3})-(?P<meridian>\d{2})W(?P<base>\d)", uwi)
    if not match:
        return "", "", "", "", ""
    groups = match.groupdict()
    return groups["lsd"], groups["section"], groups["township"], groups["range"], groups["base"]


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
