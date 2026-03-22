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
            parsed = _parse_well_table(target_path)
            if not parsed.empty:
                parsed["source"] = source.key
            return parsed

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
    if not parsed.empty:
        parsed["source"] = source.key
    return parsed


def load_general_well_data(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    return _load_aer_tabular_source(
        downloader,
        source,
        refresh,
        discovery_keywords=["general", "well", "data"],
        parser=load_general_well_data_frame,
    )


def load_st102_facility_list(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    return _load_aer_tabular_source(
        downloader,
        source,
        refresh,
        discovery_keywords=["st102", "facility", "list"],
        parser=load_st102_facility_list_frame,
    )


def load_spatial_pipelines(downloader: Downloader, source: SourcePayload, refresh: bool) -> pd.DataFrame:
    return _load_aer_tabular_source(
        downloader,
        source,
        refresh,
        discovery_keywords=["pipeline"],
        parser=load_spatial_pipelines_frame,
    )


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


def _discover_aer_artifact_url(landing_page_html: str, base_url: str, keywords: list[str]) -> str | None:
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
        if not any(keyword in searchable for keyword in keywords):
            continue
        if _is_rejected_aer_candidate(searchable):
            continue
        if not any(token in absolute_lower for token in [".csv", ".xlsx", ".xls", ".zip"]):
            continue

        score = 0
        for keyword in keywords:
            if keyword in searchable:
                score += 5
        if "all alberta" in searchable or "all-alberta" in searchable:
            score += 8
        if "report" in searchable:
            score += 2
        if absolute_lower.endswith(".xlsx") or absolute_lower.endswith(".xls"):
            score += 4
        if absolute_lower.endswith(".csv"):
            score += 3
        if absolute_lower.endswith(".zip"):
            score += 2
        candidates.append((score, absolute_url))

    if not candidates:
        return None

    best_score, best_url = max(candidates, key=lambda item: item[0])
    if best_score <= 0:
        return None
    return best_url


def _guess_artifact_file_type(url: str, default_file_type: str) -> str:
    lowered = url.lower()
    for token in ["zip", "txt", "csv", "html", "pdf", "xlsx", "xls"]:
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


def _load_aer_tabular_source(
    downloader: Downloader,
    source: SourcePayload,
    refresh: bool,
    *,
    discovery_keywords: list[str],
    parser,
) -> pd.DataFrame:
    local_live_file = (source.local_live_file or "").strip()
    if local_live_file:
        local_path = Path(local_live_file).expanduser()
        if local_path.exists() and local_path.is_file():
            target_path = _resolve_tabular_artifact(local_path, extracted_dir=None)
            if target_path is None:
                LOGGER.warning("AER local live file did not contain a parseable tabular file: %s", local_path)
                return pd.DataFrame()
            parsed = parser(_read_tabular(target_path))
            if not parsed.empty:
                parsed["source"] = source.key
            return parsed

    artifact_url = (source.dataset_url or "").strip()
    discovered_artifact = False

    if not artifact_url and not refresh:
        cached_url = _load_cached_discovered_artifact_url(downloader, source.key)
        if cached_url and not _is_rejected_aer_candidate(cached_url.lower()):
            artifact_url = cached_url

    if not artifact_url and source.landing_page_url:
        try:
            landing_page = downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
            landing_page_html = landing_page.path.read_text(encoding="utf-8", errors="replace")
            artifact_url = _discover_aer_artifact_url(landing_page_html, source.landing_page_url, discovery_keywords)
            discovered_artifact = bool(artifact_url)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch/parse AER landing page %s: %s", source.landing_page_url, exc)

    if not artifact_url:
        LOGGER.warning("Unable to discover AER artifact URL for %s", source.key)
        return pd.DataFrame()

    result = downloader.fetch(
        source.key,
        artifact_url,
        refresh=refresh,
        file_type=_guess_artifact_file_type(artifact_url, source.file_type),
        extract_zip=artifact_url.lower().endswith(".zip") or (source.file_type or "").lower() == "zip",
    )
    target_path = _resolve_tabular_artifact(result.path, result.extracted_dir)
    if target_path is None:
        LOGGER.warning("AER artifact did not contain a parseable table: %s", result.path)
        return pd.DataFrame()

    parsed = parser(target_path)
    if discovered_artifact:
        _cache_discovered_artifact_url(downloader, source.key, artifact_url)
    if not parsed.empty:
        parsed["source"] = source.key
    return parsed


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

    columns = [str(c) for c in frame.columns]
    uwi_column = _find_first_matching_column(columns, ["uwi", "well_id", "wellid", "well identifier"])
    status_column = _find_first_matching_column(columns, ["status", "well_status", "well status"])
    licensee_column = _find_best_licensee_column(columns)

    scored_candidates = _score_licensee_candidates(columns)
    LOGGER.info("ST37 detected licensee column: %s", licensee_column or "<none>")
    LOGGER.debug(
        "ST37 top licensee column candidates: %s",
        [f"{name}={score}" for name, score in scored_candidates[:5]],
    )
    if not licensee_column:
        LOGGER.warning("ST37 parser did not detect a licensee/operator column; defaulting to empty licensee values")

    out = pd.DataFrame()
    out["uwi"] = frame.get(uwi_column, "")
    out["status"] = frame.get(status_column, "UNKNOWN")
    out["licensee"] = frame.get(licensee_column, "")

    non_empty_licensee = out["licensee"].fillna("").astype(str).str.strip()
    non_empty_count = int((non_empty_licensee != "").sum())
    distinct_count = int(non_empty_licensee[non_empty_licensee != ""].nunique())
    LOGGER.info(
        "ST37 parsed licensee values: non_empty=%s distinct_pre_normalization=%s",
        non_empty_count,
        distinct_count,
    )

    for field in ["township", "range", "section", "meridian", "lat", "lon", "lsd"]:
        out[field] = frame.get(_find_first_matching_column(columns, [field]), None)
    return out.dropna(subset=["uwi"], how="all")


def _normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[\s\-/\\.,;:()\[\]{}]+", "_", str(name).strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _find_first_matching_column(columns: list[str], candidates: list[str]) -> str | None:
    normalized_lookup = {_normalize_column_name(column): column for column in columns}
    for candidate in candidates:
        normalized_candidate = _normalize_column_name(candidate)
        column = normalized_lookup.get(normalized_candidate)
        if column is not None:
            return column
    return None


def _score_licensee_candidates(columns: list[str]) -> list[tuple[str, int]]:
    scored: list[tuple[str, int]] = []
    for column in columns:
        normalized = _normalize_column_name(column)
        tokens = set(normalized.split("_")) if normalized else set()

        score = 0
        has_operator_meaning = False

        if "licensee" in tokens or "licensee" in normalized:
            score += 70
            has_operator_meaning = True
        if "operator" in tokens or "operator" in normalized:
            score += 65
            has_operator_meaning = True
        if "business_associate" in normalized:
            score += 55
            has_operator_meaning = True
        if "ba" in tokens:
            score += 40
            has_operator_meaning = True
        if "company" in tokens:
            score += 45
            has_operator_meaning = True

        has_name = "name" in tokens
        has_id = "id" in tokens
        has_code = "code" in tokens

        if has_name:
            score += 35
        if has_operator_meaning and has_name:
            score += 25

        if has_operator_meaning and has_id and not has_name:
            score -= 50
        if has_operator_meaning and has_code and not has_name:
            score -= 35

        if any(token in tokens for token in {"status", "uwi", "well", "field", "pool", "date", "latitude", "longitude"}):
            score -= 45

        if not has_operator_meaning:
            score -= 30

        scored.append((column, score))

    return sorted(scored, key=lambda item: (-item[1], _normalize_column_name(item[0]), item[0]))


def _find_best_licensee_column(columns: list[str]) -> str | None:
    scored = _score_licensee_candidates(columns)
    if not scored:
        return None
    best_column, best_score = scored[0]
    if best_score <= 0:
        return None
    return best_column


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


def _resolve_tabular_artifact(path: Path, extracted_dir: Path | None) -> Path | None:
    if path.suffix.lower() in {".txt", ".csv", ".xlsx", ".xls", ".shp", ".dbf"} or _looks_like_excel_artifact(path):
        return path

    if path.suffix.lower() == ".zip" and extracted_dir and extracted_dir.exists():
        files = sorted((p for p in extracted_dir.rglob("*") if p.is_file()), key=_tabular_candidate_priority)
        return files[0] if files else None

    return None


def _is_rejected_aer_candidate(searchable: str) -> bool:
    return any(token in searchable for token in ["greater than 9", "greater-than-9", "drilling event sequences"])


def _looks_like_excel_artifact(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return True
    header = path.read_bytes()[:8]
    if header.startswith(b"PK\x03\x04"):
        try:
            with ZipFile(path, "r") as archive:
                return "[Content_Types].xml" in archive.namelist()
        except (BadZipFile, OSError):
            return False
    return header.startswith(b"\xd0\xcf\x11\xe0")


def _tabular_candidate_priority(path: Path) -> tuple[int, int, str]:
    suffix = path.suffix.lower()
    name = path.name.lower()
    if suffix == ".shp" and "gcs" in name:
        return (0, len(name), name)
    if suffix == ".shp":
        return (1, len(name), name)
    if suffix == ".dbf" and "gcs" in name:
        return (2, len(name), name)
    if suffix == ".dbf":
        return (3, len(name), name)
    if suffix in {".xlsx", ".xls"}:
        return (4, len(name), name)
    if suffix in {".csv", ".txt"}:
        return (5, len(name), name)
    if _looks_like_excel_artifact(path):
        return (6, len(name), name)
    return (7, len(name), name)


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
        LOGGER.info("ST37 parser mode=delimited delimiter=%s", delimiter)
        parsed = _parse_delimited_st37(non_empty[header_idx:], delimiter)
    else:
        LOGGER.info("ST37 parser mode=fixed_width")
        parsed = _parse_fixed_width_st37(non_empty)

    if parsed.empty:
        return parsed
    LOGGER.info("ST37 parser rows parsed=%s", len(parsed.index))
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
        candidate_licensee = _extract_licensee_fixed_width(parts, uwi)

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




def _extract_licensee_fixed_width(parts: list[str], uwi: str) -> str:
    if not parts:
        return ""
    if uwi in parts:
        uwi_idx = parts.index(uwi)
        trailing = [p for p in parts[uwi_idx + 2 :] if p]
    elif parts and parts[0] == uwi:
        trailing = [p for p in parts[2:] if p]
    else:
        trailing = [p for p in parts[1:] if p]

    if not trailing:
        return ""

    for candidate in reversed(trailing):
        cleaned = candidate.strip()
        if not cleaned:
            continue
        upper = cleaned.upper()
        if upper in {"ACTIVE", "INACTIVE", "SUSPENDED", "SHUT-IN", "SHUTIN"}:
            continue
        if re.fullmatch(r"[A-Z]?\d+", upper):
            continue
        tokens = [t for t in re.split(r"\s+", cleaned) if t]
        alpha_tokens = [t for t in tokens if re.search(r"[A-Za-z]", t)]
        has_digits = bool(re.search(r"\d", cleaned))
        if len(alpha_tokens) >= 2:
            return cleaned
        if len(alpha_tokens) == 1 and len(alpha_tokens[0]) >= 5 and " " in cleaned and not has_digits:
            return cleaned
    return ""

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
    if suffix in {".xlsx", ".xls"} or _looks_like_excel_artifact(path):
        return pd.read_excel(path)
    return pd.DataFrame()


def _column_as_series(df: pd.DataFrame, cols: dict[str, str], aliases: tuple[str, ...], default) -> pd.Series:
    for alias in aliases:
        normalized = _normalize_column_name(alias)
        if normalized in cols:
            return df[cols[normalized]]
    return pd.Series([default] * len(df), index=df.index)


def _coerce_tabular_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    if isinstance(source, Path):
        return _read_tabular(source)
    return source


def load_general_well_data_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["uwi"] = _column_as_series(df, cols, ("uwi", "well_id", "well identifier"), "")
    out["status"] = _column_as_series(df, cols, ("current_status", "well_status", "status", "licence_status"), "UNKNOWN")
    out["licensee"] = _column_as_series(df, cols, ("licensee_name", "licensee", "operator_name"), "")
    out["license_number"] = _column_as_series(
        df,
        cols,
        ("licence_number", "license_number", "licence_no", "license_no"),
        "",
    )
    out["well_name"] = _column_as_series(df, cols, ("well_name",), "")
    out["field_name"] = _column_as_series(df, cols, ("field_name", "field"), "")
    out["pool_name"] = _column_as_series(df, cols, ("pool_name", "pool"), "")
    out["spud_date"] = _column_as_series(df, cols, ("spud_date", "spud date"), None)
    out["lsd"] = _column_as_series(df, cols, ("lsd", "legal_subdivision"), "")
    out["section"] = _column_as_series(df, cols, ("section",), None)
    out["township"] = _column_as_series(df, cols, ("township",), None)
    out["range"] = _column_as_series(df, cols, ("range",), None)
    out["meridian"] = _column_as_series(df, cols, ("meridian",), None)
    out["lat"] = _column_as_series(df, cols, ("latitude", "lat"), None)
    out["lon"] = _column_as_series(df, cols, ("longitude", "lon", "lng"), None)

    return out[out["uwi"].fillna("").astype(str).str.strip() != ""].copy()


def _read_st102_shapefile(path: Path) -> pd.DataFrame:
    try:
        import shapefile
    except ImportError as exc:  # pragma: no cover - dependency managed in environment/tests
        raise RuntimeError("pyshp is required to parse ST102 facility shapefiles") from exc

    shp_path = path if path.suffix.lower() == ".shp" else path.with_suffix(".shp")
    if not shp_path.exists():
        return pd.DataFrame()

    reader = shapefile.Reader(str(shp_path), encoding="utf-8")
    records: list[dict[str, object]] = []
    for shape_record in reader.iterShapeRecords():
        row = shape_record.record.as_dict()
        points = shape_record.shape.points
        if points:
            lon, lat = points[0][:2]
            row["longitude"] = lon
            row["latitude"] = lat
        records.append(row)
    return pd.DataFrame(records)


def load_st102_facility_list_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    if isinstance(source, Path) and source.suffix.lower() in {".shp", ".dbf"}:
        df = _read_st102_shapefile(source)
    else:
        df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    license_number = _column_as_series(
        df,
        cols,
        ("licence_number", "license_number", "facility licence number", "lic_number"),
        "",
    )
    out["facility_id"] = _column_as_series(
        df,
        cols,
        ("facility_id", "facility_ba_id", "facility_ba_identifier", "fac_id"),
        "",
    )
    out["facility_id"] = out["facility_id"].fillna("").astype(str)
    empty_id = out["facility_id"].str.strip() == ""
    out.loc[empty_id, "facility_id"] = license_number.fillna("").astype(str)[empty_id]
    out["license_number"] = license_number
    out["facility_name"] = _column_as_series(df, cols, ("facility_name", "fac_name"), "")
    out["facility_type"] = _column_as_series(df, cols, ("facility_type", "edct_descr", "lic_type", "edct_type"), "")
    out["facility_subtype"] = _column_as_series(
        df,
        cols,
        ("facility_sub_type", "facility_subtype", "facility subtype", "fac_sub_ty", "sub_code"),
        "",
    )
    out["facility_operator"] = _column_as_series(
        df,
        cols,
        ("current_operator_name", "operator_name", "licensee_name", "facility_operator", "operator"),
        "",
    )
    out["facility_status"] = _column_as_series(df, cols, ("facility_status", "status", "fac_status"), "")
    out["lsd"] = _column_as_series(df, cols, ("lsd", "legal_subdivision"), "")
    out["section"] = _column_as_series(df, cols, ("section",), None)
    out["township"] = _column_as_series(df, cols, ("township",), None)
    out["range"] = _column_as_series(df, cols, ("range",), None)
    out["meridian"] = _column_as_series(df, cols, ("meridian",), None)
    out["lat"] = _column_as_series(df, cols, ("latitude", "lat"), None)
    out["lon"] = _column_as_series(df, cols, ("longitude", "lon", "lng"), None)

    out["facility_id"] = out["facility_id"].fillna("").astype(str).str.strip()
    return out[out["facility_id"] != ""].copy()


def _legacy_load_spatial_pipelines_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    if isinstance(source, Path) and source.suffix.lower() in {".shp", ".dbf"}:
        df = _read_pipeline_shapefile(source)
    else:
        df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["pipeline_id"] = _column_as_series(df, cols, ("pipeline_licence_segment_id", "pllicsegid"), "")
    out["license_number"] = _column_as_series(df, cols, ("licence_number", "license_number", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no", "licence", "licence_no", "licence_number", "licence_number", "lic_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "licence_no", "lic_no"), "")
    out["line_number"] = _column_as_series(df, cols, ("segment_line_number", "line_no"), "")
    out["licence_line_number"] = _column_as_series(df, cols, ("licence_line_number", "lic_li_no"), "")
    out["company_name"] = _column_as_series(df, cols, ("company_name", "comp_name"), "")
    out["ba_code"] = _column_as_series(df, cols, ("ba_code",), "")
    out["segment_status"] = _column_as_series(df, cols, ("segment_status", "seg_status"), "")
    out["from_facility_type"] = _column_as_series(df, cols, ("segment_from_facility", "from_fac", "from_facility"), "")
    out["from_location"] = _column_as_series(df, cols, ("from_location", "from_loc"), "")
    out["to_facility_type"] = _column_as_series(df, cols, ("segment_to_facility", "to_fac", "to_facility"), "")
    out["to_location"] = _column_as_series(df, cols, ("to_location", "to_loc"), "")
    out["substance1"] = _column_as_series(df, cols, ("substance_1", "substance1"), "")
    out["substance2"] = _column_as_series(df, cols, ("substance_2", "substance2"), "")
    out["substance3"] = _column_as_series(df, cols, ("substance_3", "substance3"), "")
    out["segment_length_km"] = _column_as_series(df, cols, ("segment_length", "seg_length"), None)
    out["geometry_source"] = _column_as_series(df, cols, ("geometry_source", "geom_srce"), "")
    out["centroid_lat"] = _column_as_series(df, cols, ("centroid_lat",), None)
    out["centroid_lon"] = _column_as_series(df, cols, ("centroid_lon",), None)

    out["pipeline_id"] = out["pipeline_id"].fillna("").astype(str).str.strip()
    return out[out["pipeline_id"] != ""].copy()


def _read_pipeline_shapefile(path: Path) -> pd.DataFrame:
    try:
        import shapefile
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pyshp is required to parse AER pipeline shapefiles") from exc

    shp_path = path if path.suffix.lower() == ".shp" else path.with_suffix(".shp")
    if not shp_path.exists():
        return pd.DataFrame()

    reader = shapefile.Reader(str(shp_path), encoding="utf-8")
    records: list[dict[str, object]] = []
    for idx in range(reader.numRecords):
        row = reader.record(idx).as_dict()
        shape = reader.shape(idx)
        points = shape.points
        if points:
            xs = [point[0] for point in points]
            ys = [point[1] for point in points]
            row["centroid_lon"] = (min(xs) + max(xs)) / 2.0
            row["centroid_lat"] = (min(ys) + max(ys)) / 2.0
        records.append(row)
    return pd.DataFrame(records)


def load_spatial_pipelines_frame(source: pd.DataFrame | Path) -> pd.DataFrame:
    if isinstance(source, Path) and source.suffix.lower() in {".shp", ".dbf"}:
        df = _read_pipeline_shapefile(source)
    else:
        df = _coerce_tabular_frame(source)
    if df.empty:
        return pd.DataFrame()

    cols = {_normalize_column_name(column): column for column in df.columns}
    out = pd.DataFrame(index=df.index)
    out["pipeline_id"] = _column_as_series(df, cols, ("pipeline_licence_segment_id", "pllicsegid"), "")
    out["license_number"] = _column_as_series(
        df,
        cols,
        ("licence_number", "license_number", "licence_no", "lic_no", "licence"),
        "",
    )
    out["line_number"] = _column_as_series(df, cols, ("segment_line_number", "line_no"), "")
    out["licence_line_number"] = _column_as_series(df, cols, ("licence_line_number", "lic_li_no"), "")
    out["company_name"] = _column_as_series(df, cols, ("company_name", "comp_name"), "")
    out["ba_code"] = _column_as_series(df, cols, ("ba_code",), "")
    out["segment_status"] = _column_as_series(df, cols, ("segment_status", "seg_status"), "")
    out["from_facility_type"] = _column_as_series(df, cols, ("segment_from_facility", "from_fac", "from_facility"), "")
    out["from_location"] = _column_as_series(df, cols, ("from_location", "from_loc"), "")
    out["to_facility_type"] = _column_as_series(df, cols, ("segment_to_facility", "to_fac", "to_facility"), "")
    out["to_location"] = _column_as_series(df, cols, ("to_location", "to_loc"), "")
    out["substance1"] = _column_as_series(df, cols, ("substance_1", "substance1"), "")
    out["substance2"] = _column_as_series(df, cols, ("substance_2", "substance2"), "")
    out["substance3"] = _column_as_series(df, cols, ("substance_3", "substance3"), "")
    out["segment_length_km"] = _column_as_series(df, cols, ("segment_length", "seg_length"), None)
    out["geometry_source"] = _column_as_series(df, cols, ("geometry_source", "geom_srce"), "")
    out["centroid_lat"] = _column_as_series(df, cols, ("centroid_lat",), None)
    out["centroid_lon"] = _column_as_series(df, cols, ("centroid_lon",), None)

    out["pipeline_id"] = out["pipeline_id"].fillna("").astype(str).str.strip()
    return out[out["pipeline_id"] != ""].copy()
