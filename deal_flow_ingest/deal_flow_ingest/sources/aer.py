from __future__ import annotations

import logging
import re
from pathlib import Path
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

    if source.landing_page_url:
        try:
            downloader.fetch(source.key, source.landing_page_url, refresh=refresh, file_type="html")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch ST37 landing page %s: %s", source.landing_page_url, exc)
    if not source.dataset_url:
        LOGGER.warning(
            "ST37 source has no dataset_url configured. Landing page metadata fetched only; returning empty dataframe."
        )
        return pd.DataFrame()

    result = downloader.fetch(
        source.key,
        source.dataset_url,
        refresh=refresh,
        file_type=source.file_type,
        extract_zip=(source.file_type or "").lower() == "zip",
    )

    target_path = _resolve_st37_artifact(result.path, result.extracted_dir)
    if target_path is None:
        LOGGER.warning("ST37 artifact did not contain a parseable TXT/CSV file: %s", result.path)
        return pd.DataFrame()

    return _parse_well_table(target_path)


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
    preferred = sorted(text_like, key=lambda m: (0 if m.lower().endswith(".txt") else 1, len(m)))
    return preferred[0]


def _find_best_text_candidate(extracted_dir: Path) -> Path | None:
    files = [p for p in extracted_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".txt", ".csv"}]
    if not files:
        return None
    txt = [f for f in files if f.suffix.lower() == ".txt"]
    return sorted(txt or files, key=lambda p: len(str(p)))[0]


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
