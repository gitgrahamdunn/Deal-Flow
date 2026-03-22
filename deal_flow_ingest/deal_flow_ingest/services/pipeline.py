from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from deal_flow_ingest.config import get_database_url, iter_enabled_sources, load_config
from deal_flow_ingest.db.load import (
    json_compat_frame,
    load_bridge_well_facility,
    new_run_id,
    record_ingestion_run,
    replace_bridge_crown_disposition_client,
    replace_bridge_crown_disposition_land,
    replace_fact_by_month_range,
    replace_fact_liability,
    replace_fact_metrics,
    replace_fact_operator_prod,
    replace_fact_restart,
    replace_fact_well_status,
    upsert_bridge_operator_business_associate,
    _dedupe_wells_df,
    upsert_dim_business_associate,
    upsert_dim_crown_client,
    upsert_dim_crown_disposition,
    upsert_dim_facility,
    upsert_dim_operator,
    upsert_dim_pipeline,
    upsert_dim_well,
)
from deal_flow_ingest.db.migrate import upgrade_to_head
from deal_flow_ingest.db.schema import (
    DimOperator,
    DimCrownClient,
    DimCrownDisposition,
    DimPipeline,
    DimWell,
    FactFacilityProductionMonthly,
    FactOperatorLiability,
    FactOperatorMetrics,
    FactOperatorProductionMonthly,
    FactWellProductionMonthly,
    FactWellRestartScore,
    FactWellStatus,
    get_engine,
)
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources import load_dataset
from deal_flow_ingest.transform.metrics import compute_operator_metrics, compute_well_restart_scores, empty_restart_scores_df
from deal_flow_ingest.transform.normalize import month_start, normalize_lsd, normalize_operator_name, normalize_uwi

LOGGER = logging.getLogger(__name__)


class _PhaseLogger:
    def __init__(self, name: str, rows_getter=None):
        self.name = name
        self.rows_getter = rows_getter
        self.started = 0.0

    def __enter__(self):
        self.started = time.perf_counter()
        LOGGER.info("[phase] %s - start", self.name)
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.perf_counter() - self.started
        rows = None
        if self.rows_getter is not None:
            try:
                rows = self.rows_getter()
            except Exception:  # pragma: no cover - logging fallback
                rows = None
        if rows is None:
            LOGGER.info("[phase] %s - end elapsed=%.2fs", self.name, elapsed)
        else:
            LOGGER.info("[phase] %s - end rows=%s elapsed=%.2fs", self.name, rows, elapsed)


def _phase(name: str, rows_getter=None) -> _PhaseLogger:
    return _PhaseLogger(name, rows_getter)

WELL_DIM_COLUMNS = [
    "well_id",
    "uwi_raw",
    "license_number",
    "well_name",
    "field_name",
    "pool_name",
    "status",
    "licensee_operator_id",
    "spud_date",
    "lsd",
    "section",
    "township",
    "range",
    "meridian",
    "lat",
    "lon",
    "first_seen",
    "last_seen",
    "source",
]
FACILITY_DIM_COLUMNS = [
    "facility_id",
    "facility_name",
    "license_number",
    "facility_type",
    "facility_subtype",
    "facility_operator_id",
    "facility_status",
    "lsd",
    "section",
    "township",
    "range",
    "meridian",
    "lat",
    "lon",
    "source",
]
PIPELINE_DIM_COLUMNS = [
    "pipeline_id",
    "license_number",
    "line_number",
    "licence_line_number",
    "operator_id",
    "company_name",
    "ba_code",
    "segment_status",
    "from_facility_type",
    "from_location",
    "to_facility_type",
    "to_location",
    "substance1",
    "substance2",
    "substance3",
    "segment_length_km",
    "geometry_source",
    "geometry_wkt",
    "centroid_lat",
    "centroid_lon",
    "source",
]
WELL_STATUS_COLUMNS = ["well_id", "status", "status_date", "source"]
RESTART_INPUT_COLUMNS = ["well_id", "status"]
FACILITY_PROD_COLUMNS = ["month", "facility_id", "oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl", "source"]
BRIDGE_COLUMNS = ["well_id", "facility_id", "effective_from", "effective_to", "source"]
OPERATOR_PROD_COLUMNS = ["month", "operator_id", "oil_bbl", "gas_mcf", "water_bbl", "basis_level", "source"]
LIABILITY_COLUMNS = [
    "as_of_date",
    "operator_id",
    "inactive_wells",
    "active_wells",
    "deemed_assets",
    "deemed_liabilities",
    "ratio",
    "source",
]
CROWN_DISPOSITION_COLUMNS = [
    "disposition_id",
    "agreement_no",
    "disposition_type",
    "disposition_status",
    "effective_from",
    "effective_to",
    "source",
]
CROWN_CLIENT_COLUMNS = ["client_id", "client_name_raw", "client_name_norm"]
CROWN_PARTICIPANT_COLUMNS = [
    "disposition_id",
    "client_id",
    "role_type",
    "interest_pct",
    "effective_from",
    "effective_to",
    "source",
]
CROWN_LAND_COLUMNS = ["disposition_id", "tract_no", "lsd", "section", "township", "range", "meridian", "source"]


@dataclass
class PipelineResult:
    run_id: str
    sources_ok: list[str]
    sources_failed: dict[str, str]
    source_summaries: list[dict[str, object]]
    top_prod: pd.DataFrame
    top_restart: pd.DataFrame


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _parse_date(s: str | None, default: date) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date() if s else default


def _default_live_end_date() -> date:
    today = date.today()
    month_start_today = today.replace(day=1)
    return month_start_today - timedelta(days=1)


def _sqlite_db_path(db_url: str) -> Path | None:
    parsed = make_url(db_url)
    if parsed.get_backend_name() != "sqlite":
        return None
    if not parsed.database or parsed.database == ":memory:":
        return None
    db_path = Path(unquote(parsed.database))
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path


def ensure_database_dir() -> None:
    db_url = get_database_url()
    if db_url.startswith("sqlite:///"):
        db_path = Path(db_url.removeprefix("sqlite:///"))
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)


def reset_database(force: bool, include_cache: bool = False) -> tuple[int, str]:
    database_url = get_database_url()
    sqlite_path = _sqlite_db_path(database_url)

    if sqlite_path:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        if sqlite_path.exists():
            sqlite_path.unlink()
        LOGGER.info("Deleted SQLite database: %s", sqlite_path)
    elif not force:
        warning = "Refusing to drop non-SQLite database without --force"
        LOGGER.warning(warning)
        return 1, warning

    if include_cache:
        raw_dir = Path("data/raw")
        if raw_dir.exists():
            for child in raw_dir.iterdir():
                if child.is_file():
                    child.unlink(missing_ok=True)
                elif child.is_dir():
                    for nested in sorted(child.rglob("*"), reverse=True):
                        if nested.is_file():
                            nested.unlink(missing_ok=True)
                        elif nested.is_dir():
                            nested.rmdir()
                    child.rmdir()
            LOGGER.info("Cleared raw cache directory: %s", raw_dir)

    upgrade_to_head()
    LOGGER.info("Database reset complete")
    return 0, "Database reset complete"


def _latest_artifact_metadata(downloader: Downloader, source_key: str) -> dict[str, Any]:
    metadata = downloader._load_metadata(source_key)
    artifacts = metadata.get("artifacts", {})
    candidates = [value for value in artifacts.values() if isinstance(value, dict)]
    if not candidates:
        return {}
    return max(candidates, key=lambda item: str(item.get("fetched_at", "")))


def _build_source_summary(
    source,
    *,
    dry_run: bool,
    rows_parsed: int,
    load_status: str,
    artifact_downloaded: bool,
    last_result=None,
    error: str | None = None,
) -> dict[str, object]:
    summary: dict[str, object] = {
        "source": source.key,
        "data_kind": source.data_kind,
        "maturity": source.maturity,
        "required_for_live": source.required_for_live,
        "rows_parsed": rows_parsed,
        "artifact_downloaded": artifact_downloaded,
        "load_status": load_status,
    }
    if dry_run:
        summary["sample_file"] = source.local_sample or ""
    elif last_result is not None:
        summary["artifact_url"] = getattr(last_result, "url", None)
        summary["artifact_path"] = str(last_result.path)
        summary["status_code"] = int(last_result.status_code)
        summary["checksum"] = last_result.checksum
        summary["content_type"] = last_result.content_type
    if error:
        summary["error"] = error
    return summary


def _validate_required_live_sources(
    source_entries: list,
    row_counts: dict[str, Any],
    sources_failed: dict[str, str],
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    for source in source_entries:
        if source.required_for_live and source.key in sources_failed:
            errors.append(f"{source.key}: {sources_failed[source.key]}")
        if not source.required_for_live and row_counts["source_rows"].get(source.key, 0) == 0:
            warnings.append(f"{source.key}: optional live source returned 0 rows")
    return errors, warnings


def _collect_database_validation(conn) -> dict[str, Any]:
    table_counts = {
        "dim_operator": int(conn.execute(select(func.count()).select_from(DimOperator)).scalar_one()),
        "dim_well": int(conn.execute(select(func.count()).select_from(DimWell)).scalar_one()),
        "dim_crown_disposition": int(conn.execute(select(func.count()).select_from(DimCrownDisposition)).scalar_one()),
        "dim_crown_client": int(conn.execute(select(func.count()).select_from(DimCrownClient)).scalar_one()),
        "fact_facility_production_monthly": int(
            conn.execute(select(func.count()).select_from(FactFacilityProductionMonthly)).scalar_one()
        ),
        "dim_pipeline": int(conn.execute(select(func.count()).select_from(DimPipeline)).scalar_one()),
        "fact_well_production_monthly": int(
            conn.execute(select(func.count()).select_from(FactWellProductionMonthly)).scalar_one()
        ),
        "fact_operator_production_monthly": int(
            conn.execute(select(func.count()).select_from(FactOperatorProductionMonthly)).scalar_one()
        ),
        "fact_operator_liability": int(conn.execute(select(func.count()).select_from(FactOperatorLiability)).scalar_one()),
        "fact_well_status": int(conn.execute(select(func.count()).select_from(FactWellStatus)).scalar_one()),
        "fact_well_restart_score": int(conn.execute(select(func.count()).select_from(FactWellRestartScore)).scalar_one()),
        "fact_operator_metrics": int(conn.execute(select(func.count()).select_from(FactOperatorMetrics)).scalar_one()),
    }
    orphan_checks = {
        "fact_well_production_without_dim_well": int(
            conn.execute(
                select(func.count())
                .select_from(FactWellProductionMonthly)
                .outerjoin(DimWell, FactWellProductionMonthly.well_id == DimWell.well_id)
                .where(DimWell.well_id.is_(None))
            ).scalar_one()
        ),
        "fact_operator_metrics_without_dim_operator": int(
            conn.execute(
                select(func.count())
                .select_from(FactOperatorMetrics)
                .outerjoin(DimOperator, FactOperatorMetrics.operator_id == DimOperator.operator_id)
                .where(DimOperator.operator_id.is_(None))
            ).scalar_one()
        ),
    }

    errors = [f"{name}={count}" for name, count in orphan_checks.items() if count > 0]
    warnings: list[str] = []
    if table_counts["dim_well"] == 0:
        warnings.append("dim_well is empty after ingestion")
    if table_counts["fact_operator_metrics"] == 0:
        warnings.append("fact_operator_metrics is empty after ingestion")
    if table_counts["fact_well_production_monthly"] > 0 and table_counts["fact_well_restart_score"] == 0:
        warnings.append("well production loaded but no restart scores were produced")

    return {
        "table_counts": table_counts,
        "orphan_checks": orphan_checks,
        "errors": errors,
        "warnings": warnings,
    }


def prepare_source_frames(args) -> tuple[dict[str, list[pd.DataFrame]], list[str], dict[str, str], dict, list[dict[str, object]]]:
    cfg = load_config(args.config)
    source_entries = [s for s in iter_enabled_sources(cfg) if s.enabled]
    sample_dir = Path(__file__).resolve().parent.parent / "sample_data"
    downloader = Downloader(Path("data/raw"))
    end_date = _parse_date(getattr(args, "end", None), _default_live_end_date())
    start_date = _parse_date(getattr(args, "start", None), end_date - timedelta(days=365))

    frames_by_kind: dict[str, list[pd.DataFrame]] = {}
    sources_ok: list[str] = []
    sources_failed: dict[str, str] = {}
    row_counts: dict[str, Any] = {"source_rows": {}, "loaded": {}, "validation": {"errors": [], "warnings": []}}
    source_summaries: list[dict[str, object]] = []

    LOGGER.info("Enabled sources: %s", [s.key for s in source_entries])

    for source in source_entries:
        try:
            downloader.last_result = None
            df = load_dataset(downloader, sample_dir, source, args.dry_run, args.refresh, start_date=start_date, end_date=end_date)
            frames_by_kind.setdefault(source.data_kind, []).append(df)
            row_counts["source_rows"][source.key] = int(len(df))
            last_result = downloader.last_result
            artifact_meta = _latest_artifact_metadata(downloader, source.key) if not args.dry_run else {}
            if last_result is not None and artifact_meta.get("url"):
                setattr(last_result, "url", artifact_meta.get("url"))

            if not args.dry_run and source.required_for_live and df.empty:
                message = "required live source returned 0 rows"
                sources_failed[source.key] = message
                source_summaries.append(
                    _build_source_summary(
                        source,
                        dry_run=args.dry_run,
                        rows_parsed=0,
                        load_status=message,
                        artifact_downloaded=bool(last_result.downloaded) if last_result else False,
                        last_result=last_result,
                        error=message,
                    )
                )
                LOGGER.error("Required live source %s returned 0 rows", source.key)
                continue

            sources_ok.append(source.key)
            source_summaries.append(
                _build_source_summary(
                    source,
                    dry_run=args.dry_run,
                    rows_parsed=int(len(df)),
                    load_status="success",
                    artifact_downloaded=bool(last_result.downloaded) if last_result else False,
                    last_result=last_result,
                )
            )
            LOGGER.info("Loaded source %s (%s rows)", source.key, len(df))
        except Exception as exc:
            sources_failed[source.key] = str(exc)
            source_summaries.append(
                _build_source_summary(
                    source,
                    dry_run=args.dry_run,
                    rows_parsed=0,
                    load_status=f"failed: {exc}",
                    artifact_downloaded=False,
                    error=str(exc),
                )
            )
            LOGGER.exception("Failed to load source %s", source.key)

    if not args.dry_run:
        required_errors, optional_warnings = _validate_required_live_sources(source_entries, row_counts, sources_failed)
        row_counts["validation"]["errors"].extend(required_errors)
        row_counts["validation"]["warnings"].extend(optional_warnings)

    return frames_by_kind, sources_ok, sources_failed, row_counts, source_summaries


def _merge_kind_frames(frames_by_kind: dict[str, list[pd.DataFrame]], kind: str) -> pd.DataFrame:
    frames = [f for f in frames_by_kind.get(kind, []) if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _first_non_empty(series: pd.Series):
    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def _coalesce_rows_by_key(df: pd.DataFrame, key: str, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)

    working = df.copy()
    working[key] = working[key].fillna("").astype(str).str.strip()
    working = working[working[key] != ""]
    if working.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        working.groupby(key, dropna=False, sort=False)
        .agg({column: _first_non_empty for column in columns if column != key})
        .reset_index()
    )
    return grouped.reindex(columns=columns)


def _stringify_identifier(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _normalize_status_series(values, *, default: str = "UNKNOWN") -> pd.Series:
    series = values if isinstance(values, pd.Series) else pd.Series(values)
    normalized = series.fillna(default).astype(str).str.strip()
    return normalized.replace({"": default, "nan": default, "None": default, "NONE": default})


def prepare_operator_dim_inputs(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    wells = _merge_kind_frames(frames_by_kind, "wells")
    facilities = _merge_kind_frames(frames_by_kind, "facility_master")
    operators = _merge_kind_frames(frames_by_kind, "operators")
    liability = _merge_kind_frames(frames_by_kind, "liability")
    well_licensees = wells.get("licensee", pd.Series(dtype=str))
    return pd.DataFrame(
        {
            "name_raw": pd.concat(
                [
                    well_licensees,
                    facilities.get("facility_operator", pd.Series(dtype=str)),
                    operators.get("ba_name_raw", pd.Series(dtype=str)),
                    operators.get("name_raw", pd.Series(dtype=str)),
                    _merge_kind_frames(frames_by_kind, "pipelines").get("company_name", pd.Series(dtype=str)),
                    liability.get("operator", pd.Series(dtype=str)),
                ],
                ignore_index=True,
            ).fillna("").astype(str)
        }
    )


def prepare_wells_df(frames_by_kind: dict[str, list[pd.DataFrame]], op_map: dict[str, int], as_of: date) -> pd.DataFrame:
    df = _merge_kind_frames(frames_by_kind, "wells")
    out = _empty_df(WELL_DIM_COLUMNS)
    if not df.empty:
        out["uwi_raw"] = df["uwi"].astype(str)
        out["well_id"] = out["uwi_raw"].map(normalize_uwi)
        license_numbers = df.get("license_number", pd.Series([None] * len(df), index=df.index))
        out["license_number"] = license_numbers.where(pd.notna(license_numbers), None)
        out["well_name"] = df.get("well_name")
        out["field_name"] = df.get("field_name")
        out["pool_name"] = df.get("pool_name")
        out["status"] = _normalize_status_series(df.get("status", pd.Series([None] * len(df), index=df.index)))
        out["licensee_operator_id"] = df.get("licensee", "").astype(str).map(normalize_operator_name).map(op_map)
        spud_date_raw = df.get("spud_date", pd.Series([None] * len(df), index=df.index))
        out["spud_date"] = pd.to_datetime(spud_date_raw, errors="coerce").dt.date
        out["lsd"] = df.get("lsd")
        out["section"] = pd.to_numeric(df.get("section"), errors="coerce")
        out["township"] = pd.to_numeric(df.get("township"), errors="coerce")
        out["range"] = pd.to_numeric(df.get("range"), errors="coerce")
        out["meridian"] = pd.to_numeric(df.get("meridian"), errors="coerce")
        out["lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
        out["lon"] = pd.to_numeric(df.get("lon"), errors="coerce")
        out["first_seen"] = as_of
        out["last_seen"] = as_of
        out["source"] = df.get("source", "multi_source")

        licence_only = df.copy()
        licence_only["license_number"] = licence_only.get("license_number", pd.Series([None] * len(licence_only), index=licence_only.index)).map(
            _stringify_identifier
        )
        licence_only["uwi"] = licence_only.get("uwi", pd.Series([""] * len(licence_only), index=licence_only.index)).fillna("").astype(str).map(
            normalize_uwi
        )
        licence_only = licence_only[
            (licence_only["license_number"].fillna("").astype(str).str.strip() != "")
            & (licence_only["uwi"].fillna("").astype(str).str.strip() == "")
        ]
        if not licence_only.empty:
            licence_only = licence_only.assign(
                well_name=licence_only.get("well_name"),
                field_name=licence_only.get("field_name"),
                pool_name=licence_only.get("pool_name"),
                status=licence_only.get("status"),
                licensee=licence_only.get("licensee"),
                spud_date=licence_only.get("spud_date"),
                lsd=licence_only.get("lsd"),
                section=licence_only.get("section"),
                township=licence_only.get("township"),
                range=licence_only.get("range"),
                meridian=licence_only.get("meridian"),
                source=licence_only.get("source"),
            )
            licence_enrichment = (
                licence_only.groupby("license_number", dropna=False, sort=False)
                .agg(
                    {
                        "well_name": _first_non_empty,
                        "field_name": _first_non_empty,
                        "pool_name": _first_non_empty,
                        "status": _first_non_empty,
                        "licensee": _first_non_empty,
                        "spud_date": _first_non_empty,
                        "lsd": _first_non_empty,
                        "section": _first_non_empty,
                        "township": _first_non_empty,
                        "range": _first_non_empty,
                        "meridian": _first_non_empty,
                        "source": _first_non_empty,
                    }
                )
                .reset_index()
            )
            if not licence_enrichment.empty:
                out_license_numbers = out["license_number"].map(_stringify_identifier)
                for column in ("well_name", "field_name", "pool_name", "spud_date", "lsd", "section", "township", "range", "meridian", "source"):
                    existing = out[column]
                    mapped = out_license_numbers.map(licence_enrichment.set_index("license_number")[column])
                    out[column] = existing.where(existing.notna() & (existing.astype(str).str.strip() != ""), mapped)
                mapped_status = out_license_numbers.map(licence_enrichment.set_index("license_number")["status"])
                existing_status = _normalize_status_series(out["status"])
                out["status"] = existing_status.where(existing_status != "UNKNOWN", mapped_status)
                existing_operator = out["licensee_operator_id"]
                mapped_licensee = out_license_numbers.map(licence_enrichment.set_index("license_number")["licensee"])
                mapped_operator = mapped_licensee.fillna("").astype(str).map(normalize_operator_name).map(op_map)
                out["licensee_operator_id"] = existing_operator.where(existing_operator.notna(), mapped_operator)
                out["status"] = _normalize_status_series(out["status"])
                out["spud_date"] = pd.to_datetime(out["spud_date"], errors="coerce").dt.date
                out["section"] = pd.to_numeric(out["section"], errors="coerce")
                out["township"] = pd.to_numeric(out["township"], errors="coerce")
                out["range"] = pd.to_numeric(out["range"], errors="coerce")
                out["meridian"] = pd.to_numeric(out["meridian"], errors="coerce")

    bridge_raw = _merge_kind_frames(frames_by_kind, "well_facility_bridge")
    if not bridge_raw.empty and "well_id" in bridge_raw.columns:
        bridge_well_ids = bridge_raw["well_id"].fillna("").astype(str).map(normalize_uwi)
        existing_ids = set(out["well_id"].fillna("").astype(str)) if not out.empty else set()
        supplemental_ids = sorted(well_id for well_id in bridge_well_ids.unique().tolist() if well_id and well_id not in existing_ids)
        if supplemental_ids:
            supplemental = _empty_df(WELL_DIM_COLUMNS)
            supplemental["well_id"] = supplemental_ids
            supplemental["uwi_raw"] = supplemental_ids
            supplemental["license_number"] = pd.NA
            supplemental["well_name"] = pd.NA
            supplemental["field_name"] = pd.NA
            supplemental["pool_name"] = pd.NA
            supplemental["status"] = "UNKNOWN"
            supplemental["licensee_operator_id"] = pd.NA
            supplemental["spud_date"] = pd.NA
            supplemental["lsd"] = pd.NA
            supplemental["section"] = pd.NA
            supplemental["township"] = pd.NA
            supplemental["range"] = pd.NA
            supplemental["meridian"] = pd.NA
            supplemental["lat"] = pd.NA
            supplemental["lon"] = pd.NA
            supplemental["first_seen"] = as_of
            supplemental["last_seen"] = as_of
            supplemental["source"] = "petrinex_bridge"
            out = pd.concat([out, supplemental], ignore_index=True)

    out = _coalesce_rows_by_key(out, "well_id", WELL_DIM_COLUMNS)
    if not out.empty:
        out["license_number"] = out["license_number"].map(_stringify_identifier)
        out["first_seen"] = as_of
        out["last_seen"] = as_of
        out["source"] = out["source"].fillna("multi_source")
    return out


def prepare_facilities_df(frames_by_kind: dict[str, list[pd.DataFrame]], operator_map: dict[str, int]) -> pd.DataFrame:
    fac_raw = _merge_kind_frames(frames_by_kind, "facility_master")
    if fac_raw.empty:
        return pd.DataFrame()

    fac_df = pd.DataFrame(index=fac_raw.index)
    fac_df["facility_id"] = fac_raw["facility_id"].astype(str)
    fac_df["facility_name"] = fac_raw.get("facility_name")
    facility_license_numbers = fac_raw.get("license_number", pd.Series([None] * len(fac_raw), index=fac_raw.index))
    fac_df["license_number"] = facility_license_numbers.where(pd.notna(facility_license_numbers), None)
    fac_df["facility_type"] = fac_raw.get("facility_type")
    fac_df["facility_subtype"] = fac_raw.get("facility_subtype")
    fac_df["facility_operator_id"] = fac_raw.get("facility_operator", "").astype(str).map(normalize_operator_name).map(operator_map)
    fac_df["facility_status"] = fac_raw.get("facility_status")
    fac_df["lsd"] = fac_raw.get("lsd")
    fac_df["section"] = pd.to_numeric(fac_raw.get("section"), errors="coerce")
    fac_df["township"] = pd.to_numeric(fac_raw.get("township"), errors="coerce")
    fac_df["range"] = pd.to_numeric(fac_raw.get("range"), errors="coerce")
    fac_df["meridian"] = pd.to_numeric(fac_raw.get("meridian"), errors="coerce")
    fac_df["lat"] = pd.to_numeric(fac_raw.get("lat"), errors="coerce")
    fac_df["lon"] = pd.to_numeric(fac_raw.get("lon"), errors="coerce")
    fac_df["source"] = fac_raw.get("source", "multi_source")
    fac_df["facility_id"] = fac_df["facility_id"].fillna("").astype(str).str.strip()
    fac_df = _coalesce_rows_by_key(fac_df, "facility_id", FACILITY_DIM_COLUMNS)
    if not fac_df.empty:
        fac_df["license_number"] = fac_df["license_number"].map(_stringify_identifier)
    return fac_df


def prepare_pipelines_df(frames_by_kind: dict[str, list[pd.DataFrame]], operator_map: dict[str, int]) -> pd.DataFrame:
    pipe_raw = _merge_kind_frames(frames_by_kind, "pipelines")
    if pipe_raw.empty:
        return pd.DataFrame(columns=PIPELINE_DIM_COLUMNS)

    pipe_df = pd.DataFrame(index=pipe_raw.index)
    pipe_df["pipeline_id"] = pipe_raw.get("pipeline_id", "").fillna("").astype(str).str.strip()
    pipe_df["license_number"] = pipe_raw.get("license_number", pd.Series([None] * len(pipe_raw), index=pipe_raw.index)).where(
        pd.notna(pipe_raw.get("license_number", pd.Series([None] * len(pipe_raw), index=pipe_raw.index))), None
    )
    pipe_df["line_number"] = pipe_raw.get("line_number")
    pipe_df["licence_line_number"] = pipe_raw.get("licence_line_number")
    pipe_df["operator_id"] = pipe_raw.get("company_name", "").astype(str).map(normalize_operator_name).map(operator_map)
    pipe_df["company_name"] = pipe_raw.get("company_name")
    pipe_df["ba_code"] = pipe_raw.get("ba_code")
    pipe_df["segment_status"] = pipe_raw.get("segment_status")
    pipe_df["from_facility_type"] = pipe_raw.get("from_facility_type")
    pipe_df["from_location"] = pipe_raw.get("from_location")
    pipe_df["to_facility_type"] = pipe_raw.get("to_facility_type")
    pipe_df["to_location"] = pipe_raw.get("to_location")
    pipe_df["substance1"] = pipe_raw.get("substance1")
    pipe_df["substance2"] = pipe_raw.get("substance2")
    pipe_df["substance3"] = pipe_raw.get("substance3")
    pipe_df["segment_length_km"] = pd.to_numeric(pipe_raw.get("segment_length_km"), errors="coerce")
    pipe_df["geometry_source"] = pipe_raw.get("geometry_source")
    pipe_df["geometry_wkt"] = pipe_raw.get("geometry_wkt")
    pipe_df["centroid_lat"] = pd.to_numeric(pipe_raw.get("centroid_lat"), errors="coerce")
    pipe_df["centroid_lon"] = pd.to_numeric(pipe_raw.get("centroid_lon"), errors="coerce")
    pipe_df["source"] = pipe_raw.get("source", "aer_spatial_pipelines")
    pipe_df = _coalesce_rows_by_key(pipe_df, "pipeline_id", PIPELINE_DIM_COLUMNS)
    if not pipe_df.empty:
        pipe_df["license_number"] = pipe_df["license_number"].map(_stringify_identifier)
    return pipe_df


def prepare_crown_dispositions_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    raw = _merge_kind_frames(frames_by_kind, "crown_dispositions")
    if raw.empty:
        return pd.DataFrame(columns=CROWN_DISPOSITION_COLUMNS)

    out = raw.reindex(columns=CROWN_DISPOSITION_COLUMNS).copy()
    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["agreement_no"] = out["agreement_no"].fillna("").astype(str).str.strip().replace({"": None})
    out["effective_from"] = pd.to_datetime(out["effective_from"], errors="coerce").dt.date
    out["effective_to"] = pd.to_datetime(out["effective_to"], errors="coerce").dt.date
    out["source"] = out["source"].fillna("ami_crown_dispositions")
    out = out[out["disposition_id"] != ""]
    return _coalesce_rows_by_key(out, "disposition_id", CROWN_DISPOSITION_COLUMNS)


def prepare_crown_clients_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    raw = _merge_kind_frames(frames_by_kind, "crown_clients")
    if raw.empty:
        return pd.DataFrame(columns=CROWN_CLIENT_COLUMNS)

    out = raw.reindex(columns=CROWN_CLIENT_COLUMNS).copy()
    out["client_id"] = out["client_id"].fillna("").astype(str).str.strip()
    out = out[out["client_id"] != ""]
    return _coalesce_rows_by_key(out, "client_id", CROWN_CLIENT_COLUMNS)


def prepare_crown_participants_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    raw = _merge_kind_frames(frames_by_kind, "crown_participants")
    if raw.empty:
        return pd.DataFrame(columns=CROWN_PARTICIPANT_COLUMNS)

    out = raw.reindex(columns=CROWN_PARTICIPANT_COLUMNS).copy()
    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["client_id"] = out["client_id"].fillna("").astype(str).str.strip()
    out["role_type"] = out["role_type"].fillna("holder").astype(str).str.strip().replace({"": "holder"})
    out["interest_pct"] = pd.to_numeric(out["interest_pct"], errors="coerce")
    out["effective_from"] = pd.to_datetime(out["effective_from"], errors="coerce").dt.date
    out["effective_to"] = pd.to_datetime(out["effective_to"], errors="coerce").dt.date
    out["source"] = out["source"].fillna("ami_crown_participants")
    out = out[(out["disposition_id"] != "") & (out["client_id"] != "")]
    out = out.drop_duplicates(subset=["disposition_id", "client_id", "role_type"], keep="last")
    return out


def prepare_crown_land_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    raw = _merge_kind_frames(frames_by_kind, "crown_land_keys")
    if raw.empty:
        return pd.DataFrame(columns=CROWN_LAND_COLUMNS)

    out = raw.reindex(columns=CROWN_LAND_COLUMNS).copy()
    out["disposition_id"] = out["disposition_id"].fillna("").astype(str).str.strip()
    out["tract_no"] = out["tract_no"].fillna("").astype(str).str.strip().replace({"": None})
    out["lsd"] = out["lsd"].map(normalize_lsd)
    for column in ["section", "township", "range", "meridian"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out["source"] = out["source"].fillna("ami_crown_land_keys")
    out = out[out["disposition_id"] != ""]
    out = out.dropna(subset=["section", "township", "range", "meridian"])
    out[["section", "township", "range", "meridian"]] = out[["section", "township", "range", "meridian"]].astype(int)
    out = out.drop_duplicates(subset=["disposition_id", "tract_no", "lsd", "section", "township", "range", "meridian"])
    return out


def prepare_bridge_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    bridge_raw = _merge_kind_frames(frames_by_kind, "well_facility_bridge")
    if bridge_raw.empty:
        return _empty_df(BRIDGE_COLUMNS)

    effective_from_raw = bridge_raw["effective_from"] if "effective_from" in bridge_raw.columns else pd.Series([None] * len(bridge_raw), index=bridge_raw.index)
    effective_to_raw = bridge_raw["effective_to"] if "effective_to" in bridge_raw.columns else pd.Series([None] * len(bridge_raw), index=bridge_raw.index)

    bridge_df = _empty_df(BRIDGE_COLUMNS)
    bridge_df["well_id"] = bridge_raw["well_id"].astype(str).map(normalize_uwi)
    bridge_df["facility_id"] = bridge_raw["facility_id"].astype(str)
    bridge_df["effective_from"] = pd.to_datetime(effective_from_raw, errors="coerce").dt.date
    bridge_df["effective_to"] = pd.to_datetime(effective_to_raw, errors="coerce").dt.date
    bridge_df["effective_from"] = bridge_df["effective_from"].map(lambda v: None if pd.isna(v) else v)
    bridge_df["effective_to"] = bridge_df["effective_to"].map(lambda v: None if pd.isna(v) else v)
    bridge_df["source"] = "petrinex_bridge"
    bridge_df["facility_id"] = bridge_df["facility_id"].fillna("").astype(str).str.strip()
    bridge_df["well_id"] = bridge_df["well_id"].fillna("").astype(str).str.strip()
    bridge_df = bridge_df[(bridge_df["facility_id"] != "") & (bridge_df["well_id"] != "")]
    return bridge_df


def enrich_wells_with_facility_operator(wells_df: pd.DataFrame, bridge_df: pd.DataFrame, fac_df: pd.DataFrame) -> pd.DataFrame:
    if wells_df.empty or bridge_df.empty or fac_df.empty or "facility_operator_id" not in fac_df.columns:
        return wells_df

    facility_ops = fac_df[["facility_id", "facility_operator_id"]].dropna(subset=["facility_operator_id"]).copy()
    if facility_ops.empty:
        return wells_df

    well_ops = bridge_df.merge(facility_ops, on="facility_id", how="inner")
    if well_ops.empty:
        return wells_df

    well_ops["facility_operator_id"] = pd.to_numeric(well_ops["facility_operator_id"], errors="coerce")
    well_ops = well_ops.dropna(subset=["facility_operator_id"])
    if well_ops.empty:
        return wells_df

    well_ops["facility_operator_id"] = well_ops["facility_operator_id"].astype(int)
    preferred_ops = (
        well_ops.groupby(["well_id", "facility_operator_id"]).size().reset_index(name="link_count")
        .sort_values(["well_id", "link_count"], ascending=[True, False])
        .drop_duplicates(subset=["well_id"], keep="first")[["well_id", "facility_operator_id"]]
    )

    enriched = wells_df.merge(preferred_ops, on="well_id", how="left")
    current_operator = pd.to_numeric(enriched.get("licensee_operator_id"), errors="coerce")
    enriched["licensee_operator_id"] = current_operator.where(current_operator.notna(), enriched["facility_operator_id"])
    enriched = enriched.drop(columns=["facility_operator_id"])
    return enriched


def prepare_production_dfs(
    frames_by_kind: dict[str, list[pd.DataFrame]],
    bridge_df: pd.DataFrame,
    wells_df: pd.DataFrame,
    fac_df: pd.DataFrame,
    start: date,
    end: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fac_prod_raw = _merge_kind_frames(frames_by_kind, "facility_production")
    fac_prod = _empty_df(FACILITY_PROD_COLUMNS)
    if not fac_prod_raw.empty:
        fac_working = pd.DataFrame(index=fac_prod_raw.index)
        empty_series = pd.Series([""] * len(fac_prod_raw), index=fac_prod_raw.index)
        fac_working["month"] = month_start(fac_prod_raw["month"]).dt.date
        fac_working["facility_id"] = fac_prod_raw["facility_id"].fillna("").astype(str).str.strip()
        fac_working["well_id"] = fac_prod_raw.get("well_id", empty_series).fillna("").astype(str).map(normalize_uwi)
        fac_working["activity_id"] = fac_prod_raw.get("activity_id", empty_series).fillna("").astype(str).str.upper().str.strip()
        fac_working["oil_bbl"] = pd.to_numeric(fac_prod_raw.get("oil_bbl", 0), errors="coerce").fillna(0.0)
        fac_working["gas_mcf"] = pd.to_numeric(fac_prod_raw.get("gas_mcf", 0), errors="coerce").fillna(0.0)
        fac_working["water_bbl"] = pd.to_numeric(fac_prod_raw.get("water_bbl", 0), errors="coerce").fillna(0.0)
        fac_working["condensate_bbl"] = pd.to_numeric(fac_prod_raw.get("condensate_bbl", 0), errors="coerce").fillna(0.0)
        fac_working = fac_working[fac_working["facility_id"] != ""]
        fac_prod = (
            fac_working.groupby(["month", "facility_id"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl", "condensate_bbl"]]
            .sum()
            .assign(source="petrinex_production")
        )
    else:
        fac_working = pd.DataFrame()

    direct_well_prod = pd.DataFrame()
    if not fac_working.empty and "well_id" in fac_working.columns:
        direct_well_prod = fac_working[fac_working["well_id"] != ""].copy()
        if not direct_well_prod.empty:
            direct_well_prod = (
                direct_well_prod.groupby(["month", "well_id"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
                .sum()
                .assign(source="petrinex_well_reported", is_estimated=False)
            )

    if not direct_well_prod.empty:
        well_prod = direct_well_prod
    elif fac_prod.empty or bridge_df.empty:
        well_prod = _empty_df(["month", "well_id", "oil_bbl", "gas_mcf", "water_bbl", "source", "is_estimated"])
    else:
        merged = fac_prod.merge(bridge_df[["facility_id", "well_id"]], on="facility_id", how="inner")
        counts = merged.groupby(["month", "facility_id"])["well_id"].transform("count").replace(0, 1)
        merged["oil_bbl"] = merged["oil_bbl"] / counts
        merged["gas_mcf"] = merged["gas_mcf"] / counts
        merged["water_bbl"] = merged["water_bbl"] / counts
        well_prod = (
            merged.groupby(["month", "well_id", "source"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]].sum().assign(is_estimated=True)
        )
    well_prod = well_prod[well_prod["month"].between(start, end)] if not well_prod.empty else well_prod

    status_df = _empty_df(WELL_STATUS_COLUMNS)
    status_frames: list[pd.DataFrame] = []
    if not wells_df.empty:
        st37_status = _empty_df(WELL_STATUS_COLUMNS)
        st37_status["well_id"] = wells_df["well_id"]
        st37_status["status"] = _normalize_status_series(wells_df["status"])
        st37_status["status_date"] = None
        st37_status["source"] = "aer_st37"
        status_frames.append(st37_status)

    if not fac_working.empty and {"well_id", "activity_id", "month"}.issubset(fac_working.columns):
        activity_statuses = {"SHUTIN": "SHUT-IN", "SUSPENDED": "SUSPENDED", "INACTIVE": "INACTIVE"}
        petr_status = fac_working[fac_working["well_id"] != ""].copy()
        petr_status = petr_status[petr_status["activity_id"].isin(activity_statuses)]
        if not petr_status.empty:
            petr_status = petr_status[["well_id", "activity_id", "month"]].drop_duplicates()
            petr_status = petr_status.rename(columns={"month": "status_date"})
            petr_status["status"] = petr_status["activity_id"].map(activity_statuses)
            petr_status["source"] = "petrinex_monthly_activity"
            petr_status = petr_status[["well_id", "status", "status_date", "source"]]
            status_frames.append(petr_status)

    if status_frames:
        status_df = pd.concat(status_frames, ignore_index=True)
        status_df["status"] = _normalize_status_series(status_df["status"])

    op_prod = _empty_df(OPERATOR_PROD_COLUMNS)
    if not fac_prod.empty and not fac_df.empty and "facility_operator_id" in fac_df.columns:
        f2op = fac_df[["facility_id", "facility_operator_id"]].rename(columns={"facility_operator_id": "operator_id"})
        op_prod = fac_prod.merge(f2op, on="facility_id", how="left")
        op_prod = op_prod.dropna(subset=["operator_id"])
        op_prod["operator_id"] = op_prod["operator_id"].astype(int)
        op_prod = (
            op_prod.groupby(["month", "operator_id"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
            .sum()
            .assign(basis_level="facility_reported", source="petrinex_facility_reported")
        )
    elif not well_prod.empty and not wells_df.empty:
        w2op = wells_df[["well_id", "licensee_operator_id"]].rename(columns={"licensee_operator_id": "operator_id"})
        op_prod = well_prod.merge(w2op, on="well_id", how="left")
        op_prod = op_prod.dropna(subset=["operator_id"])
        op_prod["operator_id"] = op_prod["operator_id"].astype(int)
        op_prod = (
            op_prod.groupby(["month", "operator_id", "source"], as_index=False)[["oil_bbl", "gas_mcf", "water_bbl"]]
            .sum()
            .assign(basis_level="well_estimated", source="derived_well_estimated")
        )

    return fac_prod, well_prod, status_df, op_prod


def prepare_business_associate_dfs(frames_by_kind: dict[str, list[pd.DataFrame]], operator_map: dict[str, int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    ba_raw = _merge_kind_frames(frames_by_kind, "operators")
    if ba_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    ba_dim = ba_raw[[c for c in ["ba_id", "ba_name_raw", "entity_type"] if c in ba_raw.columns]].copy()
    bridge = pd.DataFrame()
    if "ba_id" in ba_raw.columns and "ba_name_raw" in ba_raw.columns:
        bridge = pd.DataFrame()
        bridge["ba_id"] = ba_raw["ba_id"].fillna("").astype(str).str.strip()
        bridge["operator_id"] = ba_raw["ba_name_raw"].astype(str).map(normalize_operator_name).map(operator_map)
        bridge["match_method"] = "name_norm_exact"
        bridge["confidence"] = 0.7
        bridge = bridge[bridge["ba_id"] != ""]
    return ba_dim, bridge


def build_restart_scores(wells_df: pd.DataFrame, well_prod: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    restart_input = wells_df[RESTART_INPUT_COLUMNS].copy() if not wells_df.empty else _empty_df(RESTART_INPUT_COLUMNS)
    if well_prod.empty:
        LOGGER.info("No well production data available; restart scoring skipped")
        return empty_restart_scores_df()
    return compute_well_restart_scores(restart_input, well_prod, as_of_date)


def build_operator_metrics(op_prod: pd.DataFrame, liability_df: pd.DataFrame, restart_df: pd.DataFrame, wells_df: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    return compute_operator_metrics(op_prod, liability_df, restart_df, wells_df, as_of_date)


def run_source_diagnostics(args) -> tuple[int, dict[str, Any]]:
    LOGGER.info("Running source diagnostics")
    _, _, sources_failed, row_counts, source_summaries = prepare_source_frames(args)
    required_failures = sum(
        1
        for item in source_summaries
        if bool(item.get("required_for_live")) and str(item.get("load_status", "")).lower() != "success"
    )
    optional_failures = sum(
        1
        for item in source_summaries
        if not bool(item.get("required_for_live")) and str(item.get("load_status", "")).lower() != "success"
    )
    report = {
        "required_failures": required_failures,
        "optional_failures": optional_failures,
        "sources_failed": sources_failed,
        "row_counts": row_counts,
        "source_summaries": source_summaries,
    }
    return (0 if required_failures == 0 else 1), report


def run_ingestion_pipeline(args) -> tuple[int, PipelineResult | None]:
    LOGGER.info("Starting ingestion run")
    ensure_database_dir()
    end = _parse_date(args.end, _default_live_end_date())
    start = _parse_date(args.start, end - timedelta(days=365))
    started_at = datetime.now(timezone.utc)
    run_id = new_run_id()

    LOGGER.info("Loading configuration from %s", args.config)
    frames_by_kind, sources_ok, sources_failed, row_counts, source_summaries = prepare_source_frames(args)

    try:
        upgrade_to_head()
        if row_counts["validation"]["errors"]:
            raise ValueError("Required live sources failed: " + "; ".join(row_counts["validation"]["errors"]))
        engine = get_engine(get_database_url())
        with engine.begin() as conn:
            with _phase("Preparing operator source inputs", lambda: len(operator_inputs)):
                operator_inputs = prepare_operator_dim_inputs(frames_by_kind)
            raw_operator_count = int(len(operator_inputs))
            distinct_normalized_operator_count = int(
                operator_inputs["name_raw"].fillna("").astype(str).map(normalize_operator_name).replace("", pd.NA).dropna().nunique()
            ) if not operator_inputs.empty else 0
            LOGGER.info(
                "Operator input profile: raw_operator_strings=%s distinct_non_empty_normalized_names=%s",
                raw_operator_count,
                distinct_normalized_operator_count,
            )
            with _phase("Upserting dim_operator", lambda: len(operator_map)):
                operator_map = upsert_dim_operator(conn, operator_inputs, source="multi_source")

            ba_dim, ba_bridge = prepare_business_associate_dfs(frames_by_kind, operator_map)
            row_counts["loaded"]["dim_business_associate"] = len(
                upsert_dim_business_associate(conn, ba_dim, source="petrinex_business_associate")
            )
            row_counts["loaded"]["bridge_operator_business_associate"] = upsert_bridge_operator_business_associate(conn, ba_bridge)

            with _phase("Preparing wells dataframe", lambda: len(wells_df)):
                wells_df = prepare_wells_df(frames_by_kind, operator_map, end)

            with _phase("Preparing facilities dataframe", lambda: len(fac_df)):
                fac_df = prepare_facilities_df(frames_by_kind, operator_map)
            with _phase("Upserting dim_facility", lambda: row_counts["loaded"].get("dim_facility", 0)):
                row_counts["loaded"]["dim_facility"] = upsert_dim_facility(conn, fac_df)
            with _phase("Preparing pipelines dataframe", lambda: len(pipe_df)):
                pipe_df = prepare_pipelines_df(frames_by_kind, operator_map)
            with _phase("Upserting dim_pipeline", lambda: row_counts["loaded"].get("dim_pipeline", 0)):
                row_counts["loaded"]["dim_pipeline"] = upsert_dim_pipeline(conn, pipe_df)
            with _phase("Preparing crown disposition dataframe", lambda: len(crown_dispositions_df)):
                crown_dispositions_df = prepare_crown_dispositions_df(frames_by_kind)
            with _phase("Upserting dim_crown_disposition", lambda: row_counts["loaded"].get("dim_crown_disposition", 0)):
                row_counts["loaded"]["dim_crown_disposition"] = upsert_dim_crown_disposition(conn, crown_dispositions_df)
            with _phase("Preparing crown client dataframe", lambda: len(crown_clients_df)):
                crown_clients_df = prepare_crown_clients_df(frames_by_kind)
            with _phase("Upserting dim_crown_client", lambda: row_counts["loaded"].get("dim_crown_client", 0)):
                row_counts["loaded"]["dim_crown_client"] = upsert_dim_crown_client(conn, crown_clients_df, source="ami_crown_clients")
            with _phase("Preparing crown participant dataframe", lambda: len(crown_participants_df)):
                crown_participants_df = prepare_crown_participants_df(frames_by_kind)
            with _phase(
                "Loading bridge_crown_disposition_client",
                lambda: row_counts["loaded"].get("bridge_crown_disposition_client", 0),
            ):
                row_counts["loaded"]["bridge_crown_disposition_client"] = replace_bridge_crown_disposition_client(
                    conn, crown_participants_df
                )
            with _phase("Preparing crown land dataframe", lambda: len(crown_land_df)):
                crown_land_df = prepare_crown_land_df(frames_by_kind)
            with _phase(
                "Loading bridge_crown_disposition_land",
                lambda: row_counts["loaded"].get("bridge_crown_disposition_land", 0),
            ):
                row_counts["loaded"]["bridge_crown_disposition_land"] = replace_bridge_crown_disposition_land(
                    conn, crown_land_df
                )

            with _phase("Preparing bridge dataframe", lambda: len(bridge_df)):
                bridge_df = prepare_bridge_df(frames_by_kind)
            with _phase("Enriching wells with facility operators", lambda: len(wells_df)):
                wells_df = enrich_wells_with_facility_operator(wells_df, bridge_df, fac_df)
            with _phase("Deduplicating wells dataframe", lambda: len(deduped_wells_preview)):
                deduped_wells_preview = _dedupe_wells_df(wells_df) if not wells_df.empty else wells_df

            LOGGER.info(
                "Wells row profile: st37_original_rows=%s deduped_rows=%s",
                len(wells_df),
                len(deduped_wells_preview),
            )
            with _phase("Upserting dim_well", lambda: row_counts["loaded"].get("dim_well", 0)):
                row_counts["loaded"]["dim_well"] = upsert_dim_well(conn, wells_df)
            with _phase("Loading bridge_well_facility", lambda: row_counts["loaded"].get("bridge_well_facility", 0)):
                row_counts["loaded"]["bridge_well_facility"] = load_bridge_well_facility(conn, bridge_df)

            with _phase("Preparing facility production dataframe", lambda: len(fac_prod)):
                fac_prod, well_prod, status_df, op_prod = prepare_production_dfs(frames_by_kind, bridge_df, wells_df, fac_df, start, end)
                if not well_prod.empty:
                    existing_well_ids = set(wells_df["well_id"].fillna("").astype(str)) if not wells_df.empty else set()
                    missing_well_ids = sorted(
                        well_id
                        for well_id in well_prod["well_id"].fillna("").astype(str).unique().tolist()
                        if well_id and well_id not in existing_well_ids
                    )
                    if missing_well_ids:
                        supplemental = _empty_df(WELL_DIM_COLUMNS)
                        supplemental["well_id"] = missing_well_ids
                        supplemental["uwi_raw"] = missing_well_ids
                        supplemental["license_number"] = pd.NA
                        supplemental["well_name"] = pd.NA
                        supplemental["field_name"] = pd.NA
                        supplemental["pool_name"] = pd.NA
                        supplemental["status"] = "UNKNOWN"
                        supplemental["licensee_operator_id"] = pd.NA
                        supplemental["spud_date"] = pd.NA
                        supplemental["lsd"] = pd.NA
                        supplemental["section"] = pd.NA
                        supplemental["township"] = pd.NA
                        supplemental["range"] = pd.NA
                        supplemental["meridian"] = pd.NA
                        supplemental["lat"] = pd.NA
                        supplemental["lon"] = pd.NA
                        supplemental["first_seen"] = end
                        supplemental["last_seen"] = end
                        supplemental["source"] = "petrinex_production"
                        wells_df = pd.concat([wells_df, supplemental], ignore_index=True)
                if not status_df.empty and not wells_df.empty:
                    latest_status = status_df[status_df["status"].fillna("").astype(str).str.strip() != ""].copy()
                    latest_status = latest_status[latest_status["status"].astype(str).str.upper() != "UNKNOWN"]
                    if not latest_status.empty:
                        latest_status["status_rank"] = latest_status["source"].eq("petrinex_monthly_activity").astype(int)
                        latest_status["status_date"] = pd.to_datetime(latest_status["status_date"], errors="coerce")
                        latest_status = (
                            latest_status.sort_values(["status_rank", "status_date"], ascending=[False, False])
                            .drop_duplicates(subset=["well_id"], keep="first")[["well_id", "status"]]
                        )
                        wells_df = wells_df.drop(columns=["status"]).merge(latest_status, on="well_id", how="left")
                        wells_df["status"] = wells_df["status"].fillna("UNKNOWN")
                row_counts["loaded"]["dim_well"] = upsert_dim_well(conn, wells_df)
            with _phase("Loading fact_facility_production_monthly", lambda: row_counts["loaded"].get("fact_facility_production_monthly", 0)):
                row_counts["loaded"]["fact_facility_production_monthly"] = replace_fact_by_month_range(
                    conn, FactFacilityProductionMonthly, fac_prod, "petrinex_production", start, end
                )

            with _phase("Preparing well production dataframe", lambda: len(well_prod)):
                well_prod_load_df = well_prod
            with _phase("Loading fact_well_production_monthly", lambda: row_counts["loaded"].get("fact_well_production_monthly", 0)):
                row_counts["loaded"]["fact_well_production_monthly"] = replace_fact_by_month_range(
                    conn, FactWellProductionMonthly, well_prod_load_df, "petrinex_production", start, end
                )

            with _phase("Preparing well status dataframe", lambda: len(status_df)):
                status_load_df = status_df
            with _phase("Loading fact_well_status", lambda: row_counts["loaded"].get("fact_well_status", 0)):
                row_counts["loaded"]["fact_well_status"] = replace_fact_well_status(conn, status_load_df, "aer_st37")

            with _phase("Preparing operator production dataframe", lambda: len(op_prod)):
                op_prod_load_df = op_prod
            with _phase("Loading fact_operator_production_monthly", lambda: row_counts["loaded"].get("fact_operator_production_monthly", 0)):
                row_counts["loaded"]["fact_operator_production_monthly"] = replace_fact_operator_prod(
                    conn, op_prod_load_df, "petrinex_facility_reported", start, end
                )

            with _phase("Preparing liability dataframe", lambda: len(liability_df)):
                liability_raw = _merge_kind_frames(frames_by_kind, "liability")
                liability_df = _empty_df(LIABILITY_COLUMNS)
                if not liability_raw.empty:
                    liability_df["as_of_date"] = pd.to_datetime(liability_raw["as_of_date"], errors="coerce").dt.date
                    liability_df["operator_id"] = liability_raw.get("operator", "").astype(str).map(normalize_operator_name).map(operator_map)
                    liability_df["inactive_wells"] = pd.to_numeric(liability_raw.get("inactive_wells", 0), errors="coerce").fillna(0).astype(int)
                    liability_df["active_wells"] = pd.to_numeric(liability_raw.get("active_wells", 0), errors="coerce").fillna(0).astype(int)
                    liability_df["deemed_assets"] = pd.to_numeric(liability_raw.get("deemed_assets", 0), errors="coerce").fillna(0.0)
                    liability_df["deemed_liabilities"] = pd.to_numeric(liability_raw.get("deemed_liabilities", 0), errors="coerce").fillna(0.0)
                    liability_df["ratio"] = pd.to_numeric(liability_raw.get("ratio", 0), errors="coerce")
                    liability_df = liability_df.dropna(subset=["as_of_date", "operator_id"]).copy()
                    liability_df["operator_id"] = liability_df["operator_id"].astype(int)
                    liability_df["source"] = "aer_llr"
            with _phase("Loading fact_operator_liability", lambda: row_counts["loaded"].get("fact_operator_liability", 0)):
                row_counts["loaded"]["fact_operator_liability"] = replace_fact_liability(conn, liability_df, "aer_llr")

            with _phase("Computing restart scores", lambda: len(restart_df)):
                restart_df = build_restart_scores(wells_df, well_prod, end)
                restart_df = json_compat_frame(conn, restart_df, ["flags"])
            with _phase("Loading fact_well_restart_score", lambda: row_counts["loaded"].get("fact_well_restart_score", 0)):
                row_counts["loaded"]["fact_well_restart_score"] = replace_fact_restart(conn, restart_df, end)

            with _phase("Computing operator metrics", lambda: len(metrics_df)):
                metrics_df = build_operator_metrics(op_prod, liability_df, restart_df, wells_df, end)
                metrics_df = json_compat_frame(conn, metrics_df, ["source_notes"])
            with _phase("Loading fact_operator_metrics", lambda: row_counts["loaded"].get("fact_operator_metrics", 0)):
                row_counts["loaded"]["fact_operator_metrics"] = replace_fact_metrics(conn, metrics_df, end)

            with _phase("Validating curated database"):
                row_counts["validation"]["database"] = _collect_database_validation(conn)
                if row_counts["validation"]["database"]["errors"]:
                    raise ValueError(
                        "Database validation failed: " + "; ".join(row_counts["validation"]["database"]["errors"])
                    )

            with _phase("Recording ingestion_run", lambda: 1):
                record_ingestion_run(
                    conn,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    status="success",
                    sources_ok=sources_ok,
                    sources_failed=sources_failed,
                    row_counts=row_counts,
                    notes="Ingestion run completed with source diagnostics and database validation",
                )

            top_prod = pd.DataFrame()
            top_restart = pd.DataFrame()
            if not metrics_df.empty:
                ops = pd.read_sql(select(DimOperator.operator_id, DimOperator.name_norm), conn)
                top_prod = metrics_df.merge(ops, on="operator_id", how="left").sort_values("avg_oil_bpd_30d", ascending=False).head(10)
                top_restart = metrics_df.merge(ops, on="operator_id", how="left").sort_values(
                    "restart_upside_bpd_est", ascending=False
                ).head(10)

        return 0, PipelineResult(run_id, sources_ok, sources_failed, source_summaries, top_prod, top_restart)
    except Exception as exc:
        LOGGER.exception("Pipeline run failed")
        engine = get_engine(get_database_url())
        with engine.begin() as conn:
            record_ingestion_run(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                status="failed",
                sources_ok=sources_ok,
                sources_failed={**sources_failed, "pipeline": str(exc)},
                row_counts=row_counts,
                notes="Pipeline failed",
            )
        return 1, PipelineResult(run_id, sources_ok, {**sources_failed, "pipeline": str(exc)}, source_summaries, pd.DataFrame(), pd.DataFrame())
