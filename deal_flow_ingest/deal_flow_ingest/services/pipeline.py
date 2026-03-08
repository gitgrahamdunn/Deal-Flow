from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
from sqlalchemy import select
from sqlalchemy.engine import make_url

from deal_flow_ingest.config import get_database_url, iter_enabled_sources, load_config
from deal_flow_ingest.db.load import (
    json_compat_frame,
    load_bridge_well_facility,
    new_run_id,
    record_ingestion_run,
    replace_fact_by_month_range,
    replace_fact_liability,
    replace_fact_metrics,
    replace_fact_operator_prod,
    replace_fact_restart,
    replace_fact_well_status,
    upsert_bridge_operator_business_associate,
    _dedupe_wells_df,
    upsert_dim_business_associate,
    upsert_dim_facility,
    upsert_dim_operator,
    upsert_dim_well,
)
from deal_flow_ingest.db.migrate import upgrade_to_head
from deal_flow_ingest.db.schema import (
    DimOperator,
    DimWell,
    FactFacilityProductionMonthly,
    FactOperatorLiability,
    FactOperatorMetrics,
    FactOperatorProductionMonthly,
    FactWellProductionMonthly,
    FactWellRestartScore,
    get_engine,
)
from deal_flow_ingest.io.downloader import Downloader
from deal_flow_ingest.sources import load_dataset
from deal_flow_ingest.transform.metrics import compute_operator_metrics, compute_well_restart_scores, empty_restart_scores_df
from deal_flow_ingest.transform.normalize import month_start, normalize_operator_name, normalize_uwi

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
    "status",
    "licensee_operator_id",
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


def prepare_source_frames(args) -> tuple[dict[str, list[pd.DataFrame]], list[str], dict[str, str], dict, list[dict[str, object]]]:
    cfg = load_config(args.config)
    source_entries = [s for s in iter_enabled_sources(cfg) if s.enabled]
    sample_dir = Path(__file__).resolve().parent.parent / "sample_data"
    downloader = Downloader(Path("data/raw"))

    frames_by_kind: dict[str, list[pd.DataFrame]] = {}
    sources_ok: list[str] = []
    sources_failed: dict[str, str] = {}
    row_counts: dict[str, int | dict] = {"source_rows": {}, "loaded": {}}
    source_summaries: list[dict[str, object]] = []

    LOGGER.info("Enabled sources: %s", [s.key for s in source_entries])

    for source in source_entries:
        try:
            df = load_dataset(downloader, sample_dir, source, args.dry_run, args.refresh)
            frames_by_kind.setdefault(source.data_kind, []).append(df)
            row_counts["source_rows"][source.key] = int(len(df))
            sources_ok.append(source.key)
            last_result = downloader.last_result
            source_summaries.append(
                {
                    "source": source.key,
                    "artifact_downloaded": bool(last_result.downloaded) if last_result else False,
                    "rows_parsed": int(len(df)),
                    "load_status": "success",
                }
            )
            LOGGER.info("Loaded source %s (%s rows)", source.key, len(df))
        except Exception as exc:
            sources_failed[source.key] = str(exc)
            source_summaries.append(
                {
                    "source": source.key,
                    "artifact_downloaded": False,
                    "rows_parsed": 0,
                    "load_status": f"failed: {exc}",
                }
            )
            LOGGER.exception("Failed to load source %s", source.key)

    return frames_by_kind, sources_ok, sources_failed, row_counts, source_summaries


def _merge_kind_frames(frames_by_kind: dict[str, list[pd.DataFrame]], kind: str) -> pd.DataFrame:
    frames = [f for f in frames_by_kind.get(kind, []) if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def prepare_operator_dim_inputs(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    wells = _merge_kind_frames(frames_by_kind, "wells")
    facilities = _merge_kind_frames(frames_by_kind, "facility_master")
    operators = _merge_kind_frames(frames_by_kind, "operators")
    liability = _merge_kind_frames(frames_by_kind, "liability")
    return pd.DataFrame(
        {
            "name_raw": pd.concat(
                [
                    wells.get("licensee", pd.Series(dtype=str)),
                    facilities.get("facility_operator", pd.Series(dtype=str)),
                    operators.get("ba_name_raw", pd.Series(dtype=str)),
                    operators.get("name_raw", pd.Series(dtype=str)),
                    liability.get("operator", pd.Series(dtype=str)),
                ],
                ignore_index=True,
            ).fillna("").astype(str)
        }
    )


def prepare_wells_df(frames_by_kind: dict[str, list[pd.DataFrame]], op_map: dict[str, int], as_of: date) -> pd.DataFrame:
    df = _merge_kind_frames(frames_by_kind, "wells")
    if df.empty:
        return _empty_df(WELL_DIM_COLUMNS)

    out = pd.DataFrame()
    out["uwi_raw"] = df["uwi"].astype(str)
    out["well_id"] = out["uwi_raw"].map(normalize_uwi)
    out["status"] = df.get("status", "UNKNOWN").astype(str)
    out["licensee_operator_id"] = df.get("licensee", "").astype(str).map(normalize_operator_name).map(op_map)
    out["lsd"] = df.get("lsd")
    out["section"] = pd.to_numeric(df.get("section"), errors="coerce")
    out["township"] = pd.to_numeric(df.get("township"), errors="coerce")
    out["range"] = pd.to_numeric(df.get("range"), errors="coerce")
    out["meridian"] = pd.to_numeric(df.get("meridian"), errors="coerce")
    out["lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
    out["lon"] = pd.to_numeric(df.get("lon"), errors="coerce")
    out["first_seen"] = as_of
    out["last_seen"] = as_of
    out["source"] = "aer_st37"
    return out


def prepare_facilities_df(frames_by_kind: dict[str, list[pd.DataFrame]], operator_map: dict[str, int]) -> pd.DataFrame:
    fac_raw = _merge_kind_frames(frames_by_kind, "facility_master")
    if fac_raw.empty:
        return pd.DataFrame()

    fac_df = pd.DataFrame()
    fac_df["facility_id"] = fac_raw["facility_id"].astype(str)
    fac_df["facility_type"] = fac_raw.get("facility_type")
    fac_df["facility_operator_id"] = fac_raw.get("facility_operator", "").astype(str).map(normalize_operator_name).map(operator_map)
    fac_df["lsd"] = fac_raw.get("lsd")
    fac_df["section"] = pd.to_numeric(fac_raw.get("section"), errors="coerce")
    fac_df["township"] = pd.to_numeric(fac_raw.get("township"), errors="coerce")
    fac_df["range"] = pd.to_numeric(fac_raw.get("range"), errors="coerce")
    fac_df["meridian"] = pd.to_numeric(fac_raw.get("meridian"), errors="coerce")
    fac_df["lat"] = pd.to_numeric(fac_raw.get("lat"), errors="coerce")
    fac_df["lon"] = pd.to_numeric(fac_raw.get("lon"), errors="coerce")
    fac_df["source"] = "petrinex_facility"
    return fac_df


def prepare_bridge_df(frames_by_kind: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    bridge_raw = _merge_kind_frames(frames_by_kind, "well_facility_bridge")
    if bridge_raw.empty:
        return _empty_df(BRIDGE_COLUMNS)

    bridge_df = _empty_df(BRIDGE_COLUMNS)
    bridge_df["well_id"] = bridge_raw["well_id"].astype(str).map(normalize_uwi)
    bridge_df["facility_id"] = bridge_raw["facility_id"].astype(str)
    bridge_df["effective_from"] = pd.to_datetime(bridge_raw.get("effective_from"), errors="coerce").dt.date
    bridge_df["effective_to"] = pd.to_datetime(bridge_raw.get("effective_to"), errors="coerce").dt.date
    bridge_df["effective_from"] = bridge_df["effective_from"].map(lambda v: None if pd.isna(v) else v)
    bridge_df["effective_to"] = bridge_df["effective_to"].map(lambda v: None if pd.isna(v) else v)
    bridge_df["source"] = "petrinex_bridge"
    return bridge_df


def prepare_production_dfs(
    frames_by_kind: dict[str, list[pd.DataFrame]],
    bridge_df: pd.DataFrame,
    wells_df: pd.DataFrame,
    start: date,
    end: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fac_prod_raw = _merge_kind_frames(frames_by_kind, "facility_production")
    fac_prod = _empty_df(FACILITY_PROD_COLUMNS)
    if not fac_prod_raw.empty:
        fac_prod["month"] = month_start(fac_prod_raw["month"]).dt.date
        fac_prod["facility_id"] = fac_prod_raw["facility_id"].astype(str)
        fac_prod["oil_bbl"] = pd.to_numeric(fac_prod_raw.get("oil_bbl", 0), errors="coerce").fillna(0.0)
        fac_prod["gas_mcf"] = pd.to_numeric(fac_prod_raw.get("gas_mcf", 0), errors="coerce").fillna(0.0)
        fac_prod["water_bbl"] = pd.to_numeric(fac_prod_raw.get("water_bbl", 0), errors="coerce").fillna(0.0)
        fac_prod["condensate_bbl"] = pd.to_numeric(fac_prod_raw.get("condensate_bbl", 0), errors="coerce").fillna(0.0)
        fac_prod["source"] = "petrinex_production"

    if fac_prod.empty or bridge_df.empty:
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
    if not wells_df.empty:
        status_df["well_id"] = wells_df["well_id"]
        status_df["status"] = wells_df["status"]
        status_df["status_date"] = None
        status_df["source"] = "aer_st37"

    op_prod = _empty_df(OPERATOR_PROD_COLUMNS)
    if not well_prod.empty and not wells_df.empty:
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


def run_ingestion_pipeline(args) -> tuple[int, PipelineResult | None]:
    LOGGER.info("Starting ingestion run")
    ensure_database_dir()
    end = _parse_date(args.end, date.today())
    start = _parse_date(args.start, end - timedelta(days=365))
    started_at = datetime.utcnow()
    run_id = new_run_id()

    LOGGER.info("Loading configuration from %s", args.config)
    frames_by_kind, sources_ok, sources_failed, row_counts, source_summaries = prepare_source_frames(args)

    try:
        upgrade_to_head()
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

            with _phase("Deduplicating wells dataframe", lambda: len(deduped_wells_preview)):
                deduped_wells_preview = _dedupe_wells_df(wells_df) if not wells_df.empty else wells_df

            LOGGER.info(
                "Wells row profile: st37_original_rows=%s deduped_rows=%s",
                len(wells_df),
                len(deduped_wells_preview),
            )
            with _phase("Upserting dim_well", lambda: row_counts["loaded"].get("dim_well", 0)):
                row_counts["loaded"]["dim_well"] = upsert_dim_well(conn, wells_df)

            with _phase("Preparing facilities dataframe", lambda: len(fac_df)):
                fac_df = prepare_facilities_df(frames_by_kind, operator_map)
            with _phase("Upserting dim_facility", lambda: row_counts["loaded"].get("dim_facility", 0)):
                row_counts["loaded"]["dim_facility"] = upsert_dim_facility(conn, fac_df)

            with _phase("Preparing bridge dataframe", lambda: len(bridge_df)):
                bridge_df = prepare_bridge_df(frames_by_kind)
            with _phase("Loading bridge_well_facility", lambda: row_counts["loaded"].get("bridge_well_facility", 0)):
                row_counts["loaded"]["bridge_well_facility"] = load_bridge_well_facility(conn, bridge_df)

            with _phase("Preparing facility production dataframe", lambda: len(fac_prod)):
                fac_prod, well_prod, status_df, op_prod = prepare_production_dfs(frames_by_kind, bridge_df, wells_df, start, end)
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
                    conn, op_prod_load_df, "derived_well_estimated", start, end
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

            with _phase("Recording ingestion_run", lambda: 1):
                record_ingestion_run(
                    conn,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=datetime.utcnow(),
                    status="success",
                    sources_ok=sources_ok,
                    sources_failed=sources_failed,
                    row_counts=row_counts,
                    notes="Resilient ingestion run completed",
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
                finished_at=datetime.utcnow(),
                status="failed",
                sources_ok=sources_ok,
                sources_failed={**sources_failed, "pipeline": str(exc)},
                row_counts=row_counts,
                notes="Pipeline failed",
            )
        return 1, PipelineResult(run_id, sources_ok, {**sources_failed, "pipeline": str(exc)}, source_summaries, pd.DataFrame(), pd.DataFrame())
