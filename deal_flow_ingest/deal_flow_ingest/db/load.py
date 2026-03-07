from __future__ import annotations

import json
import logging
from datetime import date, datetime
from itertools import islice
from uuid import uuid4

import pandas as pd
from sqlalchemy import delete, insert, select, tuple_
from sqlalchemy.engine import Connection

from deal_flow_ingest.db.schema import (
    BridgeWellFacility,
    DimFacility,
    DimOperator,
    DimWell,
    FactFacilityProductionMonthly,
    FactOperatorLiability,
    FactOperatorMetrics,
    FactOperatorProductionMonthly,
    FactWellProductionMonthly,
    FactWellRestartScore,
    FactWellStatus,
    IngestionRun,
)
from deal_flow_ingest.transform.normalize import normalize_operator_name


logger = logging.getLogger(__name__)

SQLITE_LOOKUP_CHUNK_SIZE = 500
SQLITE_INSERT_CHUNK_SIZE = 500
POSTGRES_LOOKUP_CHUNK_SIZE = 5000
POSTGRES_INSERT_CHUNK_SIZE = 5000


def chunked(iterable, size: int):
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch


def _dialect_name(conn: Connection) -> str:
    return conn.engine.dialect.name.lower()


def _lookup_chunk_size(conn: Connection) -> int:
    return SQLITE_LOOKUP_CHUNK_SIZE if _dialect_name(conn) == "sqlite" else POSTGRES_LOOKUP_CHUNK_SIZE


def _insert_chunk_size(conn: Connection) -> int:
    return SQLITE_INSERT_CHUNK_SIZE if _dialect_name(conn) == "sqlite" else POSTGRES_INSERT_CHUNK_SIZE


def _execute_insert_in_chunks(conn: Connection, model, payload: list[dict], label: str) -> int:
    if not payload:
        return 0
    chunk_size = _insert_chunk_size(conn)
    total_chunks = (len(payload) + chunk_size - 1) // chunk_size
    logger.info("Loading %s in chunks of %s (%s rows)", label, chunk_size, len(payload))
    for idx, rows in enumerate(chunked(payload, chunk_size), start=1):
        conn.execute(insert(model), rows)
        if idx == 1 or idx == total_chunks or idx % 25 == 0:
            logger.info("Inserted chunk %s/%s into %s", idx, total_chunks, label)
    return len(payload)


def _fetch_existing_values_in_chunks(conn: Connection, column, values: list[str]) -> set[str]:
    existing: set[str] = set()
    chunk_size = _lookup_chunk_size(conn)
    for chunk in chunked(values, chunk_size):
        existing.update(r[0] for r in conn.execute(select(column).where(column.in_(chunk))))
    return existing


def _delete_in_values_chunks(conn: Connection, model, column, values: list, extra_predicates: tuple = ()) -> None:
    if not values:
        return
    chunk_size = _lookup_chunk_size(conn)
    for chunk in chunked(values, chunk_size):
        predicates = (column.in_(chunk), *extra_predicates)
        conn.execute(delete(model).where(*predicates))


def _delete_tuple_in_chunks(conn: Connection, model, columns: tuple, key_tuples: list[tuple]) -> None:
    if not key_tuples:
        return
    chunk_size = _lookup_chunk_size(conn)
    for chunk in chunked(key_tuples, chunk_size):
        conn.execute(delete(model).where(tuple_(*columns).in_(chunk)))


def _json_payload(value):
    return json.dumps(value)


def _dedupe_wells_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "well_id" not in df.columns:
        return df

    working = df.copy()
    working["_row_order"] = range(len(working))

    status_non_empty = (
        working["status"].fillna("").astype(str).str.strip().ne("") if "status" in working.columns else pd.Series(False, index=working.index)
    )
    licensee_present = (
        working["licensee_operator_id"].notna() if "licensee_operator_id" in working.columns else pd.Series(False, index=working.index)
    )

    score = status_non_empty.astype(int) + licensee_present.astype(int)
    for column in ["lsd", "section", "township", "range", "meridian"]:
        score += (working[column].notna().astype(int) if column in working.columns else 0)

    has_lat_lon = (
        working[[c for c in ["lat", "lon"] if c in working.columns]].notna().any(axis=1).astype(int)
        if any(c in working.columns for c in ["lat", "lon"])
        else 0
    )
    score += has_lat_lon

    working["_completeness_score"] = score
    deduped = (
        working.sort_values(["_completeness_score", "_row_order"], ascending=[False, True])
        .drop_duplicates(subset=["well_id"], keep="first")
        .sort_values("_row_order")
        .drop(columns=["_completeness_score", "_row_order"])
    )
    return deduped


def upsert_dim_operator(conn: Connection, df: pd.DataFrame, entity_type: str = "operator", source: str | None = None) -> dict[str, int]:
    if df.empty:
        return {}
    names = df["name_raw"].fillna("").astype(str)
    payload = pd.DataFrame({"name_raw": names, "name_norm": names.map(normalize_operator_name)})
    payload = payload[payload["name_norm"] != ""].drop_duplicates(subset=["name_norm"])

    existing = {r.name_norm: r.operator_id for r in conn.execute(select(DimOperator.name_norm, DimOperator.operator_id))}
    to_insert = []
    for _, row in payload.iterrows():
        if row["name_norm"] not in existing:
            to_insert.append(
                {
                    "name_raw": row["name_raw"],
                    "name_norm": row["name_norm"],
                    "entity_type": entity_type,
                    "source_first_seen": source,
                    "source_last_seen": source,
                }
            )
    if to_insert:
        _execute_insert_in_chunks(conn, DimOperator, to_insert, "dim_operator")

    mapping = {r.name_norm: r.operator_id for r in conn.execute(select(DimOperator.name_norm, DimOperator.operator_id))}
    return mapping


def upsert_dim_well(conn: Connection, wells_df: pd.DataFrame) -> int:
    if wells_df.empty:
        return 0

    deduped_wells_df = _dedupe_wells_df(wells_df)
    duplicates_collapsed = len(wells_df) - len(deduped_wells_df)
    if duplicates_collapsed > 0:
        logger.info("Collapsed %s duplicate dim_well rows by well_id before upsert", duplicates_collapsed)

    keys = deduped_wells_df["well_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimWell.well_id, keys)
    inserts = deduped_wells_df[~deduped_wells_df["well_id"].isin(existing)].to_dict(orient="records")
    updates = deduped_wells_df[deduped_wells_df["well_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimWell, inserts, "dim_well")
    for row in updates:
        conn.execute(
            DimWell.__table__.update().where(DimWell.well_id == row["well_id"]).values(**{k: v for k, v in row.items() if k != "well_id"})
        )
    return len(inserts) + len(updates)


def upsert_dim_facility(conn: Connection, fac_df: pd.DataFrame) -> int:
    if fac_df.empty:
        return 0
    keys = fac_df["facility_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimFacility.facility_id, keys)
    inserts = fac_df[~fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    updates = fac_df[fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimFacility, inserts, "dim_facility")
    for row in updates:
        conn.execute(
            DimFacility.__table__.update().where(DimFacility.facility_id == row["facility_id"]).values(
                **{k: v for k, v in row.items() if k != "facility_id"}
            )
        )
    return len(inserts) + len(updates)


def load_bridge_well_facility(conn: Connection, bridge_df: pd.DataFrame) -> int:
    if bridge_df.empty:
        return 0
    keys = bridge_df[["well_id", "facility_id", "effective_from"]].drop_duplicates()
    key_tuples = [tuple(x) for x in keys.to_records(index=False)]
    _delete_tuple_in_chunks(
        conn,
        BridgeWellFacility,
        (BridgeWellFacility.well_id, BridgeWellFacility.facility_id, BridgeWellFacility.effective_from),
        key_tuples,
    )
    payload = bridge_df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, BridgeWellFacility, payload, "bridge_well_facility")


def replace_fact_by_month_range(conn: Connection, model, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    conn.execute(delete(model).where(model.month >= start, model.month <= end, model.source == source))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, model, payload, model.__tablename__)


def replace_fact_operator_prod(conn: Connection, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    conn.execute(
        delete(FactOperatorProductionMonthly).where(
            FactOperatorProductionMonthly.month >= start,
            FactOperatorProductionMonthly.month <= end,
            FactOperatorProductionMonthly.source == source,
        )
    )
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactOperatorProductionMonthly, payload, FactOperatorProductionMonthly.__tablename__)


def replace_fact_liability(conn: Connection, df: pd.DataFrame, source: str) -> int:
    if df.empty:
        return 0
    dates = df["as_of_date"].dropna().unique().tolist()
    _delete_in_values_chunks(conn, FactOperatorLiability, FactOperatorLiability.as_of_date, dates, (FactOperatorLiability.source == source,))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactOperatorLiability, payload, FactOperatorLiability.__tablename__)


def replace_fact_well_status(conn: Connection, df: pd.DataFrame, source: str) -> int:
    if df.empty:
        return 0
    conn.execute(delete(FactWellStatus).where(FactWellStatus.source == source))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactWellStatus, payload, FactWellStatus.__tablename__)


def replace_fact_metrics(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactOperatorMetrics).where(FactOperatorMetrics.as_of_date == as_of_date))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactOperatorMetrics, payload, FactOperatorMetrics.__tablename__)


def replace_fact_restart(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactWellRestartScore).where(FactWellRestartScore.as_of_date == as_of_date))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactWellRestartScore, payload, FactWellRestartScore.__tablename__)


def record_ingestion_run(
    conn: Connection,
    run_id: str,
    started_at: datetime,
    finished_at: datetime | None,
    status: str,
    sources_ok: list[str],
    sources_failed: dict[str, str],
    row_counts: dict,
    notes: str | None,
) -> str:
    conn.execute(
        insert(IngestionRun).values(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            sources_ok=_json_payload(sources_ok),
            sources_failed=_json_payload(sources_failed),
            row_counts_json=_json_payload(row_counts),
            notes=notes,
        )
    )
    return run_id


def new_run_id() -> str:
    return str(uuid4())
