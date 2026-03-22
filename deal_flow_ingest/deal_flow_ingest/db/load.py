from __future__ import annotations

import json
import logging
from datetime import date, datetime
from itertools import islice
from uuid import uuid4

import pandas as pd
from sqlalchemy import bindparam, delete, insert, select, tuple_, update
from sqlalchemy.engine import Connection

from deal_flow_ingest.db.schema import (
    BridgeCrownDispositionClient,
    BridgeCrownDispositionLand,
    BridgeOperatorBusinessAssociate,
    BridgeWellFacility,
    DimBusinessAssociate,
    DimCrownClient,
    DimCrownDisposition,
    DimFacility,
    DimOperator,
    DimPipeline,
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
    logger.info("Starting inserts into %s", label)
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


def _json_payload(conn: Connection, value):
    return _json_serialize_if_needed(conn, value)


def _json_serialize_if_needed(conn: Connection, value):
    if _dialect_name(conn) == "sqlite" and value is not None and isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def json_compat_frame(conn: Connection, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = out[column].map(lambda value: _json_serialize_if_needed(conn, value))
    return out


def _execute_update_in_chunks(conn: Connection, model, key_column: str, payload: list[dict], label: str) -> int:
    if not payload:
        return 0
    chunk_size = _insert_chunk_size(conn)
    table = model.__table__
    non_key_columns = [c.name for c in table.columns if c.name != key_column and c.name not in {"created_at", "updated_at"}]
    stmt = (
        update(table)
        .where(getattr(table.c, key_column) == bindparam(f"pk_{key_column}"))
        .values({col: bindparam(col) for col in non_key_columns})
    )
    updates = []
    for row in payload:
        update_row = {f"pk_{key_column}": row[key_column]}
        update_row.update({col: row.get(col) for col in non_key_columns})
        updates.append(update_row)

    total_chunks = (len(updates) + chunk_size - 1) // chunk_size
    for idx, rows in enumerate(chunked(updates, chunk_size), start=1):
        conn.execute(stmt, rows)
        if idx == 1 or idx == total_chunks or idx % 25 == 0:
            logger.info("Updated chunk %s/%s in %s", idx, total_chunks, label)
    return len(updates)


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
    for column in ["license_number", "well_name", "field_name", "pool_name", "spud_date"]:
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




def upsert_dim_business_associate(conn: Connection, df: pd.DataFrame, source: str) -> dict[str, str]:
    if df.empty:
        return {}

    working = pd.DataFrame()
    working["ba_id"] = df.get("ba_id", "").fillna("").astype(str).str.strip()
    working["ba_name_raw"] = df.get("ba_name_raw", "").replace({pd.NA: None})
    working["entity_type"] = df.get("entity_type", pd.Series([None] * len(df), index=df.index))
    working["ba_name_norm"] = (
        working["ba_name_raw"].fillna("").astype(str).map(normalize_operator_name).replace({"": None})
    )
    working = working[working["ba_id"] != ""].drop_duplicates(subset=["ba_id"], keep="last")

    if working.empty:
        return {}

    ba_ids = working["ba_id"].tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimBusinessAssociate.ba_id, ba_ids)

    inserts: list[dict] = []
    updates: list[dict] = []
    for row in working.to_dict(orient="records"):
        payload = {
            "ba_id": row["ba_id"],
            "ba_name_raw": row.get("ba_name_raw"),
            "ba_name_norm": row.get("ba_name_norm"),
            "entity_type": row.get("entity_type"),
            "source_last_seen": source,
        }
        if row["ba_id"] in existing:
            updates.append(payload)
        else:
            payload["source_first_seen"] = source
            inserts.append(payload)

    if inserts:
        _execute_insert_in_chunks(conn, DimBusinessAssociate, inserts, "dim_business_associate")

    _execute_update_in_chunks(conn, DimBusinessAssociate, "ba_id", updates, "dim_business_associate")

    return {ba_id: ba_id for ba_id in working["ba_id"].tolist()}


def upsert_bridge_operator_business_associate(conn: Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    expected_cols = ["operator_id", "ba_id", "match_method", "confidence"]
    working = df.reindex(columns=expected_cols).copy()
    working["operator_id"] = pd.to_numeric(working["operator_id"], errors="coerce")
    working["ba_id"] = working["ba_id"].fillna("").astype(str).str.strip()
    working["confidence"] = pd.to_numeric(working["confidence"], errors="coerce")
    working = working[working["operator_id"].notna() & (working["ba_id"] != "")]
    if working.empty:
        return 0

    working["operator_id"] = working["operator_id"].astype(int)
    working = working.drop_duplicates(subset=["operator_id", "ba_id"], keep="last")
    conn.execute(delete(BridgeOperatorBusinessAssociate))

    payload = working.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, BridgeOperatorBusinessAssociate, payload, "bridge_operator_business_associate")

def upsert_dim_well(conn: Connection, wells_df: pd.DataFrame) -> int:
    if wells_df.empty:
        return 0

    deduped_wells_df = _dedupe_wells_df(wells_df)
    deduped_wells_df = deduped_wells_df.astype(object).where(pd.notnull(deduped_wells_df), None)
    duplicates_collapsed = len(wells_df) - len(deduped_wells_df)
    if duplicates_collapsed > 0:
        logger.info("Collapsed %s duplicate dim_well rows by well_id before upsert", duplicates_collapsed)

    keys = deduped_wells_df["well_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimWell.well_id, keys)
    inserts = deduped_wells_df[~deduped_wells_df["well_id"].isin(existing)].to_dict(orient="records")
    updates = deduped_wells_df[deduped_wells_df["well_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimWell, inserts, "dim_well")
    _execute_update_in_chunks(conn, DimWell, "well_id", updates, "dim_well")
    return len(inserts) + len(updates)


def upsert_dim_facility(conn: Connection, fac_df: pd.DataFrame) -> int:
    if fac_df.empty:
        return 0
    cleaned_fac_df = fac_df.astype(object).where(pd.notnull(fac_df), None)
    keys = cleaned_fac_df["facility_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimFacility.facility_id, keys)
    inserts = cleaned_fac_df[~cleaned_fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    updates = cleaned_fac_df[cleaned_fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimFacility, inserts, "dim_facility")
    _execute_update_in_chunks(conn, DimFacility, "facility_id", updates, "dim_facility")
    return len(inserts) + len(updates)


def upsert_dim_pipeline(conn: Connection, pipeline_df: pd.DataFrame) -> int:
    if pipeline_df.empty:
        return 0
    cleaned_pipeline_df = pipeline_df.astype(object).where(pd.notnull(pipeline_df), None)
    keys = cleaned_pipeline_df["pipeline_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimPipeline.pipeline_id, keys)
    inserts = cleaned_pipeline_df[~cleaned_pipeline_df["pipeline_id"].isin(existing)].to_dict(orient="records")
    updates = cleaned_pipeline_df[cleaned_pipeline_df["pipeline_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimPipeline, inserts, "dim_pipeline")
    _execute_update_in_chunks(conn, DimPipeline, "pipeline_id", updates, "dim_pipeline")
    return len(inserts) + len(updates)


def upsert_dim_crown_disposition(conn: Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cleaned = df.astype(object).where(pd.notnull(df), None)
    keys = cleaned["disposition_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimCrownDisposition.disposition_id, keys)
    inserts = cleaned[~cleaned["disposition_id"].isin(existing)].to_dict(orient="records")
    updates = cleaned[cleaned["disposition_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        _execute_insert_in_chunks(conn, DimCrownDisposition, inserts, "dim_crown_disposition")
    _execute_update_in_chunks(conn, DimCrownDisposition, "disposition_id", updates, "dim_crown_disposition")
    return len(inserts) + len(updates)


def upsert_dim_crown_client(conn: Connection, df: pd.DataFrame, source: str) -> int:
    if df.empty:
        return 0

    working = df.copy()
    working["client_id"] = working.get("client_id", "").fillna("").astype(str).str.strip()
    working["client_name_raw"] = working.get("client_name_raw", pd.Series([None] * len(working), index=working.index))
    working["client_name_norm"] = working.get("client_name_norm", pd.Series([None] * len(working), index=working.index))
    working = working[working["client_id"] != ""].drop_duplicates(subset=["client_id"], keep="last")
    if working.empty:
        return 0

    cleaned = working.astype(object).where(pd.notnull(working), None)
    keys = cleaned["client_id"].dropna().astype(str).unique().tolist()
    existing = _fetch_existing_values_in_chunks(conn, DimCrownClient.client_id, keys)
    inserts: list[dict] = []
    updates: list[dict] = []
    for row in cleaned.to_dict(orient="records"):
        payload = {
            "client_id": row["client_id"],
            "client_name_raw": row.get("client_name_raw"),
            "client_name_norm": row.get("client_name_norm"),
            "source_last_seen": source,
        }
        if row["client_id"] in existing:
            updates.append(payload)
        else:
            payload["source_first_seen"] = source
            inserts.append(payload)

    if inserts:
        _execute_insert_in_chunks(conn, DimCrownClient, inserts, "dim_crown_client")
    _execute_update_in_chunks(conn, DimCrownClient, "client_id", updates, "dim_crown_client")
    return len(inserts) + len(updates)


def replace_bridge_crown_disposition_client(conn: Connection, df: pd.DataFrame) -> int:
    conn.execute(delete(BridgeCrownDispositionClient))
    if df.empty:
        return 0
    cleaned = df.astype(object).where(pd.notnull(df), None)
    payload = cleaned.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, BridgeCrownDispositionClient, payload, "bridge_crown_disposition_client")


def replace_bridge_crown_disposition_land(conn: Connection, df: pd.DataFrame) -> int:
    conn.execute(delete(BridgeCrownDispositionLand))
    if df.empty:
        return 0
    cleaned = df.astype(object).where(pd.notnull(df), None)
    payload = cleaned.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, BridgeCrownDispositionLand, payload, "bridge_crown_disposition_land")


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


def _delete_matching_fact_rows(conn: Connection, model, df: pd.DataFrame, key_columns: list[str]) -> None:
    if df.empty:
        return
    keys = df[key_columns].drop_duplicates()
    key_tuples = [tuple(x) for x in keys.to_records(index=False)]
    columns = tuple(getattr(model, column) for column in key_columns)
    _delete_tuple_in_chunks(conn, model, columns, key_tuples)


def replace_fact_by_month_range(conn: Connection, model, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    if df.empty:
        conn.execute(delete(model).where(model.month >= start, model.month <= end, model.source == source))
    else:
        min_month = df["month"].min()
        max_month = df["month"].max()
        source_values = df["source"].dropna().astype(str).unique().tolist() if "source" in df.columns else [source]
        conn.execute(delete(model).where(model.month >= min_month, model.month <= max_month, model.source.in_(source_values)))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, model, payload, model.__tablename__)


def replace_fact_operator_prod(conn: Connection, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    if df.empty:
        conn.execute(
            delete(FactOperatorProductionMonthly).where(
                FactOperatorProductionMonthly.month >= start,
                FactOperatorProductionMonthly.month <= end,
                FactOperatorProductionMonthly.source == source,
            )
        )
    else:
        min_month = df["month"].min()
        max_month = df["month"].max()
        source_values = df["source"].dropna().astype(str).unique().tolist() if "source" in df.columns else [source]
        conn.execute(
            delete(FactOperatorProductionMonthly).where(
                FactOperatorProductionMonthly.month >= min_month,
                FactOperatorProductionMonthly.month <= max_month,
                FactOperatorProductionMonthly.source.in_(source_values),
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
    cleaned = df.astype(object).where(pd.notnull(df), None)
    payload = cleaned.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactWellStatus, payload, FactWellStatus.__tablename__)


def replace_fact_metrics(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactOperatorMetrics).where(FactOperatorMetrics.as_of_date == as_of_date))
    payload = df.to_dict(orient="records")
    return _execute_insert_in_chunks(conn, FactOperatorMetrics, payload, FactOperatorMetrics.__tablename__)


def replace_fact_restart(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactWellRestartScore).where(FactWellRestartScore.as_of_date == as_of_date))
    cleaned = df.astype(object).where(pd.notnull(df), None)
    payload = cleaned.to_dict(orient="records")
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
            sources_ok=_json_payload(conn, sources_ok),
            sources_failed=_json_payload(conn, sources_failed),
            row_counts_json=_json_payload(conn, row_counts),
            notes=notes,
        )
    )
    return run_id


def new_run_id() -> str:
    return str(uuid4())
