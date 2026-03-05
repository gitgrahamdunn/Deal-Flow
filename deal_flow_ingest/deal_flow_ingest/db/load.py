from __future__ import annotations

import json
from datetime import date, datetime
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


def _json_payload(value):
    return json.dumps(value)


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
        conn.execute(insert(DimOperator), to_insert)

    mapping = {r.name_norm: r.operator_id for r in conn.execute(select(DimOperator.name_norm, DimOperator.operator_id))}
    return mapping


def upsert_dim_well(conn: Connection, wells_df: pd.DataFrame) -> int:
    if wells_df.empty:
        return 0
    keys = wells_df["well_id"].dropna().astype(str).unique().tolist()
    existing = {r.well_id for r in conn.execute(select(DimWell.well_id).where(DimWell.well_id.in_(keys)))}
    inserts = wells_df[~wells_df["well_id"].isin(existing)].to_dict(orient="records")
    updates = wells_df[wells_df["well_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        conn.execute(insert(DimWell), inserts)
    for row in updates:
        conn.execute(
            DimWell.__table__.update().where(DimWell.well_id == row["well_id"]).values(**{k: v for k, v in row.items() if k != "well_id"})
        )
    return len(inserts) + len(updates)


def upsert_dim_facility(conn: Connection, fac_df: pd.DataFrame) -> int:
    if fac_df.empty:
        return 0
    keys = fac_df["facility_id"].dropna().astype(str).unique().tolist()
    existing = {r.facility_id for r in conn.execute(select(DimFacility.facility_id).where(DimFacility.facility_id.in_(keys)))}
    inserts = fac_df[~fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    updates = fac_df[fac_df["facility_id"].isin(existing)].to_dict(orient="records")
    if inserts:
        conn.execute(insert(DimFacility), inserts)
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
    if key_tuples:
        conn.execute(
            delete(BridgeWellFacility).where(
                tuple_(BridgeWellFacility.well_id, BridgeWellFacility.facility_id, BridgeWellFacility.effective_from).in_(key_tuples)
            )
        )
    payload = bridge_df.to_dict(orient="records")
    conn.execute(insert(BridgeWellFacility), payload)
    return len(payload)


def replace_fact_by_month_range(conn: Connection, model, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    conn.execute(delete(model).where(model.month >= start, model.month <= end, model.source == source))
    payload = df.to_dict(orient="records")
    if payload:
        conn.execute(insert(model), payload)
    return len(payload)


def replace_fact_operator_prod(conn: Connection, df: pd.DataFrame, source: str, start: date, end: date) -> int:
    conn.execute(
        delete(FactOperatorProductionMonthly).where(
            FactOperatorProductionMonthly.month >= start,
            FactOperatorProductionMonthly.month <= end,
            FactOperatorProductionMonthly.source == source,
        )
    )
    payload = df.to_dict(orient="records")
    if payload:
        conn.execute(insert(FactOperatorProductionMonthly), payload)
    return len(payload)


def replace_fact_liability(conn: Connection, df: pd.DataFrame, source: str) -> int:
    if df.empty:
        return 0
    dates = df["as_of_date"].dropna().unique().tolist()
    conn.execute(delete(FactOperatorLiability).where(FactOperatorLiability.as_of_date.in_(dates), FactOperatorLiability.source == source))
    payload = df.to_dict(orient="records")
    conn.execute(insert(FactOperatorLiability), payload)
    return len(payload)


def replace_fact_well_status(conn: Connection, df: pd.DataFrame, source: str) -> int:
    if df.empty:
        return 0
    conn.execute(delete(FactWellStatus).where(FactWellStatus.source == source))
    payload = df.to_dict(orient="records")
    conn.execute(insert(FactWellStatus), payload)
    return len(payload)


def replace_fact_metrics(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactOperatorMetrics).where(FactOperatorMetrics.as_of_date == as_of_date))
    payload = df.to_dict(orient="records")
    if payload:
        conn.execute(insert(FactOperatorMetrics), payload)
    return len(payload)


def replace_fact_restart(conn: Connection, df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(FactWellRestartScore).where(FactWellRestartScore.as_of_date == as_of_date))
    payload = df.to_dict(orient="records")
    if payload:
        conn.execute(insert(FactWellRestartScore), payload)
    return len(payload)


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
