from __future__ import annotations

import json
from datetime import date, datetime
from uuid import uuid4

import pandas as pd
from sqlalchemy import delete, insert, select

from deal_flow_ingest.db.schema import dim_operator, fact_operator_metrics, fact_production_monthly, ingestion_run


def upsert_operators(conn, normalized_df: pd.DataFrame) -> dict[str, int]:
    existing = {
        row.operator_name_norm: row.operator_id
        for row in conn.execute(select(dim_operator.c.operator_id, dim_operator.c.operator_name_norm))
    }

    new_rows = []
    for _, row in normalized_df[["operator_name_raw", "operator_name_norm"]].drop_duplicates().iterrows():
        if row["operator_name_norm"] not in existing:
            new_rows.append(row.to_dict())

    if new_rows:
        conn.execute(insert(dim_operator), new_rows)

    refreshed = {
        row.operator_name_norm: row.operator_id
        for row in conn.execute(select(dim_operator.c.operator_id, dim_operator.c.operator_name_norm))
    }
    return refreshed


def load_monthly_facts(conn, df: pd.DataFrame, start_date: date, end_date: date) -> int:
    conn.execute(
        delete(fact_production_monthly).where(
            fact_production_monthly.c.month >= start_date,
            fact_production_monthly.c.month <= end_date,
        )
    )
    payload = df[
        ["month", "operator_id", "oil_bbl", "gas_mcf", "water_bbl", "source", "basis_level"]
    ].to_dict(orient="records")
    if payload:
        conn.execute(insert(fact_production_monthly), payload)
    return len(payload)


def load_metrics(conn, metrics_df: pd.DataFrame, as_of_date: date) -> int:
    conn.execute(delete(fact_operator_metrics).where(fact_operator_metrics.c.as_of_date == as_of_date))
    payload = metrics_df.to_dict(orient="records")
    if payload:
        conn.execute(insert(fact_operator_metrics), payload)
    return len(payload)


def record_ingestion_run(
    conn,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    sources_ok: list[str],
    sources_failed: dict[str, str],
    row_counts: dict,
    notes: str,
) -> str:
    run_id = str(uuid4())
    conn.execute(
        insert(ingestion_run).values(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            sources_ok=json.dumps(sources_ok),
            sources_failed=json.dumps(sources_failed),
            row_counts_json=json.dumps(row_counts),
            notes=notes,
        )
    )
    return run_id
