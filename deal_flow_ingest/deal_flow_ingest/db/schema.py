from __future__ import annotations

from sqlalchemy import JSON, Column, Date, DateTime, Float, ForeignKey, Integer, MetaData, String, Table, Text, UniqueConstraint, create_engine

metadata = MetaData()

dim_operator = Table(
    "dim_operator",
    metadata,
    Column("operator_id", Integer, primary_key=True, autoincrement=True),
    Column("operator_name_raw", Text, nullable=False),
    Column("operator_name_norm", Text, nullable=False, unique=True),
)

fact_production_monthly = Table(
    "fact_production_monthly",
    metadata,
    Column("month", Date, nullable=False),
    Column("operator_id", Integer, ForeignKey("dim_operator.operator_id"), nullable=False),
    Column("oil_bbl", Float, nullable=False),
    Column("gas_mcf", Float),
    Column("water_bbl", Float),
    Column("source", Text, nullable=False),
    Column("basis_level", Text, nullable=False),
    UniqueConstraint("month", "operator_id", "source", "basis_level", name="uq_fact_prod_monthly"),
)

fact_operator_metrics = Table(
    "fact_operator_metrics",
    metadata,
    Column("as_of_date", Date, nullable=False),
    Column("operator_id", Integer, ForeignKey("dim_operator.operator_id"), nullable=False),
    Column("avg_oil_bpd_30d", Float, nullable=False),
    Column("avg_oil_bpd_365d", Float, nullable=False),
    Column("total_oil_bbl_30d", Float, nullable=False),
    Column("total_oil_bbl_365d", Float, nullable=False),
    UniqueConstraint("as_of_date", "operator_id", name="uq_fact_operator_metrics"),
)

ingestion_run = Table(
    "ingestion_run",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("started_at", DateTime, nullable=False),
    Column("finished_at", DateTime, nullable=False),
    Column("status", String(16), nullable=False),
    Column("sources_ok", JSON().with_variant(Text, "sqlite"), nullable=False),
    Column("sources_failed", JSON().with_variant(Text, "sqlite"), nullable=False),
    Column("row_counts_json", JSON().with_variant(Text, "sqlite"), nullable=False),
    Column("notes", Text),
)


def get_engine(database_url: str):
    return create_engine(database_url, future=True)


def create_tables(engine) -> None:
    metadata.create_all(engine)
