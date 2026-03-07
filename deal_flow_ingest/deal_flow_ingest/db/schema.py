from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class DimOperator(TimestampMixin, Base):
    __tablename__ = "dim_operator"

    operator_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name_raw: Mapped[str] = mapped_column(Text, nullable=False)
    name_norm: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False, default="unknown", server_default="unknown")
    source_first_seen: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_last_seen: Mapped[str | None] = mapped_column(Text, nullable=True)


class DimBusinessAssociate(TimestampMixin, Base):
    __tablename__ = "dim_business_associate"

    ba_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ba_name_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    ba_name_norm: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_first_seen: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_last_seen: Mapped[str | None] = mapped_column(Text, nullable=True)


class BridgeOperatorBusinessAssociate(TimestampMixin, Base):
    __tablename__ = "bridge_operator_business_associate"
    __table_args__ = (UniqueConstraint("operator_id", "ba_id", name="uq_bridge_operator_business_associate"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operator_id: Mapped[int] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=False)
    ba_id: Mapped[str] = mapped_column(ForeignKey("dim_business_associate.ba_id"), nullable=False)
    match_method: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)


class DimWell(TimestampMixin, Base):
    __tablename__ = "dim_well"

    well_id: Mapped[str] = mapped_column(Text, primary_key=True)
    uwi_raw: Mapped[str | None] = mapped_column(Text)
    licensee_operator_id: Mapped[int | None] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=True)
    status: Mapped[str | None] = mapped_column(Text)
    lsd: Mapped[str | None] = mapped_column(Text)
    section: Mapped[int | None] = mapped_column(Integer)
    township: Mapped[int | None] = mapped_column(Integer)
    range: Mapped[int | None] = mapped_column(Integer)
    meridian: Mapped[int | None] = mapped_column(Integer)
    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    first_seen: Mapped[datetime | None] = mapped_column(Date)
    last_seen: Mapped[datetime | None] = mapped_column(Date)
    source: Mapped[str | None] = mapped_column(Text)


class DimFacility(TimestampMixin, Base):
    __tablename__ = "dim_facility"

    facility_id: Mapped[str] = mapped_column(Text, primary_key=True)
    facility_type: Mapped[str | None] = mapped_column(Text)
    facility_operator_id: Mapped[int | None] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=True)
    lsd: Mapped[str | None] = mapped_column(Text)
    section: Mapped[int | None] = mapped_column(Integer)
    township: Mapped[int | None] = mapped_column(Integer)
    range: Mapped[int | None] = mapped_column(Integer)
    meridian: Mapped[int | None] = mapped_column(Integer)
    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str | None] = mapped_column(Text)


class BridgeWellFacility(TimestampMixin, Base):
    __tablename__ = "bridge_well_facility"
    __table_args__ = (UniqueConstraint("well_id", "facility_id", "effective_from", name="uq_bridge_well_facility"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    well_id: Mapped[str] = mapped_column(ForeignKey("dim_well.well_id"), nullable=False)
    facility_id: Mapped[str] = mapped_column(ForeignKey("dim_facility.facility_id"), nullable=False)
    effective_from: Mapped[datetime | None] = mapped_column(Date)
    effective_to: Mapped[datetime | None] = mapped_column(Date)
    source: Mapped[str | None] = mapped_column(Text)


class FactFacilityProductionMonthly(Base):
    __tablename__ = "fact_facility_production_monthly"
    __table_args__ = (
        UniqueConstraint("month", "facility_id", "source", name="uq_fact_facility_production_monthly"),
        Index("ix_fact_facility_production_monthly_month", "month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    month: Mapped[datetime] = mapped_column(Date, nullable=False)
    facility_id: Mapped[str] = mapped_column(ForeignKey("dim_facility.facility_id"), nullable=False)
    oil_bbl: Mapped[float | None] = mapped_column(Float)
    gas_mcf: Mapped[float | None] = mapped_column(Float)
    water_bbl: Mapped[float | None] = mapped_column(Float)
    condensate_bbl: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class FactWellProductionMonthly(Base):
    __tablename__ = "fact_well_production_monthly"
    __table_args__ = (
        UniqueConstraint("month", "well_id", "source", name="uq_fact_well_production_monthly"),
        Index("ix_fact_well_production_monthly_month", "month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    month: Mapped[datetime] = mapped_column(Date, nullable=False)
    well_id: Mapped[str] = mapped_column(ForeignKey("dim_well.well_id"), nullable=False)
    oil_bbl: Mapped[float | None] = mapped_column(Float)
    gas_mcf: Mapped[float | None] = mapped_column(Float)
    water_bbl: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    is_estimated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")


class FactOperatorProductionMonthly(Base):
    __tablename__ = "fact_operator_production_monthly"
    __table_args__ = (
        UniqueConstraint("month", "operator_id", "basis_level", "source", name="uq_fact_operator_production_monthly"),
        Index("ix_fact_operator_production_monthly_month", "month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    month: Mapped[datetime] = mapped_column(Date, nullable=False)
    operator_id: Mapped[int] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=False)
    oil_bbl: Mapped[float | None] = mapped_column(Float)
    gas_mcf: Mapped[float | None] = mapped_column(Float)
    water_bbl: Mapped[float | None] = mapped_column(Float)
    basis_level: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class FactInterestOwnership(TimestampMixin, Base):
    __tablename__ = "fact_interest_ownership"
    __table_args__ = (Index("ix_fact_interest_owner_interest", "owner_operator_id", "interest_type"), Index("ix_fact_interest_asset", "asset_type", "asset_id"))

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_operator_id: Mapped[int] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=False)
    interest_type: Mapped[str] = mapped_column(Text, nullable=False, default="unknown", server_default="unknown")
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    asset_id: Mapped[str] = mapped_column(Text, nullable=False)
    interest_fraction: Mapped[float | None] = mapped_column(Float)
    effective_from: Mapped[datetime | None] = mapped_column(Date)
    effective_to: Mapped[datetime | None] = mapped_column(Date)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class FactOperatorLiability(Base):
    __tablename__ = "fact_operator_liability"
    __table_args__ = (
        UniqueConstraint("as_of_date", "operator_id", "source", name="uq_fact_operator_liability"),
        Index("ix_fact_operator_liability_as_of_date", "as_of_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    operator_id: Mapped[int] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=False)
    inactive_wells: Mapped[int | None] = mapped_column(Integer)
    active_wells: Mapped[int | None] = mapped_column(Integer)
    deemed_assets: Mapped[float | None] = mapped_column(Float)
    deemed_liabilities: Mapped[float | None] = mapped_column(Float)
    ratio: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class FactOperatorMetrics(Base):
    __tablename__ = "fact_operator_metrics"
    __table_args__ = (UniqueConstraint("as_of_date", "operator_id", name="uq_fact_operator_metrics"), Index("ix_fact_operator_metrics_as_of_date", "as_of_date"))

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    operator_id: Mapped[int] = mapped_column(ForeignKey("dim_operator.operator_id"), nullable=False)
    avg_oil_bpd_30d: Mapped[float | None] = mapped_column(Float)
    avg_oil_bpd_365d: Mapped[float | None] = mapped_column(Float)
    total_oil_bbl_30d: Mapped[float | None] = mapped_column(Float)
    total_oil_bbl_365d: Mapped[float | None] = mapped_column(Float)
    yoy_change_pct: Mapped[float | None] = mapped_column(Float)
    decline_score: Mapped[float | None] = mapped_column(Float)
    distress_score: Mapped[float | None] = mapped_column(Float)
    suspended_wells_count: Mapped[int | None] = mapped_column(Integer)
    restart_candidates_count: Mapped[int | None] = mapped_column(Integer)
    restart_upside_bpd_est: Mapped[float | None] = mapped_column(Float)
    source_notes: Mapped[dict | str | None] = mapped_column(JSON().with_variant(Text, "sqlite"))


class FactWellStatus(Base):
    __tablename__ = "fact_well_status"
    __table_args__ = (UniqueConstraint("well_id", "status", "status_date", "source", name="uq_fact_well_status"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    well_id: Mapped[str] = mapped_column(ForeignKey("dim_well.well_id"), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    status_date: Mapped[datetime | None] = mapped_column(Date)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class FactWellRestartScore(Base):
    __tablename__ = "fact_well_restart_score"
    __table_args__ = (UniqueConstraint("as_of_date", "well_id", name="uq_fact_well_restart_score"), Index("ix_fact_well_restart_score_as_of_date", "as_of_date"))

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    well_id: Mapped[str] = mapped_column(ForeignKey("dim_well.well_id"), nullable=False)
    current_status: Mapped[str | None] = mapped_column(Text)
    last_prod_month: Mapped[datetime | None] = mapped_column(Date)
    avg_oil_bpd_last_3mo_before_shutin: Mapped[float | None] = mapped_column(Float)
    avg_oil_bpd_last_12mo_before_shutin: Mapped[float | None] = mapped_column(Float)
    shutin_recency_days: Mapped[int | None] = mapped_column(Integer)
    restart_score: Mapped[float | None] = mapped_column(Float)
    flags: Mapped[dict | str | None] = mapped_column(JSON().with_variant(Text, "sqlite"))


class IngestionRun(Base):
    __tablename__ = "ingestion_run"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, nullable=False)
    sources_ok: Mapped[dict | str] = mapped_column(JSON().with_variant(Text, "sqlite"), nullable=False)
    sources_failed: Mapped[dict | str] = mapped_column(JSON().with_variant(Text, "sqlite"), nullable=False)
    row_counts_json: Mapped[dict | str] = mapped_column(JSON().with_variant(Text, "sqlite"), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)


def get_engine(database_url: str):
    return create_engine(database_url, future=True)
