"""initial schema

Revision ID: 0001_initial
Revises: 
Create Date: 2026-03-04
"""

from alembic import op
import sqlalchemy as sa


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_operator",
        sa.Column("operator_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name_raw", sa.Text(), nullable=False),
        sa.Column("name_norm", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False, server_default="unknown"),
        sa.Column("source_first_seen", sa.Text(), nullable=True),
        sa.Column("source_last_seen", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_dim_operator_name_norm", "dim_operator", ["name_norm"], unique=True)

    op.create_table(
        "dim_well",
        sa.Column("well_id", sa.Text(), primary_key=True),
        sa.Column("uwi_raw", sa.Text(), nullable=True),
        sa.Column("licensee_operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("lsd", sa.Text(), nullable=True),
        sa.Column("section", sa.Integer(), nullable=True),
        sa.Column("township", sa.Integer(), nullable=True),
        sa.Column("range", sa.Integer(), nullable=True),
        sa.Column("meridian", sa.Integer(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("first_seen", sa.Date(), nullable=True),
        sa.Column("last_seen", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "dim_facility",
        sa.Column("facility_id", sa.Text(), primary_key=True),
        sa.Column("facility_type", sa.Text(), nullable=True),
        sa.Column("facility_operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=True),
        sa.Column("lsd", sa.Text(), nullable=True),
        sa.Column("section", sa.Integer(), nullable=True),
        sa.Column("township", sa.Integer(), nullable=True),
        sa.Column("range", sa.Integer(), nullable=True),
        sa.Column("meridian", sa.Integer(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "bridge_well_facility",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("well_id", sa.Text(), sa.ForeignKey("dim_well.well_id"), nullable=False),
        sa.Column("facility_id", sa.Text(), sa.ForeignKey("dim_facility.facility_id"), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("well_id", "facility_id", "effective_from", name="uq_bridge_well_facility"),
    )

    op.create_table(
        "fact_facility_production_monthly",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("facility_id", sa.Text(), sa.ForeignKey("dim_facility.facility_id"), nullable=False),
        sa.Column("oil_bbl", sa.Float(), nullable=True),
        sa.Column("gas_mcf", sa.Float(), nullable=True),
        sa.Column("water_bbl", sa.Float(), nullable=True),
        sa.Column("condensate_bbl", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.UniqueConstraint("month", "facility_id", "source", name="uq_fact_facility_production_monthly"),
    )
    op.create_index("ix_fact_facility_production_monthly_month", "fact_facility_production_monthly", ["month"])

    op.create_table(
        "fact_well_production_monthly",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("well_id", sa.Text(), sa.ForeignKey("dim_well.well_id"), nullable=False),
        sa.Column("oil_bbl", sa.Float(), nullable=True),
        sa.Column("gas_mcf", sa.Float(), nullable=True),
        sa.Column("water_bbl", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("is_estimated", sa.Boolean(), nullable=False, server_default="0"),
        sa.UniqueConstraint("month", "well_id", "source", name="uq_fact_well_production_monthly"),
    )
    op.create_index("ix_fact_well_production_monthly_month", "fact_well_production_monthly", ["month"])

    op.create_table(
        "fact_operator_production_monthly",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=False),
        sa.Column("oil_bbl", sa.Float(), nullable=True),
        sa.Column("gas_mcf", sa.Float(), nullable=True),
        sa.Column("water_bbl", sa.Float(), nullable=True),
        sa.Column("basis_level", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.UniqueConstraint("month", "operator_id", "basis_level", "source", name="uq_fact_operator_production_monthly"),
    )
    op.create_index("ix_fact_operator_production_monthly_month", "fact_operator_production_monthly", ["month"])

    op.create_table(
        "fact_interest_ownership",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("owner_operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=False),
        sa.Column("interest_type", sa.Text(), nullable=False, server_default="unknown"),
        sa.Column("asset_type", sa.Text(), nullable=False),
        sa.Column("asset_id", sa.Text(), nullable=False),
        sa.Column("interest_fraction", sa.Float(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_fact_interest_owner_interest", "fact_interest_ownership", ["owner_operator_id", "interest_type"])
    op.create_index("ix_fact_interest_asset", "fact_interest_ownership", ["asset_type", "asset_id"])

    op.create_table(
        "fact_operator_liability",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=False),
        sa.Column("inactive_wells", sa.Integer(), nullable=True),
        sa.Column("active_wells", sa.Integer(), nullable=True),
        sa.Column("deemed_assets", sa.Float(), nullable=True),
        sa.Column("deemed_liabilities", sa.Float(), nullable=True),
        sa.Column("ratio", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.UniqueConstraint("as_of_date", "operator_id", "source", name="uq_fact_operator_liability"),
    )
    op.create_index("ix_fact_operator_liability_as_of_date", "fact_operator_liability", ["as_of_date"])

    op.create_table(
        "fact_operator_metrics",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=False),
        sa.Column("avg_oil_bpd_30d", sa.Float(), nullable=True),
        sa.Column("avg_oil_bpd_365d", sa.Float(), nullable=True),
        sa.Column("total_oil_bbl_30d", sa.Float(), nullable=True),
        sa.Column("total_oil_bbl_365d", sa.Float(), nullable=True),
        sa.Column("yoy_change_pct", sa.Float(), nullable=True),
        sa.Column("decline_score", sa.Float(), nullable=True),
        sa.Column("distress_score", sa.Float(), nullable=True),
        sa.Column("suspended_wells_count", sa.Integer(), nullable=True),
        sa.Column("restart_candidates_count", sa.Integer(), nullable=True),
        sa.Column("restart_upside_bpd_est", sa.Float(), nullable=True),
        sa.Column("source_notes", sa.JSON().with_variant(sa.Text(), "sqlite"), nullable=True),
        sa.UniqueConstraint("as_of_date", "operator_id", name="uq_fact_operator_metrics"),
    )
    op.create_index("ix_fact_operator_metrics_as_of_date", "fact_operator_metrics", ["as_of_date"])

    op.create_table(
        "fact_well_status",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("well_id", sa.Text(), sa.ForeignKey("dim_well.well_id"), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("status_date", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.UniqueConstraint("well_id", "status", "status_date", "source", name="uq_fact_well_status"),
    )

    op.create_table(
        "fact_well_restart_score",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("well_id", sa.Text(), sa.ForeignKey("dim_well.well_id"), nullable=False),
        sa.Column("current_status", sa.Text(), nullable=True),
        sa.Column("last_prod_month", sa.Date(), nullable=True),
        sa.Column("avg_oil_bpd_last_3mo_before_shutin", sa.Float(), nullable=True),
        sa.Column("avg_oil_bpd_last_12mo_before_shutin", sa.Float(), nullable=True),
        sa.Column("shutin_recency_days", sa.Integer(), nullable=True),
        sa.Column("restart_score", sa.Float(), nullable=True),
        sa.Column("flags", sa.JSON().with_variant(sa.Text(), "sqlite"), nullable=True),
        sa.UniqueConstraint("as_of_date", "well_id", name="uq_fact_well_restart_score"),
    )
    op.create_index("ix_fact_well_restart_score_as_of_date", "fact_well_restart_score", ["as_of_date"])

    op.create_table(
        "ingestion_run",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("sources_ok", sa.JSON().with_variant(sa.Text(), "sqlite"), nullable=False),
        sa.Column("sources_failed", sa.JSON().with_variant(sa.Text(), "sqlite"), nullable=False),
        sa.Column("row_counts_json", sa.JSON().with_variant(sa.Text(), "sqlite"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("ingestion_run")
    op.drop_index("ix_fact_well_restart_score_as_of_date", table_name="fact_well_restart_score")
    op.drop_table("fact_well_restart_score")
    op.drop_table("fact_well_status")
    op.drop_index("ix_fact_operator_metrics_as_of_date", table_name="fact_operator_metrics")
    op.drop_table("fact_operator_metrics")
    op.drop_index("ix_fact_operator_liability_as_of_date", table_name="fact_operator_liability")
    op.drop_table("fact_operator_liability")
    op.drop_index("ix_fact_interest_asset", table_name="fact_interest_ownership")
    op.drop_index("ix_fact_interest_owner_interest", table_name="fact_interest_ownership")
    op.drop_table("fact_interest_ownership")
    op.drop_index("ix_fact_operator_production_monthly_month", table_name="fact_operator_production_monthly")
    op.drop_table("fact_operator_production_monthly")
    op.drop_index("ix_fact_well_production_monthly_month", table_name="fact_well_production_monthly")
    op.drop_table("fact_well_production_monthly")
    op.drop_index("ix_fact_facility_production_monthly_month", table_name="fact_facility_production_monthly")
    op.drop_table("fact_facility_production_monthly")
    op.drop_table("bridge_well_facility")
    op.drop_table("dim_facility")
    op.drop_table("dim_well")
    op.drop_index("ix_dim_operator_name_norm", table_name="dim_operator")
    op.drop_table("dim_operator")
