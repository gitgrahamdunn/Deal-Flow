"""add pipeline dimension

Revision ID: 0004_pipeline_registry
Revises: 0003_asset_registry
Create Date: 2026-03-21
"""

from alembic import op
import sqlalchemy as sa


revision = "0004_pipeline_registry"
down_revision = "0003_asset_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_pipeline",
        sa.Column("pipeline_id", sa.Text(), nullable=False),
        sa.Column("license_number", sa.Text(), nullable=True),
        sa.Column("line_number", sa.Text(), nullable=True),
        sa.Column("licence_line_number", sa.Text(), nullable=True),
        sa.Column("operator_id", sa.Integer(), nullable=True),
        sa.Column("company_name", sa.Text(), nullable=True),
        sa.Column("ba_code", sa.Text(), nullable=True),
        sa.Column("segment_status", sa.Text(), nullable=True),
        sa.Column("from_facility_type", sa.Text(), nullable=True),
        sa.Column("from_location", sa.Text(), nullable=True),
        sa.Column("to_facility_type", sa.Text(), nullable=True),
        sa.Column("to_location", sa.Text(), nullable=True),
        sa.Column("substance1", sa.Text(), nullable=True),
        sa.Column("substance2", sa.Text(), nullable=True),
        sa.Column("substance3", sa.Text(), nullable=True),
        sa.Column("segment_length_km", sa.Float(), nullable=True),
        sa.Column("geometry_source", sa.Text(), nullable=True),
        sa.Column("centroid_lat", sa.Float(), nullable=True),
        sa.Column("centroid_lon", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["operator_id"], ["dim_operator.operator_id"]),
        sa.PrimaryKeyConstraint("pipeline_id"),
    )
    op.create_index("ix_dim_pipeline_licence_line_number", "dim_pipeline", ["licence_line_number"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_dim_pipeline_licence_line_number", table_name="dim_pipeline")
    op.drop_table("dim_pipeline")
