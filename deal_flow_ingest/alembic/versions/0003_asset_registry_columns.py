"""add asset registry columns to well and facility dimensions

Revision ID: 0003_asset_registry
Revises: 0002_business_associate
Create Date: 2026-03-21
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_asset_registry"
down_revision = "0002_business_associate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dim_well", sa.Column("license_number", sa.Text(), nullable=True))
    op.add_column("dim_well", sa.Column("well_name", sa.Text(), nullable=True))
    op.add_column("dim_well", sa.Column("field_name", sa.Text(), nullable=True))
    op.add_column("dim_well", sa.Column("pool_name", sa.Text(), nullable=True))
    op.add_column("dim_well", sa.Column("spud_date", sa.Date(), nullable=True))

    op.add_column("dim_facility", sa.Column("facility_name", sa.Text(), nullable=True))
    op.add_column("dim_facility", sa.Column("license_number", sa.Text(), nullable=True))
    op.add_column("dim_facility", sa.Column("facility_subtype", sa.Text(), nullable=True))
    op.add_column("dim_facility", sa.Column("facility_status", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("dim_facility", "facility_status")
    op.drop_column("dim_facility", "facility_subtype")
    op.drop_column("dim_facility", "license_number")
    op.drop_column("dim_facility", "facility_name")

    op.drop_column("dim_well", "spud_date")
    op.drop_column("dim_well", "pool_name")
    op.drop_column("dim_well", "field_name")
    op.drop_column("dim_well", "well_name")
    op.drop_column("dim_well", "license_number")
