"""enable postgis when running on postgres

Revision ID: 0007_enable_postgis
Revises: 0006_pipeline_geometry_wkt
Create Date: 2026-03-22
"""

from alembic import op


revision = "0007_enable_postgis"
down_revision = "0006_pipeline_geometry_wkt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name.lower().startswith("postgres"):
        op.execute("CREATE EXTENSION IF NOT EXISTS postgis")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name.lower().startswith("postgres"):
        op.execute("DROP EXTENSION IF EXISTS postgis")
