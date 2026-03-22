"""add pipeline geometry wkt column

Revision ID: 0006_pipeline_geometry_wkt
Revises: 0005_crown_tenure_registry
Create Date: 2026-03-22
"""

from alembic import op
import sqlalchemy as sa


revision = "0006_pipeline_geometry_wkt"
down_revision = "0005_crown_tenure_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dim_pipeline", sa.Column("geometry_wkt", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("dim_pipeline", "geometry_wkt")
