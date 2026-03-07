"""add business associate dimension and operator bridge

Revision ID: 0002_business_associate
Revises: 0001_initial
Create Date: 2026-03-07
"""

from alembic import op
import sqlalchemy as sa


revision = "0002_business_associate"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_business_associate",
        sa.Column("ba_id", sa.Text(), primary_key=True),
        sa.Column("ba_name_raw", sa.Text(), nullable=True),
        sa.Column("ba_name_norm", sa.Text(), nullable=True),
        sa.Column("entity_type", sa.Text(), nullable=True),
        sa.Column("source_first_seen", sa.Text(), nullable=True),
        sa.Column("source_last_seen", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_dim_business_associate_ba_name_norm", "dim_business_associate", ["ba_name_norm"], unique=False)

    op.create_table(
        "bridge_operator_business_associate",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("operator_id", sa.Integer(), sa.ForeignKey("dim_operator.operator_id"), nullable=False),
        sa.Column("ba_id", sa.Text(), sa.ForeignKey("dim_business_associate.ba_id"), nullable=False),
        sa.Column("match_method", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("operator_id", "ba_id", name="uq_bridge_operator_business_associate"),
    )


def downgrade() -> None:
    op.drop_table("bridge_operator_business_associate")
    op.drop_index("ix_dim_business_associate_ba_name_norm", table_name="dim_business_associate")
    op.drop_table("dim_business_associate")
