"""add crown tenure registry tables

Revision ID: 0005_crown_tenure_registry
Revises: 0004_pipeline_registry
Create Date: 2026-03-22
"""

from alembic import op
import sqlalchemy as sa


revision = "0005_crown_tenure_registry"
down_revision = "0004_pipeline_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_crown_disposition",
        sa.Column("disposition_id", sa.Text(), nullable=False),
        sa.Column("agreement_no", sa.Text(), nullable=True),
        sa.Column("disposition_type", sa.Text(), nullable=True),
        sa.Column("disposition_status", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("disposition_id"),
    )
    op.create_index("ix_dim_crown_disposition_agreement_no", "dim_crown_disposition", ["agreement_no"], unique=False)

    op.create_table(
        "dim_crown_client",
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("client_name_raw", sa.Text(), nullable=True),
        sa.Column("client_name_norm", sa.Text(), nullable=True),
        sa.Column("source_first_seen", sa.Text(), nullable=True),
        sa.Column("source_last_seen", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("client_id"),
    )
    op.create_index("ix_dim_crown_client_client_name_norm", "dim_crown_client", ["client_name_norm"], unique=False)

    op.create_table(
        "bridge_crown_disposition_client",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("disposition_id", sa.Text(), nullable=False),
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("role_type", sa.Text(), nullable=False, server_default="holder"),
        sa.Column("interest_pct", sa.Float(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["dim_crown_client.client_id"]),
        sa.ForeignKeyConstraint(["disposition_id"], ["dim_crown_disposition.disposition_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("disposition_id", "client_id", "role_type", name="uq_bridge_crown_disposition_client"),
    )

    op.create_table(
        "bridge_crown_disposition_land",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("disposition_id", sa.Text(), nullable=False),
        sa.Column("tract_no", sa.Text(), nullable=True),
        sa.Column("lsd", sa.Text(), nullable=True),
        sa.Column("section", sa.Integer(), nullable=False),
        sa.Column("township", sa.Integer(), nullable=False),
        sa.Column("range", sa.Integer(), nullable=False),
        sa.Column("meridian", sa.Integer(), nullable=False),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["disposition_id"], ["dim_crown_disposition.disposition_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "disposition_id",
            "meridian",
            "range",
            "township",
            "section",
            "lsd",
            "tract_no",
            name="uq_bridge_crown_disposition_land",
        ),
    )
    op.create_index(
        "ix_bridge_crown_disposition_land_ats",
        "bridge_crown_disposition_land",
        ["meridian", "range", "township", "section", "lsd"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_bridge_crown_disposition_land_ats", table_name="bridge_crown_disposition_land")
    op.drop_table("bridge_crown_disposition_land")
    op.drop_table("bridge_crown_disposition_client")
    op.drop_index("ix_dim_crown_client_client_name_norm", table_name="dim_crown_client")
    op.drop_table("dim_crown_client")
    op.drop_index("ix_dim_crown_disposition_agreement_no", table_name="dim_crown_disposition")
    op.drop_table("dim_crown_disposition")
