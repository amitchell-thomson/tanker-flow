"""add model_panel (Part B aligned daily grid)

Revision ID: c8d3f0a2b5e7
Revises: b7c2e9f1a3d4
Create Date: 2026-09-05
"""

from alembic import op
import sqlalchemy as sa

revision = "c8d3f0a2b5e7"
down_revision = "b7c2e9f1a3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "model_panel",
        sa.Column("bucket_date", sa.Date(), nullable=False),
        sa.Column("feature", sa.Text(), nullable=False),
        sa.Column("value", sa.Float(precision=53), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("bucket_date", "feature"),
    )


def downgrade() -> None:
    op.drop_table("model_panel")
