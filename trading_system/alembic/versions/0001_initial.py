"""initial

Revision ID: 0001
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("product_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("notional", sa.Float(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("orders")
