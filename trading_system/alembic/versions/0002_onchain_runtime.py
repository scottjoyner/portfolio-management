"""onchain runtime schema - P1.4 token/pool/event models

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-27
"""
from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Token metadata cache (ERC20 symbol/name/decimals with 24h TTL)
    op.create_table(
        "token_metadata",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("address", sa.String(66), unique=True, nullable=False),
        sa.Column("chain", sa.String(32), server_default="base"),
        sa.Column("symbol", sa.String(48), nullable=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("decimals", sa.Integer(), nullable=True),
        sa.Column("token_uri", sa.Text(), nullable=True),
        sa.Column("last_fetch_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("fetch_count", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Pool snapshots for monitoring and replay
    op.create_table(
        "pool_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("network", sa.String(32), nullable=False),
        sa.Column("pool_address", sa.String(66), nullable=True),
        sa.Column("pool_type", sa.String(48), nullable=True),  # AMM, concentrated, perp
        sa.Column("protocol", sa.String(64), nullable=True),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("pool_data_json", sa.Text(), nullable=True),
        sa.Column("health_score", sa.Numeric(6, 4), server_default="1.0"),
        sa.Column("is_valid", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Contract event logs with feed health tracking
    op.create_table(
        "contract_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("network", sa.String(32), nullable=False),
        sa.Column("contract_address", sa.String(66), nullable=False),
        sa.Column("event_type", sa.String(48), nullable=False),  # swap, transfer, approve
        sa.Column("block_number", sa.Integer(), nullable=False),
        sa.Column("transaction_hash", sa.String(66), nullable=True),
        sa.Column("indexed_args_json", sa.Text(), nullable=True),
        sa.Column("raw_log", sa.Text(), nullable=True),
        sa.Column("event_index", sa.Integer(), server_default="0"),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Feed health monitoring with metrics counters
    op.create_table(
        "feed_health_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("network", sa.String(32), nullable=False),
        sa.Column("feed_type", sa.String(32), nullable=False),  # pool_snapshot, event_listener, token_metadata
        sa.Column("status", sa.String(16), server_default="healthy"),  # healthy, degraded, error
        sa.Column("last_success_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), server_default="0"),
        sa.Column("metrics_polls_total", sa.Integer(), server_default="0"),
        sa.Column("metrics_last_error_time_seconds", sa.Numeric(15, 6), server_default="0.0"),
        sa.Column("metrics_latency_ms", sa.Numeric(12, 3), nullable=True),
        sa.Column("last_poll_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("feed_health_records")
    op.drop_table("contract_events")
    op.drop_table("pool_snapshots")
    op.drop_table("token_metadata")
