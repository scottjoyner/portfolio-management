"""initial schema

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
        "portfolios",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("objective", sa.String(64), nullable=False),
        sa.Column("nav", sa.Numeric(20, 4), server_default="0"),
        sa.Column("available_capital", sa.Numeric(20, 4), server_default="0"),
        sa.Column("locked_capital", sa.Numeric(20, 4), server_default="0"),
        sa.Column("realized_pnl", sa.Numeric(20, 4), server_default="0"),
        sa.Column("unrealized_pnl", sa.Numeric(20, 4), server_default="0"),
        sa.Column("liquidity_score", sa.Numeric(6, 4), server_default="0"),
        sa.Column("capital_efficiency", sa.Numeric(6, 4), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "portfolio_sleeves",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("portfolio_id", sa.String(64), sa.ForeignKey("portfolios.id"), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("weight", sa.Numeric(6, 4), nullable=False),
    )
    op.create_table(
        "strategy_configs",
        sa.Column("strategy_id", sa.String(128), primary_key=True),
        sa.Column("strategy_type", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), server_default="implemented"),
        sa.Column("paper_mode", sa.Boolean(), server_default="true"),
        sa.Column("live_supported", sa.Boolean(), server_default="false"),
        sa.Column("replay_supported", sa.Boolean(), server_default="true"),
        sa.Column("backtest_supported", sa.Boolean(), server_default="true"),
        sa.Column("risk_mode_hint", sa.String(32), server_default="NORMAL"),
        sa.Column("capital_bucket", sa.String(32), server_default="ACTIVE_TRADING"),
        sa.Column("enabled", sa.Boolean(), server_default="true"),
        sa.Column("config_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "strategy_runs",
        sa.Column("task_id", sa.String(64), primary_key=True),
        sa.Column("strategy_id", sa.String(128), sa.ForeignKey("strategy_configs.strategy_id"), nullable=False),
        sa.Column("status", sa.String(32), server_default="queued"),
        sa.Column("mode", sa.String(32), server_default="paper"),
        sa.Column("queued_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.String(64), unique=True, index=True),
        sa.Column("preview_id", sa.String(64), nullable=True),
        sa.Column("strategy_id", sa.String(128), sa.ForeignKey("strategy_configs.strategy_id"), nullable=False),
        sa.Column("portfolio_id", sa.String(64), sa.ForeignKey("portfolios.id"), nullable=False),
        sa.Column("sleeve_id", sa.String(64), nullable=True),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("size", sa.Numeric(20, 8), nullable=False),
        sa.Column("remaining_size", sa.Numeric(20, 8), server_default="0"),
        sa.Column("price", sa.Numeric(20, 8), nullable=True),
        sa.Column("notional", sa.Numeric(20, 4), nullable=True),
        sa.Column("order_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("maker_taker_expectation", sa.String(16), nullable=True),
        sa.Column("queue_age_s", sa.Integer(), server_default="0"),
        sa.Column("risk_mode", sa.String(32), server_default="NORMAL"),
        sa.Column("reduce_only", sa.Boolean(), server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "strategy_allocations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("portfolio_id", sa.String(64), sa.ForeignKey("portfolios.id"), nullable=False),
        sa.Column("strategy_id", sa.String(128), nullable=False),
        sa.Column("weight", sa.Numeric(6, 4), nullable=False),
    )
    op.create_table(
        "fills",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("fill_id", sa.String(64), unique=True, index=True),
        sa.Column("order_id", sa.String(64), sa.ForeignKey("orders.order_id"), nullable=False),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("side", sa.String(16), nullable=True),
        sa.Column("size", sa.Numeric(20, 8), nullable=False),
        sa.Column("price", sa.Numeric(20, 8), nullable=False),
        sa.Column("notional", sa.Numeric(20, 4), nullable=False),
        sa.Column("slippage_bps", sa.Numeric(10, 4), server_default="0"),
        sa.Column("fee", sa.Numeric(20, 4), server_default="0"),
        sa.Column("fee_currency", sa.String(16), nullable=True),
        sa.Column("liquidity", sa.String(16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "capital_buckets",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("portfolio_id", sa.String(64), sa.ForeignKey("portfolios.id"), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("bucket_type", sa.String(32), nullable=False),
        sa.Column("amount", sa.Numeric(20, 4), server_default="0"),
        sa.Column("target_weight", sa.Numeric(6, 4), server_default="0"),
        sa.Column("min_weight", sa.Numeric(6, 4), server_default="0"),
        sa.Column("max_weight", sa.Numeric(6, 4), server_default="1"),
        sa.Column("locked", sa.Boolean(), server_default="false"),
        sa.Column("status", sa.String(32), server_default="idle"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "approvals",
        sa.Column("approval_id", sa.String(64), primary_key=True),
        sa.Column("approval_type", sa.String(32), nullable=False),
        sa.Column("summary", sa.String(256), nullable=False),
        sa.Column("capital_affected", sa.Numeric(20, 4), server_default="0"),
        sa.Column("liquidity_impact", sa.String(32), nullable=True),
        sa.Column("risk_impact", sa.String(32), nullable=True),
        sa.Column("status", sa.String(32), server_default="pending"),
        sa.Column("approved_by", sa.String(128), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), index=True, nullable=False),
        sa.Column("actor", sa.String(128), nullable=True),
        sa.Column("resource_type", sa.String(64), nullable=True),
        sa.Column("resource_id", sa.String(64), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "alerts",
        sa.Column("alert_id", sa.String(64), primary_key=True),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("summary", sa.String(256), nullable=False),
        sa.Column("acknowledged", sa.Boolean(), server_default="false"),
        sa.Column("acknowledged_by", sa.String(128), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "incidents",
        sa.Column("incident_id", sa.String(64), primary_key=True),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("summary", sa.String(256), nullable=False),
        sa.Column("status", sa.String(32), server_default="monitoring"),
        sa.Column("assigned_to", sa.String(128), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "exchange_states",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("exchange", sa.String(32), server_default="coinbase"),
        sa.Column("trust_score", sa.String(16), server_default="HEALTHY"),
        sa.Column("open_orders_count", sa.Integer(), server_default="0"),
        sa.Column("unknown_fills", sa.Integer(), server_default="0"),
        sa.Column("duplicate_events", sa.Integer(), server_default="0"),
        sa.Column("stale_sequence_gaps", sa.Integer(), server_default="0"),
        sa.Column("snapshot_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "market_data_feeds",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("feed_name", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), server_default="healthy"),
        sa.Column("freshness_ms", sa.Integer(), server_default="0"),
        sa.Column("update_rate_hz", sa.Numeric(10, 4), server_default="0"),
        sa.Column("dropped_messages_1m", sa.Integer(), server_default="0"),
        sa.Column("failover_active", sa.Boolean(), server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("market_data_feeds")
    op.drop_table("exchange_states")
    op.drop_table("incidents")
    op.drop_table("alerts")
    op.drop_table("audit_events")
    op.drop_table("approvals")
    op.drop_table("capital_buckets")
    op.drop_table("fills")
    op.drop_table("strategy_allocations")
    op.drop_table("orders")
    op.drop_table("strategy_runs")
    op.drop_table("strategy_configs")
    op.drop_table("portfolio_sleeves")
    op.drop_table("portfolios")
