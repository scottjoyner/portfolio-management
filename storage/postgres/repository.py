"""Repository operations for the canonical operational PostgreSQL models."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from .models import (
    Alert,
    Approval,
    AuditEvent,
    CapitalBucket as CapitalBucketModel,
    ExchangeState,
    Fill,
    Incident,
    MarketDataFeed,
    Order,
    Portfolio,
    PortfolioSleeve,
    StrategyConfig,
    StrategyRun,
)


class OpsRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        return self.db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()

    def list_portfolios(self) -> list[Portfolio]:
        return self.db.query(Portfolio).all()

    def upsert_portfolio(self, portfolio: Portfolio) -> Portfolio:
        existing = self.get_portfolio(portfolio.id)
        target = existing or portfolio
        if existing:
            for key, value in portfolio.__dict__.items():
                if key != "_sa_instance_state" and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(timezone.utc)
        else:
            self.db.add(portfolio)
        self.db.commit()
        self.db.refresh(target)
        return target

    def list_orders(self) -> list[Order]:
        return self.db.query(Order).all()

    def get_order(self, order_id: str) -> Order | None:
        return self.db.query(Order).filter(Order.order_id == order_id).first()

    def create_order(self, order: Order) -> Order:
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        return order

    def update_order(self, order_id: str, **kwargs: object) -> Order | None:
        order = self.get_order(order_id)
        if order:
            for key, value in kwargs.items():
                if hasattr(order, key):
                    setattr(order, key, value)
            order.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            self.db.refresh(order)
        return order

    def list_fills(self) -> list[Fill]:
        return self.db.query(Fill).all()

    def create_fill(self, fill: Fill) -> Fill:
        self.db.add(fill)
        self.db.commit()
        self.db.refresh(fill)
        return fill

    def upsert_strategy_config(self, config: StrategyConfig) -> StrategyConfig:
        existing = self.db.query(StrategyConfig).filter(
            StrategyConfig.strategy_id == config.strategy_id
        ).first()
        target = existing or config
        if existing:
            for key, value in config.__dict__.items():
                if key != "_sa_instance_state" and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(timezone.utc)
        else:
            self.db.add(config)
        self.db.commit()
        self.db.refresh(target)
        return target

    def list_strategy_configs(self) -> list[StrategyConfig]:
        return self.db.query(StrategyConfig).all()

    def create_strategy_run(self, run: StrategyRun) -> StrategyRun:
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def get_strategy_run(self, task_id: str) -> StrategyRun | None:
        return self.db.query(StrategyRun).filter(StrategyRun.task_id == task_id).first()

    def update_strategy_run(self, task_id: str, **kwargs: object) -> StrategyRun | None:
        run = self.get_strategy_run(task_id)
        if run:
            for key, value in kwargs.items():
                if hasattr(run, key):
                    setattr(run, key, value)
            self.db.commit()
            self.db.refresh(run)
        return run

    def list_capital_buckets(self) -> list[CapitalBucketModel]:
        return self.db.query(CapitalBucketModel).all()

    def get_capital_bucket(self, bucket_id: str) -> CapitalBucketModel | None:
        return self.db.query(CapitalBucketModel).filter(CapitalBucketModel.id == bucket_id).first()

    def list_feed_health(self) -> list[MarketDataFeed]:
        return self.db.query(MarketDataFeed).all()

    def list_approvals(self) -> list[Approval]:
        return self.db.query(Approval).all()

    def list_alerts(self) -> list[Alert]:
        return self.db.query(Alert).all()

    def list_incidents(self) -> list[Incident]:
        return self.db.query(Incident).all()

    def list_audit_events(self) -> list[AuditEvent]:
        return self.db.query(AuditEvent).all()

    def create_audit_event(self, event: AuditEvent) -> AuditEvent:
        self.db.add(event)
        self.db.commit()
        self.db.refresh(event)
        return event

    def get_or_create_exchange_state(self, exchange: str = "coinbase") -> ExchangeState:
        state = self.db.query(ExchangeState).filter(ExchangeState.exchange == exchange).first()
        if not state:
            state = ExchangeState(exchange=exchange)
            self.db.add(state)
            self.db.commit()
            self.db.refresh(state)
        return state

    def seed_default_portfolios(self) -> None:
        if self.db.query(Portfolio).count() > 0:
            return
        p1 = Portfolio(
            id="cb-core-mm",
            name="Coinbase Core MM",
            objective="market_making",
            nav=2_500_000,
            available_capital=1_350_000,
            locked_capital=610_000,
            realized_pnl=42_300,
            unrealized_pnl=8_100,
            liquidity_score=0.82,
            capital_efficiency=0.75,
        )
        p2 = Portfolio(
            id="cb-hedge",
            name="Coinbase Hedge",
            objective="hedge",
            nav=1_100_000,
            available_capital=790_000,
            locked_capital=120_000,
            realized_pnl=13_200,
            unrealized_pnl=-2_200,
            liquidity_score=0.71,
            capital_efficiency=0.66,
        )
        self.db.add_all([p1, p2])
        self.db.flush()
        self.db.add_all(
            [
                PortfolioSleeve(portfolio_id="cb-core-mm", name="maker", weight=0.55),
                PortfolioSleeve(portfolio_id="cb-core-mm", name="taker", weight=0.10),
                PortfolioSleeve(portfolio_id="cb-core-mm", name="inventory", weight=0.35),
                PortfolioSleeve(portfolio_id="cb-hedge", name="delta_hedge", weight=0.65),
                PortfolioSleeve(portfolio_id="cb-hedge", name="tail_protection", weight=0.35),
            ]
        )
        self.db.commit()


__all__ = ["OpsRepository"]
