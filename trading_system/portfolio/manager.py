from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.orm import Session

from storage.postgres.models import CapitalBucket, Portfolio, PortfolioSleeve

log = logging.getLogger(__name__)


class PortfolioError(Exception):
    pass


@dataclass
class PortfolioManager:
    db: Session

    def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        return self.db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()

    def list_portfolios(self) -> list[Portfolio]:
        return self.db.query(Portfolio).all()

    def update_nav(self, portfolio_id: str, nav: float, realized_pnl: float | None = None, unrealized_pnl: float | None = None) -> Portfolio | None:
        portfolio = self.get_portfolio(portfolio_id)
        if not portfolio:
            return None
        portfolio.nav = nav
        if realized_pnl is not None:
            portfolio.realized_pnl = realized_pnl
        if unrealized_pnl is not None:
            portfolio.unrealized_pnl = unrealized_pnl
        self.db.commit()
        return portfolio

    def adjust_capital(self, portfolio_id: str, bucket_id: str, delta: float) -> CapitalBucket | None:
        bucket = self.db.query(CapitalBucket).filter(CapitalBucket.id == bucket_id, CapitalBucket.portfolio_id == portfolio_id).first()
        if not bucket:
            return None
        new_amount = float(bucket.amount) + delta
        if new_amount < 0:
            raise PortfolioError("insufficient capital in bucket")
        bucket.amount = new_amount
        self.db.commit()
        return bucket

    def rebalance_sleeves(self, portfolio_id: str, new_weights: dict[str, float]) -> list[PortfolioSleeve]:
        portfolio = self.get_portfolio(portfolio_id)
        if not portfolio:
            raise PortfolioError(f"portfolio {portfolio_id} not found")

        total = sum(new_weights.values())
        if abs(total - 1.0) > 0.01:
            raise PortfolioError(f"sleeve weights must sum to 1.0, got {total}")

        sleeves = self.db.query(PortfolioSleeve).filter(PortfolioSleeve.portfolio_id == portfolio_id).all()
        sleeve_map = {s.name: s for s in sleeves}

        for name, weight in new_weights.items():
            if name in sleeve_map:
                sleeve_map[name].weight = weight
            else:
                self.db.add(PortfolioSleeve(portfolio_id=portfolio_id, name=name, weight=weight))

        self.db.commit()
        return self.db.query(PortfolioSleeve).filter(PortfolioSleeve.portfolio_id == portfolio_id).all()

    def get_sleeve_allocation(self, portfolio_id: str, sleeve_name: str) -> PortfolioSleeve | None:
        return self.db.query(PortfolioSleeve).filter(
            PortfolioSleeve.portfolio_id == portfolio_id,
            PortfolioSleeve.name == sleeve_name,
        ).first()
