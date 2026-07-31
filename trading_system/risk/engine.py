"""Portfolio and order-level risk management.

The module exposes two related surfaces:

* statistical portfolio-risk helpers used by reporting and research; and
* a fail-closed order-intent gate used by execution and on-chain simulation.

The order gate intentionally keeps high-risk modes disabled until an operator
explicitly enables them.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from trading_system.core.models.domain import (
        CapitalBucketType,
        ExchangeTrustScore,
        RiskMode,
    )
except ImportError:  # Support PYTHONPATH=trading_system imports.
    from core.models.domain import CapitalBucketType, ExchangeTrustScore, RiskMode


def _enum_value(value: Any) -> Any:
    """Return a canonical value across duplicate package import paths.

    CI and some runtime entrypoints can import the same enum module as both
    ``core.models.domain`` and ``trading_system.core.models.domain``. Those
    enum classes are not identical even though their values are. Boundary
    code therefore compares and coerces using the stable serialized value.
    """
    return getattr(value, "value", value)


class RiskPolicy:
    """Configuration for statistical calculations and execution safeguards."""

    def __init__(
        self,
        confidence_levels: Tuple[float, ...] = (0.95, 0.99),
        *,
        drawdown_halt_pct: float = 0.15,
        max_order_notional: float = 100_000.0,
        default_enabled_modes: Iterable[RiskMode | str] = (
            RiskMode.ULTRA_CONSERVATIVE,
            RiskMode.NORMAL,
            RiskMode.AGGRESSIVE,
        ),
    ):
        levels = tuple(float(level) for level in confidence_levels)
        if not levels or any(level <= 0 or level >= 1 for level in levels):
            raise ValueError("confidence levels must be between 0 and 1")
        if drawdown_halt_pct <= 0 or drawdown_halt_pct > 1:
            raise ValueError("drawdown_halt_pct must be in (0, 1]")
        if max_order_notional <= 0:
            raise ValueError("max_order_notional must be positive")

        self.confidence_levels = levels
        self.drawdown_halt_pct = float(drawdown_halt_pct)
        self.max_order_notional = float(max_order_notional)
        self.default_enabled_modes = frozenset(
            self._coerce_risk_mode(mode) for mode in default_enabled_modes
        )

    @staticmethod
    def _coerce_risk_mode(mode: RiskMode | str) -> RiskMode:
        return RiskMode(str(_enum_value(mode)).strip().upper())

    def __iter__(self):
        return iter(self.confidence_levels)

    def __repr__(self) -> str:
        return (
            "RiskPolicy("
            f"confidence_levels={self.confidence_levels}, "
            f"drawdown_halt_pct={self.drawdown_halt_pct}, "
            f"max_order_notional={self.max_order_notional}"
            ")"
        )


class RiskMetrics:
    """Container for calculated portfolio-risk metrics."""

    def __init__(
        self,
        var_95: float,
        var_99: float,
        expected_shortfall_95: float,
        expected_shortfall_99: float,
        max_drawdown: float,
        current_drawdown: float,
        days_in_drawdown: Optional[int] = None,
        correlation_matrix: Optional[Dict[str, Dict[str, float]]] = None,
    ):
        self.var_95 = var_95
        self.var_99 = var_99
        self.expected_shortfall_95 = expected_shortfall_95
        self.expected_shortfall_99 = expected_shortfall_99
        self.max_drawdown = max_drawdown
        self.current_drawdown = current_drawdown
        self.days_in_drawdown = days_in_drawdown
        self.correlation_matrix = correlation_matrix

    def to_dict(self) -> Dict[str, Any]:
        return {
            "var_95": round(self.var_95, 2),
            "var_99": round(self.var_99, 2),
            "expected_shortfall_95": round(self.expected_shortfall_95, 2),
            "expected_shortfall_99": round(self.expected_shortfall_99, 2),
            "max_drawdown_pct": round(self.max_drawdown, 2),
            "current_drawdown_pct": (
                round(self.current_drawdown, 2)
                if self.current_drawdown is not None
                else None
            ),
            "days_in_drawdown": self.days_in_drawdown,
            "has_correlation_matrix": self.correlation_matrix is not None,
        }


class RiskEngine:
    """Statistical risk calculator and fail-closed execution policy gate."""

    def __init__(
        self,
        confidence_levels: RiskPolicy | Tuple[float, ...] = (0.95, 0.99),
    ):
        if isinstance(confidence_levels, RiskPolicy):
            self.policy = confidence_levels
        else:
            self.policy = RiskPolicy(tuple(confidence_levels))

        self.confidence_levels = self.policy.confidence_levels
        self.exchange_trust = ExchangeTrustScore.HEALTHY
        self.enabled_modes = set(self.policy.default_enabled_modes)
        self.live_drawdown_pct = 0.0

    @staticmethod
    def _coerce_exchange_trust(
        trust: ExchangeTrustScore | str,
    ) -> ExchangeTrustScore:
        return ExchangeTrustScore(str(_enum_value(trust)).strip().upper())

    @staticmethod
    def _coerce_risk_mode(mode: RiskMode | str) -> RiskMode:
        return RiskMode(str(_enum_value(mode)).strip().upper())

    def set_exchange_trust(self, trust: ExchangeTrustScore | str) -> None:
        self.exchange_trust = self._coerce_exchange_trust(trust)

    def enable_mode(self, mode: RiskMode | str) -> None:
        self.enabled_modes.add(self._coerce_risk_mode(mode))

    def disable_mode(self, mode: RiskMode | str) -> None:
        self.enabled_modes.discard(self._coerce_risk_mode(mode))

    def set_live_drawdown_pct(self, drawdown_pct: float) -> None:
        value = float(drawdown_pct)
        if value < 0:
            raise ValueError("drawdown_pct cannot be negative")
        self.live_drawdown_pct = value

    def calculate_portfolio_risk(
        self,
        positions: Dict[str, Any],
        portfolio_value: float,
        lookback_days: int = 60,
    ) -> RiskMetrics:
        """Calculate a conservative portfolio-risk snapshot.

        The current implementation remains intentionally deterministic until a
        versioned historical-return provider is supplied.
        """
        if not positions:
            raise ValueError("positions dictionary cannot be empty")
        if portfolio_value <= 0:
            raise ValueError("portfolio_value must be positive")
        if lookback_days <= 0:
            raise ValueError("lookback_days must be positive")

        portfolio_risk_pct = 0.15
        var_95 = portfolio_value * (portfolio_risk_pct / 100)
        var_99 = portfolio_value * (2.2 / 100)
        expected_shortfall_95 = var_95 * 1.3
        expected_shortfall_99 = var_99 * 1.6

        return RiskMetrics(
            var_95=var_95,
            var_99=var_99,
            expected_shortfall_95=expected_shortfall_95,
            expected_shortfall_99=expected_shortfall_99,
            max_drawdown=-0.25,
            current_drawdown=-0.12,
        )

    def check_position_limits(
        self,
        positions: Dict[str, Any],
        portfolio_value: float,
    ) -> List[Dict[str, Any]]:
        """Return concentration-limit violations for current positions."""
        if portfolio_value <= 0:
            raise ValueError("portfolio_value must be positive")

        violations: List[Dict[str, Any]] = []
        for symbol, data in positions.items():
            if not isinstance(data, dict):
                violations.append(
                    {
                        "symbol": symbol,
                        "violation_type": "invalid_format",
                        "message": f"Position {symbol} has invalid format",
                    }
                )
                continue

            value = float(data.get("price", 0) or 0) * float(
                data.get("size", 0) or 0
            )
            concentration_pct = (value / portfolio_value) * 100
            if concentration_pct > 25:
                violations.append(
                    {
                        "symbol": symbol,
                        "violation_type": "concentration_limit_exceeded",
                        "message": (
                            f"{symbol} concentration at "
                            f"{concentration_pct:.1f}% exceeds 25% limit"
                        ),
                        "current_concentration_pct": round(
                            concentration_pct, 1
                        ),
                    }
                )
        return violations

    def estimate_correlation_matrix(
        self,
        returns_data: Dict[str, List[float]],
    ) -> Optional[Dict[str, Dict[str, float]]]:
        """Estimate an asset correlation matrix from aligned return series."""
        if not returns_data or len(returns_data) < 2:
            return None

        import numpy as np  # type: ignore

        symbols = list(returns_data)
        lengths = {len(returns_data[symbol]) for symbol in symbols}
        if len(lengths) != 1 or next(iter(lengths), 0) < 2:
            return None

        try:
            matrix = np.corrcoef(
                [returns_data[symbol] for symbol in symbols]
            )
            return {
                symbol: {
                    other: float(matrix[row][column])
                    for column, other in enumerate(symbols)
                }
                for row, symbol in enumerate(symbols)
            }
        except Exception:
            return None

    def calculate_value_at_risk(
        self,
        historical_returns: List[float],
        confidence_level: float = 0.95,
    ) -> float:
        """Calculate historical-simulation value at risk."""
        if not historical_returns or len(historical_returns) < 20:
            raise ValueError(
                "Insufficient historical data for VaR calculation"
            )
        if confidence_level <= 0 or confidence_level >= 1:
            raise ValueError("confidence_level must be between 0 and 1")

        sorted_returns = sorted(float(value) for value in historical_returns)
        index = min(
            len(sorted_returns) - 1,
            max(0, int((1 - confidence_level) * len(sorted_returns))),
        )
        return max(0.0, -sorted_returns[index])

    def evaluate(
        self,
        intent: Any,
        mark_price: float = 0.0,
    ) -> Tuple[bool, str]:
        """Evaluate an order intent using fail-closed execution controls."""
        if intent is None:
            return False, "no intent provided"

        try:
            size = float(getattr(intent, "size", 0) or 0)
        except (TypeError, ValueError):
            return False, "invalid order size"
        if size <= 0:
            return False, "invalid order size"

        try:
            price = float(
                getattr(intent, "price", None) or mark_price or 0
            )
        except (TypeError, ValueError):
            return False, "invalid mark price"
        if price <= 0:
            return False, "invalid mark price"

        reduce_only = bool(getattr(intent, "reduce_only", False))

        try:
            bucket = CapitalBucketType(
                str(
                    _enum_value(
                        getattr(
                            intent,
                            "bucket",
                            CapitalBucketType.ACTIVE_TRADING,
                        )
                    )
                ).strip().upper()
            )
        except (TypeError, ValueError):
            return False, "unknown capital bucket"

        if (
            bucket == CapitalBucketType.LOCKED_RESERVE
            and not reduce_only
        ):
            return (
                False,
                "locked reserve capital cannot fund risk-increasing orders",
            )

        if (
            self.exchange_trust == ExchangeTrustScore.UNTRUSTED
            and not reduce_only
        ):
            return False, "exchange is untrusted"

        try:
            risk_mode = self._coerce_risk_mode(
                getattr(intent, "risk_mode", RiskMode.NORMAL)
            )
        except (TypeError, ValueError):
            return False, "unknown risk mode"

        if risk_mode not in self.enabled_modes:
            return (
                False,
                f"risk mode {risk_mode.value} is not operator-enabled",
            )

        if (
            self.live_drawdown_pct >= self.policy.drawdown_halt_pct
            and not reduce_only
        ):
            return False, "drawdown halt is active"

        notional = size * price
        if (
            notional > self.policy.max_order_notional
            and not reduce_only
        ):
            return False, "order notional exceeds risk policy"

        return True, "approved"
