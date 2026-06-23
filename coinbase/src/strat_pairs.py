from __future__ import annotations
import math
import statistics
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict, Callable

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, InstrumentType


def _ols_hedge_ratio(y: List[float], x: List[float]) -> float:
    n = min(len(y), len(x))
    if n < 10:
        return 1.0
    y, x = y[-n:], x[-n:]
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    if den == 0:
        return 1.0
    return num / den


def _adf_test(residuals: List[float]) -> float:
    n = len(residuals)
    if n < 10:
        return 0.5
    dy = [residuals[i] - residuals[i - 1] for i in range(1, n)]
    y_lag = residuals[:-1]
    n_lag = len(dy)
    mean_y = sum(y_lag) / n_lag
    mean_dy = sum(dy) / n_lag
    num = sum((y_lag[i] - mean_y) * (dy[i] - mean_dy) for i in range(n_lag))
    den = sum((y_lag[i] - mean_y) ** 2 for i in range(n_lag))
    if den == 0:
        return 0.5
    rho = num / den
    se_num = sum((dy[i] - mean_dy - rho * (y_lag[i] - mean_y)) ** 2 for i in range(n_lag))
    se_den = sum((y_lag[i] - mean_y) ** 2 for i in range(n_lag))
    if se_den == 0:
        return 0.5
    se = math.sqrt(se_num / (n_lag - 1)) / math.sqrt(se_den)
    if se == 0:
        return 0.5
    adf_stat = rho / se
    return _adf_pvalue(adf_stat, n_lag)


def _adf_pvalue(stat: float, n: int) -> float:
    if stat < -3.43:
        return 0.01
    if stat < -2.86:
        return 0.05
    if stat < -2.57:
        return 0.10
    return 0.50


@dataclass
class PairState:
    primary: str
    secondary: str
    hedge_ratio: float = 1.0
    spread: List[float] = field(default_factory=list)
    zscore: float = 0.0
    in_position: bool = False
    position_direction: Optional[Direction] = None
    cointegrated: bool = False
    last_check_bars: int = 0
    adf_pvalue: float = 0.5

    @property
    def is_ready(self) -> bool:
        return len(self.spread) >= 30 and self.cointegrated


DEFAULT_PAIRS = [
    ("ETH-USD", "BTC-USD"),
    ("SOL-USD", "BTC-USD"),
    ("AVAX-USD", "ETH-USD"),
    ("LINK-USD", "ETH-USD"),
    ("SOL-USD", "ETH-USD"),
    ("MATIC-USD", "ETH-USD") if False else None,
]


class CointegratedPairsStrategy(BaseStrategy):
    def __init__(self, pairs: Optional[List[Tuple[str, str]]] = None,
                 lookback: int = 100, z_entry: float = 2.0,
                 z_exit: float = 0.5, adf_retrain: int = 200,
                 min_coint_pvalue: float = 0.10):
        self.pairs = pairs or [
            ("ETH-USD", "BTC-USD"),
            ("SOL-USD", "BTC-USD"),
            ("AVAX-USD", "ETH-USD"),
            ("LINK-USD", "ETH-USD"),
        ]
        self.lookback = lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.adf_retrain = adf_retrain
        self.min_coint_pvalue = min_coint_pvalue
        self._name = "pairs_trade"
        self._price_cache: Dict[str, List[float]] = {}
        self._pair_states: Dict[Tuple[str, str], PairState] = {}
        self._bars_seen: int = 0
        for p, s in self.pairs:
            self._pair_states[(p, s)] = PairState(primary=p, secondary=s)
        self._price_provider: Optional[Callable[[str], float]] = None

    def set_price_provider(self, fn: Callable[[str], float]):
        self._price_provider = fn

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        product_id = getattr(self, '_current_pid', None)
        if product_id is None:
            return None
        self._bars_seen += 1
        self._store_price(product_id, bar.close)

        for (prim, sec), state in self._pair_states.items():
            if prim != product_id and sec != product_id:
                continue
            prim_prices = self._price_cache.get(prim, [])
            sec_prices = self._price_cache.get(sec, [])
            if len(prim_prices) < 30 or len(sec_prices) < 30:
                continue

            n = min(len(prim_prices), len(sec_prices), self.lookback)
            y = prim_prices[-n:]
            x = sec_prices[-n:]

            if self._bars_seen - state.last_check_bars >= self.adf_retrain:
                state.hedge_ratio = _ols_hedge_ratio(y, x)
                residuals = [y[i] - state.hedge_ratio * x[i] for i in range(n)]
                pval = _adf_test(residuals)
                state.adf_pvalue = pval
                state.cointegrated = pval <= self.min_coint_pvalue
                state.last_check_bars = self._bars_seen

            spread = [y[i] - state.hedge_ratio * x[i] for i in range(n)]
            state.spread = spread

            if len(spread) < 10:
                continue

            spread_mean = statistics.mean(spread)
            spread_std = statistics.stdev(spread) if len(spread) > 1 else 1.0
            if spread_std < 1e-9:
                continue

            z = (spread[-1] - spread_mean) / spread_std
            state.zscore = z

            if prim != product_id:
                continue

            current_price = bar.close
            atr = self._estimate_atr(
                prim_prices,
                [],
                [],
            )

            if not state.in_position:
                if state.cointegrated and abs(z) >= self.z_entry:
                    if z > 0:
                        direction = Direction.SHORT
                        entry = current_price
                        stop = entry * 1.04
                        target = entry * 0.96
                        reason = f"PAIRS: {prim}/{sec} z={z:.2f} short divergence"
                    else:
                        direction = Direction.LONG
                        entry = current_price
                        stop = entry * 0.96
                        target = entry * 1.04
                        reason = f"PAIRS: {prim}/{sec} z={z:.2f} long divergence"

                    rr = abs(target - entry) / max(abs(entry - stop), 1e-9)
                    conf = min(0.7, (abs(z) / 3.0) * (1.0 - state.adf_pvalue))
                    return BracketSetup(
                        direction=direction, entry_price=entry,
                        stop_price=stop, target_price=target,
                        risk_reward=rr, confidence=round(conf, 3),
                        reason=reason, strategy_name=self._name, atr=atr,
                        instrument_type=InstrumentType.SPOT, leverage=1.0,
                        metadata={
                            "pair": f"{prim}/{sec}",
                            "zscore": round(z, 2),
                            "hedge_ratio": round(state.hedge_ratio, 4),
                            "adf_pvalue": round(state.adf_pvalue, 4),
                            "spread_mean": round(spread_mean, 4),
                            "spread_std": round(spread_std, 4),
                        },
                    )

            elif state.in_position and state.position_direction:
                if abs(z) <= self.z_exit:
                    state.in_position = False
                    state.position_direction = None

            if len(self._price_cache) > 50:
                for pid in list(self._price_cache.keys()):
                    if len(self._price_cache[pid]) == 0:
                        del self._price_cache[pid]

        return None

    def _store_price(self, product_id: str, price: float):
        if product_id not in self._price_cache:
            self._price_cache[product_id] = []
        self._price_cache[product_id].append(price)
        if len(self._price_cache[product_id]) > self.lookback * 2:
            self._price_cache[product_id] = self._price_cache[product_id][-self.lookback * 2:]

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            prev = closes[-i - 1] if len(closes) > i else closes[-1]
            tr = max(
                (highs[-i] - lows[-i]) if i <= len(highs) and i <= len(lows) else 0,
                abs((highs[-i] if i <= len(highs) else closes[-i]) - prev),
                abs((lows[-i] if i <= len(lows) else closes[-i]) - prev),
            )
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
