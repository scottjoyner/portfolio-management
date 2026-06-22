"""Crypto Price Divergence Detector.

Compares prediction-market implied probabilities (e.g. "Will BTC reach $100k?")
against a log-normal fair-probability model anchored to the Coinbase spot price.

Usage:
    detector = CryptoPriceDivergenceDetector(coinbase_prices={"BTC-USD": 85000.0})
    for market in prediction_markets:
        result = detector.analyze(market)
        if result:
            print(result.summary())
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from math import erf, sqrt, log as _log

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]
from typing import Any, Optional

from .unified_client import PredictionMarket

logger = logging.getLogger("crypto_divergence")

# ── Asset symbol resolution ──────────────────────────────────────

SYMBOL_ALIASES: dict[str, str] = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH", "ether": "ETH",
    "solana": "SOL", "sol": "SOL",
    "avalanche": "AVAX", "avax": "AVAX",
    "polygon": "MATIC", "matic": "MATIC",
    "chainlink": "LINK", "link": "LINK",
    "dogecoin": "DOGE", "doge": "DOGE",
    "cardano": "ADA", "ada": "ADA",
    "ripple": "XRP", "xrp": "XRP",
    "polkadot": "DOT", "dot": "DOT",
    "uniswap": "UNI", "uni": "UNI",
    "arbitrum": "ARB", "arb": "ARB",
    "optimism": "OP", "op": "OP",
}

DEFAULT_ANNUALIZED_VOL: dict[str, float] = {
    "BTC": 0.60, "ETH": 0.70, "SOL": 0.90,
    "AVAX": 0.90, "MATIC": 0.85, "LINK": 0.80,
    "DOGE": 1.00, "ADA": 0.80, "XRP": 0.75,
    "DOT": 0.85, "UNI": 0.90, "ARB": 1.00, "OP": 1.00,
}

COINBASE_PRODUCT_TEMPLATE = "{symbol}-USD"

# Date parsing regexes — ordered most-to-least specific
DATE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r'(?:by|before|on|at|until)\s+'
        r'(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
        r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+\d{1,2}(?:st|nd|rd|th)?'
        r'(?:,\s*)?(\d{4})?',
        re.IGNORECASE
    ), "month_day_year"),
    (re.compile(
        r'(?:by|before|on|at|until)\s+'
        r'(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
        r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
        r'\s+(\d{4})',
        re.IGNORECASE
    ), "month_year"),
    (re.compile(r'(?:by|before|on|at|until)\s+(\d{4})'), "year"),
    (re.compile(r'(?:end\s+of\s+)(?:the\s+)?(?:year|(\d{4}))', re.IGNORECASE), "end_of_year"),
    (re.compile(r'(?:end\s+of\s+)(?:q[1-4]|q[1-4]\s+(\d{4}))', re.IGNORECASE), "end_of_quarter"),
    (re.compile(r'(\d{4})\s*(?:-|–)\s*(\d{4})'), "year_range"),
]

MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5,
    "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

MONTH_OR_DAY = (
    r'(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
    r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
    r'\s+\d{1,2}(?:st|nd|rd|th)?'
)

MONTH = (
    r'(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
    r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
)

PRICE_REGEX = r'\$?(\d+(?:,\d{3})*(?:\.\d+)?)\s*((?:k|K|million|billion|m|dollars)\b)?'


def _norm_cdf(x: float) -> float:
    return (1.0 + erf(x / sqrt(2.0))) / 2.0


def _inv_norm_cdf(p: float) -> float:
    """Acklam's approximation for the inverse normal CDF (probit)."""
    if p <= 0 or p >= 1:
        return 8.0 if p >= 1 else -8.0
    a1 = -3.969683028665376e+01
    a2 = 2.209460984245205e+02
    a3 = -2.759285104469687e+02
    a4 = 1.383577518672690e+02
    a5 = -3.066479806614716e+01
    a6 = 2.506628277459239e+00
    b1 = -5.447609879822406e+01
    b2 = 1.615858368580409e+02
    b3 = -1.556989798598866e+02
    b4 = 6.680131188771972e+01
    b5 = -1.328068155288572e+01
    c1 = -7.784894002430293e-03
    c2 = -3.223964580411365e-01
    c3 = -2.400758277161838e+00
    c4 = -2.549732539343734e+00
    c5 = 4.374664141464968e+00
    c6 = 2.938163982698783e+00
    d1 = 7.784695709041462e-03
    d2 = 3.224671290700398e-01
    d3 = 2.445134137142996e+00
    d4 = 3.754408661907416e+00
    p_low = 0.02425
    p_high = 1 - p_low
    if p < p_low:
        q = sqrt(-2 * _log(p))
        return (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * q / (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1)
    else:
        q = sqrt(-2 * _log(1 - p))
        return -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1)


def _parse_price(s: str) -> Optional[float]:
    m = re.match(PRICE_REGEX, s.strip())
    if not m:
        return None
    raw = m.group(1).replace(",", "")
    try:
        val = float(raw)
    except ValueError:
        return None
    suffix = (m.group(2) or "").lower()
    if suffix in ("k",):
        val *= 1_000
    elif suffix in ("m", "million"):
        val *= 1_000_000
    elif suffix in ("b", "billion"):
        val *= 1_000_000_000
    return val


def _parse_date_str(date_str: str, end_of_year_year: Optional[int] = None) -> Optional[float]:
    ds = date_str.lower().strip()
    now = datetime.now(timezone.utc)
    cur_year = now.year

    if "end of" in ds or "end-of" in ds:
        year = cur_year
        if end_of_year_year:
            year = end_of_year_year
        elif now.month > 9:
            year = cur_year + 1
        target = datetime(year, 12, 31, tzinfo=timezone.utc)
        return target.timestamp()

    qm = re.search(r'q([1-4])\s*(\d{4})?', ds, re.IGNORECASE)
    if qm:
        q = int(qm.group(1))
        year = int(qm.group(2)) if qm.group(2) else (cur_year if q >= (now.month - 1) // 3 + 1 else cur_year + 1)
        month = {1: 3, 2: 6, 3: 9, 4: 12}[q]
        target = datetime(year, month, 1, tzinfo=timezone.utc)
        return target.timestamp()

    dmy = re.search(
        rf'({MONTH})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*(\d{{4}})?',
        ds,
        re.IGNORECASE,
    )
    if dmy:
        month_name = dmy.group(1)[:3].lower()
        month = MONTH_MAP.get(month_name, 1)
        day = int(dmy.group(2))
        year = int(dmy.group(3)) if dmy.group(3) else cur_year
        try:
            target = datetime(year, month, day, tzinfo=timezone.utc)
            return target.timestamp()
        except ValueError:
            return None

    my = re.search(rf'({MONTH})\s+(\d{{4}})', ds, re.IGNORECASE)
    if my:
        month_name = my.group(1)[:3].lower()
        month = MONTH_MAP.get(month_name, 1)
        year = int(my.group(2))
        target = datetime(year, month, 1, tzinfo=timezone.utc)
        return target.timestamp()

    yr = re.search(r'(\d{4})', ds)
    if yr:
        year = int(yr.group(1))
        target = datetime(year, 12, 31, tzinfo=timezone.utc)
        return target.timestamp()

    return None


# ── Data Models ──────────────────────────────────────────────────

@dataclass
class CryptoDivergence:
    """A detected divergence between prediction market and spot-implied probabilities."""
    market_id: str
    platform: str
    question: str
    category: str
    asset_symbol: str
    target_price: float
    target_date: str
    market_probability: float
    fair_probability: float
    divergence: float
    divergence_pct: float
    spot_price: float
    years_to_expiry: float
    annualized_vol: float
    implied_vol: float
    kelly_fraction: float
    confidence: float
    is_significant: bool
    signal: str
    volume: float
    spread: float
    liquidity_score: float

    def summary(self) -> str:
        direction = "above fair" if self.divergence > 0 else "below fair"
        s = (
            f"[{self.platform}] {self.question[:55]} | "
            f"PM={self.market_probability:.1%} Fair={self.fair_probability:.1%} "
            f"Spot=${self.spot_price:,.0f} Target=${self.target_price:,.0f} "
            f"{direction} ({abs(self.divergence):.1%})"
        )
        if self.kelly_fraction > 0:
            s += f" Kelly={self.kelly_fraction:.2%}"
        return s

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "platform": self.platform,
            "question": self.question,
            "category": self.category,
            "asset_symbol": self.asset_symbol,
            "target_price": self.target_price,
            "target_date": self.target_date,
            "market_probability": round(self.market_probability, 4),
            "fair_probability": round(self.fair_probability, 4),
            "divergence": round(self.divergence, 4),
            "divergence_pct": round(self.divergence_pct, 4),
            "spot_price": self.spot_price,
            "years_to_expiry": round(self.years_to_expiry, 2),
            "annualized_vol": round(self.annualized_vol, 2),
            "implied_vol": round(self.implied_vol, 2),
            "kelly_fraction": round(self.kelly_fraction, 4),
            "confidence": round(self.confidence, 3),
            "is_significant": self.is_significant,
            "signal": self.signal,
            "volume": self.volume,
            "spread": round(self.spread, 4),
            "liquidity_score": round(self.liquidity_score, 3),
        }


# ── Detector ─────────────────────────────────────────────────────

class CryptoPriceDivergenceDetector:
    """Analyze prediction markets for crypto price-target questions.

    For each market that mentions a crypto asset + price target,
    compare the market-implied probability against a log-normal model
    anchored to the current Coinbase spot price.
    """

    _ASSET_NAMES = "|".join(sorted(SYMBOL_ALIASES.keys(), key=len, reverse=True))

    PATTERNS = [
        # "Will BTC reach $100k by Dec 2026?"
        re.compile(
            rf'(will|does|do|is|are|can|could|should|would|when)\s+'
            rf'({_ASSET_NAMES})\s+'
            rf'(?:reach|hit|exceed|above|below|over|under|surpass|touch|see|'
            rf'go\s+(?:above|below|over|under)|trade\s+(?:above|below)|cross|'
            rf'break\s+(?:above|below|through)|jump\s+(?:above|to|past)|'
            rf'climb\s+(?:above|to|past)|rally\s+(?:to|above|past)|'
            rf'sink\s+(?:to|below|under)|drop\s+(?:to|below|under)|'
            rf'fall\s+(?:to|below|under))\s+'
            rf'({PRICE_REGEX})',
            re.IGNORECASE,
        ),
        # "ETH above $5000" or "Ethereum price above $5000"
        re.compile(
            rf'({_ASSET_NAMES})(?:\s+(?:price|prices))?\s+(?:above|over|below|under|at)\s+'
            rf'({PRICE_REGEX})',
            re.IGNORECASE,
        ),
        # "Solana to hit $1000" — asset + to + verb + price (no leading will/does)
        re.compile(
            rf'({_ASSET_NAMES})\s+to\s+'
            rf'(?:reach|hit|exceed|surpass|rise|climb|rally|jump|go|move|trade|drop|fall|sink)\s+'
            rf'({PRICE_REGEX})',
            re.IGNORECASE,
        ),
        # "$100k Bitcoin" — reverse order
        re.compile(
            rf'({PRICE_REGEX})\s+({_ASSET_NAMES})',
            re.IGNORECASE,
        ),
        # "bitcoin $100k" — asset followed by price
        re.compile(
            rf'({_ASSET_NAMES})\s+({PRICE_REGEX})',
            re.IGNORECASE,
        ),
    ]

    def __init__(
        self,
        coinbase_prices: dict[str, float] | None = None,
        annualized_vol_overrides: dict[str, float] | None = None,
        coinbase_client: Any = None,
    ):
        self.coinbase_prices: dict[str, float] = dict(coinbase_prices or {})
        self.annualized_vol: dict[str, float] = dict(annualized_vol_overrides or {})
        self.coinbase_client = coinbase_client

    def set_coinbase_prices(self, prices: dict[str, float]) -> None:
        self.coinbase_prices = dict(prices)

    def compute_historical_vol(self, product_id: str, lookback_days: int = 90) -> Optional[float]:
        """Fetch daily candles from Coinbase and compute annualized historical vol."""
        if pd is None:
            return None
        client = self.coinbase_client
        if client is None:
            return None
        try:
            from coinbase.src.data import fetch_candles_df
            df = fetch_candles_df(client, product_id, lookback_days=lookback_days)
            if df is None or df.empty or "close" not in df.columns:
                return None
            returns = df["close"].pct_change().dropna()
            if len(returns) < 5:
                return None
            return float(returns.std() * sqrt(365.0))
        except Exception:
            logger.debug("Historical vol failed for %s", product_id, exc_info=True)
            return None

    def refresh_historical_vols(self, products: list[str], lookback_days: int = 90) -> None:
        """Fetch historical vol for all monitored products and store as overrides."""
        for pid in products:
            vol = self.compute_historical_vol(pid, lookback_days)
            if vol and vol > 0.01:
                asset = pid.replace("-USD", "")
                logger.info("Historical vol %s: %.1f%%", pid, vol * 100)
                self.annualized_vol[asset] = vol

    @staticmethod
    def implied_volatility(
        market_prob: float, spot: float, target: float, years_to_expiry: float
    ) -> Optional[float]:
        """Invert the log-normal formula to get the vol the market price implies."""
        if spot <= 0 or target <= 0 or years_to_expiry <= 0 or not (0 < market_prob < 1):
            return None
        sqrt_t = sqrt(max(years_to_expiry, 1 / 365))
        prob = market_prob
        z = _inv_norm_cdf(1 - prob)
        moneyness = _log(target / spot)
        iv = abs(moneyness / (z * sqrt_t)) if abs(z) > 1e-9 else 0
        if iv <= 0 or iv > 10:
            return None
        return iv

    def extract(self, question: str) -> Optional[dict]:
        """Parse a question for crypto symbol, target price, and target date.

        Returns dict with keys: symbol, target_price, target_date_ts, raw_date_str
        or None if not a parseable price-target question.
        """
        q = question.strip()
        if not q:
            return None

        symbol = None
        target_price = None
        raw_price_str = ""

        for pattern in self.PATTERNS:
            m = pattern.search(q)
            if not m:
                continue
            g = m.groups()

            pid = self._PATTERN_ID(pattern)
            if pid == 0 and len(g) >= 4:
                sym_candidate = (g[1] or "").strip()
                if sym_candidate.lower() in SYMBOL_ALIASES:
                    symbol = SYMBOL_ALIASES[sym_candidate.lower()]
                price_str = " ".join(x for x in g[3:] if x)
                parsed = _parse_price(price_str)
                if parsed is not None and parsed > 0:
                    target_price = parsed
                    raw_price_str = price_str

            if pid in (1, 2, 4) and len(g) >= 2:
                sym_candidate = (g[0] or "").strip()
                if sym_candidate.lower() in SYMBOL_ALIASES:
                    symbol = SYMBOL_ALIASES[sym_candidate.lower()]
                price_str = " ".join(x for x in g[1:] if x)
                parsed = _parse_price(price_str)
                if parsed is not None and parsed > 0:
                    target_price = parsed
                    raw_price_str = price_str

            elif pid == 3 and len(g) >= 2:
                sym_candidate = (g[-1] or "").strip()
                if sym_candidate.lower() in SYMBOL_ALIASES:
                    symbol = SYMBOL_ALIASES[sym_candidate.lower()]
                price_str = " ".join(x for x in g[:-1] if x)
                parsed = _parse_price(price_str)
                if parsed is not None and parsed > 0:
                    target_price = parsed
                    raw_price_str = price_str

            if symbol and target_price:
                break

        if not symbol or not target_price:
            return None

        target_date_ts = self._extract_date(q)

        raw_date_str = ""
        for dp, _ in DATE_PATTERNS:
            dm = dp.search(q)
            if dm:
                raw_date_str = dm.group(0)
                break

        return {
            "symbol": symbol,
            "target_price": target_price,
            "target_date_ts": target_date_ts,
            "raw_date_str": raw_date_str or "",
            "raw_price_str": raw_price_str,
        }

    @staticmethod
    def _PATTERN_ID(pattern: re.Pattern) -> int:
        for i, p in enumerate(CryptoPriceDivergenceDetector.PATTERNS):
            if p is pattern:
                return i
        return -1

    def _extract_date(self, q: str) -> Optional[float]:
        now = datetime.now(timezone.utc)
        cur_year = now.year
        candidates: list[float] = []

        for pattern, kind in DATE_PATTERNS:
            for m in pattern.finditer(q):
                try:
                    if kind == "month_day_year":
                        full = m.group(0)
                        year_part = m.group(1)
                        ts = _parse_date_str(full, end_of_year_year=int(year_part) if year_part else None)
                        if ts:
                            candidates.append(ts)

                    elif kind in ("month_year", "year", "end_of_year", "end_of_quarter"):
                        full = m.group(0)
                        ts = _parse_date_str(full)
                        if ts:
                            candidates.append(ts)

                    elif kind == "year_range":
                        end_year = int(m.group(2))
                        target = datetime(end_year, 12, 31, tzinfo=timezone.utc)
                        candidates.append(target.timestamp())
                except (ValueError, IndexError):
                    continue

        if not candidates:
            return None

        now_ts = now.timestamp()
        future = [c for c in candidates if c > now_ts]
        if future:
            return min(future)
        return max(candidates)

    def _get_spot_price(self, symbol: str) -> Optional[float]:
        product = COINBASE_PRODUCT_TEMPLATE.format(symbol=symbol)
        price = self.coinbase_prices.get(product)
        if price is not None:
            return float(price)
        return None

    def fair_probability(
        self,
        spot: float,
        target: float,
        years_to_expiry: float,
        asset: str,
    ) -> float:
        """Compute fair probability of price >= target at expiry under log-normal.

        P(price >= K at T) = 1 - Phi(ln(K/S) / (sigma sqrt(T)))
        """
        if spot <= 0 or target <= 0 or years_to_expiry <= 0:
            return 0.5

        sigma = self.annualized_vol.get(asset) or DEFAULT_ANNUALIZED_VOL.get(asset) or 0.70
        if years_to_expiry < 1 / 365:
            years_to_expiry = 1 / 365

        moneyness = _log(target / spot)
        denominator = sigma * sqrt(years_to_expiry)

        if denominator <= 0:
            return 0.5

        z = moneyness / denominator
        return 1.0 - _norm_cdf(z)

    def analyze(self, market: PredictionMarket) -> Optional[CryptoDivergence]:
        """Analyze a single prediction market for crypto price divergence.

        Returns a CryptoDivergence if the market is a parseable crypto
        price-target question with available spot data, None otherwise.
        """
        extracted = self.extract(market.question)
        if not extracted:
            return None

        symbol = extracted["symbol"]
        target_price = extracted["target_price"]
        target_date_ts = extracted.get("target_date_ts")

        spot = self._get_spot_price(symbol)
        if spot is None or spot <= 0:
            return None

        market_prob = max(min(market.mid_price, 0.999), 0.001)
        if market_prob <= 0 or market_prob >= 1:
            return None

        now_ts = datetime.now(timezone.utc).timestamp()

        if not target_date_ts or target_date_ts <= now_ts:
            raw_end = getattr(market, "end_date", "") or ""
            if raw_end:
                try:
                    ed = datetime.fromisoformat(raw_end.replace("Z", "+00:00"))
                    target_date_ts = ed.timestamp()
                except (ValueError, TypeError):
                    pass

        if target_date_ts and target_date_ts > now_ts:
            years_to_expiry = (target_date_ts - now_ts) / (365.25 * 86400)
        else:
            years_to_expiry = 0.5

        fair_prob = self.fair_probability(spot, target_price, years_to_expiry, symbol)

        divergence = market_prob - fair_prob
        divergence_pct = divergence / max(fair_prob, 0.01)

        sigma = self.annualized_vol.get(symbol) or DEFAULT_ANNUALIZED_VOL.get(symbol) or 0.70
        liq = getattr(market, "liquidity_score", 0) or 0
        spread_val = getattr(market, "spread", 0) or 0
        vol_val = getattr(market, "volume", 0) or 0
        vol_quality = min(vol_val / 500_000, 1.0)
        spread_quality = max(0, 1 - spread_val * 5)
        confidence = (liq * 0.5 + vol_quality * 0.3 + spread_quality * 0.2)

        if target_date_ts:
            confidence = min(confidence * 1.2, 1.0)

        is_significant = abs(divergence) > 0.10 and confidence > 0.3

        if divergence > 0.10:
            signal = "PM_OVERPRICING_YES"
        elif divergence < -0.10:
            signal = "PM_UNDERPRICING_YES"
        else:
            signal = "FAIR"

        # Implied volatility
        implied_vol = self.implied_volatility(market_prob, spot, target_price, years_to_expiry) or 0.0

        # Kelly fraction for the divergence trade
        kelly = 0.0
        if abs(divergence) > 0.05 and 0 < fair_prob < 1:
            if divergence > 0:
                # Market overprices YES → we sell YES / buy NO
                # Probability of NO = 1 - fair_prob, market price of NO = 1 - market_prob
                # f* = (P_market - P_fair) / P_market  (from selling YES)
                p_market = market_prob
                p_fair = fair_prob
                kelly = max(0, (p_market - p_fair) / p_market)
            else:
                # Market underprices YES → we buy YES
                # f* = (p_fair - p_market) / (1 - p_market)
                p_market = market_prob
                p_fair = fair_prob
                kelly = max(0, (p_fair - p_market) / (1 - p_market))

        return CryptoDivergence(
            market_id=getattr(market, "market_id", ""),
            platform=getattr(market, "platform", "unknown"),
            question=market.question,
            category=getattr(market, "category", "general"),
            asset_symbol=symbol,
            target_price=target_price,
            target_date=extracted.get("raw_date_str", ""),
            market_probability=market_prob,
            fair_probability=fair_prob,
            divergence=divergence,
            divergence_pct=divergence_pct,
            spot_price=spot,
            years_to_expiry=years_to_expiry,
            annualized_vol=sigma,
            implied_vol=implied_vol,
            kelly_fraction=kelly,
            confidence=confidence,
            is_significant=is_significant,
            signal=signal,
            volume=vol_val,
            spread=spread_val,
            liquidity_score=liq,
        )

    def analyze_markets(
        self,
        markets: list[PredictionMarket],
    ) -> list[CryptoDivergence]:
        seen_ids: set[str] = set()
        results: list[CryptoDivergence] = []
        for market in markets:
            mid = getattr(market, "market_id", "") or getattr(market, "question", "")
            if mid in seen_ids:
                continue
            seen_ids.add(mid)
            try:
                result = self.analyze(market)
                if result:
                    results.append(result)
            except Exception as e:
                logger.debug("Divergence analysis failed for %s: %s", mid, e)
        results.sort(key=lambda r: abs(r.divergence) * r.confidence, reverse=True)
        return results


# ── CLI ──────────────────────────────────────────────────────────

def main():
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from dotenv import load_dotenv
    load_dotenv()

    client = UnifiedPredictionMarketClient(
        kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
        kalshi_private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
    )

    categories = client.search_all_categories(limit_per_platform=50, min_volume=0, max_spread=1.0)
    all_markets: list[PredictionMarket] = []
    for items in categories.values():
        all_markets.extend(items)

    logger.info("Fetched %d prediction markets", len(all_markets))

    from coinbase.src.cb_client import CBClient
    cb = CBClient()
    products = ["BTC-USD", "ETH-USD", "SOL-USD"]
    try:
        books = cb.best_bid_ask(products)
        prices = {}
        for pb in books.get("pricebooks", []):
            pid = pb.get("product_id", "")
            bids = pb.get("bids", [])
            asks = pb.get("asks", [])
            bid = float(bids[0]["price"]) if bids else 0
            ask = float(asks[0]["price"]) if asks else 0
            prices[pid] = (bid + ask) / 2 if bid and ask else 0
        detector = CryptoPriceDivergenceDetector(coinbase_prices=prices)
    except Exception as e:
        logger.warning("Coinbase spot prices unavailable: %s", e)
        detector = CryptoPriceDivergenceDetector()

    results = detector.analyze_markets(all_markets)
    sig = [r for r in results if r.is_significant]

    print(f"\nTotal crypto price-target markets: {len(results)}")
    print(f"Significant divergences: {len(sig)}\n")

    for r in (sig if sig else results[:15]):
        print(r.summary())
        print(f"  Target: ${r.target_price:,.0f}  Expiry: {r.target_date or 'N/A'}  "
              f"Vol: {r.annualized_vol:.0%}  Conf: {r.confidence:.2f}")
        print()


if __name__ == "__main__":
    main()
