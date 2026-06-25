from __future__ import annotations
import json
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any


@dataclass
class BucketPosition:
    product_id: str
    side: str
    size: float
    entry_price: float
    current_price: float
    strategy: str = ""
    opened_at: float = 0.0
    bucket_id: str = ""

    @property
    def market_value(self) -> float:
        return self.size * self.current_price

    @property
    def cost_basis(self) -> float:
        return self.size * self.entry_price

    @property
    def unrealized_pnl(self) -> float:
        return self.market_value - self.cost_basis


@dataclass
class CapitalBucket:
    bucket_id: str
    name: str
    starting_balance_usd: float
    cash_usd: float
    target_volume_usd: float = 10000.0
    target_multiple: float = 2.0
    max_position_pct: float = 0.25
    allowed_strategies: List[str] = field(default_factory=list)
    active: bool = True
    realized_pnl_usd: float = 0.0
    volume_30d_usd: float = 0.0
    positions: Dict[str, BucketPosition] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def total_value(self) -> float:
        return self.cash_usd + sum(p.market_value for p in self.positions.values())

    def progress_to_volume_target(self) -> float:
        if self.target_volume_usd <= 0:
            return 1.0
        return min(1.0, self.volume_30d_usd / self.target_volume_usd)

    def progress_to_equity_target(self) -> float:
        target = self.starting_balance_usd * self.target_multiple
        if target <= 0:
            return 1.0
        return min(1.0, self.total_value() / target)

    def available_cash(self) -> float:
        return max(0.0, self.cash_usd)

    def max_notional(self) -> float:
        return self.total_value() * self.max_position_pct

    def can_trade_strategy(self, strategy: str) -> bool:
        return not self.allowed_strategies or strategy in self.allowed_strategies

    def mark_prices(self, price_map: Dict[str, float]) -> None:
        for pid, pos in self.positions.items():
            if pid in price_map:
                pos.current_price = float(price_map[pid])
        self.updated_at = time.time()

    def open_position(self, product_id: str, side: str, size: float, entry_price: float,
                      strategy: str = "") -> bool:
        notional = size * entry_price
        if notional <= 0 or notional > self.available_cash():
            return False
        if notional > self.max_notional() and self.positions:
            return False
        self.cash_usd -= notional
        self.volume_30d_usd += notional
        self.positions[product_id] = BucketPosition(
            product_id=product_id,
            side=side,
            size=size,
            entry_price=entry_price,
            current_price=entry_price,
            strategy=strategy,
            opened_at=time.time(),
            bucket_id=self.bucket_id,
        )
        self.updated_at = time.time()
        return True

    def close_position(self, product_id: str, exit_price: float) -> float:
        pos = self.positions.pop(product_id, None)
        if pos is None:
            return 0.0
        notional = pos.size * exit_price
        self.cash_usd += notional
        self.volume_30d_usd += notional
        pnl = notional - pos.cost_basis
        self.realized_pnl_usd += pnl
        self.updated_at = time.time()
        return pnl

    def to_dict(self) -> dict:
        return {
            "bucket_id": self.bucket_id,
            "name": self.name,
            "starting_balance_usd": self.starting_balance_usd,
            "cash_usd": self.cash_usd,
            "target_volume_usd": self.target_volume_usd,
            "target_multiple": self.target_multiple,
            "max_position_pct": self.max_position_pct,
            "allowed_strategies": list(self.allowed_strategies),
            "active": self.active,
            "realized_pnl_usd": self.realized_pnl_usd,
            "volume_30d_usd": self.volume_30d_usd,
            "positions": {pid: asdict(pos) for pid, pos in self.positions.items()},
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> CapitalBucket:
        bucket = cls(
            bucket_id=str(data.get("bucket_id", "default")),
            name=str(data.get("name", "default")),
            starting_balance_usd=float(data.get("starting_balance_usd", 0.0)),
            cash_usd=float(data.get("cash_usd", data.get("starting_balance_usd", 0.0))),
            target_volume_usd=float(data.get("target_volume_usd", 10000.0)),
            target_multiple=float(data.get("target_multiple", 2.0)),
            max_position_pct=float(data.get("max_position_pct", 0.25)),
            allowed_strategies=list(data.get("allowed_strategies", [])),
            active=bool(data.get("active", True)),
            realized_pnl_usd=float(data.get("realized_pnl_usd", 0.0)),
            volume_30d_usd=float(data.get("volume_30d_usd", 0.0)),
            updated_at=float(data.get("updated_at", time.time())),
        )
        for pid, pos in (data.get("positions", {}) or {}).items():
            bucket.positions[pid] = BucketPosition(
                product_id=str(pos.get("product_id", pid)),
                side=str(pos.get("side", "long")),
                size=float(pos.get("size", 0.0)),
                entry_price=float(pos.get("entry_price", 0.0)),
                current_price=float(pos.get("current_price", pos.get("entry_price", 0.0))),
                strategy=str(pos.get("strategy", "")),
                opened_at=float(pos.get("opened_at", 0.0)),
                bucket_id=str(pos.get("bucket_id", bucket.bucket_id)),
            )
        return bucket


class CapitalBucketLedger:
    def __init__(self, buckets: Optional[List[CapitalBucket]] = None,
                 state_path: str = "data/capital_buckets.json"):
        self.state_path = state_path
        self.buckets: Dict[str, CapitalBucket] = {b.bucket_id: b for b in (buckets or [])}

    @classmethod
    def from_env(cls) -> CapitalBucketLedger:
        state_path = os.environ.get("TRADER_BUCKET_STATE_PATH", "data/capital_buckets.json")
        raw = os.environ.get("TRADER_BUCKETS_JSON", "").strip()
        buckets: List[CapitalBucket] = []
        if raw:
            try:
                data = json.loads(raw)
                for item in data if isinstance(data, list) else []:
                    buckets.append(CapitalBucket.from_dict(item))
            except Exception:
                buckets = []
        if not buckets:
            start = float(os.environ.get("TRADER_CHALLENGE_CAPITAL_USDC", "100"))
            buckets = [CapitalBucket(
                bucket_id="challenge",
                name="100 USDC Challenge",
                starting_balance_usd=start,
                cash_usd=start,
                target_volume_usd=float(os.environ.get("TRADER_CHALLENGE_VOLUME_TARGET_USD", "10000")),
                target_multiple=float(os.environ.get("TRADER_CHALLENGE_TARGET_MULTIPLE", "3.0")),
                max_position_pct=float(os.environ.get("TRADER_CHALLENGE_MAX_POSITION_PCT", "0.25")),
                allowed_strategies=[],
            )]
        ledger = cls(buckets=buckets, state_path=state_path)
        ledger.load()
        return ledger

    def load(self) -> None:
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path) as f:
                payload = json.load(f)
            buckets = payload.get("buckets", payload if isinstance(payload, list) else [])
            if isinstance(buckets, list):
                self.buckets = {item["bucket_id"]: CapitalBucket.from_dict(item) for item in buckets if isinstance(item, dict) and item.get("bucket_id")}
        except Exception:
            pass

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump({"buckets": [b.to_dict() for b in self.buckets.values()]}, f, indent=2, default=str)

    def get(self, bucket_id: str) -> Optional[CapitalBucket]:
        return self.buckets.get(bucket_id)

    def choose_bucket(self, strategy: str, product_id: str, notional: float) -> Optional[CapitalBucket]:
        active = [b for b in self.buckets.values() if b.active and b.can_trade_strategy(strategy)]
        if not active:
            return None
        active.sort(key=lambda b: (b.available_cash(), b.progress_to_equity_target()), reverse=True)
        for bucket in active:
            if bucket.available_cash() >= notional and (notional <= bucket.max_notional() or not bucket.positions):
                return bucket
        return None

    def open_position(self, bucket_id: str, product_id: str, side: str, size: float,
                      entry_price: float, strategy: str = "") -> bool:
        bucket = self.get(bucket_id)
        if bucket is None:
            return False
        ok = bucket.open_position(product_id, side, size, entry_price, strategy)
        if ok:
            self.save()
        return ok

    def close_position(self, bucket_id: str, product_id: str, exit_price: float) -> float:
        bucket = self.get(bucket_id)
        if bucket is None:
            return 0.0
        pnl = bucket.close_position(product_id, exit_price)
        self.save()
        return pnl

    def mark_prices(self, price_map: Dict[str, float]) -> None:
        for bucket in self.buckets.values():
            bucket.mark_prices(price_map)

    def allocate(self, strategy: str, product_id: str, notional: float) -> Optional[str]:
        bucket = self.choose_bucket(strategy, product_id, notional)
        return bucket.bucket_id if bucket else None

    def apply_opportunity_limits(self, strategy: str, product_id: str, entry_price: float,
                                 base_size: float, quote_size: float = 0.0) -> tuple[float, Optional[str]]:
        notional = quote_size or (base_size * entry_price)
        bucket = self.choose_bucket(strategy, product_id, notional)
        if bucket is None:
            return 0.0, None
        max_notional = min(bucket.available_cash(), bucket.max_notional())
        if max_notional <= 0:
            return 0.0, bucket.bucket_id
        capped_notional = min(notional, max_notional)
        capped_size = capped_notional / max(entry_price, 1e-9)
        return capped_size, bucket.bucket_id

    def summary(self, price_map: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        if price_map:
            self.mark_prices(price_map)
        buckets = []
        for bucket in self.buckets.values():
            buckets.append({
                "bucket_id": bucket.bucket_id,
                "name": bucket.name,
                "cash_usd": round(bucket.cash_usd, 2),
                "total_value_usd": round(bucket.total_value(), 2),
                "realized_pnl_usd": round(bucket.realized_pnl_usd, 2),
                "volume_30d_usd": round(bucket.volume_30d_usd, 2),
                "target_volume_usd": round(bucket.target_volume_usd, 2),
                "target_multiple": bucket.target_multiple,
                "volume_progress": round(bucket.progress_to_volume_target(), 3),
                "equity_progress": round(bucket.progress_to_equity_target(), 3),
                "positions": len(bucket.positions),
            })
        return {"buckets": buckets, "total_value_usd": round(sum(b["total_value_usd"] for b in buckets), 2)}


def _bucket_template(bucket_id: str, name: str, starting_balance_usd: float, *,
                     target_volume_usd: float = 10000.0,
                     target_multiple: float = 3.0,
                     max_position_pct: float = 0.25,
                     allowed_strategies: Optional[List[str]] = None,
                     active: bool = True) -> dict:
    return {
        "bucket_id": bucket_id,
        "name": name,
        "starting_balance_usd": float(starting_balance_usd),
        "cash_usd": float(starting_balance_usd),
        "target_volume_usd": float(target_volume_usd),
        "target_multiple": float(target_multiple),
        "max_position_pct": float(max_position_pct),
        "allowed_strategies": list(allowed_strategies or []),
        "active": bool(active),
        "realized_pnl_usd": 0.0,
        "volume_30d_usd": 0.0,
        "positions": {},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def preset_challenge(starting_balance_usd: float = 100.0) -> dict:
    return {"buckets": [
        _bucket_template(
            "challenge",
            f"{int(starting_balance_usd) if float(starting_balance_usd).is_integer() else starting_balance_usd} USDC Challenge",
            starting_balance_usd,
            target_volume_usd=10000.0,
            target_multiple=3.0,
            max_position_pct=0.25,
        )
    ]}


def preset_challenge_amount(amount_usd: float) -> dict:
    amount = float(amount_usd)
    bucket_id = f"challenge_{int(amount)}" if amount.is_integer() else f"challenge_{str(amount).replace('.', '_')}"
    label = f"${int(amount)} Challenge" if amount.is_integer() else f"${amount:.2f} Challenge"
    return {"buckets": [
        _bucket_template(
            bucket_id,
            label,
            amount,
            target_volume_usd=10000.0,
            target_multiple=3.0,
            max_position_pct=0.25,
        )
    ]}


def preset_core(starting_balance_usd: float = 1000.0) -> dict:
    core = starting_balance_usd * 0.8
    reserve = starting_balance_usd * 0.15
    opportun = starting_balance_usd * 0.05
    return {"buckets": [
        _bucket_template("core", "Core", core, target_volume_usd=50000.0, target_multiple=1.5, max_position_pct=0.20, allowed_strategies=["ema_cross", "macd", "dca_accumulation", "adaptive_mode"]),
        _bucket_template("reserve", "Reserve", reserve, target_volume_usd=5000.0, target_multiple=1.1, max_position_pct=0.05, allowed_strategies=[], active=False),
        _bucket_template("opportunity", "Opportunity", opportun, target_volume_usd=20000.0, target_multiple=2.0, max_position_pct=0.25, allowed_strategies=["momentum_rotation", "volatility", "breakout"]),
    ]}


def preset_fee_tier(starting_balance_usd: float = 1000.0) -> dict:
    return {"buckets": [
        _bucket_template("fee_tier", "Fee Tier Generator", starting_balance_usd, target_volume_usd=10000.0, target_multiple=1.2, max_position_pct=0.40, allowed_strategies=["volume_generator", "market_making", "dca_accumulation"]),
    ]}


def preset_challenge_core_fee_tier(challenge_usd: float = 100.0,
                                    core_usd: float = 800.0,
                                    fee_tier_usd: float = 100.0) -> dict:
    return {"buckets": [
        _bucket_template("challenge", "100 USDC Challenge", challenge_usd, target_volume_usd=10000.0, target_multiple=3.0, max_position_pct=0.25),
        _bucket_template("core", "Core", core_usd, target_volume_usd=25000.0, target_multiple=1.5, max_position_pct=0.20, allowed_strategies=["ema_cross", "macd", "adaptive_mode"]),
        _bucket_template("fee_tier", "Fee Tier Generator", fee_tier_usd, target_volume_usd=10000.0, target_multiple=1.1, max_position_pct=0.40, allowed_strategies=["volume_generator", "market_making"]),
    ]}


BUCKET_PRESETS = {
    "challenge_1": lambda: preset_challenge_amount(1.0),
    "challenge_5": lambda: preset_challenge_amount(5.0),
    "challenge_10": lambda: preset_challenge_amount(10.0),
    "challenge_50": lambda: preset_challenge_amount(50.0),
    "challenge_100": lambda: preset_challenge_amount(100.0),
    "challenge": preset_challenge,
    "core": preset_core,
    "fee_tier": preset_fee_tier,
    "challenge_core_fee_tier": preset_challenge_core_fee_tier,
}


def bucket_preset_names() -> List[str]:
    return list(BUCKET_PRESETS.keys())


def build_bucket_preset(name: str, **kwargs) -> dict:
    fn = BUCKET_PRESETS.get(name)
    if fn is None:
        raise KeyError(name)
    if name in {"challenge_1", "challenge_5", "challenge_10", "challenge_50", "challenge_100"}:
        amount = kwargs.get("starting_balance_usd")
        if amount is None:
            try:
                amount = float(name.split("challenge_", 1)[-1].replace("_", "."))
            except Exception:
                amount = 100.0
        return preset_challenge_amount(float(amount))
    return fn(**kwargs)
