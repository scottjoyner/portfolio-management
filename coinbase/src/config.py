from __future__ import annotations
import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(override=False)

class Settings(BaseModel):
    dry_run: bool = os.getenv("DRY_RUN", "true").lower() == "true"
    products: list[str] = os.getenv("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")
    cash_ccy: str = os.getenv("CASH", "USD")
    bar_granularity: str = os.getenv("BAR_GRANULARITY", "ONE_HOUR")
    lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "240"))
    target_vol: float = float(os.getenv("TARGET_VOL", "0.10"))  # annualized
    risk_per_trade: float = float(os.getenv("RISK_PER_TRADE", "0.01"))
    max_drawdown: float = float(os.getenv("MAX_DD", "0.15"))
    min_notional: float = float(os.getenv("MIN_NOTIONAL", "50"))
    # Optional: portfolio name
    portfolio_name: str = os.getenv("PORTFOLIO_NAME", "quant-bot")

SETTINGS = Settings()


# --- Bracket & Risk/Reward ---
class BracketSettings(BaseModel):
    min_rr: float = float(os.getenv("MIN_RR", "2.0"))  # minimum Reward:Risk to accept a setup
    stop_atr_mult: float = float(os.getenv("STOP_ATR_MULT", "2.0"))
    target_atr_mult: float = float(os.getenv("TARGET_ATR_MULT", "3.0"))
    trail_atr_mult: float = float(os.getenv("TRAIL_ATR_MULT", "0.0"))  # 0 disables trailing
    break_even_after_r: float = float(os.getenv("BREAK_EVEN_AFTER_R", "1.0"))
    manager_poll_secs: int = int(os.getenv("MANAGER_POLL_SECS", "5"))
    max_open_brackets: int = int(os.getenv("MAX_OPEN_BRACKETS", "10"))

BRACKETS = BracketSettings()


# --- Kelly sizing & Bandit ---
class KellySettings(BaseModel):
    enable: bool = os.getenv("ENABLE_KELLY", "true").lower() == "true"
    cap: float = float(os.getenv("KELLY_CAP", "0.5"))   # maximum Kelly fraction
    floor: float = float(os.getenv("KELLY_FLOOR", "0.1"))  # minimum when enabled but unknown
    default_rr: float = float(os.getenv("DEFAULT_RR", "2.0"))

KELLY = KellySettings()

class BanditSettings(BaseModel):
    mode: str = os.getenv("BANDIT_MODE", "ucb1")  # none|ucb1|thompson
    ucb_c: float = float(os.getenv("UCB_C", "0.8"))

BANDIT = BanditSettings()


class KellyCaps(BaseModel):
    product_caps_json: str = os.getenv("KELLY_CAPS_PRODUCT_JSON", "{}")
    setup_caps_json: str = os.getenv("KELLY_CAPS_SETUP_JSON", "{}")

    @property
    def product_caps(self) -> dict:
        import json
        try:
            return json.loads(self.product_caps_json or "{}")
        except Exception:
            return {}

    @property
    def setup_caps(self) -> dict:
        import json
        try:
            return json.loads(self.setup_caps_json or "{}")
        except Exception:
            return {}

KELLY_CAPS = KellyCaps()

class TCostSettings(BaseModel):
    taker_fee_bps: float = float(os.getenv("TAKER_FEE_BPS", "8.0"))
    slippage_bps: float = float(os.getenv("SLIPPAGE_BPS", "0.0"))
    impact_coeff: float = float(os.getenv("IMPACT_COEFF", "1.5"))

TCOST = TCostSettings()
