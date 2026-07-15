"""Coverage tests for the lightweight BaseSignalStrategy stub modules.

Each stub module only defines a subclass whose ``__init__`` wires up
``metadata``/``config`` via ``super().__init__``.  Instantiating the class
covers the module's own lines/branches; we also exercise the inherited
``generate_signal`` / ``is_disabled`` / ``supports_mode`` paths so the
strategy is genuinely usable.
"""
import importlib
import pytest

MODULES = [
    "strategies.mean_reversion.zscore",
    "strategies.mean_reversion.grid_capture",
    "strategies.market_making.adaptive_spread_mm",
    "strategies.market_making.stair_step_mm",
    "strategies.special.basis_carry",
    "strategies.special.liquidity_snapback",
    "strategies.volatility.vol_breakout",
    "strategies.accumulation.dca",
    "strategies.trend.breakout",
]


@pytest.mark.parametrize("modname", MODULES)
def test_stub_instantiates_and_signals(modname):
    mod = importlib.import_module(modname)
    cls = [c for n, c in vars(mod).items()
           if isinstance(c, type) and not n.startswith("_")
           and getattr(c, "__module__", None) == mod.__name__][0]
    s = cls()
    # metadata() exercises inherited machinery
    md = s.metadata()
    assert isinstance(md, dict) and md

    req = s.required_inputs()
    # Build a market_state satisfying every required input with a strong score.
    ms = {k: (True if k == "warmup_complete" else 0.9) for k in req}
    ms["product_id"] = "BTC-USD"
    ms["score"] = 0.9
    sig = s.generate_signal(ms)
    assert sig is not None and sig.score == 0.9

    # Below-threshold score -> no signal
    low = dict(ms); low["score"] = 0.0
    assert s.generate_signal(low) is None

    # Missing required input -> no signal
    missing = dict(ms); missing.pop(next(iter(req)), None)
    assert s.generate_signal(missing) is None

    # supports_mode / is_disabled / explain_trade
    assert isinstance(s.supports_mode("paper"), bool)
    disabled, _ = s.is_disabled(ms)
    assert isinstance(disabled, bool)
    if sig:
        assert isinstance(s.explain_trade(sig), str)
