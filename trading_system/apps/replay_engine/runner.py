import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import argparse
import json

from execution.maker_engine.engine import MakerConfig, MakerQuoteEngine, MakerState
from market_data.microstructure.features import TopOfBook, TradePrint, ToxicFlowEstimator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="apps/replay_engine/fixtures/maker_toxic_flow.jsonl")
    args = parser.parse_args()

    est = ToxicFlowEstimator(bucket_volume=3.0)
    engine = MakerQuoteEngine(MakerConfig())
    state = MakerState()

    fades = 0
    toxic_hits = 0
    cancel_pressure_samples: list[float] = []
    inv_drift_samples: list[float] = []

    for line in Path(args.fixture).read_text(encoding="utf-8").splitlines():
        evt = json.loads(line)
        book = TopOfBook(evt["bid_px"], evt["bid_sz"], evt["ask_px"], evt["ask_sz"])
        toxic = est.update(TradePrint(evt["trade_side"], evt["trade_size"], evt["trade_px"]))
        if toxic >= engine.cfg.toxic_flow_threshold:
            toxic_hits += 1
        state.quote_age_ms = evt.get("quote_age_ms", 0)
        state.cancel_replace_count = evt.get("cancel_replace_count", 0)
        state.inventory = evt.get("inventory", 0)
        quotes, queue = engine.build_ladder(
            book=book,
            state=state,
            volatility_bps=evt.get("vol_bps", 5),
            toxic_flow=toxic,
            queue_ahead=evt.get("queue_ahead", 2),
            trade_rate=evt.get("trade_rate", 1.0),
            cancel_rate=evt.get("cancel_rate", 1.0),
        )
        if engine.should_fade_quotes(state, toxic, evt.get("microprice_drift_bps", 0.0)):
            fades += 1
        cancel_pressure_samples.append(engine.cancel_replace_pressure(state))
        inv_drift_samples.append(engine.inventory_drift(state, book))
        if quotes and queue.stale_quote_decay > 0.8:
            state.cancel_replace_count += 1

    out = {
        "events": len(cancel_pressure_samples),
        "toxic_hits": toxic_hits,
        "fade_events": fades,
        "avg_cancel_replace_pressure": sum(cancel_pressure_samples) / max(len(cancel_pressure_samples), 1),
        "avg_inventory_drift_notional": sum(inv_drift_samples) / max(len(inv_drift_samples), 1),
    }
    print(json.dumps(out))


if __name__ == "__main__":
    main()
