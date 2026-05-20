import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from time import perf_counter

from execution.maker_engine.engine import MakerConfig, MakerQuoteEngine, MakerState
from market_data.microstructure.features import TopOfBook


def run(iterations: int = 100_000) -> dict:
    engine = MakerQuoteEngine(MakerConfig())
    state = MakerState(inventory=0.2, cancel_replace_count=4, quote_age_ms=100)
    book = TopOfBook(100.0, 2.0, 100.1, 2.2)

    t0 = perf_counter()
    q_count = 0
    for i in range(iterations):
        quotes, _ = engine.build_ladder(
            book=book,
            state=state,
            volatility_bps=8 + (i % 5),
            toxic_flow=(i % 10) / 20,
            queue_ahead=2 + (i % 4),
            trade_rate=1.2,
            cancel_rate=0.8,
        )
        q_count += len(quotes)
    dt = perf_counter() - t0
    return {
        "iterations": iterations,
        "quotes_generated": q_count,
        "elapsed_s": dt,
        "ops_per_sec": iterations / max(dt, 1e-9),
    }


if __name__ == "__main__":
    print(run())
