from __future__ import annotations

from coinbase.src.protocols import Bar, Direction


def bars_from(closes, opens=None, highs=None, lows=None, vols=None):
    n = len(closes)
    if opens is None:
        opens = list(closes)
    if highs is None:
        highs = [max(o, c) + 1.0 for o, c in zip(opens, closes)]
    if lows is None:
        lows = [min(o, c) - 1.0 for o, c in zip(opens, closes)]
    if vols is None:
        vols = [1000.0] * n
    return [
        Bar(
            timestamp=float(i),
            open=opens[i],
            high=highs[i],
            low=lows[i],
            close=closes[i],
            volume=vols[i],
        )
        for i in range(n)
    ]


def feed(closes, opens=None, highs=None, lows=None, vols=None):
    bars = bars_from(closes, opens, highs, lows, vols)
    return bars[:-1], bars[-1]


def flat_closes(n, base=100.0):
    return [base] * n
