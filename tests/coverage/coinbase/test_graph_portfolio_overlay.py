import unittest
from unittest import mock

from coinbase.src.graph.portfolio_overlay import (
    graph_weight_overlays, fetch_graph_weight_overlays, apply_graph_overlay,
)
from coinbase.src.graph.models import GraphAssetSignal


class TestGraphWeightOverlays(unittest.TestCase):
    def test_neutral_score(self):
        sig = GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=0.5)
        overlays = graph_weight_overlays([sig])
        self.assertAlmostEqual(overlays["BTC-USD"], 1.0)

    def test_high_score_boost(self):
        sig = GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=1.0)
        overlays = graph_weight_overlays([sig])
        self.assertAlmostEqual(overlays["BTC-USD"], 1.35)

    def test_low_score_clip(self):
        sig = GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=0.0)
        overlays = graph_weight_overlays([sig])
        self.assertAlmostEqual(overlays["BTC-USD"], 0.65)

    def test_multiple_signals(self):
        sigs = [
            GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=0.9),
            GraphAssetSignal(product_id="ETH-USD", symbol="ETH", graph_score=0.2),
        ]
        overlays = graph_weight_overlays(sigs)
        self.assertEqual(set(overlays), {"BTC-USD", "ETH-USD"})


class TestFetchGraphWeightOverlays(unittest.TestCase):
    def test_fetch(self):
        sig = GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=0.9)
        with mock.patch("coinbase.src.graph.portfolio_overlay.CryptoGraphStore") as Store:
            store = Store.return_value
            store.asset_signal.return_value = sig
            store.close.return_value = None
            overlays = fetch_graph_weight_overlays(["BTC-USD"])
        self.assertAlmostEqual(overlays["BTC-USD"], 1.0 + (0.9 - 0.5) * 2 * 0.35)
        store.close.assert_called_once()


class TestApplyGraphOverlay(unittest.TestCase):
    def test_apply_and_normalize(self):
        weights = {"BTC-USD": 1.0, "ETH-USD": 1.0}
        overlays = {"BTC-USD": 1.35, "ETH-USD": 0.65}
        out = apply_graph_overlay(weights, overlays)
        self.assertAlmostEqual(sum(out.values()), 1.0)
        self.assertAlmostEqual(out["BTC-USD"], 1.35 / (1.35 + 0.65))

    def test_missing_overlay_defaults_one(self):
        weights = {"BTC-USD": 2.0}
        out = apply_graph_overlay(weights, {})
        self.assertEqual(out["BTC-USD"], 1.0)


if __name__ == "__main__":
    unittest.main()
