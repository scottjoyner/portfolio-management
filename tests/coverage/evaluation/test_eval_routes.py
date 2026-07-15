import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import evaluation.routes as er  # noqa: E402


class _Q:
    def __init__(self, first=None, all=None):
        self._first = first
        self._all = all if all is not None else []

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def limit(self, n):
        return self

    def first(self):
        return self._first

    def all(self):
        return self._all


class TestEvaluationRoutes(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock(name="db")
        self.store = {}
        self.db.query.side_effect = lambda model: _Q(
            first=self.store.get(model.__name__ + "_first"),
            all=self.store.get(model.__name__ + "_all", []),
        )

    def _patch_service(self, **attrs):
        inst = MagicMock(name="svc")
        for k, v in attrs.items():
            setattr(inst, k, v)
        return inst

    def test_evaluate_instrument(self):
        inst = self._patch_service(
            evaluate_instrument=MagicMock(return_value={"instrument": "BTC-USD"}))
        with patch.object(er, "EvaluationService", return_value=inst):
            out = er.evaluate_instrument("BTC-USD", db=self.db)
        self.assertEqual(out["instrument"], "BTC-USD")

    def test_evaluate_portfolio_success(self):
        inst = self._patch_service(
            evaluate_portfolio=MagicMock(return_value={"portfolio_id": "p1", "results": {}}))
        with patch.object(er, "EvaluationService", return_value=inst):
            out = er.evaluate_portfolio("p1", db=self.db)
        self.assertEqual(out["portfolio_id"], "p1")

    def test_evaluate_portfolio_error(self):
        inst = self._patch_service(
            evaluate_portfolio=MagicMock(return_value={"portfolio_id": "p1", "error": "not found"}))
        with patch.object(er, "EvaluationService", return_value=inst):
            with self.assertRaises(Exception):  # HTTPException 404
                er.evaluate_portfolio("p1", db=self.db)

    def test_evaluation_history(self):
        r1 = MagicMock(analyst="a1", rating_text="BUY", price_target=100.0,
                       created_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        r2 = MagicMock(analyst="a2", rating_text="HOLD", price_target=None,
                       created_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        e1 = MagicMock(dcf_intrinsic_value=200.0, technical_score=70.0,
                       confidence_score=0.8,
                       timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc))
        e2 = MagicMock(dcf_intrinsic_value=None, technical_score=None,
                       confidence_score=0.5,
                       timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc))
        s1 = MagicMock(regime="BULLISH", sentiment_score=0.5,
                       timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc))
        self.store["AnalystRating_all"] = [r1, r2]
        self.store["PriceEstimate_all"] = [e1, e2]
        self.store["SentimentAnalysis_all"] = [s1]
        out = er.evaluation_history("BTC-USD", db=self.db)
        self.assertEqual(out["instrument"], "BTC-USD")
        self.assertEqual(len(out["analyst_ratings"]), 2)
        self.assertEqual(out["analyst_ratings"][0]["price_target"], 100.0)
        self.assertIsNone(out["analyst_ratings"][1]["price_target"])
        self.assertEqual(out["price_estimates"][0]["dcf_value"], 200.0)
        self.assertIsNone(out["price_estimates"][1]["dcf_value"])

    def test_stale_data_warnings(self):
        m1 = MagicMock(feed_name="f1", state="stale", freshness_ms=1000)
        self.store["MarketDataFeed_all"] = [m1]
        out = er.stale_data_warnings(db=self.db)
        self.assertEqual(out[0]["feed"], "f1")
        self.assertEqual(out[0]["state"], "stale")


class TestEvaluationRoutesClient(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(er.router)
        from storage.postgres.session import get_db as real_get_db

        self.app.dependency_overrides[real_get_db] = lambda: MagicMock()
        self.client = TestClient(self.app)

    def test_client_evaluate_instrument(self):
        inst = MagicMock()
        inst.evaluate_instrument.return_value = {
            "instrument": "BTC-USD", "consensus": {}, "agents": [],
            "evidence": [], "evaluated_at": "2024-01-01T00:00:00+00:00",
        }
        with patch.object(er, "EvaluationService", return_value=inst):
            resp = self.client.post("/evaluations/instruments/BTC-USD/evaluate")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["instrument"], "BTC-USD")

    def test_client_evaluate_portfolio_error(self):
        inst = MagicMock()
        inst.evaluate_portfolio.return_value = {"portfolio_id": "p1", "error": "not found"}
        with patch.object(er, "EvaluationService", return_value=inst):
            resp = self.client.post("/evaluations/portfolios/p1/evaluate")
        self.assertEqual(resp.status_code, 404)


if __name__ == "__main__":
    unittest.main()
