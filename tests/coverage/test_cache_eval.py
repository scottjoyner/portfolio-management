from __future__ import annotations

import asyncio
from datetime import datetime
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock

from trading_system.cache import redis as cache_redis
from trading_system.cache.redis import RedisCacheManager
from trading_system.cache import create_cache_manager, get_cache_for_endpoint, set_cache_for_endpoint


class _FakeRedis:
    def __init__(self):
        self.store: dict = {}
        self.ttls: dict = {}

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.store[key] = value
        self.ttls[key] = ttl

    def ttl(self, key):
        return self.ttls.get(key, 0)

    def scan_iter(self, match=None, count=100):
        import fnmatch
        for k in self.store:
            if fnmatch.fnmatch(k, match or "*"):
                yield k

    def delete(self, key):
        self.store.pop(key, None)

    def ping(self):
        return True

    def info(self, section=None):
        return {"connected_keys": len(self.store)}


class TestRedisCacheManager(TestCase):
    def test_none_client_get_set(self):
        m = RedisCacheManager(redis_client=None)
        self.assertIsNone(m.get("metrics", response_data={"a": 1}))
        self.assertFalse(m.set("metrics", response_data={"a": 1}))

    def test_none_client_invalidate(self):
        m = RedisCacheManager(redis_client=None)
        self.assertEqual(m.invalidate(), 0)
        self.assertEqual(m.invalidate("ts_*"), 0)

    def test_set_and_get_dict_hit(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        self.assertTrue(m.set("metrics", response_data={"a": 1}))
        got = m.get("metrics", response_data={"a": 1})
        self.assertEqual(got, {"a": 1})
        self.assertEqual(m.stats["hits"], 1)
        self.assertEqual(m.stats["misses"], 0)

    def test_get_miss(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        self.assertIsNone(m.get("metrics", response_data={"a": 1}))
        self.assertEqual(m.stats["misses"], 1)

    def test_set_existing_no_refresh(self):
        client = _FakeRedis()
        m = RedisCacheManager(redis_client=client)
        self.assertTrue(m.set("metrics", response_data={"a": 1}))
        # second set with same key (already cached) returns True
        self.assertTrue(m.set("metrics", response_data={"a": 1}))
        # force_refresh re-sets
        self.assertTrue(m.set("metrics", response_data={"a": 1}, force_refresh=True))

    def test_get_deserialize_error(self):
        client = _FakeRedis()
        client.store["badkey"] = b"\xff\xfe not valid"
        m = RedisCacheManager(redis_client=client)
        self.assertIsNone(m.get("metrics", key="badkey"))
        self.assertEqual(m.stats["errors"], 1)

    def test_invalidate_pattern(self):
        client = _FakeRedis()
        client.store["ts_metrics:1"] = "x"
        client.store["ts_accounts:1"] = "y"
        m = RedisCacheManager(redis_client=client)
        n = m.invalidate("ts_metrics:*")
        self.assertEqual(n, 1)

    def test_get_stats_no_requests(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        self.assertEqual(m.get_stats()["hit_rate_pct"], 0)

    def test_get_stats_with_requests(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        m.set("metrics", response_data={"a": 1})
        m.get("metrics", response_data={"a": 1})  # hit
        m.get("metrics", response_data={"b": 2})  # miss
        stats = m.get_stats()
        self.assertEqual(stats["total_requests"], 2)
        self.assertAlmostEqual(stats["hit_rate_pct"], 50.0)

    def test_health_check_no_redis(self):
        m = RedisCacheManager(redis_client=None)
        self.assertEqual(m.health_check()["redis_connected"], False)

    def test_health_check_with_redis(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        h = m.health_check()
        self.assertTrue(h["redis_connected"])

    def test_make_key_dict(self):
        m = RedisCacheManager(redis_client=_FakeRedis())
        k = m._make_key("metrics", {"a": 1})
        self.assertIn("ts_metrics:", k)


class TestCacheFactory(TestCase):
    def test_create_mock(self):
        m = create_cache_manager(use_mock=True)
        self.assertTrue(m.is_mock)

    def test_create_no_url_uses_mock(self):
        m = create_cache_manager()
        self.assertTrue(m.is_mock)

    def test_create_with_url(self):
        # redis may not be installed; either real or mock both acceptable
        m = create_cache_manager(redis_url="redis://localhost:6379/0")
        self.assertIsNotNone(m)

    def test_endpoint_helpers(self):
        m = create_cache_manager(use_mock=True)
        self.assertTrue(set_cache_for_endpoint("metrics", {"x": 1}, m))
        self.assertEqual(get_cache_for_endpoint("metrics", m), None)


class TestCacheDecorator(IsolatedAsyncioTestCase):
    def test_cache_decorator(self):
        m = create_cache_manager(use_mock=True)

        @cache_redis.cache("metrics")
        async def get_metrics(db=None):
            return {"fresh": True}

        res = asyncio.run(get_metrics(cache_manager=m))
        self.assertEqual(res, {"fresh": True})


class TestEvaluationBase(TestCase):
    def test_enums(self):
        from trading_system.evaluation.base import Action, Philosophy, Evidence, AgentResult, BaseAgent
        self.assertEqual(Action.STRONG_BUY.value, "strong_buy")
        self.assertEqual(Philosophy.MEAN_REVERSION.value, "mean_reversion")
        ev = Evidence(source="s", metric="m", value=1.0, weight=0.5)
        self.assertEqual(ev.weight, 0.5)
        res = AgentResult(agent_name="a", instrument="BTC", action=Action.BUY,
                          confidence=0.9, rationale="r", risk_score=0.1,
                          philosophy=Philosophy.MOMENTUM)
        self.assertEqual(res.action, Action.BUY)
        self.assertIsNotNone(res.created_at)


class TestPricingModels(IsolatedAsyncioTestCase):
    def test_price_target_enum(self):
        from trading_system.evaluation.pricing_models import PriceTargetModel
        self.assertEqual(PriceTargetModel.FUNDAMENTAL_BASED.value, "fundamental")

    async def test_estimate_price(self):
        from trading_system.evaluation.pricing_models import (
            PriceEstimationEngine, PriceTargetModel)
        e = PriceEstimationEngine(config={"price_source": "fundamental"})
        res = await e.estimate_price("ETH", PriceTargetModel.TECHNICAL_ANALYSIS,
                                     {"current_price": "5000"})
        self.assertEqual(res["buy_level"], 4750.0)
        self.assertEqual(res["model_used"], "technical")

    async def test_calculate_position_quality(self):
        from trading_system.evaluation.pricing_models import PriceEstimationEngine
        e = PriceEstimationEngine()
        m = await e.calculate_position_quality({
            "entry_price": "4500", "current_price": "5000",
            "correlation_to_index": 0.85, "volatility_regime": "high",
        })
        self.assertGreater(m.risk_score, 0.3)
        self.assertGreater(m.alpha_score, 0)

    async def test_calculate_position_quality_zero_entry(self):
        from trading_system.evaluation.pricing_models import PriceEstimationEngine
        e = PriceEstimationEngine()
        m = await e.calculate_position_quality({"entry_price": "0", "current_price": "0"})
        self.assertEqual(m.alpha_score, 0.0)


if __name__ == "__main__":
    import unittest

    unittest.main()
