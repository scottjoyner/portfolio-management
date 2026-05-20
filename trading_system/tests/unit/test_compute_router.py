from compute.feature_pipelines.engine import ComputeRouter


def test_compute_router_cpu_fallback():
    router = ComputeRouter(prefer="cupy")
    out = router.rolling_zscore([1, 2, 3, 4, 5], window=3)
    assert out.backend in {"cupy", "numpy"}
    assert len(out.values) == 5
