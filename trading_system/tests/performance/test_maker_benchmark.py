from benchmarks.maker_path_benchmark import run


def test_maker_benchmark_has_minimum_throughput():
    result = run(iterations=5000)
    assert result["quotes_generated"] > 0
    assert result["ops_per_sec"] > 1000
