from __future__ import annotations

import subprocess
import sys
import types
import unittest

import numpy as np

from compute.feature_pipelines.engine import ComputeResult, ComputeRouter


class FakeCupy:
    """Minimal cupy shim executing numpy-compatible ops via numpy."""

    @staticmethod
    def asarray(arr):
        return np.asarray(arr)

    @staticmethod
    def ones(n):
        return np.ones(n)

    @staticmethod
    def convolve(a, b, mode="same"):
        return np.convolve(a, b, mode=mode)

    @staticmethod
    def sqrt(a):
        return np.sqrt(a)

    @staticmethod
    def asnumpy(a):
        return np.asarray(a)


class FakeTorch:
    @staticmethod
    def tensor(arr):
        return torch_wrap(np.asarray(arr))

    @staticmethod
    def ones(n):
        return torch_wrap(np.ones(n))

    @staticmethod
    def conv1d(t, kernel, padding=0):
        a = t._arr
        k = kernel._arr if hasattr(kernel, "_arr") else np.asarray(kernel)
        return torch_wrap(np.convolve(a, k, mode="same"))

    @staticmethod
    def sqrt(a):
        return torch_wrap(np.sqrt(a._arr if hasattr(a, "_arr") else a))


class torch_wrap:
    def __init__(self, arr):
        self._arr = np.asarray(arr)

    def view(self, *shape):
        return self

    def pow(self, p):
        return torch_wrap(self._arr ** p)

    def __sub__(self, other):
        o = other._arr if hasattr(other, "_arr") else other
        return torch_wrap(self._arr - o)

    def __truediv__(self, other):
        o = other._arr if hasattr(other, "_arr") else other
        return torch_wrap(self._arr / o)

    def __add__(self, other):
        o = other._arr if hasattr(other, "_arr") else other
        return torch_wrap(self._arr + o)

    def __radd__(self, other):
        return self.__add__(other)

    def numpy(self):
        return self._arr


class TestComputeRouter(unittest.TestCase):
    def test_compute_result(self):
        r = ComputeResult("numpy", [1.0])
        self.assertEqual(r.backend, "numpy")

    def test_numpy_small_array(self):
        r = ComputeRouter(prefer="numpy").rolling_zscore([1.0, 2.0], window=5)
        self.assertEqual(r.backend, "numpy")
        self.assertEqual(len(r.values), 2)
        self.assertTrue(np.allclose(r.values, 0.0))

    def test_numpy_full(self):
        vals = list(range(1, 41))
        r = ComputeRouter(prefer="numpy").rolling_zscore(vals, window=20)
        self.assertEqual(r.backend, "numpy")
        self.assertEqual(len(r.values), len(vals))

    def test_cupy_path(self):
        sys.modules["cupy"] = FakeCupy()
        try:
            r = ComputeRouter(prefer="cupy").rolling_zscore(list(range(1, 41)), window=20)
            self.assertEqual(r.backend, "cupy")
        finally:
            sys.modules.pop("cupy", None)

    def test_cupy_import_fails_falls_back(self):
        real = sys.modules.get("cupy")

        class Boom:
            def find_module(self, name, path=None):
                if name == "cupy":
                    return self
                return None

            def load_module(self, name):
                raise ImportError("no cupy")

        sys.modules.pop("cupy", None)
        r = ComputeRouter(prefer="cupy").rolling_zscore(list(range(1, 41)), window=20)
        self.assertEqual(r.backend, "numpy")

    def test_torch_path(self):
        sys.modules["torch"] = FakeTorch()
        try:
            r = ComputeRouter(prefer="torch").rolling_zscore(list(range(1, 41)), window=20)
            self.assertEqual(r.backend, "torch")
        finally:
            sys.modules.pop("torch", None)

    def test_torch_import_fails_falls_back(self):
        sys.modules.pop("torch", None)
        r = ComputeRouter(prefer="torch").rolling_zscore(list(range(1, 41)), window=20)
        self.assertEqual(r.backend, "numpy")


class TestMakerPathBenchmark(unittest.TestCase):
    def test_run(self):
        from benchmarks.maker_path_benchmark import run

        result = run(iterations=50)
        self.assertEqual(result["iterations"], 50)
        self.assertGreater(result["quotes_generated"], 0)
        self.assertIn("ops_per_sec", result)
        self.assertGreaterEqual(result["elapsed_s"], 0.0)

    def test_main_runs(self):
        completed = subprocess.run(
            [sys.executable, "trading_system/benchmarks/maker_path_benchmark.py"],
            cwd=".", capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(completed.returncode, 0)
        self.assertIn("ops_per_sec", completed.stdout)


if __name__ == "__main__":
    unittest.main()
