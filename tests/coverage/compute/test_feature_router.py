import sys
import types
import unittest

import numpy as np


def _install_fake_gpu():
    # --- fake cupy ---
    cupy = types.ModuleType("cupy")
    cupy.asarray = lambda a: np.asarray(a, dtype=float)
    cupy.convolve = np.convolve
    cupy.ones = lambda *a, **k: np.ones(*a, **k)
    cupy.sqrt = np.sqrt
    cupy.asnumpy = lambda a: a
    sys.modules["cupy"] = cupy

    # --- fake torch (minimal tensor wrapper) ---
    class _T:
        def __init__(self, arr):
            self.a = np.asarray(arr, dtype=float)

        def view(self, *a, **k):
            return self

        def pow(self, e):
            return _T(self.a ** e)

        def numpy(self):
            return self.a

        def __add__(self, o):
            return _T(self.a + (o.a if isinstance(o, _T) else o))

        def __sub__(self, o):
            return _T(self.a - (o.a if isinstance(o, _T) else o))

        def __truediv__(self, o):
            return _T(self.a / (o.a if isinstance(o, _T) else o))

    torch = types.ModuleType("torch")
    torch.tensor = lambda a: _T(a)
    torch.ones = lambda *a, **k: _T(np.ones(*a, **k))
    torch.sqrt = lambda x: _T(np.sqrt(x.a))

    def _conv1d(t, k, padding=0):
        return _T(np.convolve(t.a, k.a, mode="same"))

    torch.conv1d = _conv1d
    sys.modules["torch"] = torch


_install_fake_gpu()

from trading_system.compute.feature_pipelines.engine import ComputeRouter, ComputeResult


class TestComputeRouter(unittest.TestCase):
    def test_numpy_short(self):
        r = ComputeRouter(prefer="numpy").rolling_zscore([1.0, 2.0], window=5)
        self.assertIsInstance(r, ComputeResult)
        self.assertEqual(r.backend, "numpy")
        self.assertEqual(list(r.values), [0.0, 0.0])

    def test_numpy_full(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        r = ComputeRouter(prefer="numpy").rolling_zscore(vals, window=3)
        self.assertEqual(r.backend, "numpy")
        self.assertEqual(len(r.values), len(vals))

    def test_cupy_path(self):
        r = ComputeRouter(prefer="cupy").rolling_zscore(
            [1.0, 2.0, 3.0, 4.0, 5.0], window=3
        )
        self.assertEqual(r.backend, "cupy")
        self.assertEqual(len(r.values), 5)

    def test_torch_path(self):
        r = ComputeRouter(prefer="torch").rolling_zscore(
            [1.0, 2.0, 3.0, 4.0, 5.0], window=3
        )
        self.assertEqual(r.backend, "torch")
        self.assertEqual(len(r.values), 5)


if __name__ == "__main__":
    unittest.main()
