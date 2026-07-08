"""
GPU-accelerated compute backend with CPU fallback.

Architecture::

    ComputeBackend (abstract base)
    ├── NumpyBackend   (always available, CPU)
    └── TorchBackend   (accelerated: CUDA / ROCm / MPS / CPU)

Matrix layout for batch operations::

    closes:  (n_products, n_candles)   — all products, full candle history
    volumes: (n_products, n_candles)
    highs:   (n_products, n_candles)
    lows:    (n_products, n_candles)

The batch dimension (axis=0) is where GPU parallelism shines —
computing RSI for 34 products simultaneously vs 34 sequential calls.

Usage::

    backend = get_compute_backend()
    rsi_all = backend.batch_rsi(closes)          # (34, ) — RSI for every product
    ema_all = backend.batch_ema(closes, 12)      # (34, ) — fast EMA
    macd_line, sig, hist = backend.batch_macd(closes)  # all MACD components
"""

from __future__ import annotations
import logging
import time
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# ── Backend registry ──────────────────────────────────────────────────

_BACKEND_INSTANCE = None


def get_compute_backend(force: Optional[str] = None) -> "ComputeBackend":
    """Return singleton backend.  ``force`` = ``"numpy"`` | ``"torch"``."""
    global _BACKEND_INSTANCE
    if _BACKEND_INSTANCE is not None and force is None:
        return _BACKEND_INSTANCE
    if force == "numpy":
        _BACKEND_INSTANCE = NumpyBackend()
    elif force == "torch":
        _BACKEND_INSTANCE = TorchBackend()
    else:
        _BACKEND_INSTANCE = _detect_best_backend()
    return _BACKEND_INSTANCE


def _detect_best_backend() -> "ComputeBackend":
    """Auto-detect: prefer Numpy (fastest CPU) unless GPU is available."""
    # Torch is only beneficial when GPU (CUDA/ROCm/MPS) is available.
    # On CPU, Numpy is 3-4x faster than Torch for these operations.
    torch_backend = TorchBackend()
    if torch_backend.available and torch_backend.device != "cpu":
        logger.info("Compute backend: Torch (%s) — GPU acceleration active", torch_backend.device)
        return torch_backend
    numpy_backend = NumpyBackend()
    logger.info("Compute backend: Numpy (CPU)")
    return numpy_backend


# ── Abstract base ─────────────────────────────────────────────────────

class ComputeBackend:
    """Abstract computation backend.

    Subclasses must set ``self.available``, ``self.device``, ``self.name``.
    """

    available: bool = False
    device: str = "cpu"
    name: str = "abstract"

    # ── Batch indicator helpers ────────────────────────────────────

    def batch_rsi(self, closes, period: int = 14):
        """RSI for every product in ``closes`` (n_products, n_candles)."""
        raise NotImplementedError

    def batch_ema(self, closes, period: int):
        """Exponential Moving Average — last value for every product."""
        raise NotImplementedError

    def batch_ema_slice(self, closes, period: int):
        """Full EMA series for every product."""
        raise NotImplementedError

    def batch_sma(self, closes, period: int):
        """Simple Moving Average — last value for every product."""
        raise NotImplementedError

    def batch_bollinger(self, closes, period: int = 20, std_mult: float = 2.0):
        """Returns (lower, middle, upper, bandwidth) each as (n_products,)."""
        raise NotImplementedError

    def batch_macd(self, closes, fast: int = 12, slow: int = 26, signal: int = 9):
        """Returns (macd_line, signal_line, histogram) each (n_products,)."""
        raise NotImplementedError

    def batch_trix(self, closes, period: int = 14):
        """TRIX value for every product."""
        raise NotImplementedError

    def batch_zscore(self, closes, period: int = 30):
        """Returns (zscore, mean, std) each (n_products,)."""
        raise NotImplementedError

    # ── Batch signal generation ────────────────────────────────────

    def batch_signals(self, closes, volumes, highs, lows) -> Dict[str, List[str]]:
        """Run ALL strategies against ALL products, return per-product signal list.

        Returns::

            {"ema_cross": ["BUY", "HOLD", "SELL", ...],   # one per product
             "rsi_revert": ["HOLD", "BUY", ...],
             ...}
        """
        raise NotImplementedError

    # ── Benchmark ──────────────────────────────────────────────────

    def benchmark(self, n_products: int = 34, n_candles: int = 100, n_iter: int = 100) -> dict:
        """Run a micro-benchmark and return timing stats."""
        import numpy as np
        rng = np.random.default_rng(42)
        closes = rng.random((n_products, n_candles)) * 100 + 49000
        volumes = rng.random((n_products, n_candles)) * 1e6
        highs = closes * (1 + rng.random((n_products, n_candles)) * 0.02)
        lows = closes * (1 - rng.random((n_products, n_candles)) * 0.02)

        def _time(fn, name):
            start = time.perf_counter()
            for _ in range(n_iter):
                fn()
            elapsed = time.perf_counter() - start
            return {"op": name, "iter": n_iter, "total_ms": round(elapsed * 1000, 1),
                    "avg_us": round(elapsed / n_iter * 1e6, 1)}

        results = [
            _time(lambda: self.batch_rsi(closes), "rsi"),
            _time(lambda: self.batch_ema(closes, 12), "ema_12"),
            _time(lambda: self.batch_bollinger(closes), "bollinger"),
            _time(lambda: self.batch_macd(closes), "macd"),
            _time(lambda: self.batch_trix(closes, 14), "trix"),
        ]
        total_ms = sum(r["total_ms"] for r in results)
        results.append({"op": "TOTAL", "iter": n_iter,
                        "total_ms": round(total_ms, 1), "avg_us": round(total_ms / n_iter * 1e3, 1)})
        return {"backend": self.name, "device": self.device,
                "n_products": n_products, "n_candles": n_candles,
                "ops": results}


# ── Numpy backend (always available) ──────────────────────────────────

class NumpyBackend(ComputeBackend):
    """CPU-only backend using NumPy vectorized operations."""

    def __init__(self):
        import numpy as np
        self.np = np
        self.available = True
        self.device = "cpu"
        self.name = "numpy"

    # ── helpers ────────────────────────────────────────────────────

    def _to_array(self, data):
        if isinstance(data, list):
            return self.np.array(data, dtype=self.np.float64)
        if hasattr(data, "numpy"):
            return data.numpy()
        return self.np.asarray(data, dtype=self.np.float64)

    def _ema_vector(self, arr, period: int):
        """Vectorized EMA for a 2D array (n_products, n_candles).
        Returns the last EMA value for each product.
        """
        n = arr.shape[1]
        if n < period:
            return self.np.full(arr.shape[0], self.np.nan)
        k = 2.0 / (period + 1.0)
        # Seed with SMA
        ema = self.np.mean(arr[:, :period], axis=1)
        for i in range(period, n):
            ema = arr[:, i] * k + ema * (1.0 - k)
        return ema

    def _ema_slice_vector(self, arr, period: int):
        """Full EMA series (n_products, n_candles)."""
        n = arr.shape[1]
        result = self.np.full_like(arr, self.np.nan)
        if n < period:
            return result
        k = 2.0 / (period + 1.0)
        ema = self.np.mean(arr[:, :period], axis=1)
        result[:, period - 1] = ema
        for i in range(period, n):
            ema = arr[:, i] * k + ema * (1.0 - k)
            result[:, i] = ema
        return result

    # ── batch indicators ───────────────────────────────────────────

    def batch_rsi(self, closes, period: int = 14):
        arr = self._to_array(closes)
        n = arr.shape[1]
        if n < period + 1:
            return self.np.full(arr.shape[0], 50.0)
        # Compute deltas: differences between consecutive candles
        deltas = self.np.diff(arr, axis=1)  # (n_products, n_candles-1)
        # Use only the last `period` deltas
        recent_deltas = deltas[:, -period:]  # (n_products, period)
        gains = self.np.clip(recent_deltas, 0, None)
        losses = self.np.clip(-recent_deltas, 0, None)
        avg_gain = self.np.mean(gains, axis=1)
        avg_loss = self.np.mean(losses, axis=1)
        # Handle zero-loss edge case
        rs = self.np.divide(avg_gain, avg_loss, out=self.np.ones_like(avg_gain),
                            where=avg_loss > 0)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        rsi[avg_loss == 0] = 100.0
        return rsi

    def batch_ema(self, closes, period: int):
        return self._ema_vector(self._to_array(closes), period)

    def batch_ema_slice(self, closes, period: int):
        return self._ema_slice_vector(self._to_array(closes), period)

    def batch_sma(self, closes, period: int):
        arr = self._to_array(closes)
        if arr.shape[1] < period:
            return self.np.full(arr.shape[0], self.np.nan)
        return self.np.mean(arr[:, -period:], axis=1)

    def batch_bollinger(self, closes, period: int = 20, std_mult: float = 2.0):
        arr = self._to_array(closes)
        n = arr.shape[1]
        out_nan = (self.np.full(arr.shape[0], self.np.nan),) * 4
        if n < period:
            return out_nan
        window = arr[:, -period:]
        mean = self.np.mean(window, axis=1)
        variance = self.np.var(window, axis=1, ddof=0)
        std = self.np.sqrt(variance)
        lower = mean - std_mult * std
        upper = mean + std_mult * std
        bandwidth = self.np.divide(upper - lower, mean, out=self.np.zeros_like(mean),
                                   where=mean != 0)
        return lower, mean, upper, bandwidth

    def batch_macd(self, closes, fast: int = 12, slow: int = 26, signal: int = 9):
        arr = self._to_array(closes)
        n = arr.shape[1]
        if n < slow + signal:
            return (self.np.full(arr.shape[0], self.np.nan),) * 3
        ema_fast = self._ema_slice_vector(arr, fast)
        ema_slow = self._ema_slice_vector(arr, slow)
        macd_line = ema_fast - ema_slow
        # Signal line: EMA of MACD line
        sig_line = self._ema_vector(macd_line, signal)
        hist = macd_line[:, -1] - sig_line
        return macd_line[:, -1], sig_line, hist

    def batch_trix(self, closes, period: int = 14):
        arr = self._to_array(closes)
        ema1 = self._ema_slice_vector(arr, period)
        ema2 = self._ema_slice_vector(ema1, period)
        ema3 = self._ema_slice_vector(ema2, period)
        n = ema3.shape[1]
        if n < 2:
            return self.np.full(arr.shape[0], self.np.nan)
        prev = ema3[:, -2]
        curr = ema3[:, -1]
        trix = self.np.divide(curr - prev, prev, out=self.np.full_like(curr, self.np.nan),
                              where=prev != 0)
        return trix

    def batch_zscore(self, closes, period: int = 30):
        arr = self._to_array(closes)
        n = arr.shape[1]
        if n < period:
            return (self.np.full(arr.shape[0], self.np.nan),) * 3
        window = arr[:, -period:]
        mean = self.np.mean(window, axis=1)
        std = self.np.std(window, axis=1, ddof=0)
        z = self.np.divide(arr[:, -1] - mean, std, out=self.np.zeros_like(mean),
                           where=std > 0)
        return z, mean, std

    def batch_signals(self, closes, volumes, highs, lows) -> Dict[str, List[str]]:
        """Generate all 5 Rust-supported strategy signals for ALL products at once."""
        arr_c = self._to_array(closes)
        price = arr_c[:, -1]
        n_prod = arr_c.shape[0]

        # Compute all indicators in one shot (vectorized)
        rsi_vals = self.batch_rsi(arr_c, 14)
        ema_12 = self.batch_ema(arr_c, 12)
        ema_26 = self.batch_ema(arr_c, 26)
        _, _, upper, _ = self.batch_bollinger(arr_c, 20, 2.0)
        lower, _, _, _ = self.batch_bollinger(arr_c, 20, 2.0)
        macd_line, sig_line, _ = self.batch_macd(arr_c, 12, 26, 9)
        trix_vals = self.batch_trix(arr_c, 14)

        results: Dict[str, List[str]] = {
            "ema_cross": ["HOLD"] * n_prod,
            "rsi_revert": ["HOLD"] * n_prod,
            "boll_break": ["HOLD"] * n_prod,
            "macd": ["HOLD"] * n_prod,
            "trix": ["HOLD"] * n_prod,
        }

        for i in range(n_prod):
            # EMA crossover
            if self.np.isfinite(ema_12[i]) and self.np.isfinite(ema_26[i]):
                if ema_12[i] > ema_26[i]:
                    results["ema_cross"][i] = "BUY" if price[i] > ema_12[i] else "HOLD"
                else:
                    results["ema_cross"][i] = "SELL" if price[i] < ema_26[i] else "HOLD"

            # RSI
            r = rsi_vals[i]
            if self.np.isfinite(r):
                if r < 30:
                    results["rsi_revert"][i] = "BUY"
                elif r > 70:
                    results["rsi_revert"][i] = "SELL"

            # Bollinger
            if self.np.isfinite(lower[i]) and self.np.isfinite(upper[i]):
                if price[i] <= lower[i]:
                    results["boll_break"][i] = "BUY"
                elif price[i] >= upper[i]:
                    results["boll_break"][i] = "SELL"

            # MACD
            if self.np.isfinite(macd_line[i]) and self.np.isfinite(sig_line[i]):
                if macd_line[i] > sig_line[i]:
                    results["macd"][i] = "BUY"
                else:
                    results["macd"][i] = "SELL"

            # TRIX
            t = trix_vals[i]
            if self.np.isfinite(t):
                if t > 0:
                    results["trix"][i] = "BUY"
                else:
                    results["trix"][i] = "SELL"

        return results


# ── Torch backend (CPU / CUDA / ROCm / MPS) ──────────────────────────

class TorchBackend(ComputeBackend):
    """PyTorch-accelerated backend. Uses GPU when available."""

    def __init__(self):
        self._torch = None
        self.available = False
        self.device = "cpu"
        self.name = "torch"
        try:
            import torch
            self._torch = torch
            self.available = True
            # Detect best available device
            if torch.cuda.is_available():
                self.device = "cuda"
                self.name = "torch_cuda"
                logger.info("TorchBackend: CUDA device detected (%s)",
                            torch.cuda.get_device_name(0) if torch.cuda.is_available() else "?")
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                self.device = "mps"
                self.name = "torch_mps"
                logger.info("TorchBackend: MPS (Apple Silicon) detected")
            else:
                logger.info("TorchBackend: CPU mode (no GPU accelerator found)")
        except ImportError:
            self.available = False

    # ── Tensor conversion helpers ──────────────────────────────────

    def _to_tensor(self, data, dtype=None):
        import numpy as np
        if dtype is None:
            dtype = self._torch.float32
        if isinstance(data, self._torch.Tensor):
            return data.to(device=self.device)
        if isinstance(data, np.ndarray):
            return self._torch.from_numpy(data.astype(np.float32)).to(device=self.device)
        if isinstance(data, (list, tuple)):
            return self._torch.tensor(data, dtype=dtype, device=self.device)
        return self._torch.tensor(data, dtype=dtype, device=self.device)

    def _to_numpy(self, tensor):
        if tensor is None:
            return None
        return tensor.cpu().numpy() if tensor.device.type != "cpu" else tensor.numpy()

    # ── Batch indicators (tensor ops) ──────────────────────────────

    def batch_rsi(self, closes, period: int = 14):
        t = self._to_tensor(closes)
        n = t.shape[1]
        if n < period + 1:
            return self._to_numpy(self._torch.full((t.shape[0],), 50.0))
        deltas = t[:, 1:] - t[:, :-1]
        recent = deltas[:, -period:]
        gains = self._torch.clamp(recent, min=0)
        losses = self._torch.clamp(-recent, min=0)
        avg_gain = gains.mean(dim=1)
        avg_loss = losses.mean(dim=1)
        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        rsi[avg_loss == 0] = 100.0
        return self._to_numpy(rsi)

    def batch_ema(self, closes, period: int):
        return self._to_numpy(self._ema_vector(self._to_tensor(closes), period))

    def _ema_vector(self, t, period: int):
        n = t.shape[1]
        if n < period:
            return self._torch.full((t.shape[0],), float("nan"), device=self.device)
        k = 2.0 / (period + 1.0)
        ema = t[:, :period].mean(dim=1)
        for i in range(period, n):
            ema = t[:, i] * k + ema * (1.0 - k)
        return ema

    def _ema_slice_vector(self, t, period: int):
        n = t.shape[1]
        result = self._torch.full_like(t, float("nan"))
        if n < period:
            return result
        k = 2.0 / (period + 1.0)
        ema = t[:, :period].mean(dim=1)
        result[:, period - 1] = ema
        for i in range(period, n):
            ema = t[:, i] * k + ema * (1.0 - k)
            result[:, i] = ema
        return result

    def batch_sma(self, closes, period: int):
        t = self._to_tensor(closes)
        if t.shape[1] < period:
            return self._to_numpy(self._torch.full((t.shape[0],), float("nan")))
        return self._to_numpy(t[:, -period:].mean(dim=1))

    def batch_bollinger(self, closes, period: int = 20, std_mult: float = 2.0):
        t = self._to_tensor(closes)
        n = t.shape[1]
        nan_t = self._torch.full((t.shape[0],), float("nan"), device=self.device)
        if n < period:
            return (self._to_numpy(nan_t),) * 4
        window = t[:, -period:]
        mean = window.mean(dim=1)
        var = window.var(dim=1, unbiased=False)
        std = var.sqrt()
        lower = mean - std_mult * std
        upper = mean + std_mult * std
        bandwidth = (upper - lower) / (mean + 1e-10)
        return (self._to_numpy(lower), self._to_numpy(mean),
                self._to_numpy(upper), self._to_numpy(bandwidth))

    def batch_macd(self, closes, fast: int = 12, slow: int = 26, signal: int = 9):
        t = self._to_tensor(closes)
        n = t.shape[1]
        nan_t = self._to_numpy(self._torch.full((t.shape[0],), float("nan")))
        if n < slow + signal:
            return (nan_t,) * 3
        ema_fast = self._ema_slice_vector(t, fast)
        ema_slow = self._ema_slice_vector(t, slow)
        macd_line = ema_fast - ema_slow
        sig_line = self._ema_vector(macd_line, signal)
        hist = macd_line[:, -1] - sig_line
        return (self._to_numpy(macd_line[:, -1]),
                self._to_numpy(sig_line),
                self._to_numpy(hist))

    def batch_trix(self, closes, period: int = 14):
        t = self._to_tensor(closes)
        ema1 = self._ema_slice_vector(t, period)
        ema2 = self._ema_slice_vector(ema1, period)
        ema3 = self._ema_slice_vector(ema2, period)
        if ema3.shape[1] < 2:
            return self._to_numpy(self._torch.full((t.shape[0],), float("nan")))
        prev = ema3[:, -2]
        curr = ema3[:, -1]
        trix = (curr - prev) / (prev + 1e-10)
        return self._to_numpy(trix)

    def batch_zscore(self, closes, period: int = 30):
        t = self._to_tensor(closes)
        n = t.shape[1]
        nan3 = (self._to_numpy(self._torch.full((t.shape[0],), float("nan"))),) * 3
        if n < period:
            return nan3
        window = t[:, -period:]
        mean = window.mean(dim=1)
        std = window.std(dim=1, unbiased=False)
        z = (t[:, -1] - mean) / (std + 1e-10)
        return self._to_numpy(z), self._to_numpy(mean), self._to_numpy(std)

    def batch_signals(self, closes, volumes, highs, lows) -> Dict[str, List[str]]:
        """Generate all signals for ALL products at once using tensor ops.
        Returns numpy arrays for cross-backend compatibility.
        """
        import numpy as np
        t_c = self._to_tensor(closes)
        n_prod = t_c.shape[0]
        price_np = self._to_numpy(t_c[:, -1])

        # Compute all indicators (they return numpy arrays)
        rsi_vals = self.batch_rsi(t_c, 14)
        ema_12 = self.batch_ema(t_c, 12)
        ema_26 = self.batch_ema(t_c, 26)
        lower, _, upper, _ = self.batch_bollinger(t_c, 20, 2.0)
        macd_line, sig_line, _ = self.batch_macd(t_c, 12, 26, 9)
        trix_vals = self.batch_trix(t_c, 14)

        def _isfinite(a, i):
            return bool(np.isfinite(a[i]))

        results: Dict[str, List[str]] = {
            "ema_cross": ["HOLD"] * n_prod,
            "rsi_revert": ["HOLD"] * n_prod,
            "boll_break": ["HOLD"] * n_prod,
            "macd": ["HOLD"] * n_prod,
            "trix": ["HOLD"] * n_prod,
        }

        for i in range(n_prod):
            if _isfinite(ema_12, i) and _isfinite(ema_26, i):
                if float(ema_12[i]) > float(ema_26[i]):
                    results["ema_cross"][i] = "BUY" if price_np[i] > float(ema_12[i]) else "HOLD"
                else:
                    results["ema_cross"][i] = "SELL" if price_np[i] < float(ema_26[i]) else "HOLD"

            r = rsi_vals[i]
            if _isfinite(rsi_vals, i):
                if r < 30:
                    results["rsi_revert"][i] = "BUY"
                elif r > 70:
                    results["rsi_revert"][i] = "SELL"

            if _isfinite(lower, i) and _isfinite(upper, i):
                if price_np[i] <= float(lower[i]):
                    results["boll_break"][i] = "BUY"
                elif price_np[i] >= float(upper[i]):
                    results["boll_break"][i] = "SELL"

            if _isfinite(macd_line, i) and _isfinite(sig_line, i):
                results["macd"][i] = "BUY" if float(macd_line[i]) > float(sig_line[i]) else "SELL"

            t = trix_vals[i]
            if _isfinite(trix_vals, i):
                results["trix"][i] = "BUY" if float(t) > 0 else "SELL"

        return results


# ── Benchmark CLI ─────────────────────────────────────────────────────

def run_benchmark(n_products: int = 34, n_candles: int = 100, n_iter: int = 200):
    """Benchmark all available backends and print comparison."""
    import numpy as np

    results = []
    for name in ("numpy", "torch"):
        try:
            backend = get_compute_backend(force=name)
            if not backend.available:
                continue
            r = backend.benchmark(n_products, n_candles, n_iter)
            results.append(r)
        except Exception as e:
            logger.warning("Benchmark %s failed: %s", name, e)

    print(f"\n{'='*80}")
    print(f"COMPUTE BACKEND BENCHMARK: {n_products} products × {n_candles} candles × {n_iter} iterations")
    print(f"{'='*80}\n")

    for r in results:
        print(f"Backend: {r['backend']:20s}  Device: {r['device']}")
        print(f"{'─'*60}")
        for op in r["ops"]:
            print(f"  {op['op']:20s}  {op['avg_us']:>8.1f} µs/iter  ({op['total_ms']:>8.1f} ms total)")
        print()

    # Comparative analysis
    if len(results) >= 2:
        print(f"{'─'*60}")
        print("Speedup (Torch vs NumPy):")
        for n_op in results[1]["ops"]:
            for p_op in results[0]["ops"]:
                if n_op["op"] == p_op["op"] and p_op["avg_us"] > 0:
                    ratio = p_op["avg_us"] / n_op["avg_us"]
                    label = "GPU FASTER" if ratio > 1.2 else "CPU FASTER" if ratio < 0.8 else "~SAME"
                    print(f"  {n_op['op']:20s}  {ratio:>6.2f}×  ({label})")
                    break

    print(f"{'='*80}\n")


# ── CLI entry point ───────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Compute backend benchmark")
    p.add_argument("--products", type=int, default=34, help="Number of products")
    p.add_argument("--candles", type=int, default=100, help="Candles per product")
    p.add_argument("--iter", type=int, default=200, help="Iterations")
    p.add_argument("--backend", choices=["numpy", "torch", "auto"], default="auto")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

    if args.backend != "auto":
        b = get_compute_backend(force=args.backend)
        print(f"Backend: {b.name} / {b.device}  available={b.available}")
        if b.available:
            r = b.benchmark(args.products, args.candles, args.iter)
            for op in r["ops"]:
                print(f"  {op['op']:20s}  {op['avg_us']:>8.1f} µs/iter")
    else:
        run_benchmark(args.products, args.candles, args.iter)
