from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class ComputeResult:
    backend: str
    values: Any


class ComputeRouter:
    """CPU-first compute abstraction with optional accelerated backends."""

    def __init__(self, prefer: str = "numpy") -> None:
        self.prefer = prefer

    def rolling_zscore(self, values: list[float], window: int = 20) -> ComputeResult:
        arr = np.asarray(values, dtype=float)
        if arr.size < window:
            return ComputeResult("numpy", np.zeros_like(arr))
        if self.prefer == "cupy":
            try:
                import cupy as cp

                gpu = cp.asarray(arr)
                m = cp.convolve(gpu, cp.ones(window) / window, mode="same")
                s = cp.sqrt(cp.convolve((gpu - m) ** 2, cp.ones(window) / window, mode="same") + 1e-9)
                return ComputeResult("cupy", cp.asnumpy((gpu - m) / s))
            except Exception:
                pass
        if self.prefer == "torch":
            try:
                import torch

                t = torch.tensor(arr)
                kernel = torch.ones(window) / window
                m = torch.conv1d(t.view(1, 1, -1), kernel.view(1, 1, -1), padding=window // 2).view(-1)
                s = torch.sqrt(
                    torch.conv1d((t - m).pow(2).view(1, 1, -1), kernel.view(1, 1, -1), padding=window // 2).view(-1) + 1e-9
                )
                return ComputeResult("torch", ((t - m) / s).numpy())
            except Exception:
                pass
        return ComputeResult("numpy", (arr - arr.mean()) / (arr.std() + 1e-9))
