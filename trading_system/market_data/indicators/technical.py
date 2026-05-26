from __future__ import annotations



class TechnicalIndicatorSet:
    def __init__(self, max_samples: int = 500) -> None:
        self._prices: list[float] = []
        self._volumes: list[float] = []
        self._max_samples = max_samples

    def ingest(self, price: float, volume: float = 0.0) -> None:
        self._prices.append(price)
        self._volumes.append(volume)
        if len(self._prices) > self._max_samples:
            self._prices = self._prices[-self._max_samples:]
            self._volumes = self._volumes[-self._max_samples:]

    def sma(self, period: int = 20) -> float:
        if len(self._prices) < period:
            return 0.0
        return sum(self._prices[-period:]) / period

    def ema(self, period: int = 20) -> float:
        if len(self._prices) < period:
            return self.sma(period)
        multiplier = 2.0 / (period + 1)
        result = sum(self._prices[-period:]) / period
        for price in self._prices[-(period - 1):]:
            result = (price - result) * multiplier + result
        return result

    def rsi(self, period: int = 14) -> float:
        if len(self._prices) < period + 1:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(len(self._prices) - period, len(self._prices)):
            change = self._prices[i] - self._prices[i - 1]
            if change > 0:
                gains += change
            else:
                losses -= change
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _stddev(self, period: int) -> float:
        if len(self._prices) < period:
            return 0.0
        recent = self._prices[-period:]
        mean = sum(recent) / period
        variance = sum((p - mean) ** 2 for p in recent) / period
        return variance ** 0.5

    def bollinger_bands(self, period: int = 20, num_std: float = 2.0) -> dict[str, float]:
        mid = self.sma(period)
        std = self._stddev(period)
        return {"upper": mid + num_std * std, "mid": mid, "lower": mid - num_std * std}

    def zscore(self, period: int = 20) -> float:
        if len(self._prices) < period:
            return 0.0
        recent = self._prices[-period:]
        mean = sum(recent) / period
        std = self._stddev(period)
        return (self._prices[-1] - mean) / std if std > 0 else 0.0

    def volume_sma(self, period: int = 20) -> float:
        if len(self._volumes) < period:
            return 0.0
        return sum(self._volumes[-period:]) / period

    def volume_ratio(self, period: int = 20) -> float:
        avg = self.volume_sma(period)
        if avg <= 0:
            return 1.0
        return (self._volumes[-1] if self._volumes else 0) / avg
