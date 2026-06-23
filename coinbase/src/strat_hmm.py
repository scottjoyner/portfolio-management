from __future__ import annotations
import math
import random
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


@dataclass
class HMModel:
    n_states: int = 3
    n_obs: int = 5
    pi: List[float] = field(default_factory=lambda: [1/3, 1/3, 1/3])
    A: List[List[float]] = field(default_factory=lambda: [
        [0.7, 0.2, 0.1],
        [0.2, 0.6, 0.2],
        [0.1, 0.3, 0.6],
    ])
    B: List[List[float]] = field(default_factory=lambda: [
        [0.4, 0.3, 0.15, 0.1, 0.05],
        [0.1, 0.2, 0.4, 0.2, 0.1],
        [0.05, 0.1, 0.2, 0.3, 0.35],
    ])

    def forward(self, obs: List[int]) -> List[List[float]]:
        T = len(obs)
        alpha = [[0.0] * self.n_states for _ in range(T)]
        for s in range(self.n_states):
            alpha[0][s] = self.pi[s] * self.B[s][obs[0]]
        norm = sum(alpha[0])
        if norm > 0:
            for s in range(self.n_states):
                alpha[0][s] /= norm
        for t in range(1, T):
            for s in range(self.n_states):
                alpha[t][s] = sum(alpha[t-1][i] * self.A[i][s] for i in range(self.n_states)) * self.B[s][obs[t]]
            norm = sum(alpha[t])
            if norm > 0:
                for s in range(self.n_states):
                    alpha[t][s] /= norm
        return alpha

    def backward(self, obs: List[int]) -> List[List[float]]:
        T = len(obs)
        beta = [[0.0] * self.n_states for _ in range(T)]
        for s in range(self.n_states):
            beta[T-1][s] = 1.0
        for t in range(T-2, -1, -1):
            for s in range(self.n_states):
                beta[t][s] = sum(self.A[s][j] * self.B[j][obs[t+1]] * beta[t+1][j] for j in range(self.n_states))
            norm = sum(beta[t])
            if norm > 0:
                for s in range(self.n_states):
                    beta[t][s] /= norm
        return beta

    def viterbi(self, obs: List[int]) -> List[int]:
        T = len(obs)
        delta = [[0.0] * self.n_states for _ in range(T)]
        psi = [[0] * self.n_states for _ in range(T)]
        for s in range(self.n_states):
            delta[0][s] = math.log(max(self.pi[s], 1e-300)) + math.log(max(self.B[s][obs[0]], 1e-300))
        for t in range(1, T):
            for s in range(self.n_states):
                max_val = -float('inf')
                max_idx = 0
                for i in range(self.n_states):
                    val = delta[t-1][i] + math.log(max(self.A[i][s], 1e-300))
                    if val > max_val:
                        max_val = val
                        max_idx = i
                delta[t][s] = max_val + math.log(max(self.B[s][obs[t]], 1e-300))
                psi[t][s] = max_idx
        states = [0] * T
        states[T-1] = max(range(self.n_states), key=lambda s: delta[T-1][s])
        for t in range(T-2, -1, -1):
            states[t] = psi[t+1][states[t+1]]
        return states

    def baum_welch(self, obs: List[int], max_iter: int = 50):
        T = len(obs)
        for _ in range(max_iter):
            alpha = self.forward(obs)
            beta = self.backward(obs)
            gamma = [[alpha[t][s] * beta[t][s] for s in range(self.n_states)] for t in range(T)]
            for t in range(T):
                norm = sum(gamma[t])
                if norm > 0:
                    for s in range(self.n_states):
                        gamma[t][s] /= norm
            xi = [[[0.0] * self.n_states for _ in range(self.n_states)] for _ in range(T-1)]
            for t in range(T-1):
                denom = sum(alpha[t][i] * self.A[i][j] * self.B[j][obs[t+1]] * beta[t+1][j]
                            for i in range(self.n_states) for j in range(self.n_states))
                if denom > 0:
                    for i in range(self.n_states):
                        for j in range(self.n_states):
                            xi[t][i][j] = alpha[t][i] * self.A[i][j] * self.B[j][obs[t+1]] * beta[t+1][j] / denom
            for s in range(self.n_states):
                self.pi[s] = gamma[0][s]
            for i in range(self.n_states):
                denom = sum(gamma[t][i] for t in range(T-1))
                if denom > 0:
                    for j in range(self.n_states):
                        self.A[i][j] = sum(xi[t][i][j] for t in range(T-1)) / denom
            for j in range(self.n_states):
                denom = sum(gamma[t][j] for t in range(T))
                if denom > 0:
                    for k in range(self.n_obs):
                        self.B[j][k] = sum(gamma[t][j] for t in range(T) if obs[t] == k) / denom


PRICE_RETURN_BINS = [
    (-float('inf'), -0.03),
    (-0.03, -0.01),
    (-0.01, 0.01),
    (0.01, 0.03),
    (0.03, float('inf')),
]


def _discretize_return(ret: float) -> int:
    for i, (lo, hi) in enumerate(PRICE_RETURN_BINS):
        if lo < ret <= hi:
            return i
    return 2


_STATE_MEANINGS = {0: "bear", 1: "range", 2: "bull"}


class HMMRegimeStrategy(BaseStrategy):
    def __init__(self, n_states: int = 3, lookback: int = 100,
                 retrain_freq: int = 50, min_confidence: float = 0.4):
        self.n_states = n_states
        self.lookback = lookback
        self.retrain_freq = retrain_freq
        self.min_confidence = min_confidence
        self._name = "hmm_regime"
        self._hmm = HMModel(n_states=n_states)
        self._bars_seen = 0
        self._last_state: int = 1
        self._state_probs: List[float] = [0.0, 1.0, 0.0]

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        self._bars_seen += 1
        if len(bars) < 50:
            return None

        closes = [b.close for b in bars]
        returns = [(closes[i] - closes[i-1]) / max(closes[i-1], 1e-9) for i in range(1, len(closes))]
        obs = [_discretize_return(r) for r in returns]

        train_obs = obs[-min(len(obs), self.lookback):]
        if self._bars_seen % self.retrain_freq == 0 and len(train_obs) >= 30:
            try:
                self._hmm.baum_welch(train_obs, max_iter=20)
            except Exception:
                pass

        if len(train_obs) < 10:
            return None

        alpha = self._hmm.forward(train_obs)
        self._state_probs = alpha[-1] if alpha[-1] else [0.0, 1.0, 0.0]
        self._last_state = max(range(self.n_states), key=lambda s: self._state_probs[s])

        states = self._hmm.viterbi(train_obs)
        recent_states = states[-min(10, len(states)):]
        transitions = sum(1 for i in range(1, len(recent_states)) if recent_states[i] != recent_states[i-1])

        atr = self._estimate_atr(closes, [b.high for b in bars], [b.low for b in bars])
        current = closes[-1]
        current_state_prob = self._state_probs[self._last_state]

        if current_state_prob < self.min_confidence:
            return None

        state_name = _STATE_MEANINGS.get(self._last_state, "unknown")

        if transitions <= 2 and len(recent_states) >= 3:
            if self._last_state == 2:
                direction = Direction.LONG
                stop = current - atr * 2.0
                target = current + atr * 3.0
                reason = f"HMM: bull regime (p={current_state_prob:.2f})"
            elif self._last_state == 0:
                direction = Direction.SHORT
                stop = current + atr * 2.0
                target = current - atr * 3.0
                reason = f"HMM: bear regime (p={current_state_prob:.2f})"
            else:
                return None
        else:
            if len(recent_states) >= 2:
                if recent_states[-1] == 2 and recent_states[-2] != 2:
                    direction = Direction.LONG
                    stop = current - atr * 1.5
                    target = current + atr * 2.5
                    reason = f"HMM: bull transition (p={current_state_prob:.2f})"
                elif recent_states[-1] == 0 and recent_states[-2] != 0:
                    direction = Direction.SHORT
                    stop = current + atr * 1.5
                    target = current - atr * 2.5
                    reason = f"HMM: bear transition (p={current_state_prob:.2f})"
                else:
                    return None
            else:
                return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.2:
            return None

        conf = min(0.8, current_state_prob * (1.0 + 0.3 if transitions <= 2 else 1.0))
        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=stop, target_price=target,
            risk_reward=rr, confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={"hmm_state": state_name, "hmm_prob": round(current_state_prob, 3)},
        )

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i - 1]),
                     abs(lows[-i] - closes[-i - 1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
