from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class GraphAsset:
    cg_id: str
    symbol: str
    name: str = ""
    product_id: str = ""
    market_cap_rank: Optional[int] = None
    market_cap: Optional[float] = None
    available_on_coinbase: bool = False
    categories: list[str] = field(default_factory=list)
    networks: list[str] = field(default_factory=list)

    @property
    def symbol_key(self) -> str:
        return self.symbol.upper()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"symbol_key": self.symbol_key}


@dataclass(frozen=True)
class TokenContract:
    key: str
    asset_id: str
    network: str
    address: str
    symbol: str = ""
    name: str = ""
    decimals: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WalletObservation:
    address: str
    project_slug: str = ""
    source: str = "manual"
    first_seen: Optional[str] = None
    labels: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GraphAssetSignal:
    product_id: str
    symbol: str
    graph_score: float
    category_count: int = 0
    network_count: int = 0
    token_count: int = 0
    wallet_count: int = 0
    tx_count: int = 0
    market_cap_rank: Optional[int] = None
    available_on_coinbase: bool = False
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
