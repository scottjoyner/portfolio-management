from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProxyInfo:
    is_proxy: bool = False
    implementation: str = ""
    proxy_type: str = ""
    confidence: float = 0.0


@dataclass
class ProxyDetectionService:
    known_proxies: dict[str, ProxyInfo] = field(default_factory=dict)

    def register(self, address: str, chain: str, info: ProxyInfo) -> None:
        self.known_proxies[f"{chain}:{address.lower()}"] = info

    def detect(self, address: str, chain: str, contract_code: bytes | None = None) -> ProxyInfo:
        key = f"{chain}:{address.lower()}"
        existing = self.known_proxies.get(key)
        if existing:
            return existing
        return ProxyInfo()
