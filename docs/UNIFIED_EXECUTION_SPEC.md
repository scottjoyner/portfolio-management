# Unified Execution Specification (UES)
## Version 1.0 (Proposed)

### Overview
This document outlines the technical specification for unifying the execution capabilities of the `portfolio-management` system, bridging traditional brokerages (e.g., Coinbase, Interactive Brokers) and on-chain decentralized exchanges (e.g., Uniswap, Curve).

---

### Phase 1: Unified Execution Interface (The "Universal Adapter")

The goal is to create a single, high-level abstraction layer that treats every execution venue as a provider of liquidity, regardless of whether the underlying mechanism is an order book (CEX) or an automated market maker (DEX).

#### 1.1 Core Data Models (The "Universal" Types)

To achieve true abstraction, we must define models that encapsulate both traditional and on-chain nuances.

**`UniversalAsset`**
| Field | Type | Description |
| :--- | :--- | :--- |
| `asset_id` | `str` | Internal unique identifier (e.g., `BTC-USD`). |
| `symbol` | `str` | Trading symbol (e.g., `BTC`). |
| `base_currency` | `str` | The asset being bought/sold. |
| `quote_currency` | `str` | The asset being used for payment. |
| `chain_id` | `str \| None` | The blockchain (e.g., `1` for Ethereum, `None` for CEX). |
| `contract_address` | `str \| None` | The smart contract address (for DEXs/Onchain). |
| `decimals` | `int` | Precision of the asset. |

**`UniversalOrder`**
| Field | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `str` | Internal/Client UUID. |
| `venue_order_id`| `str \| None` | The ID returned by the exchange/protocol. |
| `asset` | `UniversalAsset` | The asset pair. |
| `side` | `OrderSide` | `BUY` | `SELL` |
| `order_type` | `OrderType` | `MARKET` \| `LIMIT` \| `STOP` \| `TWAP` \| `ICEBERG` |
| `size` | `Decimal` | Amount of the base asset. |
| `price` | `Decimal \| None` | Limit price or target price. |
| `slippage_pct` | `Decimal` | Max allowable slippage. |
| `gas_limit` | `int \| None` | Maximum gas for on-chain transactions. |
| `status` | `OrderStatus` | `PENDING`, `OPEN`, `FILLED`, `PARTIAL`, `CANCELLED`, `REJECTED` |
| `metadata` | `dict` | Venue-specific parameters (e.g., `priority_fee`, `callback_url`). |

**`UniversalFill`**
| Field | Type | Description |
| :--- |:--- |:--- |
| `fill_id` | `str` | Unique fill identifier. |
| `order_id` | `str` | Reference to the parent order. |
| `venue_fill_id` | `str \| None` | Exchange/Tx hash. |
| `asset` | `UniversalAsset` | The asset filled. |
| `size` | `Decimal` | Amount filled in this execution. |
| `price` | `Decimal` | Execution price. |
| `fee` | `Decimal` | Transaction/Broker fee. |
| `timestamp` | `datetime` | Execution time. |

**`UniversalPosition`**
| Field | Type | Description |
| :--- |:--- |:--- |
| `asset` | `UniversalAsset` | The asset held. |
| `size` | `Decimal` | Current net position. |
| `avg_entry_price`| `Decimal` | Cost basis. |
| `unrealized_pnl` | `Decimal` | PnL based on current market price. |
| `realized_pnl` | `Decimal` | Realized PnL from closed positions. |

---

#### 1.2 The `ExchangeAdapter` Interface (The Abstract Base)

All adapters (Coinbase, Uniswap, etc.) must implement this interface.

```python
class ExchangeAdapter(ABC):
    @property
    @abstractmethod
    def venue_name(self) -> str: ...
    
    @property
    @abstractmethod
    def supported_chains(self) -> set[str]: ...

    # --- Account Management ---
    @abstractmethod
    async def get_accounts(self) -> list[UniversalAccount]: ...
    
    @abstractmethod
    async def get_balances(self, account_id: str) -> list[UniversalBalance]: ...

    # --- Market Data ---
    @abstractmethod
    async def get_ticker(self, asset: UniversalAsset) -> TickerInfo: ...
    
    @abstractmethod
    async def get_orderbook(self, asset: UniversalAsset, depth: int = 10) -> Orderbook: ...

    # --- Execution ---
    @abstractmethod
    async def execute_order(self, order: UniversalOrder) -> UniversalOrder: ...
    
    @abstractmethod
    async def cancel_order(self, venue_order_id: str) -> bool: ...
    
    @abstractmethod
    async def get_order_status(self, venue_order_id: str) -> OrderStatus: ...

    # --- Lifecycle ---
    @abstractmethod
    async def health_check(self) -> HealthStatus: ...
```

---

### Phase 2: Agentic & Smart Execution Enhancements

Moving from manual/static execution to autonomous, intelligent execution.

#### 2.1 The "Agentic Evaluator" Pipeline
A multi-stage verification loop for every strategy before it touches live funds.

1.  **Hypothesis Generation (Research Agent):** Generates trading ideas based on signal/alpha.
2.  **Backtest (Historical Engine):** Validates the idea against high-fidelity historical data (L2/L3 orderbook data).
3.  **Simulation (Onchain/Shadow Engine):** 
    *   **Shadow Execution:** Runs the strategy in real-time using live market data but with "virtual" funds to test latency, slippage, and gas costs.
    *   **Forked Simulation:** For DEXs, executes the transaction on a local/private fork of the blockchain to ensure success.
4.  **Live Execution (Production):** The strategy is deployed with a strict `RiskManager` watchdog.

#### 2.2 Smart Execution (The Intelligence Layer)

Execution logic that adapts to market conditions.

*   **Regime-Aware Execution:**
    *   `High Volatility`: Use `Limit Orders` with aggressive spreads or `IOC` (Immediate-or-Cancel).
    *   `Low Liquidity`: Use `TWAP` (Time-Weighted Average Price) over extended periods to minimize market impact.
    *   `High Gas (Onchain)`: Delay non-urgent rebalancing or use `L2` batches.
*   **MEV-Awareness:**
    *   Integration with private RPCs (e.g., Flashbots) for Ethereum/Base.
    *   Automatic routing to avoid "sandwich" attacks on DEXs.

---

### Phase 3: Production-Ready Hardening

#### 3.1 Persistent State & Data Integrity
Transition from ephemeral in-memory state to a robust, audit-ready database layer.

*   **Relational DB (PostgreSQL):** Stores `Orders`, `Fills`, `Account Balances`, and `Strategy State`.
*   **Time-Series DB (InfluxDB/TimescaleDB):** Stores high-frequency market data and performance metrics.
*   **Event Store (Kafka/Redpanda):** Every state change (Order Created $\to$ Partial Fill $\to$ Filled) is an immutable event for auditability and replayability.

#### 3.2 Observability & Monitoring
*   **Real-time Dashboards (Grafana):** Track slippage vs. expected, latency per venue, and PnL.
*   **Alerting (Prometheus/Alertmanager):** Immediate notification on `RiskLimit` breach or `ExchangeAPIError`.

#### 3.3 Financial Connectivity (Plaid)
*   Unified interface to bridge bank accounts to the `UniversalAccount` model, allowing the system to monitor cash balances and trigger automated fiat-to-crypto funding flows.
