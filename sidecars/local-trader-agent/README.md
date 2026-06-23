# Local Trader Agent

A local-first trading research scaffold that recreates the workflow you described:

- local LLM endpoint through `llama.cpp` / turboquant-compatible OpenAI API
- safe-ish local agent loop with tool calls
- deterministic RSI strategy backtester
- yfinance market data research fetcher
- strict no-overlapping-position state machine
- Plotly single-file HTML report with candlesticks, RSI, trade markers, equity curve, summary stats, and trade table

This is research software only. It does **not** place trades and intentionally blocks brokerage/API execution in the local shell tool.

## 1. Start your local llama.cpp server

Example matching the post you shared:

```bash
llama-server \
  -m gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf \
  -c 64000 \
  --cache-type-k q8_0 \
  --cache-type-v turbo3 \
  --port 8080
```

If your build exposes `/v1/chat/completions`, the agent mode can talk to it at `http://127.0.0.1:8080/v1`.

## 2. Install

```bash
git clone <your-repo-url> local-trader-agent
cd local-trader-agent
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -e .
```

Or from this folder:

```bash
bash scripts/bootstrap_ubuntu.sh
```

## 3. Run deterministic backtest directly

This does not require an LLM:

```bash
local-trader-agent backtest \
  --ticker GOOGL \
  --start 2020-01-01 \
  --interval 1d \
  --rsi-period 14 \
  --buy-rsi-cross 30 \
  --take-profit-pct 0.02 \
  --stop-loss-pct 0.01 \
  --initial-cash 10000 \
  --same-bar-policy stop_first \
  --output-html reports/googl_rsi_report.html
```

You can also use:

```bash
local-trader-agent backtest --config config/example.yaml
```

## 4. Run local agent mode

Agent mode gives the local model tools and asks it to complete the workflow.

```bash
local-trader-agent agent \
  --base-url http://127.0.0.1:8080/v1 \
  --model local-model \
  --workspace ./workspace \
  "Backtest GOOGL. Buy when RSI crosses above 30. Sell at +2% profit or -1% stop loss. No overlapping positions. Use yfinance. Generate a polished single-file HTML report."
```

The agent can call these tools:

- `run_shell`: environment-audit only, allowlisted commands such as `python --version`, `pip list`, `where python`, `which python`, `pwd`, `ls`, `dir`
- `run_backtest`: runs the deterministic research pipeline
- `write_file` / `read_file`: workspace-local file operations
- `finish`: final response

It cannot run live trading commands, deletion commands, `curl`, `wget`, `ssh`, `sudo`, Docker, cloud CLIs, or broker libraries through the shell tool.

## 5. Design

```text
User task
  ↓
Local LLM via llama.cpp /v1/chat/completions
  ↓ JSON action loop
Tool Runtime
  ├─ Safe shell environment audit
  ├─ yfinance OHLCV fetch
  ├─ RSI + signal generation
  ├─ No-overlap state-machine backtester
  ├─ Plotly report renderer
  └─ Workspace artifacts
```

The backtester is intentionally deterministic. The LLM does not calculate PnL by itself; it delegates to code. That prevents hallucinated win rates and makes the report reproducible.

## 6. Strategy semantics

Default strategy:

- Ticker: `GOOGL`
- Entry: buy at the close of the bar where RSI crosses from `<= 30` to `> 30`
- Exit: first later bar that hits either:
  - take profit: `entry_price * 1.02`
  - stop loss: `entry_price * 0.99`
- No overlapping positions
- Same-bar ambiguity defaults to `stop_first`, which is conservative when a candle touches both take-profit and stop-loss levels
- Open positions are closed at the final close for reporting

## 7. Files produced

For a report path like `reports/googl_rsi_report.html`, the tool also writes:

- `reports/googl_rsi_report.html`
- `reports/googl_rsi_report.trades.csv`
- `reports/googl_rsi_report.data.csv`

## 8. Notes for production-grade research

Before using results for decision-making, add:

- commission/slippage model
- corporate actions validation
- licensed market data provider
- walk-forward validation
- parameter sweep controls
- train/test split and overfitting checks
- benchmark comparison
- risk metrics such as Sharpe, Sortino, exposure, ulcer index, and time-in-market
- audit log of model prompts/tool calls

## 9. Disclaimer

Not financial advice. This project is for local research/backtesting only. Backtests are not guarantees of future performance.
