.PHONY: install install-dev test lint typecheck backtest agent

install:
	python -m pip install -e .

install-dev:
	python -m pip install -e '.[dev]'

test:
	pytest -q

lint:
	ruff check .

typecheck:
	mypy local_trader_agent

backtest:
	local-trader-agent backtest --config config/example.yaml

agent:
	local-trader-agent agent --config config/example.yaml "Backtest GOOGL using RSI cross above 30, +2% take profit, -1% stop loss, no overlapping positions, then produce a report."
