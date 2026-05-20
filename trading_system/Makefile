install:
	pip install -e .[dev]

lint:
	ruff check .

typecheck:
	mypy .

test:
	pytest -q

ci: lint typecheck test

api:
	uvicorn apps.api.main:app --reload --host 0.0.0.0 --port 8000

worker:
	python -m apps.worker.main

backtest-demo:
	python -m apps.backtester.runner --config configs/backtest_demo.yaml

paper-demo:
	python -m apps.paper_exchange.runner --config configs/paper_demo.yaml
