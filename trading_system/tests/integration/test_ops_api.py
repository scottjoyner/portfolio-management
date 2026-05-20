from fastapi.testclient import TestClient

from apps.api.main import app


client = TestClient(app)


def test_dashboard_and_portfolio_endpoints() -> None:
    dash = client.get('/ops/dashboard/snapshot')
    assert dash.status_code == 200
    payload = dash.json()
    assert payload['total_nav'] > 0
    assert len(payload['portfolios']) >= 2
    assert 'feed_health' in payload
    assert 'active_issues' in payload
    assert 'quick_actions' in payload

    delta = client.get('/ops/dashboard/delta')
    assert delta.status_code == 200
    assert 'pnl_delta_5m' in delta.json()

    feed_health = client.get('/ops/feeds/health')
    assert feed_health.status_code == 200
    assert len(feed_health.json()) >= 1

    portfolio_id = payload['portfolios'][0]['portfolio_id']
    detail = client.get(f'/ops/portfolios/{portfolio_id}')
    assert detail.status_code == 200
    assert 'sleeves' in detail.json()


def test_treasury_preview_and_execute() -> None:
    preview = client.post(
        '/ops/treasury/preview',
        json={
            'source_portfolio': 'cb-core-mm',
            'destination_portfolio': 'cb-hedge',
            'asset': 'USDC',
            'amount': 10000,
            'rationale': 'fund hedge demand',
        },
    )
    assert preview.status_code == 200
    preview_id = preview.json()['preview_id']

    execute = client.post('/ops/treasury/execute', json={'preview_id': preview_id})
    assert execute.status_code == 200
    assert execute.json()['status'] == 'executed'


def test_treasury_validation_failure() -> None:
    preview = client.post(
        '/ops/treasury/preview',
        json={
            'source_portfolio': 'cb-core-mm',
            'destination_portfolio': 'cb-core-mm',
            'asset': 'USDC',
            'amount': 10000,
            'rationale': 'invalid transfer',
        },
    )
    assert preview.status_code == 400


def test_order_preview_submit_and_cancel() -> None:
    preview = client.post(
        '/ops/orders/preview',
        json={
            'portfolio_id': 'cb-core-mm',
            'sleeve_id': 'maker',
            'strategy_id': 'adaptive_spread_mm',
            'product_id': 'BTC-USD',
            'side': 'buy',
            'order_type': 'limit',
            'size': 0.5,
            'limit_price': 60000,
        },
    )
    assert preview.status_code == 200

    submitted = client.post('/ops/orders/submit', json={'preview_id': preview.json()['preview_id']})
    assert submitted.status_code == 200
    order_id = submitted.json()['order_id']

    open_orders = client.get('/ops/orders/open')
    assert open_orders.status_code == 200
    assert any(order['order_id'] == order_id for order in open_orders.json())

    canceled = client.post(f'/ops/orders/{order_id}/cancel')
    assert canceled.status_code == 200
    assert canceled.json()['status'] == 'canceled'


def test_strategy_actions_theme_and_realtime_outcomes() -> None:
    backtest = client.post(
        '/ops/strategies/backtest/start',
        json={
            'strategy_id': 'adaptive_spread_mm',
            'universe': ['BTC-USD', 'ETH-USD'],
            'lookback_days': 30,
            'capital': 250000,
        },
    )
    assert backtest.status_code == 200
    assert backtest.json()['status'] == 'queued'

    start = client.post('/ops/strategies/adaptive_spread_mm/start')
    assert start.status_code == 200
    assert start.json()['status'] == 'running'

    outcomes = client.get('/ops/strategies/outcomes/realtime')
    assert outcomes.status_code == 200
    assert len(outcomes.json()) >= 1

    theme = client.get('/ops/ui/theme')
    assert theme.status_code == 200
    assert theme.json()['mode'] == 'dark'

    labels = client.get('/ops/ui/labels')
    assert labels.status_code == 200
    assert labels.json()['strategy'] == 'strategies'
