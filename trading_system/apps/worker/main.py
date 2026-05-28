from __future__ import annotations

import asyncio
import signal as signal_module
from decimal import Decimal

from apps.paper_exchange.engine import PaperExchangeEngine
from apps.worker.engine import WorkerEngine
from core.config.settings import Settings
from core.events.ws_hub import hub
from core.logging.structured import get_logger
from storage.postgres.session import init_db

POLL_INTERVAL = 1.0


async def run() -> None:
    log = get_logger("worker")
    settings = Settings.from_env()

    if settings.database_url:
        init_db(settings.database_url)

    engine = WorkerEngine(settings=settings)
    paper = PaperExchangeEngine(starting_cash=Decimal("100000"), products=["BTC-USD", "ETH-USD"])
    paper.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))
    paper.set_market_price("ETH-USD", Decimal("3000"), Decimal("8"))

    log.info("worker_initialized", strategy_count=len(engine.strategies))

    stop_event = asyncio.Event()

    def _shutdown() -> None:
        log.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal_module.SIGINT, _shutdown)
    loop.add_signal_handler(signal_module.SIGTERM, _shutdown)

    while not stop_event.is_set():
        for product_id in paper.products:
            mid = paper.mid_prices.get(product_id, Decimal("100"))
            market_state = {
                "product_id": product_id,
                "price": float(mid),
                "mid_price": float(mid),
                "timestamp": asyncio.get_event_loop().time(),
            }
            signals = engine.evaluate_market_state(product_id, market_state)
            for sig in signals:
                hub.publish_sync("signals", {
                    "event": "signal_generated",
                    "product_id": product_id,
                    "strategy_id": sig["strategy_id"],
                    "signal": sig.get("signal"),
                    "price": float(mid),
                    "timestamp": market_state.get("timestamp"),
                })
                allowed, reason = engine.evaluate_order(sig, market_state)
                if allowed:
                    order = paper.place_order(
                        strategy_id=sig["strategy_id"],
                        portfolio_id="worker",
                        product_id=product_id,
                        side="buy",
                        order_type="limit",
                        size=Decimal("0.01"),
                        limit_price=Decimal(str(market_state["price"])),
                    )
                    hub.publish_sync("orders", {
                        "event": "signal_to_fill",
                        "order_id": order.order_id,
                        "strategy_id": sig["strategy_id"],
                        "product_id": product_id,
                        "status": "placed",
                    })
                    log.info("order_placed", order_id=order.order_id, strategy_id=sig["strategy_id"])
                else:
                    hub.publish_sync("orders", {
                        "event": "order_rejected",
                        "strategy_id": sig["strategy_id"],
                        "product_id": product_id,
                        "reason": reason,
                    })
                    log.info("order_rejected", strategy_id=sig["strategy_id"], reason=reason)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass

    paper.stop()
    log.info("worker_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(run())
