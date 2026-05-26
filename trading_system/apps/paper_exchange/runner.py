from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal

import yaml

from apps.paper_exchange.engine import PaperExchangeEngine


async def main_async(config_path: str) -> None:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    products: list[str] = cfg.get("products", ["BTC-USD"])
    starting_cash = Decimal(str(cfg.get("starting_cash", 10_000)))

    engine = PaperExchangeEngine(starting_cash=starting_cash, products=products)
    engine.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))
    for p in products:
        if p not in engine.mid_prices:
            engine.set_market_price(p, Decimal("100"), Decimal("10"))

    print({"status": "paper_exchange_started", "config": config_path, "products": products, "starting_cash": float(starting_cash)})

    try:
        await engine.run()
    except KeyboardInterrupt:
        engine.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    asyncio.run(main_async(args.config))


if __name__ == "__main__":
    main()
