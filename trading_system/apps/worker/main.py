import asyncio
from core.logging.structured import get_logger


async def run() -> None:
    log = get_logger("worker")
    while True:
        log.info("heartbeat")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run())
