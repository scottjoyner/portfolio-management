import logging
import uuid


def get_logger(name: str, correlation_id: str | None = None) -> logging.LoggerAdapter:
    cid = correlation_id or str(uuid.uuid4())
    base = logging.getLogger(name)
    if not base.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s correlation_id=%(correlation_id)s %(message)s")
        handler.setFormatter(fmt)
        base.addHandler(handler)
        base.setLevel(logging.INFO)
    return logging.LoggerAdapter(base, {"correlation_id": cid})
