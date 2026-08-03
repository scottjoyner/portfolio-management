import logging
import uuid
from typing import Any, MutableMapping


_LOGGING_KWARGS = {"exc_info", "stack_info", "stacklevel", "extra"}


class StructuredLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that accepts structured fields as keyword arguments.

    The standard ``logging.Logger`` rejects arbitrary keywords. Application
    code throughout the trading system uses calls such as
    ``log.info("order_placed", order_id=...)``; this adapter moves those fields
    into ``extra`` while preserving standard logging options.
    """

    def process(
        self,
        msg: object,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[object, MutableMapping[str, Any]]:
        structured = {
            key: kwargs.pop(key)
            for key in list(kwargs)
            if key not in _LOGGING_KWARGS
        }
        extra = dict(self.extra or {})
        extra.update(kwargs.pop("extra", {}) or {})
        extra.update(structured)
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(name: str, correlation_id: str | None = None) -> StructuredLoggerAdapter:
    cid = correlation_id or str(uuid.uuid4())
    base = logging.getLogger(name)
    if not base.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s "
            "correlation_id=%(correlation_id)s %(message)s"
        )
        handler.setFormatter(fmt)
        base.addHandler(handler)
        base.setLevel(logging.INFO)
    return StructuredLoggerAdapter(base, {"correlation_id": cid})
