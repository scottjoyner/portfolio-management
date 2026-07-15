"""Tests for trading_system.core.logging.structured."""

import logging
import uuid

from trading_system.core.logging import structured


def test_get_logger_default_correlation_id():
    logger = structured.get_logger("test_struct_default")
    assert isinstance(logger, logging.LoggerAdapter)
    cid = logger.extra["correlation_id"]
    # should be a valid uuid
    uuid.UUID(cid)


def test_get_logger_provided_correlation_id():
    logger = structured.get_logger("test_struct_provided", correlation_id="abc-123")
    assert logger.extra["correlation_id"] == "abc-123"


def test_get_logger_adds_handler_once():
    name = "test_struct_handler"
    # remove any pre-existing handler for a clean branch path
    prev = logging.getLogger(name)
    for h in list(prev.handlers):
        prev.removeHandler(h)
    l1 = structured.get_logger(name)
    assert l1.logger.handlers  # branch True: handlers added
    l2 = structured.get_logger(name)
    assert len(l2.logger.handlers) == 1  # branch False: not re-added


def test_get_logger_emits_with_correlation_id(caplog):
    import io

    name = "test_struct_emit"
    lg = logging.getLogger(name)
    for h in list(lg.handlers):
        lg.removeHandler(h)
    stream = io.StringIO()
    captured = structured.get_logger(name)
    # attach a stream handler to verify correlation_id propagation
    sh = logging.StreamHandler(stream)
    sh.setFormatter(logging.Formatter("%(message)s %(correlation_id)s"))
    captured.logger.addHandler(sh)
    captured.logger.setLevel(logging.INFO)
    captured.info("hello")
    out = stream.getvalue()
    assert "hello" in out
    assert captured.extra["correlation_id"] in out
