import os
import tempfile
import json

import portfolio_optimizer as po


class _FakeOptimizer:
    """Minimal stand-in exposing only what _check_inbox_approvals touches."""
    def __init__(self, pending_file):
        self.pending_file = pending_file
        self.require_approval = True
        self.executed = []

    def _execute_approved(self, entry):
        self.executed.append(entry)


def _write_inbox(inbox_dir, token, entry):
    os.makedirs(inbox_dir, exist_ok=True)
    with open(os.path.join(inbox_dir, f"{token}.json"), "w") as f:
        json.dump(entry, f)


def test_inbox_approved_executed_and_deleted():
    tmp = tempfile.mkdtemp()
    pf = os.path.join(tmp, "pending_approvals.json")
    inbox = os.path.join(tmp, "approvals_inbox")
    _write_inbox(inbox, "app1", {"status": "approved", "side": "BUY", "currency": "BTC", "size_usd": 25})
    _write_inbox(inbox, "pend1", {"status": "pending", "side": "BUY", "currency": "ETH", "size_usd": 10})
    _write_inbox(inbox, "deny1", {"status": "denied", "side": "SELL", "currency": "SOL", "size_usd": 5})

    fake = _FakeOptimizer(pf)
    po.PortfolioOptimizer._check_inbox_approvals(fake)

    assert len(fake.executed) == 1
    assert fake.executed[0].get("currency") == "BTC"
    # approved -> deleted; pending left; denied -> deleted
    remaining = set(os.listdir(inbox))
    assert "app1.json" not in remaining
    assert "deny1.json" not in remaining
    assert "pend1.json" in remaining


def test_inbox_missing_dir_is_noop():
    tmp = tempfile.mkdtemp()
    pf = os.path.join(tmp, "pending_approvals.json")  # no approvals_inbox dir
    fake = _FakeOptimizer(pf)
    po.PortfolioOptimizer._check_inbox_approvals(fake)  # must not raise
    assert fake.executed == []
