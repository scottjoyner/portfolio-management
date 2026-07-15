import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
# Ensure ROOT is the first entry so the top-level `src` package resolves to
# repo_root/src rather than the coincidentally-named coinbase/src subpackage
# (coinbase is also added to sys.path below for legacy import support).
if ROOT in sys.path:
    sys.path.remove(ROOT)
sys.path.insert(0, ROOT)

# Some pytest startup paths can cache `trading_system` as a *namespace* package
# (its __file__ ends up None), which then makes every `trading_system.*` import
# fail with "unknown location". Drop any cached trading_system modules so the
# first real import resolves to the regular package on ROOT.
for _m in list(sys.modules):
    if _m == "trading_system" or _m.startswith("trading_system."):
        del sys.modules[_m]

# Force background/worker threads (including ThreadPoolExecutor workers) to be
# daemon so the test process always exits cleanly instead of hanging on
# non-daemon threads spawned by modules under test.
import threading as _threading

_orig_thread_init = _threading.Thread.__init__


def _daemon_thread_init(self, *args, **kwargs):
    if "daemon" not in kwargs:
        kwargs["daemon"] = True
    _orig_thread_init(self, *args, **kwargs)


_threading.Thread.__init__ = _daemon_thread_init
