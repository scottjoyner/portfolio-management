import os
import sys

ROOT = "/home/scott/git/portfolio-management"

# Ensure all import variants work:
#  - repo root (for `backtester`, `multi_strategy_paper_trading` if present)
#  - graph-alpha-bot (for `app.*` packages)
#  - graph-alpha-bot/app/strategies (for `coinbase_universe`, `unified_signal_generator` fallbacks)
for p in (ROOT, os.path.join(ROOT, "graph-alpha-bot"), os.path.join(ROOT, "graph-alpha-bot", "app", "strategies")):
    if p not in sys.path:
        sys.path.insert(0, p)

# The strategy modules live under the ``graph-alpha-bot`` directory (with a
# hyphen), which is not a valid Python package name. Coverage is measured under
# the dotted name ``graph_alpha_bot.app.strategies.<module>`` (with underscores),
# so we expose an importable ``graph_alpha_bot`` package via a symlink to the
# real ``graph-alpha-bot`` tree. This lets tests import the modules under the
# ``graph_alpha_bot.`` prefix so ``coverage --source=graph_alpha_bot...`` collects
# data, while the modules' own ``from app.strategies...`` imports still resolve
# through the ``graph-alpha-bot`` path entry above.
_link = os.path.join(ROOT, "graph_alpha_bot")
if not os.path.exists(_link):
    try:
        os.symlink(os.path.join(ROOT, "graph-alpha-bot"), _link)
    except (OSError, NotImplementedError):
        pass
