import os
import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[3])
LEGACY_ROOT = os.path.join(ROOT, "graph-alpha-bot")

# Ensure all import variants work:
#  - repo root (for `backtester`, `multi_strategy_paper_trading` if present)
#  - graph-alpha-bot (for `app.*` packages)
#  - graph-alpha-bot/app/strategies (for direct strategy fallbacks)
for path in (
    ROOT,
    LEGACY_ROOT,
    os.path.join(LEGACY_ROOT, "app", "strategies"),
):
    if path not in sys.path:
        sys.path.insert(0, path)

# The strategy project is stored under a hyphenated directory.  A historical
# checkout left an absolute symlink named ``graph_alpha_bot`` that is broken on
# CI hosts. Replace only a broken symlink, then expose a portable relative link
# for imports under ``graph_alpha_bot.app``.
link = os.path.join(ROOT, "graph_alpha_bot")
if os.path.lexists(link) and os.path.islink(link) and not os.path.exists(link):
    try:
        os.unlink(link)
    except OSError:
        pass
if not os.path.lexists(link):
    try:
        os.symlink(LEGACY_ROOT, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        pass
