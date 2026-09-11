from pathlib import Path

path = Path('apps/api/src/opportunityFlowsLegacy.mjs')
text = path.read_text()
old = "  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd || 50000);"
new = "  const maxPositionSize = nonNegative(state.config?.maxPositionSizeUsd ?? 50000);"
if old not in text:
    if new in text:
        print('zero-cap patch already present')
        raise SystemExit(0)
    raise SystemExit('zero-cap anchor not found')
path.write_text(text.replace(old, new, 1))
