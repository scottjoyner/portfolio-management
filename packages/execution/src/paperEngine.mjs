export function previewPaperOrder({ strategy, account, signal, quote }) {
  if (!strategy) return { ok: false, errors: ['strategy_required'] };
  if (!account) return { ok: false, errors: ['account_required'] };
  if (!signal?.symbol) return { ok: false, errors: ['symbol_required'] };
  const side = signal.side || 'buy';
  const price = Number(quote?.price || signal.price || 100);
  const quantity = Number(signal.quantity || 1);
  if (!Number.isFinite(price) || price <= 0) return { ok: false, errors: ['invalid_price'] };
  if (!Number.isFinite(quantity) || quantity <= 0) return { ok: false, errors: ['invalid_quantity'] };
  const notional = Number((price * quantity).toFixed(8));
  const fee = Number((notional * Number(signal.feeBps || 5) / 10000).toFixed(8));
  const slippage = Number((notional * Number(signal.slippageBps || 10) / 10000).toFixed(8));
  const requiredCash = side === 'buy' ? Number((notional + fee + slippage).toFixed(8)) : 0;
  if (side === 'buy' && Number(account.cash || 0) < requiredCash) return { ok: false, errors: ['insufficient_paper_cash'], requiredCash };
  return { ok: true, preview: { strategyId: strategy.id, accountId: account.id, symbol: signal.symbol, side, quantity, price, notional, fee, slippage, requiredCash, createdAt: signal.createdAt || new Date().toISOString() } };
}

export function fillPaperOrder(preview, now = new Date().toISOString()) {
  return {
    id: `fill-${now.replace(/[^0-9]/g, '').slice(0, 14)}-${Math.random().toString(16).slice(2, 8)}`,
    preview,
    symbol: preview.symbol,
    side: preview.side,
    quantity: preview.quantity,
    price: preview.price,
    fee: preview.fee,
    slippage: preview.slippage,
    filledAt: now,
    status: 'filled'
  };
}

export function applyPaperFill(state, execution, fill) {
  const account = state.accounts.find(a => a.id === execution.accountId);
  if (!account) return { ok: false, errors: ['account_not_found'] };
  const signedQuantity = fill.side === 'buy' ? fill.quantity : -fill.quantity;
  const cashImpact = fill.side === 'buy'
    ? -(fill.quantity * fill.price + fill.fee + fill.slippage)
    : (fill.quantity * fill.price - fill.fee - fill.slippage);
  account.cash = Number((Number(account.cash || 0) + cashImpact).toFixed(8));
  account.nav = Number((Number(account.nav || 0) + cashImpact).toFixed(8));
  account.updatedAt = fill.filledAt;

  let position = state.positions.find(p => p.strategyId === execution.strategyId && p.symbol === fill.symbol && p.status === 'open');
  if (!position) {
    position = { id: `pos-${state.positions.length + 1}`, strategyId: execution.strategyId, symbol: fill.symbol, quantity: 0, averagePrice: fill.price, markPrice: fill.price, status: 'open', openedAt: fill.filledAt, realizedPnl: 0 };
    state.positions.push(position);
  }
  const oldQty = Number(position.quantity || 0);
  const newQty = Number((oldQty + signedQuantity).toFixed(8));
  if (newQty === 0) {
    position.quantity = 0;
    position.status = 'closed';
    position.closedAt = fill.filledAt;
    position.markPrice = fill.price;
  } else {
    position.averagePrice = oldQty === 0 || Math.sign(oldQty) === Math.sign(signedQuantity)
      ? Number(((Math.abs(oldQty) * Number(position.averagePrice || fill.price) + Math.abs(signedQuantity) * fill.price) / Math.abs(newQty)).toFixed(8))
      : Number(position.averagePrice || fill.price);
    position.quantity = newQty;
    position.markPrice = fill.price;
  }
  execution.fills = [...(execution.fills || []), fill];
  execution.lastHeartbeatAt = fill.filledAt;
  execution.reconciliation = reconcilePaperState(state, execution);
  return { ok: true, account, position, execution };
}

export function reconcilePaperState(state, execution) {
  const account = state.accounts.find(a => a.id === execution.accountId);
  const fills = execution.fills || [];
  const hasNegativeCash = account && Number(account.cash || 0) < -0.00001;
  const openPositions = state.positions.filter(p => p.strategyId === execution.strategyId && p.status === 'open');
  return { status: hasNegativeCash ? 'break' : 'ok', checkedAt: new Date().toISOString(), fillCount: fills.length, openPositionCount: openPositions.length, issues: hasNegativeCash ? ['negative_paper_cash'] : [] };
}

export function executePaperSignal(state, executionId, signal, quote = {}) {
  const execution = state.paperExecutions.find(e => e.id === executionId);
  if (!execution) return { ok: false, errors: ['paper_execution_not_found'] };
  if (execution.status !== 'running') return { ok: false, errors: ['paper_execution_not_running'] };
  const strategy = state.strategies.find(s => s.id === execution.strategyId);
  const account = state.accounts.find(a => a.id === execution.accountId);
  const previewResult = previewPaperOrder({ strategy, account, signal, quote });
  if (!previewResult.ok) return previewResult;
  const fill = fillPaperOrder(previewResult.preview);
  const applied = applyPaperFill(state, execution, fill);
  if (!applied.ok) return applied;
  return { ok: true, preview: previewResult.preview, fill, reconciliation: execution.reconciliation };
}
