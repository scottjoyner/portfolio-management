#!/usr/bin/env python3
"""
Optimizer Dashboard — Streamlit UI for the Portfolio Optimizer.

Shows portfolio state, fee tier progression, strategy backtest results,
opportunity pipeline, and trade log.

Usage:
    cd /home/scott/git/portfolio-management
    PYTHONPATH=. .venv/bin/streamlit run optimizer_dashboard.py
"""

import os
import sys
import json
import time
import math
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional

import streamlit as st

from portfolio_optimizer import (
    PortfolioOptimizer, OpportunityType, COINBASE_FEE_TIERS, TARGET_ALLOCATION,
)
from strategy_engine import ALL_STRATEGIES, CLASS_STRATEGIES, backtest_strategy

st.set_page_config(page_title="Portfolio Optimizer", layout="wide")


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

@st.cache_resource(ttl=120)
def get_optimizer():
    """Single optimizer instance shared across reruns."""
    return PortfolioOptimizer(dry_run=True)


@st.cache_data(ttl=120)
def fetch_state(_opt):
    _opt._tick()
    return _opt.state, _opt.trade_log, _opt._bt_cache


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def main():
    st.title("Portfolio Optimizer")
    opt = get_optimizer()

    # Sidebar controls
    with st.sidebar:
        st.header("Controls")
        if st.button("Refresh Now"):
            st.cache_data.clear()
            st.rerun()

        st.subheader("About")
        st.markdown(
            "Polls Coinbase every 5 minutes, detects opportunities across "
            "4 dimensions (TLH, fee tier, rebalance, strategy signals), "
            "validates each with a backtest, and dry-runs or executes trades."
        )
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

    state, trade_log, bt_cache = fetch_state(opt)
    if not state:
        st.error("No portfolio state — is Coinbase CLI configured?")
        return

    tabs = st.tabs(["Portfolio", "Fee Tier", "Strategies", "Trades"])

    # ── Tab 1: Portfolio ──────────────────────────────────────────
    with tabs[0]:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Value", f"${state.total_value:,.0f}")
        col2.metric("Holdings", f"{len(state.holdings)}")
        col3.metric("USDC", f"${state.usdc_balance:,.0f}")
        col4.metric("Fee Tier", f"${state.fee_tier[0]:,.0f}+")

        # Allocation
        by_class: dict = defaultdict(float)
        for h in state.holdings.values():
            by_class[h["classification"]] += h["value"]
        st.subheader("Allocation")
        alc = [
            {"Class": cls.capitalize(), "Value": round(val, 2),
             "Pct": f"{val / state.total_value * 100:.1f}%" if state.total_value > 0 else "0%",
             "Target": f"{TARGET_ALLOCATION.get(cls, 0) * 100:.0f}%"}
            for cls, val in sorted(by_class.items(), key=lambda x: x[1], reverse=True)
        ]
        st.dataframe(alc, use_container_width=True, hide_index=True)

        # Holdings table
        st.subheader("Holdings")
        rows = []
        for cur, h in sorted(state.holdings.items(), key=lambda x: x[1]["value"], reverse=True):
            pnl = h.get("unrealized_pnl_pct")
            pnl_str = f"{pnl:+.1f}%" if pnl is not None else "—"
            rows.append({
                "Asset": cur, "Balance": f"{h['total']:.6g}",
                "Price": f"${h['price']:.4f}" if h["price"] < 100 else f"${h['price']:,.2f}",
                "Value": f"${h['value']:,.0f}", "Alloc": f"{h['allocation_pct']:.1f}%",
                "Class": h["classification"].capitalize(), "PnL": pnl_str,
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)

    # ── Tab 2: Fee Tier ───────────────────────────────────────────
    with tabs[1]:
        st.subheader("Fee Tier Progression")
        vol = state.fee_volume_30d
        vol_to_next = state.volume_to_next_tier

        col1, col2, col3 = st.columns(3)
        col1.metric("30d Volume", f"${vol:,.0f}")
        col2.metric("Current Maker", f"{state.fee_tier[1]*100:.2f}%")
        col3.metric("Current Taker", f"{state.fee_tier[2]*100:.2f}%")

        # Tier progress bar
        if vol_to_next > 0:
            current_min = state.fee_tier[0]
            for mv, mk, tk in COINBASE_FEE_TIERS:
                if mv > current_min:
                    next_min = mv
                    next_maker = mk
                    break
            else:
                next_min = 0
                next_maker = 0

            if next_min > 0:
                progress = min(vol / next_min, 1.0)
                st.progress(progress, text=f"${vol:,.0f} / ${next_min:,.0f} (${vol_to_next:,.0f} to go)")
                st.metric("Next Tier Maker", f"{next_maker*100:.2f}%")
        else:
            st.success("At top fee tier!")

        # All tiers table
        st.subheader("Fee Tier Schedule")
        tiers = []
        for i, (mv, mk, tk) in enumerate(COINBASE_FEE_TIERS):
            label = "← Current" if mv == state.fee_tier[0] else ("← Next" if vol_to_next > 0 and i > 0 and COINBASE_FEE_TIERS[i-1][0] == state.fee_tier[0] else "")
            tiers.append({"Min Volume": f"${mv:>10,.0f}+", "Maker": f"{mk*100:.2f}%",
                          "Taker": f"{tk*100:.2f}%", "Status": label})
        st.dataframe(tiers, use_container_width=True, hide_index=True)

    # ── Tab 3: Strategies ─────────────────────────────────────────
    with tabs[2]:
        st.subheader("Strategy Backtest Results")

        bt_rows = []
        for key, verdict in bt_cache.items():
            strat, cur = key.split("/", 1)
            bt_rows.append({
                "Strategy": strat, "Asset": cur,
                "Trades": verdict.total_trades,
                "Win Rate": f"{verdict.win_rate*100:.0f}%",
                "Sharpe": verdict.sharpe_ratio,
                "Profit Factor": verdict.profit_factor,
                "Max DD": f"{verdict.max_drawdown_pct:.1f}%",
                "Regime": verdict.regime,
                "Status": "✅ Pass" if verdict.passed else "⛔ Skip",
                "Reason": verdict.reason,
            })

        if bt_rows:
            st.dataframe(bt_rows, use_container_width=True, hide_index=True)
        else:
            st.info("No backtest results yet — run the optimizer first.")

        # Strategy registry
        st.subheader("Strategy Registry")
        reg = []
        for name, cls in sorted(ALL_STRATEGIES.items()):
            classes = [c for c, names in CLASS_STRATEGIES.items() if name in names]
            reg.append({"Name": name, "Class": cls.__name__, "Applies To": ", ".join(classes)})
        st.dataframe(reg, use_container_width=True, hide_index=True)

    # ── Tab 4: Trades ─────────────────────────────────────────────
    with tabs[3]:
        st.subheader("Trade Log")

        by_type = defaultdict(list)
        for t in trade_log:
            by_type[t["type"]].append(t)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Trades", f"{len(trade_log)}")
        total_vol = sum(t["size_usd"] for t in trade_log)
        col2.metric("Total Volume", f"${total_vol:,.0f}")
        total_fees = sum(t["fee"] for t in trade_log)
        col3.metric("Total Fees", f"${total_fees:,.2f}")

        if trade_log:
            # Summary by type
            st.subheader("Volume by Type")
            type_summary = [
                {"Type": t, "Count": len(v), "Volume": f"${sum(x['size_usd'] for x in v):,.0f}"}
                for t, v in sorted(by_type.items())
            ]
            st.dataframe(type_summary, use_container_width=True, hide_index=True)

            # Table
            st.subheader("Recent Trades")
            rows = []
            for t in reversed(trade_log[-50:]):
                rows.append({
                    "Time": t["timestamp"][11:19],
                    "Type": t["type"], "Side": t["side"],
                    "Asset": t["currency"], "Size": f"${t['size_usd']:,.0f}",
                    "Fee": f"${t['fee']:.2f}", "Reason": t["reason"][:60],
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("No trades yet — run the optimizer with --once to generate them.")


if __name__ == "__main__":
    main()
