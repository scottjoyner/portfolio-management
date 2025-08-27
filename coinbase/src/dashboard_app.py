#!/usr/bin/env python3
import os, pandas as pd, time
import streamlit as st
from src.analytics import rolling_stats, TRADES_CSV
from src.state import load_state
from src.config import KELLY_CAPS, BANDIT

st.set_page_config(page_title="Coinbase Quant Bot Dashboard", layout="wide")
st.title("📈 Coinbase Quant Bot — Live Dashboard")

st.header("Trades & Performance")
if os.path.exists(TRADES_CSV):
    df = pd.read_csv(TRADES_CSV)
    df["ts_close"] = pd.to_datetime(df["ts_close"], unit="s", errors="coerce")
    df.sort_values("ts_close", inplace=True)
    df["equity_usd"] = df["pnl_usd"].cumsum()
    c1, c2, c3, c4 = st.columns(4)
    total = len(df); win_rate = (df["r_multiple"]>0).mean() if total>0 else 0.0
    c1.metric("Total trades", f"{total}")
    c2.metric("Win rate", f"{100*win_rate:.1f}%")
    c3.metric("Avg R", f"{df['r_multiple'].mean():.2f}" if total>0 else "0.00")
    c4.metric("PnL (USD)", f"{df['pnl_usd'].sum():,.2f}" if total>0 else "0.00")
    st.line_chart(df.set_index("ts_close")[["equity_usd"]])
    st.dataframe(df.tail(50), use_container_width=True)
else:
    st.info("No trades logged yet.")

st.header("Setups — Rolling Stats & Bandit Scores")
stats = rolling_stats()
if stats:
    s = pd.DataFrame(stats).T
    st.dataframe(s, use_container_width=True)
    arms = list(s.index)
    mode = st.selectbox("Bandit mode", ["ucb1","thompson","none"], index=["ucb1","thompson","none"].index(BANDIT.mode if BANDIT.mode in ["ucb1","thompson","none"] else "ucb1"))
    from src.bandit import ucb1_scores, thompson_scores
    import time as _t
    scores = ucb1_scores(int(_t.time()), arms, c=BANDIT.ucb_c) if mode=="ucb1" else (thompson_scores(arms) if mode=="thompson" else {a:0.0 for a in arms})
    st.subheader("Bandit Scores"); st.json(scores)
else:
    st.info("No rolling stats yet.")

st.header("Open Brackets")
state = load_state(); br = [b for b in state.get("brackets", []) if b.get("active")]
st.dataframe(pd.DataFrame(br), use_container_width=True) if br else st.info("No active brackets.")

st.header("Kelly Caps"); st.json({"product_caps": KELLY_CAPS.product_caps, "setup_caps": KELLY_CAPS.setup_caps})
st.caption("Run with: PYTHONPATH=. streamlit run src/dashboard_app.py")
