#!/usr/bin/env python3
import os, json, time, math, pandas as pd, numpy as np
import streamlit as st

from src.analytics import rolling_stats
from src.bandit import ucb1_scores, thompson_scores
from src.config import KELLY_CAPS, BANDIT
from src.state import load_state
from src.analytics import TRADES_CSV

st.set_page_config(page_title="Coinbase Quant Bot Dashboard", layout="wide")

st.title("📈 Coinbase Quant Bot — Live Dashboard")

# --- Trades & Stats ---
st.header("Trades & Performance")

if os.path.exists(TRADES_CSV):
    df = pd.read_csv(TRADES_CSV)
    df["ts_open"] = pd.to_datetime(df["ts_open"], unit="s", errors="coerce")
    df["ts_close"] = pd.to_datetime(df["ts_close"], unit="s", errors="coerce")
    df.sort_values("ts_close", inplace=True)
    df["equity_usd"] = df["pnl_usd"].cumsum()
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    win_rate = (df["r_multiple"] > 0).mean() if total>0 else 0.0
    avg_R = df["r_multiple"].mean() if total>0 else 0.0
    pnl = df["pnl_usd"].sum() if total>0 else 0.0
    col1.metric("Total trades", f"{total}")
    col2.metric("Win rate", f"{100*win_rate:.1f}%")
    col3.metric("Avg R", f"{avg_R:.2f}")
    col4.metric("PnL (USD)", f"{pnl:,.2f}")
    st.line_chart(df.set_index("ts_close")[["equity_usd"]])
    st.dataframe(df.tail(50), use_container_width=True)
else:
    st.info("No trades logged yet. When brackets close, trades are appended to `state/trades.csv`.")

# --- Setup-level stats & Bandit scores ---
st.header("Setups — Rolling Stats & Bandit Scores")
stats = rolling_stats()
if stats:
    s = pd.DataFrame(stats).T
    st.dataframe(s, use_container_width=True)
    arms = list(s.index)
    mode = st.selectbox("Bandit mode", ["ucb1","thompson","none"], index=["ucb1","thompson","none"].index(BANDIT.mode if BANDIT.mode in ["ucb1","thompson","none"] else "ucb1"))
    if mode == "ucb1":
        scores = ucb1_scores(int(time.time()), arms, c=BANDIT.ucb_c)
    elif mode == "thompson":
        scores = thompson_scores(arms)
    else:
        scores = {a: 0.0 for a in arms}
    st.subheader("Bandit Scores")
    st.json(scores)
else:
    st.info("No rolling stats yet. Take a few trades to populate.")

# --- Open Brackets ---
st.header("Open Brackets")
st.write("Active synthetic OCO positions managed by the bot.")
state = load_state()
br = [b for b in state.get("brackets", []) if b.get("active")]
if br:
    st.dataframe(pd.DataFrame(br), use_container_width=True)
else:
    st.info("No active brackets.")

# --- Kelly caps ---
st.header("Kelly Caps")
st.write("Per-product and per-setup Kelly caps currently in effect.")
st.json({"product_caps": KELLY_CAPS.product_caps, "setup_caps": KELLY_CAPS.setup_caps})

st.caption("Tip: run with `PYTHONPATH=. streamlit run src/dashboard_app.py` from the repo root.")
