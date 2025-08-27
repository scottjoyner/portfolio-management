#!/usr/bin/env python3
import argparse
from app.strategies.ma_crossover import MovingAverageCrossover
from app.strategies.news_centrality_momentum import NewsCentralityMomentum
from app.strategies.supply_chain_shock_diffusion import SupplyChainShockDiffusion
from app.strategies.insider_cluster_drift import InsiderClusterDrift

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, required=True)
    args = ap.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    strategies = [
        MovingAverageCrossover(),
        NewsCentralityMomentum(),
        SupplyChainShockDiffusion(),
        InsiderClusterDrift(),
    ]

    total = 0
    for strat in strategies:
        n = strat.generate(symbols)
        print(f"{strat.name}: {n} signals")
        total += n
        strat.close()
    print(f"Total signals: {total}")

if __name__ == "__main__":
    main()
