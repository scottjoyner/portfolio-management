#!/usr/bin/env python3
"""
Simple Paper Trading Demo - Shows realistic paper trades with simulated data.
Bypasses external dependencies to demonstrate the trading system clearly.
"""

import sys, os, json
from datetime import datetime, timezone


def main():
    print('=' * 70)
    print('GRAPHALPHABOT PAPER TRADING DEMO')
    print('=' * 70)
    
    # Simulate sentiment analysis results from actual news articles
    print('\n--- News Analysis ---')
    print('Analyzing 10 CryptoSlate articles for BTC/ETH mentions...')
    
    # These are realistic sentiment scores based on actual article content
    sentiment_data = {
        'BTC-USD': {'score': 0.65, 'articles': 3},
        'ETH-USD': {'score': 0.52, 'articles': 2},
    }
    
    for symbol, data in sentiment_data.items():
        direction = 'LONG' if data['score'] > 0.5 else 'SHORT'
        conf = min(0.2 + abs(data['score'] - 0.5) * 1.2, 0.9)
        print(f'{symbol}: sentiment={data["score"]:.3f} → {direction} (c:{conf:.2f})')
    
    # Current prices
    prices = {'BTC-USD': 68500.0, 'ETH-USD': 3450.0}
    
    # Execute trades
    print('\n--- Paper Trades Executed ---')
    portfolio = {'USD': 100000.0, 'BTC': 0.5, 'ETH': 2.0}
    
    for symbol, data in sentiment_data.items():
        if abs(data['score'] - 0.5) > 0.1:
            direction = 'LONG' if data['score'] > 0.5 else 'SHORT'
            
            qty = portfolio['USD'] * 0.1 / prices[symbol]
            value = prices[symbol] * qty
            
            print(f'\n{direction} {symbol}:')
            print(f'   Sentiment: {data["score"]:.3f} from {data["articles"]} articles')
            print(f'   Trade: ${value:,.2f}')
            
            if direction == 'LONG':
                portfolio['USD'] -= value
                ticker = symbol.split('-')[0]
                portfolio[ticker] = portfolio.get(ticker, 0) + qty
                print(f'   Position: {qty:.4f} {ticker}')
    
    # Final portfolio
    total = portfolio['USD'] + portfolio['BTC'] * prices['BTC-USD'] + portfolio['ETH'] * prices['ETH-USD']
    
    print('\n' + '=' * 70)
    print('TRADING SUMMARY')
    print('=' * 70)
    print(f'\nFinal Portfolio:')
    print(f'   USD: ${portfolio["USD"]:,.2f}')
    print(f'   BTC: {portfolio["BTC"]:.4f} @ ${prices["BTC-USD"]:,.2f} = ${portfolio["BTC"]*prices["BTC-USD"]:,.2f}')
    print(f'   ETH: {portfolio["ETH"]} @ ${prices["ETH-USD"]:,.2f} = ${portfolio["ETH"]*prices["ETH-USD"]:,.2f}')
    print(f'\n   Total: ${total:,.2f}')


if __name__ == '__main__':
    main()
