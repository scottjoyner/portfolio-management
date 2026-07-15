#!/usr/bin/env python3
"""
Live Paper Trading Demo - Shows what trades are actually being executed.
This script generates signals and executes paper trades in one run.
"""

import sys, os, json, logging, time
from datetime import datetime, timezone
from pathlib import Path

# Setup
logging.basicConfig(level=logging.INFO)
app_dir = Path('app')


def analyze_article_sentiment(title: str, summary: str) -> float:
    """Assign sentiment score based on article content keywords."""
    text = f"{title} {summary}".lower()
    
    # Bullish indicators (positive weight)
    bullish_keywords = [
        'breakout', 'surge', 'rally', 'record', 'high', 'bullish', 'uptrend',
        'institutional adoption', 'approval', 'regulatory clarity', 'demand',
        'futures', 'etf', 'mining', 'halving', 'scarcity', 'bought'
    ]
    
    # Bearish indicators (negative weight)
    bearish_keywords = [
        'crash', 'sell-off', 'drop', 'decline', 'bearish', 'regulatory crackdown',
        'investigation', 'security breach', 'hack', 'fraud', 'risk'
    ]
    
    score = 0.5  # Neutral baseline
    
    for kw in bullish_keywords:
        if kw in text:
            score += 0.15
    
    for kw in bearish_keywords:
        if kw in text:
            score -= 0.15
    
    return max(0.1, min(0.9, score))


def main():
    print('=' * 70)
    print('LIVE PAPER TRADING DEMO')
    print('=' * 70)
    
    from connectors.coinbase_connector import CoinbaseConnector
    from strategies.signal_generator import SignalGenerator
    
    # Step 1: Fetch latest news
    print('\n[1/4] Fetching crypto news...')
    from pipelines.news_ingestion import NewsIngestionPipeline
    pipeline = NewsIngestionPipeline()
    result = pipeline.run_once()
    print(f'✓ Fetched {result["articles_collected"]} articles')
    
    # Step 2: Load and analyze articles for sentiment
    print('\n[2/4] Analyzing article sentiment...')
    kg_file = app_dir / 'data/knowledge_graph.json'
    if not kg_file.exists():
        print('No knowledge graph found - no signals will be generated')
        return
    
    with open(kg_file) as f:
        kg = json.load(f)
    
    articles = kg.get('articles', [])
    print(f'Loaded {len(articles)} articles for analysis')
    
    # Analyze sentiment for BTC and ETH
    def get_sentiment_for_ticker(ticker, articles):
        matching = [a for a in articles if ticker.lower() in str(a.get('title', '')).lower()]
        
        if len(matching) == 0:
            return None
        
        scores = []
        for article in matching:
            title = article.get('title', '')
            summary = article.get('summary', '')
            score = analyze_article_sentiment(title, summary)
            scores.append(score)
        
        avg_score = sum(scores) / len(scores) if scores else 0.5
        
        return {
            'sentiment_score': round(avg_score, 3),
            'news_count': len(matching),
            'articles': matching[:3]
        }
    
    # Generate signals based on sentiment analysis
    print('\n--- Sentiment Analysis Results ---')
    btc_sentiment = get_sentiment_for_ticker('bitcoin', articles)
    eth_sentiment = get_sentiment_for_ticker('ethereum', articles)
    
    def format_signal(ticker, sentiment_data):
        if not sentiment_data:
            return None
        
        direction = 'LONG' if sentiment_data['sentiment_score'] > 0.5 else 'SHORT'
        confidence = min(0.2 + abs(sentiment_data['sentiment_score'] - 0.5) * 1.2, 0.9)
        
        return {
            'symbol': f'{ticker}-USD',
            'direction': direction,
            'confidence': round(confidence, 3),
            'sentiment_score': sentiment_data['sentiment_score'],
            'news_count': sentiment_data['news_count'],
            'signal_reason': f"{'Positive' if direction == 'LONG' else 'Negative'} news sentiment ({sentiment_data['sentiment_score']:.2f}) from {sentiment_data['news_count']} articles",
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    btc_signal = format_signal('BTC', btc_sentiment)
    eth_signal = format_signal('ETH', eth_sentiment)
    
    if btc_sentiment:
        print(f'BTC-USD: sentiment={btc_sentiment["sentiment_score"]:.3f}, count={btc_sentiment["news_count"]}')
    if eth_sentiment:
        print(f'ETH-USD: sentiment={eth_sentiment["sentiment_score"]:.3f}, count={eth_sentiment["news_count"]}')
    
    # Step 3: Execute paper trades
    print('\n[3/4] Executing paper trades...')
    cc = CoinbaseConnector()
    
    current_prices = {
        'BTC-USD': 68500.0,
        'ETH-USD': 3450.0,
        'SOL-USD': 175.0,
    }
    
    orders_executed = []
    
    for ticker in ['BTC', 'ETH']:
        signal_data = btc_signal if ticker == 'BTC' else eth_signal
        
        if not signal_data:
            continue
        
        # Execute trade only if sentiment exceeds threshold
        if abs(signal_data['sentiment_score'] - 0.5) > 0.1:
            symbol = f'{ticker}-USD'
            price = current_prices[symbol]
            
            position_size_pct = 0.10  # 10% of portfolio
            order_value = cc.portfolio['USD'] * position_size_pct
            quantity = order_value / price
            
            direction = signal_data['direction']
            side = 'BUY' if direction == 'LONG' else 'SELL'
            
            print(f'\n>>> {side} {ticker}:')
            print(f'   Sentiment: {signal_data["sentiment_score"]:.3f}')
            print(f'   Confidence: {signal_data["confidence"]:.2f}')
            print(f'   Reason: {signal_data["signal_reason"]}')
            
            # Execute the order
            signal_dict = signal_data.copy()
            del signal_dict['timestamp']  # Remove timestamp for compatibility
            
            if side == 'BUY':
                cc.portfolio['USD'] -= order_value
                cc.portfolio[ticker] = cc.portfolio.get(ticker, 0) + quantity
                
                orders_executed.append({
                    "status": "filled",
                    "order_id": f"paper_trade_{ticker}_{int(time.time())}",
                    "symbol": symbol,
                    "side": side,
                    "quantity": round(quantity, 6),
                    "price": price,
                    "value_usd": order_value,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "reason": signal_data['signal_reason'],
                    'sentiment_score': signal_data['sentiment_score']
                })
            else:
                # SHORT position (sell)
                sell_qty = min(cc.portfolio.get(ticker, 0), quantity)
                
                cc.portfolio['USD'] += price * sell_qty
                cc.portfolio[ticker] -= sell_qty
                
                orders_executed.append({
                    "status": "filled",
                    "order_id": f"paper_trade_{ticker}_{int(time.time())}",
                    "symbol": symbol,
                    "side": side,
                    "quantity": round(sell_qty, 6),
                    "price": price,
                    "value_usd": price * sell_qty,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "reason": signal_data['signal_reason'],
                    'sentiment_score': signal_data['sentiment_score']
                })
    
    # Step 4: Display results
    print('\n[4/4] TRADING SUMMARY')
    print('-' * 50)
    
    if orders_executed:
        for order in orders_executed:
            print(f"✓ Order Executed:")
            print(f"   {order['side']} {order['symbol']}")
            print(f"   Qty: {order['quantity']:.4f}")
            print(f"   Price: ${order['price']:,.2f}")
            print(f"   Value: ${order['value_usd']:,.2f}")
            
            if order.get('reason'):
                print(f"   Reason: {order['reason']}")
    else:
        print("No trades executed - sentiment scores were neutral (near 0.5)")
    
    print('\nFinal Portfolio:')
    total_value = cc.portfolio.get('USD', 0) + cc.portfolio.get('BTC', 0) * current_prices['BTC-USD'] + cc.portfolio.get('ETH', 0) * current_prices['ETH-USD']
    print(f"   USD: ${cc.portfolio.get('USD', 0):,.2f}")
    print(f"   BTC: {cc.portfolio.get('BTC', 0)} @ ${current_prices['BTC-USD']:,.2f} = ${cc.portfolio.get('BTC', 0) * current_prices['BTC-USD']:,.2f}")
    print(f"   ETH: {cc.portfolio.get('ETH', 0)} @ ${current_prices['ETH-USD']:,.2f} = ${cc.portfolio.get('ETH', 0) * current_prices['ETH-USD']:,.2f}")
    print(f"\n   Total: ${total_value:,.2f}")


if __name__ == '__main__':
    import time
    main()
