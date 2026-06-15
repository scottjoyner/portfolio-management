
import sys
import os
sys.path.append(os.getcwd())

from portfolio_manager import Portfolio, Position

p = Portfolio(100000)
p.buy('AAPL', 0.0012, 8475273.0)
print(f'AAPL quantity: {p.positions["AAPL"].quantity}')
print(f'AAPL avg cost: {p.positions["AAPL"].average_cost_basis}')
print(f'Cash after buy: {p.cash}')
print(f'Portfolio summary: {p.get_summary()}')
