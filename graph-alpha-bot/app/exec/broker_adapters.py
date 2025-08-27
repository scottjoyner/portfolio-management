import time, requests
from abc import ABC, abstractmethod
from typing import Dict, Any
from app.settings import (SNAPTRADE_CLIENT_ID, SNAPTRADE_CONSUMER_KEY,
                          SNAPTRADE_SIGNATURE, SNAPTRADE_CONNECTION_ID)

class Order(dict): pass

class Broker(ABC):
    @abstractmethod
    def preview(self, order: 'Order'): ...
    @abstractmethod
    def place(self, order: 'Order'): ...
    @abstractmethod
    def positions(self): ...
    @abstractmethod
    def cancel(self, broker_order_id: str): ...

class FidelityViaSnapTrade(Broker):
    BASE="https://api.snaptrade.com/api/v1"
    def __init__(self):
        self.h = {
            "X-Snaptrade-Client-Id": SNAPTRADE_CLIENT_ID,
            "X-Snaptrade-Consumer-Key": SNAPTRADE_CONSUMER_KEY,
            "X-Snaptrade-Signature": SNAPTRADE_SIGNATURE,
            "Content-Type": "application/json",
        }
        self.connection_id = SNAPTRADE_CONNECTION_ID

    def preview(self, order: 'Order'):
        r = requests.post(f"{self.BASE}/trade/preview",
                          headers=self.h,
                          json={"connectionId": self.connection_id, **order},
                          timeout=30)
        r.raise_for_status()
        return r.json()

    def place(self, order: 'Order'):
        r = requests.post(f"{self.BASE}/trade/place",
                          headers=self.h,
                          json={"connectionId": self.connection_id, **order},
                          timeout=30)
        r.raise_for_status()
        time.sleep(1.1)  # Be gentle; respect limits
        return r.json()

    def positions(self):
        r = requests.get(f"{self.BASE}/holdings",
                         headers=self.h,
                         params={"connectionId": self.connection_id},
                         timeout=30)
        r.raise_for_status()
        return r.json()

    def cancel(self, broker_order_id: str):
        raise NotImplementedError("SnapTrade cancel endpoint varies; implement as needed.")

class MerrillReadOnly(Broker):
    def preview(self, order: 'Order'):
        raise NotImplementedError("Merrill has no public trading API.")
    def place(self, order: 'Order'):
        raise NotImplementedError("Merrill has no public trading API.")
    def positions(self):
        return {}
    def cancel(self, broker_order_id: str):
        raise NotImplementedError("Merrill has no public trading API.")
