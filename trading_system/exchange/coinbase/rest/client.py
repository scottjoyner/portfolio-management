import httpx


class CoinbaseRestClient:
    def __init__(self, api_key: str, api_secret: str, passphrase: str, base_url: str = "https://api.coinbase.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=10.0)

    async def list_products(self) -> dict:
        # stub: replace signing + authenticated requests for production
        return {"products": []}

    async def close(self) -> None:
        await self.client.aclose()
