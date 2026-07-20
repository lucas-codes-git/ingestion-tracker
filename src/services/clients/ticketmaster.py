import asyncio
import httpx
import logging

from src.services.utils import fetch_secrets

logger = logging.getLogger(__name__)

class TicketMasterClient():
    def __init__(self):
        self.key = fetch_secrets().get("ticketMasterKey")
        self.base_url = "https://app.ticketmaster.com/discovery/v2"
        self.client = httpx.AsyncClient()
        
    def build_url(self, endpoint: str) -> str:
        final_url = f"{self.base_url}/{endpoint}"
        return final_url

    async def fetch_data(self, endpoint: str, params: dict) -> dict[str, any]:
        MAX_RETRIES = 3
        url = self.build_url(endpoint)
        
        params = {
            "apikey": self.key,
            **params,
        }
        
        logger.info(f"Starting request to ticketmaster for: {endpoint} data")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                logger.info(f"Successfully pulled {endpoint} data from ticketmaster")
                return data

            except (httpx.RequestError, httpx.HTTPStatusError, httpx.HTTPError, httpx.NetworkError) as e:
                remaining = MAX_RETRIES - attempt - 1
                attempt += 1

                logger.warning(
                    f"Failed to pull {endpoint} data on attempt: {attempt}/{MAX_RETRIES},\n"
                    f"Remaining attempts: {remaining},\n"
                    f"error: {e}"
                )

                await asyncio.sleep(2 ** attempt - 1)

                if attempt == MAX_RETRIES:
                    raise
                    
    async def close(self):
        await self.client.aclose()