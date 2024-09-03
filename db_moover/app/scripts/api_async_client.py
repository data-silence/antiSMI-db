import aiohttp
import asyncio
from typing import List, Dict, Any
from loguru import logger


class NewsAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def fetch_news_without_embeddings(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/services/get_missing_embs") as response:
                return await response.json()

    async def make_embs(self, news_list: list[str]) -> list[float]:
        timeout = aiohttp.ClientTimeout(total=36000)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self.base_url}/models/generate_embs", json=news_list) as response:
                return await response.json()

    async def delete_news_batch(self, urls: list[str]) -> dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            payload = {
                "condition": {
                    "url": urls
                }
            }
            async with session.delete(f"{self.base_url}/crud/delete/news", json=payload) as response:
                return await response.json()

    async def insert_news_batch(self, news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=36000)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self.base_url}/crud/insert/news_batch", json={"items": news_items}) as response:
                return await response.json()

    async def update_news(self, url: str, updated_data: Dict[str, Any]) -> Dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{self.base_url}/crud/update/news?url={url}", json=updated_data) as response:
                return await response.json()

    async def update_news_batch(self, updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=36000)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = {"updates": [{"url": item["url"], "embedding": item["embedding"]} for item in updates]}
            async with session.put(f"{self.base_url}/crud/update/news_batch", json=payload) as response:
                return await response.json()
