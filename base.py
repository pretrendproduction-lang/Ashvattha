"""Shared base for all agents."""
import asyncio
import httpx
import logging
from typing import Optional

logger = logging.getLogger("ashvattha")

class BaseAgent:
    name = "base"

    def __init__(self, db):
        self.db = db
        self.client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self.client = httpx.AsyncClient(
            timeout=45.0,
            headers={"User-Agent": "Ashvattha/1.0 (https://ashvattha.onrender.com)"},
            follow_redirects=True
        )
        logger.info(f"▶ [{self.name}] started")
        while True:
            try:
                await self.tick()
            except Exception as e:
                logger.error(f"[{self.name}] unhandled error: {e}")
                await asyncio.sleep(20)

    async def tick(self):
        raise NotImplementedError

    async def rate_check(self, r) -> bool:
        if r.status_code == 429:
            logger.warning(f"[{self.name}] rate limited — sleeping 90s")
            await asyncio.sleep(90)
            return False
        return True

    async def log(self, person_id, person_name, action, detail):
        try:
            await self.db(
                "INSERT INTO agent_log (person_id,person_name,action,detail) VALUES (:p,:n,:a,:d)",
                {"p": person_id, "n": person_name, "a": action, "d": detail}, "none")
        except:
            pass
