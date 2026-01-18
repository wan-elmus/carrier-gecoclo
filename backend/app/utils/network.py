import aiohttp
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from app.config import settings
from app.utils.logger import setup_logger

logger = setup_logger(__name__)

class NetworkClient:
    """HTTP client for inter-node communication"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=10)  # increased slightly

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def call_node(
        self,
        node_id: str,
        endpoint: str,
        method: str = "POST",
        data: Optional[Dict] = None,
        retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """Call another node with retry logic"""
        if node_id not in settings.NODE_URLS:
            logger.error(f"Unknown node: {node_id}")
            return None

        node_url = settings.NODE_URLS[node_id].rstrip('/')
        url = f"{node_url}{endpoint}"

        headers = {
            "X-API-Key": settings.API_KEY,
            "Content-Type": "application/json",
            "X-Node-ID": settings.NODE_ID
        }

        logger.debug(f"Calling {node_id} → {url} (method={method})")

        for attempt in range(retries):
            try:
                async with self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data
                ) as response:
                    text_resp = await response.text()
                    logger.debug(f"Response from {node_id}: {response.status} - {text_resp[:200]}")

                    if response.status == 200:
                        try:
                            return await response.json()
                        except:
                            logger.warning(f"Non-JSON response from {node_id}: {text_resp}")
                            return None
                    elif response.status in (404, 503):
                        logger.warning(f"Node {node_id} returned {response.status} (attempt {attempt+1}/{retries})")
                        if attempt < retries - 1:
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue
                    else:
                        logger.error(f"Node {node_id} returned {response.status}")
                        return None

            except aiohttp.ClientError as e:
                logger.warning(f"Network error calling {node_id} ({url}): {type(e).__name__}: {str(e)} (attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
            except Exception as e:
                logger.error(f"Unexpected error calling {node_id} ({url}): {type(e).__name__}: {str(e)}")
                return None

        logger.error(f"Failed to call {node_id} ({url}) after {retries} attempts")
        return None

    async def broadcast(
        self,
        node_ids: List[str],
        endpoint: str,
        method: str = "POST",
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        tasks = [self.call_node(nid, endpoint, method, data) for nid in node_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        responses = {}
        for node_id, result in zip(node_ids, results):
            if isinstance(result, Exception):
                responses[node_id] = {"error": str(result), "success": False}
            else:
                responses[node_id] = {"response": result, "success": result is not None}
        return responses

async def simulate_network_delay(delay_ms: int) -> None:
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)

async def get_network_latency(from_node: str, to_node: str) -> Optional[int]:
    try:
        from sqlalchemy import text
        from app.database import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            await session.execute(text("SET search_path TO shared_schema,public"))

            result = await session.execute(
                text("""
                    SELECT latency_ms
                    FROM network_topology
                    WHERE (from_node = :from AND to_node = :to) OR (from_node = :to AND to_node = :from)
                      AND status = 'UP'
                    LIMIT 1
                """),
                {"from": from_node, "to": to_node}
            )

            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Failed to get network latency: {e}")
        return None