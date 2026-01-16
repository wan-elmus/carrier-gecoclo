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
        self.timeout = aiohttp.ClientTimeout(total=5)
    
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
        
        node_url = settings.NODE_URLS[node_id]
        url = f"{node_url}{endpoint}"
        
        headers = {
            "X-API-Key": settings.API_KEY,
            "Content-Type": "application/json",
            "X-Node-ID": settings.NODE_ID
        }
        
        for attempt in range(retries):
            try:
                async with self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data
                ) as response:
                    
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 503:  # Service unavailable
                        logger.warning(f"Node {node_id} unavailable (attempt {attempt+1}/{retries})")
                        if attempt < retries - 1:
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue
                    else:
                        logger.error(f"Node {node_id} returned {response.status}")
                        return None
                        
            except aiohttp.ClientError as e:
                logger.warning(f"Network error calling {node_id}: {e} (attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
            except Exception as e:
                logger.error(f"Unexpected error calling {node_id}: {e}")
                return None
        
        logger.error(f"Failed to call {node_id} after {retries} attempts")
        return None
    
    async def broadcast(
        self,
        node_ids: List[str],
        endpoint: str,
        method: str = "POST",
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Broadcast request to multiple nodes"""
        tasks = []
        for node_id in node_ids:
            tasks.append(self.call_node(node_id, endpoint, method, data))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        responses = {}
        for node_id, result in zip(node_ids, results):
            if isinstance(result, Exception):
                responses[node_id] = {"error": str(result), "success": False}
            else:
                responses[node_id] = {"response": result, "success": result is not None}
        
        return responses

async def simulate_network_delay(delay_ms: int) -> None:
    """Simulate network delay for testing"""
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)

async def get_network_latency(from_node: str, to_node: str) -> Optional[int]:
    """Get latency between nodes from topology table"""
    try:
        from sqlalchemy import text
        from app.database import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            await session.execute(text("SET search_path TO shared_schema,public"))
            
            result = await session.execute(
                text(
                    """
                    SELECT latency_ms 
                    FROM network_topology 
                    WHERE from_node = :from AND to_node = :to AND status = 'UP'
                    """
                ),
                {"from": from_node, "to": to_node}
            )
            
            row = result.fetchone()
            return row[0] if row else None
            
    except Exception as e:
        logger.error(f"Failed to get network latency: {e}")
        return None