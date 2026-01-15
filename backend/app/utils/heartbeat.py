import asyncio
import aiohttp
import psutil
from datetime import datetime
from typing import Dict, Optional
from sqlalchemy import text
from app.config import settings
from app.database import AsyncSessionLocal
from utils.logger import setup_logger, TelecomLogger

logger = setup_logger(__name__)
telecom_logger = TelecomLogger()

class NodeHealth:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.metrics = {"cpu_percent": 0.0, "memory_percent": 0.0}

    async def collect_metrics(self) -> Dict[str, float]:
        try:
            self.metrics["cpu_percent"] = psutil.cpu_percent(interval=0.1)
            self.metrics["memory_percent"] = psutil.virtual_memory().percent
            telecom_logger.log_metric(logger, "cpu_usage", self.metrics["cpu_percent"], {"node_id": self.node_id})
            telecom_logger.log_metric(logger, "memory_usage", self.metrics["memory_percent"], {"node_id": self.node_id})
            return self.metrics
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            return self.metrics

class HeartbeatManager:
    def __init__(self):
        self.node_health = NodeHealth(settings.NODE_ID)
        self.failed_nodes: list = []

    async def send_heartbeat(self) -> None:
        try:
            async with AsyncSessionLocal() as session:
                metrics = await self.node_health.collect_metrics()
                await session.execute(
                    text("""
                        INSERT INTO shared_schema.consensus_state 
                        (node_id, last_heartbeat, status, cpu_percent, memory_percent)
                        VALUES (:node_id, NOW(), 'ACTIVE', :cpu, :memory)
                        ON CONFLICT (node_id) 
                        DO UPDATE SET 
                            last_heartbeat = NOW(), 
                            status = 'ACTIVE',
                            cpu_percent = :cpu,
                            memory_percent = :memory
                    """),
                    {"node_id": settings.NODE_ID, "cpu": metrics["cpu_percent"], "memory": metrics["memory_percent"]}
                )
                await self._check_failed_nodes(session)
                await session.commit()
                logger.debug(f"Heartbeat sent for {settings.NODE_ID}")
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")

    async def _check_failed_nodes(self, session) -> None:
        timeout_sec = settings.HEARTBEAT_TIMEOUT_MS / 1000
        result = await session.execute(
            text("""
                SELECT node_id, last_heartbeat 
                FROM shared_schema.consensus_state 
                WHERE last_heartbeat < NOW() - :timeout * INTERVAL '1 second'
                AND status != 'FAILED'
            """),
            {"timeout": timeout_sec}
        )
        for row in result.fetchall():
            node_id, last = row
            if node_id not in self.failed_nodes:
                logger.warning(f"Node {node_id} failed (last: {last})")
                self.failed_nodes.append(node_id)
                await session.execute(
                    text("UPDATE shared_schema.consensus_state SET status = 'FAILED' WHERE node_id = :nid"),
                    {"nid": node_id}
                )
                await session.execute(
                    text("""
                        INSERT INTO monitoring_schema.fault_logs 
                        (node_id, fault_type, injected_at, detected_at)
                        VALUES (:nid, 'crash', NOW(), NOW())
                    """),
                    {"nid": node_id}
                )

    async def check_node_health(self, node_url: str) -> Optional[Dict]:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as s:
                async with s.get(
                    f"{node_url}/health",
                    headers={"X-API-Key": settings.API_KEY}
                ) as resp:
                    if resp.status == 200:
                        return {"status": "HEALTHY", "data": await resp.json()}
                    return {"status": "UNHEALTHY", "error": f"HTTP {resp.status}"}
        except asyncio.TimeoutError:
            return {"status": "TIMEOUT"}
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}

_heartbeat_manager: Optional[HeartbeatManager] = None

async def start_heartbeat():
    global _heartbeat_manager
    if _heartbeat_manager is None:
        _heartbeat_manager = HeartbeatManager()
    logger.info(f"Heartbeat started for {settings.NODE_ID}")
    interval = settings.HEARTBEAT_INTERVAL_MS / 1000.0
    while True:
        await _heartbeat_manager.send_heartbeat()
        await asyncio.sleep(interval)

def get_heartbeat_manager() -> HeartbeatManager:
    if _heartbeat_manager is None:
        raise RuntimeError("Heartbeat not initialized")
    return _heartbeat_manager