import asyncio
import random
import psutil
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, update
from app.models import ConsensusState, NodeMetrics
from app.database import AsyncSessionLocal, get_db
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient, simulate_network_delay
from app.utils.clocks import get_clock_manager
from app.utils.locks import DistributedLock

logger = setup_logger(__name__)
clock_manager = get_clock_manager()  # Assume initialized in startup

class FaultService:
    """Fault injection and recovery service"""

    @staticmethod
    async def inject_crash_fault(node_id: str, duration_ms: int):
        """Inject crash fault: simulate node crash and recovery"""
        try:
            logger.warning(f"Injecting crash into {node_id} for {duration_ms}ms")

            async with AsyncSessionLocal() as db:
                await FaultService._mark_node_status(node_id, "CRASHED", db)

            await asyncio.sleep(duration_ms / 1000.0)

            await FaultService._recover_node(node_id)

            logger.info(f"Node {node_id} recovered from crash")

            clock_manager.create_event({"type": "crash_injected", "node_id": node_id})
        except Exception as e:
            logger.error(f"Crash injection failed: {e}")

    @staticmethod
    async def inject_network_delay(node_id: str, duration_ms: int, severity: str):
        """Inject network delay fault"""
        try:
            delays = {"low": (50, 100), "medium": (200, 500), "high": (1000, 2000)}
            min_d, max_d = delays.get(severity, (100, 300))

            logger.warning(f"Injecting delay on {node_id}: {min_d}-{max_d}ms for {duration_ms}ms")

            start = datetime.utcnow()
            while (datetime.utcnow() - start).total_seconds() * 1000 < duration_ms:
                delay = random.uniform(min_d, max_d)
                await simulate_network_delay(delay)
                await asyncio.sleep(0.01)

            logger.info(f"Delay fault ended on {node_id}")

            clock_manager.create_event({"type": "delay_injected", "node_id": node_id})
        except Exception as e:
            logger.error(f"Delay injection failed: {e}")

    @staticmethod
    async def inject_packet_loss(node_id: str, duration_ms: int, severity: str):
        """Inject packet loss fault"""
        try:
            rates = {"low": 0.1, "medium": 0.3, "high": 0.7}
            rate = rates.get(severity, 0.2)

            logger.warning(f"Injecting loss on {node_id}: {rate*100}% for {duration_ms}ms")

            start = datetime.utcnow()
            while (datetime.utcnow() - start).total_seconds() * 1000 < duration_ms:
                if random.random() < rate:
                    logger.debug(f"Packet dropped for {node_id}")
                    continue  # Simulate drop
                await asyncio.sleep(0.1)  # Simulate interval

            logger.info(f"Loss fault ended on {node_id}")

            clock_manager.create_event({"type": "loss_injected", "node_id": node_id})
        except Exception as e:
            logger.error(f"Loss injection failed: {e}")

    @staticmethod
    async def inject_cpu_spike(node_id: str, duration_ms: int):
        """Inject CPU spike fault"""
        try:
            logger.warning(f"Injecting CPU spike on {node_id} for {duration_ms}ms")

            start = datetime.utcnow()
            while (datetime.utcnow() - start).total_seconds() * 1000 < duration_ms:
                for _ in range(1000000):  # Busy loop to spike CPU
                    pass
                await asyncio.sleep(0.01)

            await FaultService._record_cpu_spike(node_id, duration_ms / 1000.0)

            logger.info(f"CPU spike ended on {node_id}")

            clock_manager.create_event({"type": "cpu_spike_injected", "node_id": node_id})
        except Exception as e:
            logger.error(f"CPU spike injection failed: {e}")

    @staticmethod
    async def inject_byzantine_fault(node_id: str, duration_ms: int):
        """Inject Byzantine fault: node sends incorrect/malicious responses"""
        try:
            logger.warning(f"Injecting Byzantine on {node_id} for {duration_ms}ms")

            start = datetime.utcnow()
            while (datetime.utcnow() - start).total_seconds() * 1000 < duration_ms:
                # Simulate malicious: log as if corrupting data (real: alter responses)
                logger.debug(f"Byzantine: simulating data corruption on {node_id}")
                await asyncio.sleep(1)

            logger.info(f"Byzantine fault ended on {node_id}")

            clock_manager.create_event({"type": "byzantine_injected", "node_id": node_id})
        except Exception as e:
            logger.error(f"Byzantine injection failed: {e}")

    @staticmethod
    async def recover_network_fault(from_node: str, to_node: str, duration_ms: int):
        """Recover network fault after duration"""
        try:
            await asyncio.sleep(duration_ms / 1000.0)

            async with AsyncSessionLocal() as db:
                await db.execute(
                    text("""
                        UPDATE shared_schema.network_topology
                        SET status = 'UP'
                        WHERE (from_node = :from AND to_node = :to) OR (from_node = :to AND to_node = :from)
                    """),
                    {"from": from_node, "to": to_node}
                )
                await db.commit()

            logger.info(f"Network {from_node}-{to_node} restored")

            clock_manager.create_event({"type": "network_recovered", "from": from_node, "to": to_node})
        except Exception as e:
            logger.error(f"Network recovery failed: {e}")

    @staticmethod
    async def _mark_node_status(node_id: str, status: str, db: AsyncSession):
        """Mark node status in shared schema"""
        try:
            await db.execute(
                update(ConsensusState)
                .where(ConsensusState.node_id == node_id)
                .values(status=status)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"Mark status failed: {e}")

    @staticmethod
    async def _recover_node(node_id: str):
        """Recover node from fault"""
        try:
            async with AsyncSessionLocal() as db:
                await FaultService._mark_node_status(node_id, "ACTIVE", db)

            async with NetworkClient() as client:
                await client.call_node(
                    "core_1",
                    "/consensus/recover",
                    method="POST",
                    data={"node_id": node_id, "recovered_at": datetime.utcnow().isoformat()}
                )

            logger.info(f"Node {node_id} recovery notified")
        except Exception as e:
            logger.warning(f"Recovery notification failed: {e}")

    @staticmethod
    async def _record_cpu_spike(node_id: str, duration: float):
        """Record CPU spike in metrics"""
        try:
            async with AsyncSessionLocal() as db:
                spike_metric = NodeMetrics(
                    node_id=node_id,
                    metric_type="cpu_spike",
                    metric_value=duration
                )
                db.add(spike_metric)
                await db.commit()
        except Exception as e:
            logger.warning(f"Record spike failed: {e}")

    @staticmethod
    async def detect_failures():
        """Periodically detect node failures (run in background from startup)"""
        try:
            while True:
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(ConsensusState.node_id).where(
                            datetime.utcnow() - ConsensusState.last_heartbeat > timedelta(milliseconds=settings.HEARTBEAT_TIMEOUT_MS)
                        ).where(ConsensusState.status != "FAILED")
                    )
                    failed = result.scalars().all()

                    for nid in failed:
                        logger.warning(f"Detected failure: {nid}")
                        await FaultService._mark_node_status(nid, "FAILED", db)

                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_MS / 1000)
        except Exception as e:
            logger.error(f"Detection error: {e}")

    @staticmethod
    async def auto_recover_failures():
        """Attempt automatic recovery of failed nodes (run in background from startup)"""
        try:
            while True:
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(ConsensusState.node_id).where(ConsensusState.status == "FAILED")
                    )
                    failed = result.scalars().all()

                    for nid in failed:
                        if random.random() > 0.5:  # 50% recovery success sim
                            await FaultService._recover_node(nid)
                            logger.info(f"Auto-recovered {nid}")

                await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Auto-recovery error: {e}")