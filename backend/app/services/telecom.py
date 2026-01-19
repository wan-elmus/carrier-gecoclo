import asyncio
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, text, update
from app.models import Subscriber, DbSession, TransactionLog
from app.schemas import SubscriberCreate, SessionCreate
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient, get_network_latency
from app.utils.clocks import get_clock_manager
from app.utils.locks import DistributedLock
from app.services.replication import ReplicationService
from app.database import AsyncSessionLocal

logger = setup_logger(__name__)
clock_manager = get_clock_manager()

class TelecomService:
    @staticmethod
    async def create_subscriber(
        subscriber_data: SubscriberCreate,
        db: AsyncSession
    ) -> Subscriber:
        try:
            result = await db.execute(
                select(Subscriber).where(Subscriber.subscriber_id == subscriber_data.subscriber_id)
            )
            existing = result.scalar_one_or_none()
            if existing:
                raise ValueError(f"Subscriber {subscriber_data.subscriber_id} exists")

            subscriber = Subscriber(**subscriber_data.model_dump())
            db.add(subscriber)
            await db.commit()
            await db.refresh(subscriber)

            logger.info(f"Created subscriber {subscriber.subscriber_id} on {settings.NODE_ID}")

            await TelecomService._log_transaction(
                "CREATE_SUBSCRIBER", "subscribers", subscriber.subscriber_id,
                None, subscriber_data.model_dump(), db
            )

            clock_manager.create_event({"type": "subscriber_created", "id": subscriber.subscriber_id})

            asyncio.create_task(TelecomService.replicate_to_core(subscriber, "CREATE"))

            return subscriber
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to create subscriber: {e}")
            raise

    @staticmethod
    async def setup_session(
        session_data: SessionCreate,
        db: AsyncSession
    ) -> DbSession:
        try:
            result = await db.execute(
                select(Subscriber).where(
                    Subscriber.subscriber_id == session_data.subscriber_id,
                    Subscriber.status == "ACTIVE"
                )
            )
            subscriber = result.scalar_one_or_none()
            if not subscriber:
                raise ValueError(f"Active subscriber {session_data.subscriber_id} not found")

            session_id = f"sess_{uuid.uuid4().hex[:10]}"

            threshold = TelecomService._get_latency_threshold(session_data.session_type, session_data.qos_profile or {})

            session = DbSession(
                session_id=session_id,
                subscriber_id=session_data.subscriber_id,
                session_type=session_data.session_type,
                source_node=settings.NODE_ID,
                current_node=settings.NODE_ID,
                destination_node=session_data.destination_node,
                qos_profile=session_data.qos_profile or {},
                latency_threshold_ms=threshold,
                start_time=datetime.utcnow(),
                end_time=None,
                data_volume=0,
                migrated_from=None,
                migration_count=0,
                status="ACTIVE"
            )

            db.add(session)
            await db.commit()
            await db.refresh(session)

            logger.info(f"Setup {session.session_type} session {session_id} for {session.subscriber_id} on {settings.NODE_ID}")

            await TelecomService._log_transaction(
                "CREATE_SESSION", "sessions", session_id,
                None, session_data.model_dump(), db
            )

            clock_manager.create_event({"type": "session_setup", "id": session_id})

            return session

        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to setup session for {session_data.subscriber_id}: {e}", exc_info=True)
            raise

    @staticmethod
    async def monitor_session_qos(session_id: str, threshold_ms: int):
        logger.info(f"QoS monitor started for {session_id}")

        while True:
            try:
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(DbSession).where(DbSession.session_id == session_id)
                    )
                    session = result.scalar_one_or_none()
                    if not session or session.status != "ACTIVE":
                        break

                    latency = await TelecomService.calculate_current_latency(session_id, db)

                    if latency > threshold_ms:
                        await TelecomService._record_qos_violation(
                            session_id, latency, threshold_ms, db
                        )

                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"QoS monitoring error for {session_id}: {e}")
                await asyncio.sleep(5)

        logger.info(f"QoS monitoring stopped for {session_id}")

    @staticmethod
    def _get_latency_threshold(session_type: str, qos_profile: Dict) -> int:
        defaults = {"VOICE": 150, "DATA": 300, "SMS": 1000, "SIGNALING": 100}
        threshold = defaults.get(session_type, 300)
        return qos_profile.get("max_latency_ms", threshold)

    @staticmethod
    async def _record_qos_violation(session_id: str, actual: float, threshold: int, db: AsyncSession):
        try:
            await TelecomService._log_transaction(
                "QOS_VIOLATION", "sessions", session_id,
                {"latency": threshold}, {"latency": actual}, db
            )
        except Exception as e:
            logger.error(f"Failed to record QoS violation: {e}")

    @staticmethod
    async def handover_session(
        session_id: str,
        target_node: str,
        db: AsyncSession
    ) -> bool:
        try:
            async with DistributedLock("session", session_id) as lock:
                if not await lock.acquire(timeout_ms=5000):
                    raise ValueError(f"Session {session_id} locked")

                result = await db.execute(
                    select(DbSession).where(DbSession.session_id == session_id).where(DbSession.status == "ACTIVE")
                )
                session = result.scalar_one_or_none()
                if not session:
                    raise ValueError(f"Active session {session_id} not found")

                old_node = session.current_node

                await db.execute(
                    update(DbSession)
                    .where(DbSession.session_id == session_id)
                    .values(
                        current_node=target_node,
                        migrated_from=old_node,
                        migration_count=DbSession.migration_count + 1
                    )
                )
                await db.commit()

                logger.info(f"Handover {session_id} from {old_node} to {target_node}")

                await TelecomService._log_transaction(
                    "HANDOVER_SESSION", "sessions", session_id,
                    {"current_node": old_node}, {"current_node": target_node}, db
                )

                clock_manager.create_event({"type": "session_handover", "id": session_id, "target": target_node})

                # Fixed: pass db to replication call
                asyncio.create_task(ReplicationService.replicate_session_update(session_id, target_node, db))

                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to handover session: {e}", exc_info=True)
            return False

    @staticmethod
    async def terminate_session(session_id: str, db: AsyncSession) -> bool:
        try:
            result = await db.execute(
                select(DbSession).where(DbSession.session_id == session_id)
            )
            session = result.scalar_one_or_none()
            if not session:
                raise ValueError(f"Session {session_id} not found")

            await db.execute(
                update(DbSession)
                .where(DbSession.session_id == session_id)
                .values(status="TERMINATED", end_time=datetime.utcnow())
            )
            await db.commit()

            logger.info(f"Terminated session {session_id}")

            await TelecomService._log_transaction(
                "TERMINATE_SESSION", "sessions", session_id,
                {"status": session.status}, {"status": "TERMINATED"}, db
            )

            clock_manager.create_event({"type": "session_terminated", "id": session_id})

            asyncio.create_task(ReplicationService.replicate_session_termination(session_id, db))

            return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to terminate session: {e}")
            return False

    @staticmethod
    async def calculate_current_latency(session_id: str, db: AsyncSession = None) -> float:
        try:
            if db is None:
                async with AsyncSessionLocal() as db_temp:
                    result = await db_temp.execute(
                        select(DbSession.current_node).where(DbSession.session_id == session_id)
                    )
                    current_node = result.scalar()
            else:
                result = await db.execute(
                    select(DbSession.current_node).where(DbSession.session_id == session_id)
                )
                current_node = result.scalar()

            if not current_node:
                return 0.0

            latency = await get_network_latency(settings.NODE_ID, current_node)
            return latency or TelecomService._simulate_current_latency()

        except Exception as e:
            logger.error(f"Failed to calculate latency: {e}")
            return 0.0

    @staticmethod
    def _simulate_current_latency() -> float:
        hour = datetime.utcnow().hour
        base = 50 + (10 if 9 <= hour <= 17 else 0)
        return max(20, base + random.uniform(-10, 20))

    @staticmethod
    async def replicate_to_core(data: Any, operation: str):
        try:
            async with NetworkClient() as client:
                response = await client.call_node(
                    "core_1",
                    "/data/replicate",
                    method="POST",
                    data={"source_node": settings.NODE_ID, "operation": operation, "data": data.model_dump() if hasattr(data, 'model_dump') else data.__dict__}
                )
                if response:
                    logger.debug(f"Replicated {operation} to core")
                else:
                    logger.warning(f"Replication to core failed")
        except Exception as e:
            logger.error(f"Replication failed: {e}")

    @staticmethod
    async def _log_transaction(
        operation: str,
        table: str,
        record_id: str,
        old_state: Optional[Dict],
        new_state: Dict,
        db: AsyncSession
    ):
        try:
            log = TransactionLog(
                transaction_id=str(uuid.uuid4()),
                node_id=settings.NODE_ID,
                operation_type=operation,
                table_name=table,
                record_id=record_id,
                old_state=old_state,
                new_state=new_state
            )
            db.add(log)
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.warning(f"Failed to log transaction: {e}")

    @staticmethod
    async def get_session_statistics(db: AsyncSession) -> Dict[str, Any]:
        try:
            active_result = await db.execute(select(func.count()).select_from(DbSession).where(DbSession.status == "ACTIVE"))
            active_sessions = active_result.scalar() or 0

            type_result = await db.execute(
                text("""
                    SELECT session_type, COUNT(*) as count
                    FROM sessions
                    WHERE status = 'ACTIVE'
                    GROUP BY session_type
                """)
            )
            sessions_by_type = {row.session_type: row.count for row in type_result.fetchall()}

            violation_result = await db.execute(
                select(func.count()).select_from(TransactionLog)
                .where(TransactionLog.operation_type == "QOS_VIOLATION")
            )
            violations = violation_result.scalar() or 0

            latency_result = await db.execute(select(func.avg(DbSession.latency_threshold_ms)).select_from(DbSession))
            avg_latency = latency_result.scalar() or 0.0

            return {
                "active_sessions": active_sessions,
                "sessions_by_type": sessions_by_type,
                "qos_violations": violations,
                "average_latency_ms": round(avg_latency, 2),
                "node_id": settings.NODE_ID,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get session statistics: {e}")
            return {}