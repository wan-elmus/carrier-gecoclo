import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, text, update
from app.models import Subscriber, DbSession, TransactionLog
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.clocks import get_clock_manager
from app.utils.locks import DistributedLock
from app.database import SCHEMAS, set_session_schema

logger = setup_logger(__name__)
clock_manager = get_clock_manager()

class ReplicationService:
    @staticmethod
    async def replicate_subscriber(subscriber_data: Dict, db: AsyncSession) -> bool:
        try:
            async with DistributedLock("replicate_sub", subscriber_data["subscriber_id"]) as lock:
                if not await lock.acquire(timeout_ms=3000):
                    raise ValueError(f"Subscriber {subscriber_data['subscriber_id']} replication locked")

                await set_session_schema(db, SCHEMAS["cloud"])

                result = await db.execute(
                    select(Subscriber).where(Subscriber.subscriber_id == subscriber_data["subscriber_id"])
                )
                existing = result.scalar_one_or_none()

                if existing:
                    await db.execute(
                        update(Subscriber)
                        .where(Subscriber.subscriber_id == subscriber_data["subscriber_id"])
                        .values(
                            imsi=subscriber_data["imsi"],
                            msisdn=subscriber_data["msisdn"],
                            status=subscriber_data.get("status", existing.status),
                            location_area=subscriber_data.get("location_area", existing.location_area),
                            service_profile=subscriber_data.get("service_profile", existing.service_profile),
                            updated_at=datetime.utcnow()
                        )
                    )
                else:
                    subscriber = Subscriber(**subscriber_data)
                    db.add(subscriber)

                await db.commit()

                logger.debug(f"Replicated subscriber {subscriber_data['subscriber_id']} to cloud")

                clock_manager.create_event({"type": "subscriber_replicated", "id": subscriber_data["subscriber_id"]})

                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to replicate subscriber: {e}")
            return False

    @staticmethod
    async def replicate_session(session_data: Dict, db: AsyncSession) -> bool:
        try:
            async with DistributedLock("replicate_sess", session_data["session_id"]) as lock:
                if not await lock.acquire(timeout_ms=3000):
                    raise ValueError(f"Session {session_data['session_id']} replication locked")

                await set_session_schema(db, SCHEMAS["cloud"])

                result = await db.execute(
                    select(DbSession).where(DbSession.session_id == session_data["session_id"])
                )
                existing = result.scalar_one_or_none()

                if existing:
                    await db.execute(
                        update(DbSession)
                        .where(DbSession.session_id == session_data["session_id"])
                        .values(
                            subscriber_id=session_data["subscriber_id"],
                            session_type=session_data.get("session_type", existing.session_type),
                            source_node=session_data.get("source_node", existing.source_node),
                            destination_node=session_data.get("destination_node", existing.destination_node),
                            current_node=session_data.get("current_node", existing.current_node),
                            status=session_data.get("status", existing.status),
                            qos_profile=session_data.get("qos_profile", existing.qos_profile),
                            latency_threshold_ms=session_data.get("latency_threshold_ms", existing.latency_threshold_ms),
                            data_volume=session_data.get("data_volume", existing.data_volume),
                            migrated_from=session_data.get("migrated_from", existing.migrated_from),
                            migration_count=session_data.get("migration_count", existing.migration_count),
                            end_time=session_data.get("end_time", existing.end_time)
                        )
                    )
                else:
                    session = DbSession(**session_data)
                    db.add(session)

                await db.commit()

                logger.debug(f"Replicated session {session_data['session_id']} to cloud")

                clock_manager.create_event({"type": "session_replicated", "id": session_data["session_id"]})

                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to replicate session: {e}")
            return False

    @staticmethod
    async def replicate_transaction_log(log_data: Dict, db: AsyncSession) -> bool:
        try:
            async with DistributedLock("replicate_log", log_data["transaction_id"]) as lock:
                if not await lock.acquire(timeout_ms=3000):
                    raise ValueError(f"Log {log_data['transaction_id']} replication locked")

                await set_session_schema(db, SCHEMAS["cloud"])

                result = await db.execute(
                    select(TransactionLog).where(TransactionLog.transaction_id == log_data["transaction_id"])
                    .where(TransactionLog.node_id == log_data["node_id"])
                )
                existing = result.scalar_one_or_none()

                if existing and existing.committed == log_data.get("committed"):
                    return True

                log = TransactionLog(**log_data)
                db.add(log)
                await db.commit()

                logger.debug(f"Replicated log {log_data['transaction_id']} to cloud")

                clock_manager.create_event({"type": "log_replicated", "id": log_data["transaction_id"]})

                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to replicate log: {e}")
            return False

    @staticmethod
    async def replicate_session_update(session_id: str, new_node: str, db: AsyncSession) -> bool:
        """Replicate session update to other nodes (fixed: now accepts db)"""
        try:
            async with NetworkClient() as client:
                target_nodes = [nid for nid in settings.NODE_URLS if nid.startswith("edge_") or nid.startswith("core_")]
                tasks = []
                for nid in target_nodes:
                    if nid != settings.NODE_ID:
                        tasks.append(
                            client.call_node(
                                nid,
                                f"/sessions/{session_id}/update",
                                method="POST",
                                data={"session_id": session_id, "current_node": new_node, "updated_at": datetime.utcnow().isoformat()}
                            )
                        )

                results = await asyncio.gather(*tasks, return_exceptions=True)
                failures = sum(1 for r in results if isinstance(r, Exception))
                if failures > 0:
                    logger.warning(f"{failures}/{len(tasks)} replication updates failed")

            logger.info(f"Replicated update for {session_id} to {len(target_nodes) - 1} nodes")

            clock_manager.create_event({"type": "session_update_replicated", "id": session_id})

            return True
        except Exception as e:
            logger.error(f"Session update replication failed: {e}")
            return False

    @staticmethod
    async def replicate_session_termination(session_id: str, db: AsyncSession) -> bool:
        try:
            async with NetworkClient() as client:
                target_nodes = [nid for nid in settings.NODE_URLS if nid.startswith("edge_") or nid.startswith("core_")]
                tasks = []
                for nid in target_nodes:
                    if nid != settings.NODE_ID:
                        tasks.append(
                            client.call_node(
                                nid,
                                f"/sessions/{session_id}/terminate",
                                method="POST",
                                data={"session_id": session_id, "terminated_at": datetime.utcnow().isoformat()}
                            )
                        )

                results = await asyncio.gather(*tasks, return_exceptions=True)
                failures = sum(1 for r in results if isinstance(r, Exception))
                if failures > 0:
                    logger.warning(f"{failures}/{len(tasks)} termination replications failed")

            logger.info(f"Replicated termination for {session_id} to {len(target_nodes) - 1} nodes")

            clock_manager.create_event({"type": "session_termination_replicated", "id": session_id})

            return True
        except Exception as e:
            logger.error(f"Session termination replication failed: {e}")
            return False

    @staticmethod
    async def sync_schemas(source_schema: str, target_schema: str, table_name: str, db: AsyncSession) -> Dict:
        try:
            await set_session_schema(db, target_schema)
            max_id_result = await db.execute(text(f"SELECT COALESCE(MAX(log_id), 0) FROM {table_name}"))
            max_target_id = max_id_result.scalar() or 0

            await set_session_schema(db, source_schema)
            new_result = await db.execute(
                text(f"SELECT * FROM {table_name} WHERE log_id > :max_id ORDER BY log_id"),
                {"max_id": max_target_id}
            )
            new_records = new_result.fetchall()

            sync_stats = {"records_synced": 0, "errors": 0, "duration_ms": 0}
            start = datetime.utcnow()

            if new_records:
                await set_session_schema(db, target_schema)

                for record in new_records:
                    try:
                        columns = record._fields
                        values = dict(record._mapping)
                        insert_stmt = insert(table_name).values(values).on_conflict_do_update(
                            index_elements=['log_id'],
                            set_=values
                        )
                        await db.execute(insert_stmt)
                        sync_stats["records_synced"] += 1
                    except Exception as e:
                        logger.error(f"Sync record failed: {e}")
                        sync_stats["errors"] += 1

                await db.commit()

            sync_stats["duration_ms"] = int((datetime.utcnow() - start).total_seconds() * 1000)

            logger.info(f"Synced {sync_stats['records_synced']} records from {source_schema}.{table_name} to {target_schema}")

            clock_manager.create_event({"type": "schema_synced", "table": table_name, "stats": sync_stats})

            return sync_stats
        except Exception as e:
            await db.rollback()
            logger.error(f"Schema sync failed: {e}")
            raise

    @staticmethod
    async def perform_full_replication(db: AsyncSession):
        try:
            report = {"start_time": datetime.utcnow().isoformat(), "tables_replicated": [], "total_records": 0, "errors": 0}

            tables = ["subscribers", "sessions", "transaction_logs"]
            sources = ["edge_schema", "core_schema"]

            for source in sources:
                for table in tables:
                    try:
                        stats = await ReplicationService.sync_schemas(source, "cloud_schema", table, db)
                        report["tables_replicated"].append({"table": table, "source": source, **stats})
                        report["total_records"] += stats["records_synced"]
                        report["errors"] += stats["errors"]
                    except Exception as e:
                        logger.error(f"Replication failed for {source}.{table}: {e}")
                        report["errors"] += 1

            report["end_time"] = datetime.utcnow().isoformat()
            report["success"] = report["errors"] == 0

            logger.info(f"Full replication: {report['total_records']} records, {report['errors']} errors")

            clock_manager.create_event({"type": "full_replication", "report": report})

            return report
        except Exception as e:
            logger.error(f"Full replication failed: {e}")
            raise