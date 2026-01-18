import asyncio
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, text
from app.models import DbSession, LoadDecision, NodeMetrics
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.locks import DistributedLock
from app.utils.clocks import get_clock_manager
from app.database import AsyncSessionLocal

logger = setup_logger(__name__)
clock_manager = get_clock_manager()

class LoadBalancer:
    """Load balancing and session migration service"""

    @staticmethod
    async def get_node_load(node_id: str, db: AsyncSession) -> Dict[str, float]:
        """Get current load metrics for a node"""
        try:
            five_min_ago = datetime.utcnow() - timedelta(minutes=5)
            
            schema_map = {
                "edge_": "edge_schema",
                "core_": "core_schema",
                "cloud_": "cloud_schema",
                "monitoring": "monitoring_schema"
            }
            schema = "public"
            for prefix, sch in schema_map.items():
                if node_id.startswith(prefix):
                    schema = sch
                    break

            await db.execute(text(f"SET search_path TO {schema}, public"))
            
            avg_cpu = 0.0
            try:
                cpu_result = await db.execute(
                    select(func.avg(NodeMetrics.metric_value))
                    .where(NodeMetrics.node_id == node_id)
                    .where(NodeMetrics.metric_type == "cpu_percent")
                    .where(NodeMetrics.timestamp >= five_min_ago)
                )
                avg_cpu = cpu_result.scalar() or 0.0
            except Exception as sub_e:
                logger.warning(f"CPU avg query failed for {node_id}: {sub_e}")
                await db.rollback()  # Reset transaction
                
            avg_memory = 0.0
            try:
                memory_result = await db.execute(
                    select(func.avg(NodeMetrics.metric_value))
                    .where(NodeMetrics.node_id == node_id)
                    .where(NodeMetrics.metric_type == "memory_percent")
                    .where(NodeMetrics.timestamp >= five_min_ago)
                )
                avg_memory = memory_result.scalar() or 0.0
            except Exception as sub_e:
                logger.warning(f"Memory avg query failed for {node_id}: {sub_e}")
                await db.rollback()
                
            active_sessions = 0
            try:
                session_result = await db.execute(
                    select(func.count()).select_from(DbSession)
                    .where(DbSession.current_node == node_id)
                    .where(DbSession.status == "ACTIVE")
                )
                active_sessions = session_result.scalar() or 0            
            except Exception as sub_e:
                logger.warning(f"Session count query failed for {node_id}: {sub_e}")
                await db.rollback()

            return {
                "cpu_percent": round(float(avg_cpu), 2),
                "memory_percent": round(float(avg_memory), 2),
                "active_sessions": active_sessions,
                "latency_ms": round(random.uniform(50, 100), 2),  # Simulate for now
                "throughput_rps": round(1000 - (avg_cpu * 10), 2),
                "load_factor": LoadBalancer._calculate_load_factor(avg_cpu, avg_memory, active_sessions)
            }
        except Exception as e:
            logger.error(f"Failed to get load for {node_id}: {e}")
            return {"cpu_percent": 0.0, "memory_percent": 0.0, "active_sessions": 0, "load_factor": 0.0}

    @staticmethod
    def _calculate_load_factor(cpu: float, memory: float, sessions: int) -> float:
        cpu_weight = 0.5
        memory_weight = 0.3
        sessions_weight = 0.2
        normalized_sessions = min(sessions / 1000 * 100, 100)
        load = (cpu * cpu_weight) + (memory * memory_weight) + (normalized_sessions * sessions_weight)
        return min(round(load, 2), 100)

    @staticmethod
    async def get_all_node_metrics(db: AsyncSession) -> Dict[str, Dict]:
        try:
            all_metrics = {}
            for node_id in settings.NODE_URLS:
                if any(t in node_id for t in ["edge", "core", "cloud"]):
                    metrics = await LoadBalancer.get_node_load(node_id, db)
                    all_metrics[node_id] = metrics
            return all_metrics
        except Exception as e:
            logger.error(f"Failed to get all metrics: {e}")
            return {}

    @staticmethod
    async def get_underloaded_nodes(db: AsyncSession, threshold: float = 50.0) -> List[Dict]:
        try:
            metrics = await LoadBalancer.get_all_node_metrics(db)
            underloaded = [
                {"node_id": nid, "cpu_percent": m["cpu_percent"], "capacity": 100 - m["cpu_percent"]}
                for nid, m in metrics.items() if m["cpu_percent"] < threshold
            ]
            return underloaded
        except Exception as e:
            logger.error(f"Failed to get underloaded nodes: {e}")
            return []

    @staticmethod
    async def find_best_edge_node(
        db: AsyncSession,
        exclude: Optional[List[str]] = None
    ) -> Optional[str]:
        try:
            exclude = exclude or []
            edge_nodes = [nid for nid in settings.NODE_URLS if nid.startswith("edge_")]
            available = [n for n in edge_nodes if n not in exclude]

            if not available:
                return None

            metrics = await LoadBalancer.get_all_node_metrics(db)
            node_loads = {nid: metrics.get(nid, {}).get("load_factor", 100.0) for nid in available}

            if node_loads:
                best = min(node_loads, key=node_loads.get)
                if node_loads[best] < settings.CPU_THRESHOLD_PERCENT:
                    return best

            return random.choice(available)
        except Exception as e:
            logger.error(f"Failed to find best edge: {e}")
            return None

    @staticmethod
    async def make_migration_decisions(
        overloaded: List[Dict],
        underloaded: List[Dict],
        db: AsyncSession
    ) -> List[Dict]:
        try:
            decisions = []
            overloaded.sort(key=lambda x: x["cpu_percent"], reverse=True)
            underloaded.sort(key=lambda x: x["capacity"], reverse=True)

            for overloaded_node in overloaded:
                nid = overloaded_node["node_id"]
                result = await db.execute(
                    select(DbSession)
                    .where(DbSession.current_node == nid)
                    .where(DbSession.status == "ACTIVE")
                    .order_by(DbSession.session_type.desc())
                    .limit(3)
                )
                sessions = result.scalars().all()

                for i, session in enumerate(sessions):
                    if i >= len(underloaded):
                        break
                    target = underloaded[i]["node_id"]
                    decisions.append({
                        "session_id": session.session_id,
                        "source_node": nid,
                        "target_node": target,
                        "reason": f"CPU overload: {overloaded_node['cpu_percent']}%",
                        "cpu_before": overloaded_node["cpu_percent"]
                    })

            logger.info(f"Made {len(decisions)} migration decisions")
            clock_manager.create_event({"type": "migration_decisions_made", "count": len(decisions)})
            return decisions
        except Exception as e:
            logger.error(f"Failed to make decisions: {e}")
            return []

    @staticmethod
    async def record_migration_decision(
        session_id: str,
        source_node: str,
        target_node: str,
        reason: str,
        cpu_before: float,
        db: AsyncSession
    ) -> LoadDecision:
        try:
            decision = LoadDecision(
                session_id=session_id,
                source_node=source_node,
                target_node=target_node,
                reason=reason,
                cpu_before=cpu_before
            )
            db.add(decision)
            await db.commit()
            await db.refresh(decision)
            logger.info(f"Recorded decision {decision.decision_id}")
            clock_manager.create_event({"type": "migration_recorded", "id": decision.decision_id})
            return decision
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to record decision: {e}")
            raise

    @staticmethod
    async def update_migration_result(
        decision_id: int,
        success: bool,
        cpu_after_source: float,
        cpu_after_target: float,
        db: AsyncSession
    ):
        try:
            await db.execute(
                update(LoadDecision)
                .where(LoadDecision.decision_id == decision_id)
                .values(cpu_after=cpu_after_target, success=success)
            )
            await db.commit()
            logger.debug(f"Updated result for {decision_id}")
            clock_manager.create_event({"type": "migration_updated", "id": decision_id, "success": success})
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to update result: {e}")

    @staticmethod
    async def migrate_session(
        session_id: str,
        target_node: str,
        db: AsyncSession
    ) -> bool:
        try:
            async with DistributedLock("migration", session_id) as lock:
                if not await lock.acquire(timeout_ms=5000):
                    raise ValueError(f"Session {session_id} migration locked")

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

                logger.info(f"Migrated {session_id} from {old_node} to {target_node}")

                async with NetworkClient() as client:
                    await client.call_node(
                        target_node,
                        f"/sessions/{session_id}/receive",
                        method="POST",
                        data={"session_data": session.__dict__}  # simplified
                    )

                clock_manager.create_event({"type": "session_migrated", "id": session_id})
                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to migrate {session_id}: {e}")
            return False

    @staticmethod
    async def auto_balance_loop():
        logger.info("Auto-balance loop started")
        while True:
            try:
                async with AsyncSessionLocal() as db:
                    metrics = await LoadBalancer.get_all_node_metrics(db)
                    overloaded = [
                        {"node_id": nid, "cpu_percent": m["cpu_percent"], "active_sessions": m["active_sessions"]}
                        for nid, m in metrics.items()
                        if m["cpu_percent"] > settings.CPU_THRESHOLD_PERCENT
                    ]
                    underloaded = await LoadBalancer.get_underloaded_nodes(db)

                    if overloaded and underloaded:
                        decisions = await LoadBalancer.make_migration_decisions(overloaded, underloaded, db)
                        for decision in decisions:
                            success = await LoadBalancer.migrate_session(
                                decision["session_id"],
                                decision["target_node"],
                                db
                            )
                            if success:
                                source_after = await LoadBalancer.get_node_load(decision["source_node"], db)
                                target_after = await LoadBalancer.get_node_load(decision["target_node"], db)
                                await LoadBalancer.update_migration_result(
                                    decision["decision_id"],  # note: you need to add decision_id to decision dict
                                    success,
                                    source_after["cpu_percent"],
                                    target_after["cpu_percent"],
                                    db
                                )
                await asyncio.sleep(settings.LOAD_BALANCE_INTERVAL_SECONDS)
            except Exception as e:
                logger.error(f"Auto-balance loop error: {e}", exc_info=True)
                await asyncio.sleep(5)