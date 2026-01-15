import asyncio
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from app.models import DbSession, LoadDecision, NodeMetrics
from app.schemas import LoadDecisionResponse
from app.config import settings
from utils.logger import setup_logger
from utils.network import NetworkClient
from utils.locks import DistributedLock
from utils.clocks import get_clock_manager

logger = setup_logger(__name__)
clock_manager = get_clock_manager()  # Assume initialized in startup

class LoadBalancer:
    """Load balancing and session migration service"""

    @staticmethod
    async def get_node_load(node_id: str, db: AsyncSession) -> Dict[str, float]:
        """Get current load metrics for a node"""
        try:
            five_min_ago = datetime.utcnow() - timedelta(minutes=5)

            cpu_result = await db.execute(
                select(func.avg(NodeMetrics.metric_value))
                .where(NodeMetrics.node_id == node_id)
                .where(NodeMetrics.metric_type == "cpu_percent")
                .where(NodeMetrics.timestamp >= five_min_ago)
            )
            avg_cpu = cpu_result.scalar() or 0

            memory_result = await db.execute(
                select(func.avg(NodeMetrics.metric_value))
                .where(NodeMetrics.node_id == node_id)
                .where(NodeMetrics.metric_type == "memory_percent")
                .where(NodeMetrics.timestamp >= five_min_ago)
            )
            avg_memory = memory_result.scalar() or 0

            session_result = await db.execute(
                select(func.count()).select_from(DbSession)
                .where(DbSession.current_node == node_id)
                .where(DbSession.status == "ACTIVE")
            )
            active_sessions = session_result.scalar() or 0

            return {
                "cpu_percent": round(avg_cpu, 2),
                "memory_percent": round(avg_memory, 2),
                "active_sessions": active_sessions,
                "latency_ms": round(random.uniform(50, 100), 2),  # Simulate
                "throughput_rps": round(1000 - (avg_cpu * 10), 2),
                "load_factor": LoadBalancer._calculate_load_factor(avg_cpu, avg_memory, active_sessions)
            }
        except Exception as e:
            logger.error(f"Failed to get load for {node_id}: {e}")
            return {"cpu_percent": 0, "memory_percent": 0, "active_sessions": 0, "load_factor": 0}

    @staticmethod
    def _calculate_load_factor(cpu: float, memory: float, sessions: int) -> float:
        """Calculate overall load factor (0-100)"""
        cpu_weight = 0.5
        memory_weight = 0.3
        sessions_weight = 0.2
        normalized_sessions = min(sessions / 1000 * 100, 100)  # Cap at 100%
        load = (cpu * cpu_weight) + (memory * memory_weight) + (normalized_sessions * sessions_weight)
        return min(round(load, 2), 100)

    @staticmethod
    async def get_all_node_metrics(db: AsyncSession) -> Dict[str, Dict]:
        """Get load metrics for all nodes"""
        try:
            all_metrics = {}
            for node_id in settings.NODE_URLS:
                if "edge" in node_id or "core" in node_id or "cloud" in node_id:
                    metrics = await LoadBalancer.get_node_load(node_id, db)
                    all_metrics[node_id] = metrics
            return all_metrics
        except Exception as e:
            logger.error(f"Failed to get all metrics: {e}")
            return {}

    @staticmethod
    async def get_underloaded_nodes(db: AsyncSession, threshold: float = 50) -> List[Dict]:
        """Get list of underloaded nodes"""
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
        """Find the best edge node for new session"""
        try:
            exclude = exclude or []

            edge_nodes = [nid for nid in settings.NODE_URLS if nid.startswith("edge_")]
            available = [n for n in edge_nodes if n not in exclude]

            if not available:
                return None

            metrics = await LoadBalancer.get_all_node_metrics(db)
            node_loads = {nid: metrics.get(nid, {}).get("load_factor", 100) for nid in available}

            if node_loads:
                best = min(node_loads, key=node_loads.get)
                if node_loads[best] < settings.CPU_THRESHOLD_PERCENT:
                    return best

            return random.choice(available)  # Fallback round-robin
        except Exception as e:
            logger.error(f"Failed to find best edge: {e}")
            return None

    @staticmethod
    async def make_migration_decisions(
        overloaded: List[Dict],
        underloaded: List[Dict],
        db: AsyncSession
    ) -> List[Dict]:
        """Make decisions about which sessions to migrate"""
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
                    .order_by(DbSession.session_type.desc())  # Priority: VOICE > DATA
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
        """Record a migration decision in database"""
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
        """Update migration decision with results"""
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
        """Execute session migration to target node"""
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
                        data={"session_data": session.model_dump()}
                    )

                clock_manager.create_event({"type": "session_migrated", "id": session_id})

                return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Failed to migrate {session_id}: {e}")
            return False

    @staticmethod
    async def auto_balance_load(db: AsyncSession):
        """Automatic load balancing: check and migrate sessions if needed"""
        try:
            logger.info("Running auto-balance")

            metrics = await LoadBalancer.get_all_node_metrics(db)

            overloaded = [
                {"node_id": nid, "cpu_percent": m["cpu_percent"], "active_sessions": m["active_sessions"]}
                for nid, m in metrics.items() if m["cpu_percent"] > settings.CPU_THRESHOLD_PERCENT
            ]
            underloaded = await LoadBalancer.get_underloaded_nodes(db)

            if overloaded and underloaded:
                decisions = await LoadBalancer.make_migration_decisions(overloaded, underloaded, db)

                successes = 0
                for decision in decisions:
                    success = await LoadBalancer.migrate_session(decision["session_id"], decision["target_node"], db)
                    if success:
                        successes += 1
                        source_after = await LoadBalancer.get_node_load(decision["source_node"], db)
                        target_after = await LoadBalancer.get_node_load(decision["target_node"], db)
                        await LoadBalancer.update_migration_result(
                            decision["decision_id"], success, source_after["cpu_percent"], target_after["cpu_percent"], db
                        )

                logger.info(f"Auto-balance: {successes}/{len(decisions)} successful")

                clock_manager.create_event({"type": "auto_balance", "successes": successes})

                return successes
            logger.info("No balancing needed")
            return 0
        except Exception as e:
            logger.error(f"Auto-balance failed: {e}")
            return 0