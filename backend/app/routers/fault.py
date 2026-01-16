from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
import asyncio
import random
from datetime import datetime

from app.database import get_db
from app.models import FaultLog
from app.schemas import FaultInjectionRequest, FaultLogResponse, MessageResponse
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.locks import DistributedLock
from app.utils.clocks import get_clock_manager
from app.services.fault import FaultService

router = APIRouter()
logger = setup_logger(__name__)

clock_manager = get_clock_manager()  # Assume initialized in startup

@router.post("/inject/node/{node_id}", response_model=MessageResponse)
async def inject_node_fault(
    node_id: str,
    fault_request: FaultInjectionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Inject fault into a specific node"""
    try:
        if node_id not in settings.NODE_URLS:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found"
            )

        async with DistributedLock("fault_injection", node_id) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Node {node_id} locked for fault injection"
                )

            fault_log = FaultLog(
                node_id=node_id,
                fault_type=fault_request.fault_type,
                injected_at=datetime.utcnow(),
                severity=fault_request.severity
            )
            db.add(fault_log)
            await db.commit()
            await db.refresh(fault_log)

            if fault_request.fault_type == "crash":
                background_tasks.add_task(
                    FaultService.inject_crash_fault,
                    node_id,
                    fault_request.duration_ms
                )
            elif fault_request.fault_type == "network_delay":
                background_tasks.add_task(
                    FaultService.inject_network_delay,
                    node_id,
                    fault_request.duration_ms,
                    fault_request.severity
                )
            elif fault_request.fault_type == "packet_loss":
                background_tasks.add_task(
                    FaultService.inject_packet_loss,
                    node_id,
                    fault_request.duration_ms,
                    fault_request.severity
                )
            elif fault_request.fault_type == "cpu_spike":
                background_tasks.add_task(
                    FaultService.inject_cpu_spike,
                    node_id,
                    fault_request.duration_ms
                )
            elif fault_request.fault_type == "byzantine":
                background_tasks.add_task(
                    FaultService.inject_byzantine_fault,
                    node_id,
                    fault_request.duration_ms
                )

            logger.warning(f"Injected {fault_request.fault_type} into {node_id} (duration: {fault_request.duration_ms}ms, severity: {fault_request.severity})")

            clock_manager.create_event({"type": "fault_injected", "node_id": node_id, "fault_type": fault_request.fault_type})

            return MessageResponse(message=f"Injected {fault_request.fault_type} into {node_id}. Fault ID: {fault_log.fault_id}")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to inject fault: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to inject fault: {str(e)}"
        )

@router.post("/inject/network/{from_node}/{to_node}", response_model=MessageResponse)
async def inject_network_fault(
    from_node: str,
    to_node: str,
    fault_request: Dict,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Inject network fault between two nodes"""
    try:
        if from_node not in settings.NODE_URLS or to_node not in settings.NODE_URLS:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or both nodes not found"
            )

        fault_type = fault_request.get("fault_type", "partition")
        duration_ms = fault_request.get("duration_ms", 10000)
        severity = fault_request.get("severity", "medium")

        await db.execute(text("SET search_path TO shared_schema,public"))

        await db.execute(
            text("""
                UPDATE network_topology
                SET status = :status
                WHERE (from_node = :from AND to_node = :to) OR (from_node = :to AND to_node = :from)
            """),
            {"status": "DOWN" if fault_type == "partition" else "DEGRADED", "from": from_node, "to": to_node}
        )
        await db.commit()

        logger.warning(f"Injected network {fault_type} between {from_node} and {to_node} (duration: {duration_ms}ms)")

        background_tasks.add_task(
            FaultService.recover_network_fault,
            from_node,
            to_node,
            duration_ms
        )

        clock_manager.create_event({"type": "network_fault_injected", "from": from_node, "to": to_node, "fault_type": fault_type})

        return MessageResponse(message=f"Injected network {fault_type} between {from_node} and {to_node}. Recover in {duration_ms}ms")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to inject network fault: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to inject network fault: {str(e)}"
        )

@router.post("/inject/random", response_model=MessageResponse)
async def inject_random_fault(
    fault_request: FaultInjectionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Inject random fault into a random node (for batch testing)"""
    try:
        random_node = random.choice(list(settings.NODE_URLS.keys()))

        fault_log = FaultLog(
            node_id=random_node,
            fault_type=fault_request.fault_type,
            injected_at=datetime.utcnow(),
            severity=fault_request.severity
        )
        db.add(fault_log)
        await db.commit()
        await db.refresh(fault_log)

        if fault_request.fault_type == "crash":
            background_tasks.add_task(
                FaultService.inject_crash_fault,
                random_node,
                fault_request.duration_ms
            )

        # (add other types similarly as in inject_node_fault)

        logger.warning(f"Injected random {fault_request.fault_type} into {random_node}")

        clock_manager.create_event({"type": "random_fault_injected", "node_id": random_node, "fault_type": fault_request.fault_type})

        return MessageResponse(message=f"Injected random {fault_request.fault_type} into {random_node}. Fault ID: {fault_log.fault_id}")
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to inject random fault: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to inject random fault: {str(e)}"
        )

@router.get("/recovery/stats", response_model=Dict)
async def get_recovery_stats(db: AsyncSession = Depends(get_db)):
    """Get fault recovery statistics"""
    try:
        await db.execute(text("SET search_path TO monitoring_schema,public"))

        result = await db.execute(
            text("""
                SELECT
                    COUNT(*) as total_faults,
                    AVG(COALESCE(recovery_time_ms, 0)) as avg_recovery_time,
                    MIN(recovery_time_ms) as min_recovery_time,
                    MAX(recovery_time_ms) as max_recovery_time,
                    SUM(affected_sessions) as total_affected_sessions
                FROM fault_logs
                WHERE recovered_at IS NOT NULL
            """)
        )
        stats = result.fetchone() or (0, 0, 0, 0, 0)

        type_result = await db.execute(
            text("""
                SELECT fault_type, COUNT(*) as count
                FROM fault_logs
                GROUP BY fault_type
            """)
        )
        by_type = {row.fault_type: row.count for row in type_result.fetchall()}

        recent_result = await db.execute(
            text("""
                SELECT *
                FROM fault_logs
                WHERE injected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY injected_at DESC
                LIMIT 10
            """)
        )
        recent_faults = [FaultLogResponse.model_validate(row) for row in recent_result.fetchall()]

        return {
            "total_faults": stats.total_faults,
            "avg_recovery_time_ms": float(stats.avg_recovery_time),
            "min_recovery_time_ms": stats.min_recovery_time,
            "max_recovery_time_ms": stats.max_recovery_time,
            "total_affected_sessions": stats.total_affected_sessions,
            "faults_by_type": by_type,
            "recent_faults": recent_faults,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get recovery stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get recovery stats: {str(e)}"
        )

@router.get("/logs", response_model=List[FaultLogResponse])
async def get_fault_logs(
    limit: int = 100,
    node_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get fault logs with optional filtering"""
    try:
        await db.execute(text("SET search_path TO monitoring_schema,public"))

        query = select(FaultLog).order_by(FaultLog.injected_at.desc())
        if node_id:
            query = query.where(FaultLog.node_id == node_id)
        if limit > 0:
            query = query.limit(limit)

        result = await db.execute(query)
        logs = result.scalars().all()

        return [FaultLogResponse.model_validate(log) for log in logs]
    except Exception as e:
        logger.error(f"Failed to get fault logs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get fault logs: {str(e)}"
        )

@router.post("/recover/node/{node_id}", response_model=MessageResponse)
async def recover_node(
    node_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Manually trigger recovery for a node"""
    try:
        if node_id not in settings.NODE_URLS:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found"
            )

        await db.execute(text("SET search_path TO monitoring_schema,public"))

        result = await db.execute(
            text("""
                UPDATE fault_logs
                SET recovered_at = NOW(),
                    detected_at = COALESCE(detected_at, NOW()),
                    recovery_time_ms = EXTRACT(EPOCH FROM (NOW() - injected_at)) * 1000
                WHERE node_id = :node_id AND recovered_at IS NULL
                RETURNING affected_sessions
            """),
            {"node_id": node_id}
        )
        affected = sum(row.affected_sessions for row in result.fetchall())

        await db.execute(text("SET search_path TO shared_schema,public"))

        await db.execute(
            text("""
                UPDATE consensus_state
                SET status = 'ACTIVE'
                WHERE node_id = :node_id
            """),
            {"node_id": node_id}
        )

        await db.execute(
            text("""
                UPDATE network_topology
                SET status = 'UP'
                WHERE (from_node = :node_id OR to_node = :node_id) AND status != 'UP'
            """),
            {"node_id": node_id}
        )
        await db.commit()

        logger.info(f"Recovered node {node_id} (affected sessions: {affected})")

        clock_manager.create_event({"type": "node_recovered", "node_id": node_id})

        return MessageResponse(message=f"Node {node_id} recovered (affected sessions: {affected})")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to recover node: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to recover node: {str(e)}"
        )