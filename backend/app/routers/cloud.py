from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import json

from app.database import get_db
from app.models import Subscriber, DbSession, TransactionLog, NodeMetrics, FaultLog
from app.schemas import MessageResponse, SystemMetricsResponse
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.clocks import get_clock_manager
from app.utils.locks import DistributedLock
from app.services.replication import ReplicationService

router = APIRouter()
logger = setup_logger(__name__)

clock_manager = get_clock_manager()
backup_dir = Path("backups") 
backup_dir.mkdir(exist_ok=True)

@router.post("/data/replicate", response_model=MessageResponse)
async def replicate_data(
    replication_request: Dict,
    db: AsyncSession = Depends(get_db)
):
    """Replicate data from edge/core to cloud storage"""
    try:
        source_node = replication_request.get("source_node")
        data_type = replication_request.get("data_type")
        data = replication_request.get("data")
        if not all([source_node, data_type, data]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing source_node, data_type, or data"
            )

        async with DistributedLock("replication", data_type) as lock:
            if not await lock.acquire(timeout_ms=5000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Replication for {data_type} locked"
                )

            success = False
            if data_type == "subscriber":
                success = await ReplicationService.replicate_subscriber(data, db)
            elif data_type == "session":
                success = await ReplicationService.replicate_session(data, db)
            elif data_type == "transaction_log":
                success = await ReplicationService.replicate_transaction_log(data, db)
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported data_type: {data_type}"
                )

            if success:
                logger.info(f"Replicated {data_type} from {source_node}")

                clock_manager.create_event({"type": "data_replicated", "data_type": data_type, "source": source_node})

                return MessageResponse(message=f"{data_type.capitalize()} replicated from {source_node}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to replicate {data_type}"
            )
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to replicate data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to replicate data: {str(e)}"
        )

@router.get("/analytics/overview", response_model=SystemMetricsResponse)
async def get_analytics_overview(db: AsyncSession = Depends(get_db)):
    """Get cloud analytics overview"""
    try:
        await db.execute(text("SET search_path TO monitoring_schema,public"))

        one_hour_ago = datetime.utcnow() - timedelta(hours=1)

        result = await db.execute(
            text("""
                SELECT
                    AVG(edge_cpu_avg) as edge_cpu,
                    AVG(core_cpu_avg) as core_cpu,
                    AVG(cloud_cpu_avg) as cloud_cpu,
                    AVG(overall_latency_avg) as latency,
                    AVG(transaction_rate) as tx_rate,
                    AVG(packet_loss_rate) as packet_loss
                FROM system_metrics
                WHERE timestamp >= :since
            """),
            {"since": one_hour_ago}
        )
        metrics = result.fetchone()

        return SystemMetricsResponse(
            edge_cpu_avg=float(metrics.edge_cpu) if metrics.edge_cpu else 0.0,
            core_cpu_avg=float(metrics.core_cpu) if metrics.core_cpu else 0.0,
            cloud_cpu_avg=float(metrics.cloud_cpu) if metrics.cloud_cpu else 0.0,
            overall_latency_avg=float(metrics.latency) if metrics.latency else 0.0,
            transaction_rate=int(metrics.tx_rate) if metrics.tx_rate else 0,
            packet_loss_rate=float(metrics.packet_loss) if metrics.packet_loss else 0.0,
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        logger.error(f"Failed to get analytics overview: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analytics overview: {str(e)}"
        )

@router.get("/analytics/subscribers", response_model=Dict)
async def get_subscriber_analytics(db: AsyncSession = Depends(get_db)):
    """Get subscriber analytics"""
    try:
        await db.execute(text("SET search_path TO cloud_schema,public"))

        total_result = await db.execute(select(func.count()).select_from(Subscriber))
        total_subscribers = total_result.scalar() or 0

        status_result = await db.execute(
            text("""
                SELECT status, COUNT(*) as count
                FROM subscribers
                GROUP BY status
            """)
        )
        by_status = {row.status: row.count for row in status_result.fetchall()}

        node_result = await db.execute(
            text("""
                SELECT node_id, COUNT(*) as count
                FROM subscribers
                GROUP BY node_id
                ORDER BY count DESC
            """)
        )
        by_node = [{"node_id": row.node_id, "count": row.count} for row in node_result.fetchall()]

        return {
            "total_subscribers": total_subscribers,
            "by_status": by_status,
            "by_node": by_node,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get subscriber analytics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get subscriber analytics: {str(e)}"
        )

@router.get("/analytics/sessions", response_model=Dict)
async def get_session_analytics(db: AsyncSession = Depends(get_db)):
    """Get session analytics"""
    try:
        await db.execute(text("SET search_path TO cloud_schema,public"))

        active_result = await db.execute(
            select(func.count()).select_from(DbSession).where(DbSession.status == "ACTIVE")
        )
        active_sessions = active_result.scalar() or 0

        type_result = await db.execute(
            text("""
                SELECT session_type, COUNT(*) as count
                FROM sessions
                WHERE status = 'ACTIVE'
                GROUP BY session_type
            """)
        )
        by_type = {row.session_type: row.count for row in type_result.fetchall()}

        node_result = await db.execute(
            text("""
                SELECT current_node, COUNT(*) as count
                FROM sessions
                WHERE status = 'ACTIVE'
                GROUP BY current_node
                ORDER BY count DESC
            """)
        )
        by_node = [{"node_id": row.current_node, "count": row.count} for row in node_result.fetchall()]

        mig_result = await db.execute(
            text("""
                SELECT
                    COUNT(*) as total_migrations,
                    AVG(migration_count) as avg_migrations,
                    AVG(latency_threshold_ms) as avg_latency
                FROM sessions
                WHERE migration_count > 0
            """)
        )
        migration_stats = mig_result.fetchone()

        return {
            "active_sessions": active_sessions,
            "by_type": by_type,
            "by_node": by_node,
            "total_migrations": migration_stats.total_migrations if migration_stats else 0,
            "avg_migrations_per_session": float(migration_stats.avg_migrations) if migration_stats and migration_stats.avg_migrations else 0.0,
            "avg_latency_threshold": float(migration_stats.avg_latency) if migration_stats and migration_stats.avg_latency else 0.0,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get session analytics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get session analytics: {str(e)}"
        )

@router.post("/backup/create", response_model=MessageResponse)
async def create_backup(db: AsyncSession = Depends(get_db)):
    """Create cloud backup of critical data"""
    try:
        await db.execute(text("SET search_path TO cloud_schema,public"))

        backup_id = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        sub_result = await db.execute(select(Subscriber))
        subscribers = [sub.model_dump() for sub in sub_result.scalars().all()]

        sess_result = await db.execute(select(DbSession).where(DbSession.status == "ACTIVE"))
        sessions = [sess.model_dump() for sess in sess_result.scalars().all()]

        backup_data = {
            "backup_id": backup_id,
            "timestamp": datetime.utcnow().isoformat(),
            "subscribers": subscribers,
            "sessions": sessions
        }

        backup_file = backup_dir / f"{backup_id}.json"
        with backup_file.open("w") as f:
            json.dump(backup_data, f, default=str)

        logger.info(f"Created backup {backup_id}: {len(subscribers)} subscribers, {len(sessions)} sessions")

        clock_manager.create_event({"type": "backup_created", "id": backup_id})

        return MessageResponse(message=f"Backup {backup_id} created: {backup_file}")
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create backup: {str(e)}"
        )

@router.get("/backup/list", response_model=Dict)
async def list_backups():
    """List available backups"""
    try:
        backups = []
        total_size = 0
        for file in backup_dir.glob("*.json"):
            stat = file.stat()
            backups.append({
                "id": file.stem,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
            total_size += stat.st_size / (1024 * 1024)

        backups.sort(key=lambda b: b["created"], reverse=True)

        return {
            "backups": backups,
            "total_backups": len(backups),
            "total_size_mb": round(total_size, 2)
        }
    except Exception as e:
        logger.error(f"Failed to list backups: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list backups: {str(e)}"
        )

@router.post("/metrics/aggregate", response_model=MessageResponse)
async def aggregate_metrics(db: AsyncSession = Depends(get_db)):
    """Aggregate metrics from all nodes"""
    try:
        await db.execute(text("SET search_path TO monitoring_schema,public"))

        all_metrics = {}
        async with NetworkClient() as client:
            for node_id, url in settings.NODE_URLS.items():
                metrics = await client.call_node(node_id, "/load")  # Assume /load returns full metrics
                if metrics:
                    all_metrics[node_id] = metrics
                else:
                    logger.warning(f"No metrics from {node_id}")

        if not all_metrics:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No node metrics available"
            )

        tx_result = await db.execute(
            select(func.count()).select_from(TransactionLog).where(TransactionLog.log_timestamp >= datetime.utcnow() - timedelta(hours=1))
        )
        tx_rate = tx_result.scalar() or 0

        edge_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "edge" in nid]
        core_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "core" in nid]
        cloud_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "cloud" in nid]

        await db.execute(
            text("""
                INSERT INTO system_metrics
                (edge_cpu_avg, core_cpu_avg, cloud_cpu_avg, overall_latency_avg, transaction_rate, packet_loss_rate, timestamp)
                VALUES (:edge_cpu, :core_cpu, :cloud_cpu, :latency, :tx_rate, :loss, NOW())
            """),
            {
                "edge_cpu": sum(edge_cpus) / len(edge_cpus) if edge_cpus else 0,
                "core_cpu": sum(core_cpus) / len(core_cpus) if core_cpus else 0,
                "cloud_cpu": sum(cloud_cpus) / len(cloud_cpus) if cloud_cpus else 0,
                "latency": sum(m.get("latency_ms", 0) for m in all_metrics.values()) / len(all_metrics) if all_metrics else 0,
                "tx_rate": tx_rate,
                "loss": 0  # Simulate or calculate from logs
            }
        )
        await db.commit()

        logger.info(f"Aggregated metrics from {len(all_metrics)} nodes")

        clock_manager.create_event({"type": "metrics_aggregated", "nodes": len(all_metrics)})

        return MessageResponse(message=f"Aggregated metrics from {len(all_metrics)} nodes")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to aggregate metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to aggregate metrics: {str(e)}"
        )