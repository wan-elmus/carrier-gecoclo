from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
import psutil
import asyncio
from datetime import datetime, timedelta

from app.database import get_db
from app.models import NodeMetrics, DbSession
from app.schemas import NodeMetricsResponse, SystemMetricsResponse, MessageResponse
from app.config import settings
from app.utils.logger import setup_logger, TelecomLogger
from app.utils.heartbeat import NodeHealth
from app.utils.clocks import get_clock_manager
from app.utils.locks import DistributedLock
from app.utils.network import NetworkClient

router = APIRouter()
logger = setup_logger(__name__)
telecom_logger = TelecomLogger()

clock_manager = get_clock_manager()  # Assume initialized in startup

@router.get("/node", response_model=Dict)
async def get_node_metrics(db: AsyncSession = Depends(get_db)):
    """Get current node metrics"""
    try:
        node_health = NodeHealth(settings.NODE_ID)
        metrics = await node_health.collect_metrics()

        result = await db.execute(
            select(func.count()).select_from(DbSession)
            .where(DbSession.current_node == settings.NODE_ID)
            .where(DbSession.status == "ACTIVE")
        )
        active_sessions = result.scalar() or 0

        simulated_latency = 50 + (metrics["cpu_percent"] * 0.5)
        throughput_rps = max(0, 1000 - (metrics["cpu_percent"] * 10))

        response_metrics = {
            "node_id": settings.NODE_ID,
            "node_type": settings.NODE_TYPE,
            "cpu_percent": round(metrics["cpu_percent"], 2),
            "memory_percent": round(metrics["memory_percent"], 2),
            "active_sessions": active_sessions,
            "latency_ms": round(simulated_latency, 2),
            "throughput_rps": round(throughput_rps, 2),
            "timestamp": datetime.utcnow().isoformat()
        }

        await store_node_metrics(response_metrics, db)

        clock_manager.create_event({"type": "node_metrics_collected", "metrics": response_metrics})

        return response_metrics
    except Exception as e:
        logger.error(f"Failed to get node metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get node metrics: {str(e)}"
        )

@router.get("/system", response_model=SystemMetricsResponse)
async def get_system_metrics(db: AsyncSession = Depends(get_db)):
    """Get system-wide aggregated metrics"""
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
                    AVG(packet_loss_rate) as packet_loss,
                    timestamp
                FROM system_metrics
                WHERE timestamp >= :since
            """),
            {"since": one_hour_ago}
        )

        aggregates = result.fetchone()

        return SystemMetricsResponse(
            edge_cpu_avg=float(aggregates.edge_cpu) if aggregates.edge_cpu else 0.0,
            core_cpu_avg=float(aggregates.core_cpu) if aggregates.core_cpu else 0.0,
            cloud_cpu_avg=float(aggregates.cloud_cpu) if aggregates.cloud_cpu else 0.0,
            overall_latency_avg=float(aggregates.latency) if aggregates.latency else 0.0,
            transaction_rate=int(aggregates.tx_rate) if aggregates.tx_rate else 0,
            packet_loss_rate=float(aggregates.packet_loss) if aggregates.packet_loss else 0.0,
            timestamp=aggregates.timestamp if aggregates.timestamp else datetime.utcnow()
        )
    except Exception as e:
        logger.error(f"Failed to get system metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system metrics: {str(e)}"
        )

@router.get("/history", response_model=Dict)
async def get_metrics_history(
    metric_type: str = "cpu",
    hours: int = 24,
    db: AsyncSession = Depends(get_db)
):
    """Get historical metrics data"""
    try:
        since_time = datetime.utcnow() - timedelta(hours=hours)

        result = await db.execute(
            select(NodeMetrics)
            .where(NodeMetrics.metric_type == metric_type)
            .where(NodeMetrics.timestamp >= since_time)
            .order_by(NodeMetrics.timestamp)
        )

        metrics = result.scalars().all()

        data = [
            NodeMetricsResponse.model_validate(m) for m in metrics
        ]

        return {
            "metric_type": metric_type,
            "data_points": len(data),
            "time_range_hours": hours,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get metrics history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get metrics history: {str(e)}"
        )

@router.get("/health", response_model=Dict)
async def get_health_status(db: AsyncSession = Depends(get_db)):
    """Get detailed health status of all nodes"""
    try:
        await db.execute(text("SET search_path TO shared_schema,public"))

        result = await db.execute(
            text("""
                SELECT
                    node_id,
                    status,
                    last_heartbeat,
                    cpu_percent,
                    memory_percent,
                    EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) * 1000 as ms_since
                FROM consensus_state
                ORDER BY node_id
            """)
        )

        nodes = []
        healthy_count = warning_count = failed_count = 0

        for row in result.fetchall():
            node_id, status, last, cpu, memory, ms_since = row

            if ms_since > settings.HEARTBEAT_TIMEOUT_MS:
                health = "FAILED"
                failed_count += 1
            elif ms_since > settings.HEARTBEAT_TIMEOUT_MS * 0.7 or cpu > settings.CPU_THRESHOLD_PERCENT:
                health = "WARNING"
                warning_count += 1
            else:
                health = "HEALTHY"
                healthy_count += 1

            nodes.append({
                "node_id": node_id,
                "status": status,
                "health": health,
                "last_heartbeat": last.isoformat() if last else None,
                "cpu_percent": float(cpu) if cpu else 0.0,
                "memory_percent": float(memory) if memory else 0.0,
                "ms_since_heartbeat": int(ms_since) if ms_since else 0
            })

        total_nodes = len(nodes)
        health_percentage = (healthy_count / total_nodes * 100) if total_nodes > 0 else 0

        return {
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_count,
            "warning_nodes": warning_count,
            "failed_nodes": failed_count,
            "health_percentage": round(health_percentage, 2),
            "nodes": nodes,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get health status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get health status: {str(e)}"
        )

@router.get("/telecom", response_model=Dict)
async def get_telecom_metrics(db: AsyncSession = Depends(get_db)):
    """Get telecom-specific metrics (QoS, latency, etc.)"""
    try:
        await db.execute(text("SET search_path TO monitoring_schema,public"))

        active_result = await db.execute(
            select(func.count()).select_from(DbSession).where(DbSession.status == "ACTIVE")
        )
        total_sessions = active_result.scalar() or 0

        type_result = await db.execute(
            text("""
                SELECT session_type, COUNT(*) as count
                FROM sessions
                WHERE status = 'ACTIVE'
                GROUP BY session_type
            """)
        )
        session_types = {row.session_type: row.count for row in type_result.fetchall()}

        mig_result = await db.execute(
            text("""
                SELECT
                    COUNT(*) as total_migrations,
                    AVG(migration_count) as avg_migrations
                FROM sessions
                WHERE migration_count > 0
            """)
        )
        migration_stats = mig_result.fetchone()

        latency_result = await db.execute(
            text("""
                SELECT AVG(metric_value) as avg_latency
                FROM node_metrics
                WHERE metric_type = 'latency_ms'
                AND timestamp >= NOW() - INTERVAL '1 hour'
            """)
        )
        avg_latency = latency_result.scalar() or 0.0

        sessions_within_qos = total_sessions  # Simulate: all for MVP; real: query based on threshold

        qos_compliance = (sessions_within_qos / total_sessions * 100) if total_sessions > 0 else 100

        return {
            "total_active_sessions": total_sessions,
            "sessions_by_type": session_types,
            "average_latency_ms": round(avg_latency, 2),
            "qos_compliance_percent": round(qos_compliance, 2),
            "total_migrations": migration_stats.total_migrations if migration_stats else 0,
            "avg_migrations_per_session": float(migration_stats.avg_migrations) if migration_stats and migration_stats.avg_migrations else 0.0,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get telecom metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get telecom metrics: {str(e)}"
        )

@router.post("/collect", response_model=MessageResponse)
async def collect_metrics(db: AsyncSession = Depends(get_db)):
    """Trigger manual metrics collection"""
    try:
        async with DistributedLock("metrics_collect", settings.NODE_ID) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Metrics collection locked"
                )

            node_health = NodeHealth(settings.NODE_ID)
            metrics = await node_health.collect_metrics()

            await store_node_metrics(metrics, db)

            logger.info(f"Manual metrics collected for {settings.NODE_ID}")

            clock_manager.create_event({"type": "manual_metrics_collect", "metrics": metrics})

            return MessageResponse(message=f"Metrics collected: CPU={metrics['cpu_percent']}%, Memory={metrics['memory_percent']}%")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to collect metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to collect metrics: {str(e)}"
        )

@router.post("/aggregate", response_model=MessageResponse)
async def aggregate_system_metrics(db: AsyncSession = Depends(get_db)):
    """Aggregate system-wide metrics from all nodes"""
    try:
        async with NetworkClient() as client:
            all_metrics = {}
            for node_id, url in settings.NODE_URLS.items():
                metrics = await client.call_node(node_id, "/metrics/node")
                if metrics:
                    all_metrics[node_id] = metrics

        if not all_metrics:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No node metrics available"
            )

        edge_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "edge" in nid]
        core_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "core" in nid]
        cloud_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "cloud" in nid]

        latency_avg = sum(m["latency_ms"] for m in all_metrics.values()) / len(all_metrics) if all_metrics else 0

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
                "latency": latency_avg,
                "tx_rate": sum(m["throughput_rps"] for m in all_metrics.values()),
                "loss": 0  # Simulate
            }
        )
        await db.commit()

        logger.info(f"Aggregated metrics from {len(all_metrics)} nodes")

        clock_manager.create_event({"type": "metrics_aggregated", "nodes": len(all_metrics)})

        return MessageResponse(message=f"Aggregated from {len(all_metrics)} nodes")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to aggregate metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to aggregate metrics: {str(e)}"
        )

async def store_node_metrics(metrics: Dict, db: AsyncSession) -> None:
    """Store node metrics in database"""
    try:
        for m_type, value in metrics.items():
            if m_type in ["cpu_percent", "memory_percent", "latency_ms", "throughput_rps"]:
                db_metric = NodeMetrics(
                    node_id=settings.NODE_ID,
                    metric_type=m_type,
                    metric_value=value
                )
                db.add(db_metric)

        await db.commit()
    except Exception as e:
        logger.warning(f"Failed to store metrics: {e}")
        await db.rollback()