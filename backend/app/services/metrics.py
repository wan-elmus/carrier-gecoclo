import asyncio
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import psutil
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from app.models import NodeMetrics, SystemMetrics
from app.config import settings
from utils.logger import setup_logger, TelecomLogger
from utils.network import NetworkClient
from utils.clocks import get_clock_manager
from utils.locks import DistributedLock
from app.database import AsyncSessionLocal

logger = setup_logger(__name__)
telecom_logger = TelecomLogger()
clock_manager = get_clock_manager()  # Assume initialized in startup

class MetricsService:
    """Metrics collection and analysis service"""

    @staticmethod
    async def collect_node_metrics() -> Dict[str, float]:
        """Collect current node metrics"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.5)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            disk_io = psutil.disk_io_counters()
            disk_read_mb = disk_io.read_bytes / (1024 * 1024) if disk_io else 0
            disk_write_mb = disk_io.write_bytes / (1024 * 1024) if disk_io else 0
            net_io = psutil.net_io_counters()
            net_sent_mb = net_io.bytes_sent / (1024 * 1024) if net_io else 0
            net_recv_mb = net_io.bytes_recv / (1024 * 1024) if net_io else 0

            metrics = {
                "cpu_percent": round(cpu_percent, 2),
                "memory_percent": round(memory_percent, 2),
                "disk_read_mb": round(disk_read_mb, 2),
                "disk_write_mb": round(disk_write_mb, 2),
                "network_sent_mb": round(net_sent_mb, 2),
                "network_recv_mb": round(net_recv_mb, 2),
                "timestamp": datetime.utcnow().isoformat()
            }

            telecom_logger.log_metric(logger, "node_metrics", 1.0, {
                "node_id": settings.NODE_ID,
                "cpu": metrics["cpu_percent"],
                "memory": metrics["memory_percent"],
                "disk_read": metrics["disk_read_mb"],
                "disk_write": metrics["disk_write_mb"]
            })

            clock_manager.create_event({"type": "node_metrics_collected", "metrics": metrics})

            return metrics
        except Exception as e:
            logger.error(f"Collect node metrics failed: {e}")
            return {}

    @staticmethod
    async def store_node_metrics(metrics: Dict[str, float], db: AsyncSession):
        """Store node metrics in database"""
        try:
            async with DistributedLock("metrics_store", settings.NODE_ID) as lock:
                if not await lock.acquire(timeout_ms=3000):
                    raise ValueError("Metrics store locked")

                for m_type, value in metrics.items():
                    if m_type != "timestamp":
                        metric = NodeMetrics(
                            node_id=settings.NODE_ID,
                            metric_type=m_type,
                            metric_value=value
                        )
                        db.add(metric)

                await db.commit()
            logger.debug(f"Stored metrics for {settings.NODE_ID}")

            clock_manager.create_event({"type": "metrics_stored"})
        except Exception as e:
            await db.rollback()
            logger.warning(f"Store metrics failed: {e}")

    @staticmethod
    async def collect_system_metrics():
        """Collect system-wide metrics (for monitoring node, run in background from startup)"""
        try:
            logger.info("Starting system metrics collection")

            while True:
                all_metrics = await MetricsService._collect_all_node_metrics()

                if all_metrics:
                    aggregated = await MetricsService._aggregate_metrics(all_metrics)
                    await MetricsService._store_system_metrics(aggregated)
                    await MetricsService._export_metrics_to_csv(aggregated)

                await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"System collection failed: {e}")

    @staticmethod
    async def _collect_all_node_metrics() -> Dict[str, Dict]:
        """Collect metrics from all nodes"""
        try:
            all_metrics = {}
            async with NetworkClient() as client:
                for nid, url in settings.NODE_URLS.items():
                    metrics = await client.call_node(nid, "/metrics/node")
                    if metrics:
                        all_metrics[nid] = metrics
                    else:
                        all_metrics[nid] = {"cpu_percent": 0, "memory_percent": 0, "active_sessions": 0}
            return all_metrics
        except Exception as e:
            logger.error(f"Collect all metrics failed: {e}")
            return {}

    @staticmethod
    async def _aggregate_metrics(all_metrics: Dict[str, Dict]) -> Dict[str, float]:
        """Aggregate metrics from all nodes"""
        try:
            edge_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "edge" in nid]
            core_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "core" in nid]
            cloud_cpus = [m["cpu_percent"] for nid, m in all_metrics.items() if "cloud" in nid]

            edge_cpu_avg = sum(edge_cpus) / len(edge_cpus) if edge_cpus else 0
            core_cpu_avg = sum(core_cpus) / len(core_cpus) if core_cpus else 0
            cloud_cpu_avg = sum(cloud_cpus) / len(cloud_cpus) if cloud_cpus else 0

            latency_avg = sum(m["latency_ms"] for m in all_metrics.values()) / len(all_metrics) if all_metrics else 0
            tx_rate = sum(m["throughput_rps"] for m in all_metrics.values())  # Simulate tx_rate

            return {
                "edge_cpu_avg": round(edge_cpu_avg, 2),
                "core_cpu_avg": round(core_cpu_avg, 2),
                "cloud_cpu_avg": round(cloud_cpu_avg, 2),
                "overall_latency_avg": round(latency_avg, 2),
                "transaction_rate": int(tx_rate),
                "packet_loss_rate": 0,  # Simulate
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Aggregate metrics failed: {e}")
            return {}

    @staticmethod
    async def _store_system_metrics(metrics: Dict[str, float]):
        """Store system metrics in monitoring schema"""
        try:
            async with AsyncSessionLocal() as db:
                await db.execute(text("SET search_path TO monitoring_schema,public"))

                system_metric = SystemMetrics(**metrics)
                db.add(system_metric)
                await db.commit()

            logger.debug(f"Stored system metrics at {metrics['timestamp']}")

            clock_manager.create_event({"type": "system_metrics_stored"})
        except Exception as e:
            logger.error(f"Store system metrics failed: {e}")

    @staticmethod
    async def _export_metrics_to_csv(metrics: Dict[str, float]):
        """Export metrics to CSV file for analysis"""
        try:
            csv_path = Path("logs/system_metrics.csv")
            csv_path.parent.mkdir(exist_ok=True)

            write_header = not csv_path.exists()

            with csv_path.open("a", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(metrics.keys())
                writer.writerow(metrics.values())

            logger.debug(f"Exported metrics to CSV")

            clock_manager.create_event({"type": "metrics_exported"})
        except Exception as e:
            logger.error(f"Export to CSV failed: {e}")

    @staticmethod
    async def generate_performance_report(
        start_time: datetime,
        end_time: datetime,
        db: AsyncSession
    ) -> Dict:
        """Generate performance report for time period"""
        try:
            await db.execute(text("SET search_path TO monitoring_schema,public"))

            result = await db.execute(
                select(SystemMetrics)
                .where(SystemMetrics.timestamp.between(start_time, end_time))
                .order_by(SystemMetrics.timestamp)
            )
            data = result.scalars().all()

            if not data:
                return {"error": "No data for period"}

            edge_cpus = [m.edge_cpu_avg for m in data]
            core_cpus = [m.core_cpu_avg for m in data]
            cloud_cpus = [m.cloud_cpu_avg for m in data]
            latencies = [m.overall_latency_avg for m in data]
            tx_rates = [m.transaction_rate for m in data]

            def stats(values: List[float]) -> Dict:
                if not values:
                    return {}
                return {
                    "avg": round(sum(values) / len(values), 2),
                    "min": round(min(values), 2),
                    "max": round(max(values), 2),
                    "p95": round(sorted(values)[int(len(values) * 0.95)], 2) if len(values) > 1 else values[0]
                }

            report = {
                "period": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                    "duration_hours": round((end_time - start_time).total_seconds() / 3600, 2)
                },
                "statistics": {
                    "edge_cpu": stats(edge_cpus),
                    "core_cpu": stats(core_cpus),
                    "cloud_cpu": stats(cloud_cpus),
                    "latency": stats(latencies),
                    "transaction_rate": stats(tx_rates)
                },
                "summary": {
                    "total_data_points": len(data),
                    "avg_system_load": round((sum(edge_cpus + core_cpus + cloud_cpus) / (3 * len(data))), 2),
                    "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0,
                    "avg_transaction_rate": round(sum(tx_rates) / len(tx_rates), 2) if tx_rates else 0
                }
            }

            logger.info(f"Generated report for {len(data)} points")

            clock_manager.create_event({"type": "performance_report_generated"})

            return report
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return {"error": str(e)}

    @staticmethod
    async def cleanup_old_metrics(days_to_keep: int = 7, db: AsyncSession = None):
        """Clean up old metrics data"""
        try:
            if db is None:
                async with AsyncSessionLocal() as session:
                    cutoff = datetime.utcnow() - timedelta(days=days_to_keep)

                    await session.execute(
                        text("""
                            DELETE FROM node_metrics
                            WHERE timestamp < :cutoff
                        """),
                        {"cutoff": cutoff}
                    )

                    await session.execute(text("SET search_path TO monitoring_schema,public"))

                    await session.execute(
                        text("""
                            DELETE FROM system_metrics
                            WHERE timestamp < :cutoff
                        """),
                        {"cutoff": cutoff}
                    )

                    await session.commit()
            else:
                session = db
                cutoff = datetime.utcnow() - timedelta(days=days_to_keep)

                await session.execute(
                    text("""
                        DELETE FROM node_metrics
                        WHERE timestamp < :cutoff
                    """),
                    {"cutoff": cutoff}
                )

                await session.execute(text("SET search_path TO monitoring_schema,public"))

                await session.execute(
                    text("""
                        DELETE FROM system_metrics
                        WHERE timestamp < :cutoff
                    """),
                    {"cutoff": cutoff}
                )

                await session.commit()

            logger.info(f"Cleaned metrics older than {days_to_keep} days")

            clock_manager.create_event({"type": "metrics_cleaned"})
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")