from sqlalchemy import Column, String, Integer, BigInteger, Boolean, DateTime, Float, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from app.database import Base
from app.config import settings
from datetime import datetime


class Subscriber(Base):
    __tablename__ = "subscribers"
    __table_args__ = {
        "schema": settings.node_schema,
        "extend_existing": True
    }

    subscriber_id = Column(String(50), primary_key=True)
    imsi = Column(String(15), unique=True)
    msisdn = Column(String(15), unique=True)
    status = Column(String(20), default="ACTIVE")
    location_area = Column(String(50))
    service_profile = Column(JSONB)
    node_id = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sessions = relationship("DbSession", back_populates="subscriber")


class DbSession(Base):
    __tablename__ = "sessions"
    __table_args__ = {
        "schema": settings.node_schema,
        "extend_existing": True
    }

    session_id = Column(String(50), primary_key=True)
    subscriber_id = Column(String(50), ForeignKey(f"{settings.node_schema}.subscribers.subscriber_id"))
    session_type = Column(String(20))
    source_node = Column(String(50), nullable=False)
    destination_node = Column(String(50))
    current_node = Column(String(50))
    status = Column(String(20), default="ACTIVE")
    qos_profile = Column(JSONB)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime)
    latency_threshold_ms = Column(Integer, default=100)
    data_volume = Column(BigInteger, default=0)
    migrated_from = Column(String(50))
    migration_count = Column(Integer, default=0)

    subscriber = relationship("Subscriber", back_populates="sessions")


class TransactionLog(Base):
    __tablename__ = "transaction_logs"
    __table_args__ = {
        "schema": settings.node_schema,
        "extend_existing": True
    }

    log_id = Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id = Column(String(36), nullable=False)
    node_id = Column(String(50), nullable=False)
    operation_type = Column(String(50))
    table_name = Column(String(50))
    record_id = Column(String(100))
    old_state = Column(JSONB)
    new_state = Column(JSONB)
    log_timestamp = Column(DateTime, default=datetime.utcnow)
    committed = Column(Boolean, default=False)


class NodeMetrics(Base):
    __tablename__ = "node_metrics"
    __table_args__ = {
        "schema": settings.node_schema,
        "extend_existing": True
    }

    metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    node_id = Column(String(50), nullable=False)
    metric_type = Column(String(50))
    metric_value = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)


class DistributedTransaction(Base):
    __tablename__ = "distributed_transactions"
    __table_args__ = {"schema": "core_schema"}

    xid = Column(String(36), primary_key=True)
    coordinator_node = Column(String(50), nullable=False)
    status = Column(String(20), default="INIT")
    participants = Column(JSONB, nullable=False)
    transaction_type = Column(String(50))
    timeout_ms = Column(Integer, default=10000)
    created_at = Column(DateTime, default=datetime.utcnow)
    prepared_at = Column(DateTime)
    decided_at = Column(DateTime)


class LoadDecision(Base):
    __tablename__ = "load_decisions"
    __table_args__ = {"schema": "core_schema"}

    decision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(String(50))
    source_node = Column(String(50))
    target_node = Column(String(50))
    reason = Column(String(100))
    cpu_before = Column(Float)
    cpu_after = Column(Float)
    decision_time = Column(DateTime, default=datetime.utcnow)


class FaultLog(Base):
    __tablename__ = "fault_logs"
    __table_args__ = {"schema": "monitoring_schema"}

    fault_id = Column(BigInteger, primary_key=True, autoincrement=True)
    node_id = Column(String(50))
    fault_type = Column(String(50))
    injected_at = Column(DateTime, default=datetime.utcnow)
    detected_at = Column(DateTime)
    recovered_at = Column(DateTime)
    recovery_time_ms = Column(Integer)
    affected_sessions = Column(Integer, default=0)


class SystemMetrics(Base):
    __tablename__ = "system_metrics"
    __table_args__ = {"schema": "monitoring_schema"}

    metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    edge_cpu_avg = Column(Float)
    core_cpu_avg = Column(Float)
    cloud_cpu_avg = Column(Float)
    overall_latency_avg = Column(Float)
    transaction_rate = Column(Integer)
    packet_loss_rate = Column(Float, default=0.0)
    timestamp = Column(DateTime, default=datetime.utcnow)


class ConsensusState(Base):
    __tablename__ = "consensus_state"
    __table_args__ = {"schema": "shared_schema"}

    node_id = Column(String(50), primary_key=True)
    current_term = Column(Integer, default=0)
    voted_for = Column(String(50))
    leader_id = Column(String(50))
    last_heartbeat = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default="FOLLOWER")
    cpu_percent = Column(Float, default=0.0)
    memory_percent = Column(Float, default=0.0)


class ServiceRegistry(Base):
    __tablename__ = "service_registry"
    __table_args__ = {"schema": "shared_schema"}

    service_id = Column(String(50), primary_key=True)
    service_type = Column(String(50))
    node_id = Column(String(50))
    endpoint = Column(String(100))
    status = Column(String(20), default="ACTIVE")
    last_updated = Column(DateTime, default=datetime.utcnow)


class NetworkTopology(Base):
    __tablename__ = "network_topology"
    __table_args__ = {"schema": "shared_schema"}

    from_node = Column(String(50), primary_key=True)
    to_node = Column(String(50), primary_key=True)
    latency_ms = Column(Integer)
    bandwidth_mbps = Column(Integer, default=1000)
    status = Column(String(20), default="UP")
    last_checked = Column(DateTime, default=datetime.utcnow)


class Locks(Base):
    __tablename__ = "locks"
    __table_args__ = {"schema": "shared_schema"}

    resource_type = Column(String(50), primary_key=True)
    resource_id = Column(String(100), primary_key=True)
    node_id = Column(String(50), nullable=False)
    acquired_at = Column(DateTime, nullable=False, default=datetime.utcnow)