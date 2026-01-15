from pydantic import BaseModel, validator, Field
from datetime import datetime
from typing import Optional, Dict, List, Any, Literal

# Subscriber Models
class SubscriberCreate(BaseModel):
    subscriber_id: str = Field(..., min_length=1, max_length=50)
    imsi: str = Field(..., pattern=r"^\d{15}$")
    msisdn: str = Field(..., pattern=r"^\+\d{10,15}$")
    location_area: Optional[str] = None
    service_profile: Optional[Dict[str, Any]] = None
    node_id: str = Field(..., min_length=1, max_length=50)

class SubscriberUpdate(BaseModel):
    status: Optional[Literal["ACTIVE", "SUSPENDED", "TERMINATED"]] = None
    location_area: Optional[str] = None
    service_profile: Optional[Dict[str, Any]] = None

class SubscriberResponse(SubscriberCreate):
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# Session Models
class SessionCreate(BaseModel):
    subscriber_id: str
    session_type: Literal["VOICE", "DATA", "SMS", "SIGNALING"] = "VOICE"
    destination_node: Optional[str] = None
    qos_profile: Optional[Dict[str, Any]] = None
    latency_threshold_ms: int = Field(100, ge=50, le=500)

    @validator("session_type")
    def valid_session_type(cls, v):
        return v

class SessionUpdate(BaseModel):
    status: Optional[Literal["ACTIVE", "TERMINATED", "MIGRATING"]] = None
    current_node: Optional[str] = None
    end_time: Optional[datetime] = None
    data_volume: Optional[int] = Field(None, ge=0)

class SessionResponse(BaseModel):
    session_id: str
    subscriber_id: str
    session_type: str
    source_node: str
    destination_node: Optional[str]
    current_node: str
    status: str
    qos_profile: Dict[str, Any]
    start_time: datetime
    end_time: Optional[datetime]
    latency_threshold_ms: int
    data_volume: int
    migrated_from: Optional[str]
    migration_count: int

    class Config:
        from_attributes = True

# Distributed Transaction Models
class TransactionParticipant(BaseModel):
    node_id: str
    url: str
    vote: Optional[Literal["PREPARE_OK", "PREPARE_FAIL", "UNKNOWN"]] = "UNKNOWN"
    decided: Optional[Literal["COMMIT", "ABORT"]] = None

class TransactionRequest(BaseModel):
    transaction_type: Literal["SESSION_SETUP", "HANDOVER", "BILLING_UPDATE", "MIGRATION"]
    participants: List[str]  # list of node_ids
    operation_data: Dict[str, Any]
    timeout_ms: Optional[int] = 10000

class TransactionPrepareResponse(BaseModel):
    xid: str
    node_id: str
    vote: Literal["PREPARE_OK", "PREPARE_FAIL"]

class TransactionResponse(BaseModel):
    xid: str
    coordinator_node: str
    status: Literal["INIT", "PREPARING", "PREPARED", "COMMITTING", "COMMITTED", "ABORTING", "ABORTED"]
    participants: List[TransactionParticipant]
    transaction_type: str
    created_at: datetime
    prepared_at: Optional[datetime]
    decided_at: Optional[datetime]

    class Config:
        from_attributes = True

# Fault Injection & Recovery
class FaultInjectionRequest(BaseModel):
    fault_type: Literal["crash", "network_delay", "packet_loss", "cpu_spike", "byzantine"]
    target_node: str
    duration_ms: int = Field(5000, ge=1000, le=30000)
    severity: Literal["low", "medium", "high"] = "medium"

class FaultLogResponse(BaseModel):
    fault_id: int
    node_id: str
    fault_type: str
    injected_at: datetime
    detected_at: Optional[datetime]
    recovered_at: Optional[datetime]
    recovery_time_ms: Optional[int]
    affected_sessions: int = 0

    class Config:
        from_attributes = True

# Load Balancing & Migration
class LoadBalanceRequest(BaseModel):
    session_id: str
    reason: str = "cpu_overload"
    preferred_target: Optional[str] = None

class LoadDecisionResponse(BaseModel):
    decision_id: int
    session_id: str
    source_node: str
    target_node: str
    reason: str
    cpu_before: Optional[int]
    cpu_after: Optional[int]
    decision_time: datetime

    class Config:
        from_attributes = True

# Metrics & Health
class NodeMetricsResponse(BaseModel):
    node_id: str
    metric_type: str
    metric_value: float
    timestamp: datetime

    class Config:
        from_attributes = True

class SystemMetricsResponse(BaseModel):
    edge_cpu_avg: float
    core_cpu_avg: float
    cloud_cpu_avg: float
    overall_latency_avg: float
    transaction_rate: int
    packet_loss_rate: float = 0.0
    timestamp: datetime

    class Config:
        from_attributes = True

class NodeHealthResponse(BaseModel):
    node_id: str
    node_type: str
    status: Literal["HEALTHY", "WARNING", "FAILED"]
    last_heartbeat: datetime
    cpu_percent: float
    memory_percent: float
    active_sessions: int = 0

# Utility / Shared
class MessageResponse(BaseModel):
    message: str

class ErrorResponse(BaseModel):
    detail: str
    node_id: Optional[str] = None