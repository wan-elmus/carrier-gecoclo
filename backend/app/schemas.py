from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, List, Any, Literal


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


class SessionCreate(BaseModel):
    subscriber_id: str
    session_type: Literal["VOICE", "DATA", "SMS", "SIGNALING"] = "VOICE"
    destination_node: Optional[str] = None
    qos_profile: Optional[Dict[str, Any]] = None
    latency_threshold_ms: int = Field(100, ge=50, le=500)
    source_node: Optional[str] = None


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
    current_latency_ms: Optional[float] = None

    class Config:
        from_attributes = True


class TransactionParticipant(BaseModel):
    node_id: str
    url: str
    vote: Optional[Literal["PREPARE_OK", "PREPARE_FAIL", "UNKNOWN"]] = "UNKNOWN"
    decided: Optional[Literal["COMMIT", "ABORT"]] = None


class TransactionRequest(BaseModel):
    transaction_type: Literal["SESSION_SETUP", "HANDOVER", "BILLING_UPDATE", "MIGRATION"]
    participants: List[TransactionParticipant]
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


class LoadBalanceRequest(BaseModel):
    session_id: str
    reason: str = "cpu_overload"
    preferred_target: Optional[str] = None


class LoadDecisionResponse(BaseModel):
    decision_id: Optional[int] = None
    session_id: str
    source_node: str
    target_node: str
    reason: str
    cpu_before: Optional[float] = None
    cpu_after: Optional[float] = None
    decision_time: Optional[datetime] = None

    class Config:
        from_attributes = True


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
    detected_at: Optional[datetime] = None
    recovered_at: Optional[datetime] = None
    recovery_time_ms: Optional[int] = None
    affected_sessions: int = 0

    class Config:
        from_attributes = True


class MessageResponse(BaseModel):
    message: str