from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
import uuid
import asyncio
from datetime import datetime

from app.database import get_db
from app.models import Subscriber, DbSession
from app.schemas import (
    SubscriberCreate, SubscriberResponse, SubscriberUpdate,
    SessionCreate, SessionResponse, SessionUpdate,
    LoadBalanceRequest, MessageResponse
)
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.locks import DistributedLock
from app.utils.network import NetworkClient, get_network_latency
from app.utils.clocks import get_clock_manager
from app.services.telecom import TelecomService
from app.services.load import LoadBalancer

router = APIRouter()
logger = setup_logger(__name__)

clock_manager = get_clock_manager()  # Assume initialized in startup

@router.post("/subscribers", response_model=SubscriberResponse, status_code=status.HTTP_201_CREATED)
async def create_subscriber(
    subscriber: SubscriberCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new telecom subscriber (simulating HLR registration)"""
    try:
        result = await db.execute(
            select(Subscriber).where(Subscriber.subscriber_id == subscriber.subscriber_id)
        )
        existing = result.scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Subscriber {subscriber.subscriber_id} already exists"
            )

        db_subscriber = Subscriber(
            subscriber_id=subscriber.subscriber_id,
            imsi=subscriber.imsi,
            msisdn=subscriber.msisdn,
            location_area=subscriber.location_area or "unknown",
            service_profile=subscriber.service_profile or {},
            node_id=subscriber.node_id
        )
        db.add(db_subscriber)
        await db.commit()
        await db.refresh(db_subscriber)

        logger.info(f"Created subscriber {subscriber.subscriber_id} on node {settings.NODE_ID}")

        # Replicate to core (async)
        asyncio.create_task(TelecomService.replicate_to_core(db_subscriber, "CREATE"))

        # Event with vector clock
        clock_manager.create_event({"type": "subscriber_created", "id": subscriber.subscriber_id})

        return db_subscriber
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create subscriber: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create subscriber: {str(e)}"
        )

@router.get("/subscribers/{subscriber_id}", response_model=SubscriberResponse)
async def get_subscriber(
    subscriber_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get subscriber information"""
    try:
        result = await db.execute(
            select(Subscriber).where(Subscriber.subscriber_id == subscriber_id)
        )
        subscriber = result.scalar_one_or_none()
        if not subscriber:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscriber {subscriber_id} not found"
            )
        return subscriber
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get subscriber: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get subscriber: {str(e)}"
        )

@router.post("/sessions/start", response_model=SessionResponse, status_code=status.HTTP_201_CREATED)
async def start_session(
    session: SessionCreate,
    db: AsyncSession = Depends(get_db)
):
    try:
        db_session = await TelecomService.setup_session(session, db)
        return db_session
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session_status(
    session_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get session status and metrics"""
    try:
        result = await db.execute(
            select(DbSession).where(DbSession.session_id == session_id)
        )
        db_session = result.scalar_one_or_none()
        if not db_session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session {session_id} not found"
            )

        if db_session.current_node != settings.NODE_ID:
            logger.warning(f"Session {session_id} migrated to {db_session.current_node}")

        current_latency = await TelecomService.calculate_current_latency(session_id)

        response = SessionResponse.model_validate(db_session)
        response.current_latency_ms = current_latency  # Assuming SessionResponse extended with this field

        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get session status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get session status: {str(e)}"
        )

@router.post("/sessions/{session_id}/migrate", response_model=MessageResponse)
async def migrate_session(
    session_id: str,
    migration_request: LoadBalanceRequest,
    db: AsyncSession = Depends(get_db)
):
    success = await TelecomService.handover_session(
        session_id,
        migration_request.preferred_target,
        db
    )
    if not success:
        raise HTTPException(status_code=400, detail="Migration failed")

    return MessageResponse(
        message=f"Session {session_id} migrated successfully"
    )

@router.post("/sessions/{session_id}/end", response_model=MessageResponse)
async def end_session(
    session_id: str,
    db: AsyncSession = Depends(get_db)
):
    success = await TelecomService.terminate_session(session_id, db)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to terminate session")
    return MessageResponse(message=f"Session {session_id} terminated successfully")

@router.get("/load", response_model=Dict)
async def get_edge_load(db: AsyncSession = Depends(get_db)):
    """Get current edge node load metrics"""
    try:
        load_metrics = await LoadBalancer.get_node_load(settings.NODE_ID, db)

        result = await db.execute(
            select(DbSession)
            .where(DbSession.current_node == settings.NODE_ID)
            .where(DbSession.status == "ACTIVE")
        )
        active_sessions = len(result.scalars().all())

        load_metrics["active_sessions"] = active_sessions
        load_metrics["node_id"] = settings.NODE_ID
        load_metrics["node_type"] = "edge"
        load_metrics["timestamp"] = datetime.utcnow().isoformat()

        return load_metrics
    except Exception as e:
        logger.error(f"Failed to get load metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get load metrics: {str(e)}"
        )