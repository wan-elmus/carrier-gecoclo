from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
import uuid
import asyncio
from datetime import datetime

from app.database import get_db
from app.models import Subscriber, Session as DbSession
from app.schemas import (
    SubscriberCreate, SubscriberResponse, SubscriberUpdate,
    SessionCreate, SessionResponse, SessionUpdate,
    LoadBalanceRequest, MessageResponse
)
from app.config import settings
from utils.logger import setup_logger
from utils.locks import DistributedLock
from utils.network import NetworkClient, get_network_latency
from utils.clocks import get_clock_manager
from services.telecom import TelecomService
from services.load import LoadBalancer

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
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Start a new telecom session (call/data session)"""
    try:
        result = await db.execute(
            select(Subscriber).where(Subscriber.subscriber_id == session.subscriber_id)
        )
        subscriber = result.scalar_one_or_none()
        if not subscriber:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscriber {session.subscriber_id} not found"
            )

        session_id = f"sess_{uuid.uuid4().hex[:10]}"

        # Check load & potentially redirect
        current_load = await LoadBalancer.get_node_load(settings.NODE_ID, db)
        if current_load["cpu_percent"] > settings.CPU_THRESHOLD_PERCENT:
            target_node = await LoadBalancer.find_best_edge_node(db, exclude=[settings.NODE_ID])
            if target_node and target_node != settings.NODE_ID:
                logger.info(f"Overloaded ({current_load['cpu_percent']}%). Redirect to {target_node}")
                # In MVP, simulate redirect by raising (real: proxy request)
                raise HTTPException(
                    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
                    detail=f"Redirect to {target_node}",
                    headers={"Location": f"{settings.NODE_URLS[target_node]}/sessions/start"}
                )

        db_session = DbSession(
            session_id=session_id,
            subscriber_id=session.subscriber_id,
            session_type=session.session_type,
            source_node=settings.NODE_ID,
            destination_node=session.destination_node,
            current_node=settings.NODE_ID,
            qos_profile=session.qos_profile or {},
            latency_threshold_ms=session.latency_threshold_ms
        )
        db.add(db_session)
        await db.commit()
        await db.refresh(db_session)

        logger.info(f"Started {session.session_type} session {session_id} for {session.subscriber_id}")

        # Background QoS monitor
        background_tasks.add_task(
            TelecomService.monitor_session_qos, session_id, session.latency_threshold_ms
        )

        # Replicate to core
        asyncio.create_task(TelecomService.replicate_to_core(db_session, "CREATE"))

        # Vector clock event
        clock_manager.create_event({"type": "session_started", "id": session_id})

        return db_session
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to start session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start session: {str(e)}"
        )

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
    """Migrate session to another edge node (load balancing)"""
    try:
        async with DistributedLock("session", session_id) as lock:
            if not await lock.acquire(timeout_ms=5000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Session {session_id} is locked for migration"
                )

            result = await db.execute(
                select(DbSession).where(DbSession.session_id == session_id)
            )
            db_session = result.scalar_one_or_none()
            if not db_session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Session {session_id} not found"
                )

            target_node = migration_request.preferred_target
            if not target_node:
                target_node = await LoadBalancer.find_best_edge_node(db, exclude=[settings.NODE_ID])
            if not target_node or target_node == settings.NODE_ID:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No suitable target node found for migration"
                )

            async with NetworkClient() as client:
                health = await client.call_node(target_node, "/health")
                if not health or health.get("status") != "healthy":
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Target node {target_node} is not healthy"
                    )

            await db.execute(
                update(DbSession)
                .where(DbSession.session_id == session_id)
                .values(
                    current_node=target_node,
                    migrated_from=settings.NODE_ID,
                    migration_count=DbSession.migration_count + 1
                )
            )

            await LoadBalancer.record_migration_decision(
                session_id=session_id,
                source_node=settings.NODE_ID,
                target_node=target_node,
                reason=migration_request.reason,
                db=db
            )
            await db.commit()

            logger.info(f"Migrated session {session_id} from {settings.NODE_ID} to {target_node}")

            asyncio.create_task(TelecomService.replicate_migration(session_id, target_node))

            # Vector clock event
            clock_manager.create_event({"type": "session_migrated", "id": session_id, "target": target_node})

            return MessageResponse(message=f"Session {session_id} migrated to {target_node}")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to migrate session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to migrate session: {str(e)}"
        )

@router.post("/sessions/{session_id}/end", response_model=MessageResponse)
async def end_session(
    session_id: str,
    db: AsyncSession = Depends(get_db)
):
    """End a telecom session"""
    try:
        async with DistributedLock("session", session_id) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Session {session_id} is locked"
                )

            result = await db.execute(
                select(DbSession).where(DbSession.session_id == session_id)
            )
            db_session = result.scalar_one_or_none()
            if not db_session or db_session.status == "TERMINATED":
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Session {session_id} not found or already terminated"
                )

            await db.execute(
                update(DbSession)
                .where(DbSession.session_id == session_id)
                .values(status="TERMINATED", end_time=datetime.utcnow())
            )
            await db.commit()

            logger.info(f"Ended session {session_id}")

            asyncio.create_task(TelecomService.replicate_session_termination(session_id))

            # Vector clock event
            clock_manager.create_event({"type": "session_ended", "id": session_id})

            return MessageResponse(message=f"Session {session_id} terminated successfully")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to end session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to end session: {str(e)}"
        )

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