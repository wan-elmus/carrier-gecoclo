from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict
import uuid
import asyncio
from datetime import datetime

from app.database import get_db
from app.models import DistributedTransaction, LoadDecision, FaultLog
from app.schemas import (
    TransactionRequest, TransactionResponse, TransactionPrepareResponse,
    LoadBalanceRequest, LoadDecisionResponse, MessageResponse
)
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.locks import DistributedLock
from app.utils.clocks import get_clock_manager
from app.services.transaction import TwoPhaseCommit
from app.services.consensus import ConsensusService
from app.services.load import LoadBalancer

router = APIRouter()
logger = setup_logger(__name__)

clock_manager = get_clock_manager()  # Assume initialized in startup

@router.post("/transaction/begin", response_model=TransactionResponse, status_code=status.HTTP_201_CREATED)
async def begin_transaction(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Begin a distributed transaction (2PC coordinator)"""
    try:
        xid = f"tx_{uuid.uuid4().hex[:8]}"

        valid_participants = []
        for node_id in transaction.participants:
            if node_id in settings.NODE_URLS:
                valid_participants.append({
                    "node_id": node_id,
                    "url": settings.NODE_URLS[node_id],
                    "vote": None,
                    "decided": None
                })
            else:
                logger.warning(f"Unknown participant: {node_id}")

        if not valid_participants:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid participants"
            )

        db_transaction = DistributedTransaction(
            xid=xid,
            coordinator_node=settings.NODE_ID,
            status="INIT",
            participants=valid_participants,
            transaction_type=transaction.transaction_type,
            timeout_ms=transaction.timeout_ms
        )
        db.add(db_transaction)
        await db.commit()
        await db.refresh(db_transaction)

        logger.info(f"Started tx {xid} with {len(valid_participants)} participants")

        background_tasks.add_task(
            TwoPhaseCommit.coordinate_transaction,
            xid,
            transaction.operation_data,
            db
        )

        clock_manager.create_event({"type": "tx_begun", "xid": xid})

        return TransactionResponse.model_validate(db_transaction)
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to begin tx: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to begin tx: {str(e)}"
        )

@router.post("/transaction/{xid}/prepare", response_model=TransactionPrepareResponse)
async def prepare_transaction(
    xid: str,
    db: AsyncSession = Depends(get_db)
):
    """Participant endpoint: Prepare for transaction (2PC phase 1)"""
    try:
        async with DistributedLock("transaction", xid) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Tx {xid} locked"
                )

            async with NetworkClient() as client:
                coordinator_data = await client.call_node(
                    "core_1",  # Assume primary; use backup if failed
                    f"/core/transaction/{xid}"
                )
                if not coordinator_data:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Tx {xid} not found"
                    )

            vote = await TwoPhaseCommit.prepare_local(xid, coordinator_data, db)

            logger.info(f"Node {settings.NODE_ID} voted {vote} for tx {xid}")

            clock_manager.create_event({"type": "tx_prepare", "xid": xid, "vote": vote})

            return TransactionPrepareResponse(xid=xid, node_id=settings.NODE_ID, vote=vote)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to prepare tx: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to prepare tx: {str(e)}"
        )

@router.post("/transaction/{xid}/commit", response_model=MessageResponse)
async def commit_transaction(
    xid: str,
    db: AsyncSession = Depends(get_db)
):
    """Participant endpoint: Commit transaction (2PC phase 2)"""
    try:
        success = await TwoPhaseCommit.commit_local(xid, db)
        if success:
            logger.info(f"Node {settings.NODE_ID} committed tx {xid}")

            clock_manager.create_event({"type": "tx_commit", "xid": xid})

            return MessageResponse(message=f"Tx {xid} committed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to commit tx {xid}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to commit tx: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to commit tx: {str(e)}"
        )

@router.post("/transaction/{xid}/abort", response_model=MessageResponse)
async def abort_transaction(
    xid: str,
    db: AsyncSession = Depends(get_db)
):
    """Participant endpoint: Abort transaction (2PC phase 2)"""
    try:
        success = await TwoPhaseCommit.abort_local(xid, db)
        if success:
            logger.info(f"Node {settings.NODE_ID} aborted tx {xid}")

            clock_manager.create_event({"type": "tx_abort", "xid": xid})

            return MessageResponse(message=f"Tx {xid} aborted")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to abort tx {xid}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to abort tx: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to abort tx: {str(e)}"
        )

@router.get("/transaction/{xid}", response_model=TransactionResponse)
async def get_transaction_status(
    xid: str,
    db: AsyncSession = Depends(get_db)
):
    """Get transaction status (coordinator endpoint)"""
    try:
        result = await db.execute(
            select(DistributedTransaction).where(DistributedTransaction.xid == xid)
        )
        transaction = result.scalar_one_or_none()
        if not transaction:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tx {xid} not found"
            )

        return TransactionResponse.model_validate(transaction)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get tx status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get tx status: {str(e)}"
        )

@router.post("/load/balance", response_model=LoadDecisionResponse)
async def balance_load(
    balance_request: LoadBalanceRequest,
    db: AsyncSession = Depends(get_db)
):
    """Make load balancing decision (migrate session between nodes)"""
    try:
        all_metrics = await LoadBalancer.get_all_node_metrics(db)
        if not all_metrics:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No node metrics available"
            )

        overloaded = [
            {"node_id": nid, "cpu_percent": m["cpu_percent"], "active_sessions": m.get("active_sessions", 0)}
            for nid, m in all_metrics.items() if m.get("cpu_percent", 0) > settings.CPU_THRESHOLD_PERCENT
        ]
        underloaded = [
            {"node_id": nid, "cpu_percent": m["cpu_percent"], "capacity": 100 - m["cpu_percent"]}
            for nid, m in all_metrics.items() if m.get("cpu_percent", 0) < 50
        ]

        if not underloaded:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No suitable target nodes"
            )

        decisions = await LoadBalancer.make_migration_decisions(
            overloaded=overloaded,
            underloaded=underloaded,
            db=db
        )

        recorded_decisions = []
        for decision in decisions:
            recorded = await LoadBalancer.record_load_decision(
                session_id=decision["session_id"],
                source_node=decision["source_node"],
                target_node=decision["target_node"],
                reason=decision["reason"],
                cpu_before=decision["cpu_before"],
                db=db
            )
            recorded_decisions.append(recorded)

        await db.commit()

        logger.info(f"Made {len(recorded_decisions)} load decisions")

        if recorded_decisions:
            return LoadDecisionResponse.model_validate(recorded_decisions[0])
        return LoadDecisionResponse(message="No migrations needed")
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to balance load: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to balance load: {str(e)}"
        )

@router.post("/consensus/vote", response_model=Dict)
async def vote_consensus(
    vote_request: Dict,
    db: AsyncSession = Depends(get_db)
):
    """Participate in consensus voting (leader election, configuration changes)"""
    try:
        term = vote_request.get("term")
        candidate = vote_request.get("candidate_id")
        if not term or not candidate:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing term or candidate_id"
            )

        vote_granted = await ConsensusService.should_grant_vote(
            term=term,
            candidate_id=candidate,
            db=db
        )

        logger.info(f"Node {settings.NODE_ID} voted {'YES' if vote_granted else 'NO'} for {candidate} in term {term}")

        return {
            "term": term,
            "vote_granted": vote_granted,
            "node_id": settings.NODE_ID,
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process vote: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process vote: {str(e)}"
        )

@router.post("/consensus/leader", response_model=Dict)
async def announce_leader(
    announcement: Dict,
    db: AsyncSession = Depends(get_db)
):
    """Receive leader announcement"""
    try:
        leader_id = announcement.get("leader_id")
        term = announcement.get("term")
        if not leader_id or not term:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing leader_id or term"
            )

        await ConsensusService.update_leader(
            leader_id=leader_id,
            term=term,
            db=db
        )

        logger.info(f"Node {settings.NODE_ID} acknowledging {leader_id} as leader for term {term}")

        return {
            "acknowledged": True,
            "node_id": settings.NODE_ID,
            "leader_id": leader_id,
            "term": term
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process leader announcement: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process leader announcement: {str(e)}"
        )

@router.get("/system/overview", response_model=Dict)
async def get_system_overview(db: AsyncSession = Depends(get_db)):
    """Get system-wide overview (node status, load distribution, etc.)"""
    try:
        node_metrics = await LoadBalancer.get_all_node_metrics(db)

        fault_result = await db.execute(
            select(FaultLog).limit(None)
        )
        faults = len(fault_result.scalars().all())

        active_tx_result = await db.execute(
            select(DistributedTransaction).where(DistributedTransaction.status.in_(["PREPARING", "PREPARED"]))
        )
        active_transactions = len(active_tx_result.scalars().all())

        total_nodes = len(node_metrics)
        healthy_nodes = sum(1 for m in node_metrics.values() if m.get("cpu_percent", 100) < 90)
        system_health = (healthy_nodes / total_nodes * 100) if total_nodes > 0 else 0

        return {
            "system_health_percent": system_health,
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_nodes,
            "active_sessions": sum(m.get("active_sessions", 0) for m in node_metrics.values()),
            "active_transactions": active_transactions,
            "recent_faults": faults,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get system overview: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system overview: {str(e)}"
        )