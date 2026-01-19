from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict
import uuid
import asyncio
from datetime import datetime
from fastapi.responses import JSONResponse

from app.database import get_db
from app.models import DistributedTransaction, LoadDecision, FaultLog
from app.schemas import (
    TransactionRequest, TransactionResponse, TransactionPrepareResponse,
    LoadBalanceRequest, LoadDecisionResponse, MessageResponse, TransactionParticipant
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

clock_manager = get_clock_manager()

@router.post("/transaction/begin", response_model=TransactionResponse, status_code=status.HTTP_201_CREATED)
async def begin_transaction(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    try:
        xid = f"tx_{uuid.uuid4().hex[:8]}"

        participants_list = []
        for p in transaction.participants:
            participants_list.append({
                "node_id": p.node_id,
                "url": p.url,
                "vote": "UNKNOWN",
                "decided": None
            })

        db_transaction = DistributedTransaction(
            xid=xid,
            coordinator_node=settings.NODE_ID,
            status="INIT",
            participants=participants_list,
            transaction_type=transaction.transaction_type,
            timeout_ms=transaction.timeout_ms
        )
        db.add(db_transaction)
        await db.commit()
        await db.refresh(db_transaction)

        logger.info(f"Started tx {xid} with {len(participants_list)} participants")

        background_tasks.add_task(
            TwoPhaseCommit.coordinate_transaction,
            xid,
            transaction.operation_data,
            db
        )

        clock_manager.create_event({"type": "tx_begun", "xid": xid})

        response_data = {
            "xid": db_transaction.xid,
            "coordinator_node": db_transaction.coordinator_node,
            "status": db_transaction.status,
            "participants": [
                TransactionParticipant(**p) for p in db_transaction.participants
            ],
            "transaction_type": db_transaction.transaction_type,
            "created_at": db_transaction.created_at
        }

        return TransactionResponse(**response_data)

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to begin tx: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to begin tx: {str(e)}"
        )

@router.post("/transaction/{xid}/prepare", response_model=TransactionPrepareResponse)
async def prepare_transaction(
    xid: str,
    db: AsyncSession = Depends(get_db)
):
    try:
        async with DistributedLock("transaction", xid) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Tx {xid} locked"
                )

            vote = await TwoPhaseCommit.prepare_local(xid, db)

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
    try:
        db_metrics = await LoadBalancer.get_all_node_metrics(db)

        overloaded = [
            {"node_id": nid, "cpu_percent": m["cpu_percent"], "active_sessions": m.get("active_sessions", 0)}
            for nid, m in db_metrics.items() if m.get("cpu_percent", 0) > settings.CPU_THRESHOLD_PERCENT
        ]
        underloaded = [
            {"node_id": nid, "cpu_percent": m["cpu_percent"], "capacity": 100 - m["cpu_percent"]}
            for nid, m in db_metrics.items() if m.get("cpu_percent", 0) < 50
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

        if not recorded_decisions:
            return JSONResponse(
                status_code=200,
                content={"message": "No migrations needed - all nodes under threshold"}
            )
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

@router.post("/consensus/heartbeat", response_model=Dict)
async def receive_heartbeat(
    heartbeat: Dict,
    db: AsyncSession = Depends(get_db)
):
    try:
        term = heartbeat.get("term")
        leader_id = heartbeat.get("leader_id")
        if not term or not leader_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing term or leader_id"
            )

        from app.services.consensus import get_consensus_service
        service = get_consensus_service()
        if term < service.current_term:
            return {"term": service.current_term, "success": False, "reason": "Stale term"}

        service.last_heartbeat = datetime.utcnow()
        service.leader_id = leader_id
        service.current_term = max(service.current_term, term)
        await service._save_state(db)

        logger.debug(f"Heartbeat received from {leader_id} for term {term}")

        clock_manager.create_event({"type": "heartbeat_received", "leader": leader_id})

        return {
            "success": True,
            "term": service.current_term,
            "node_id": settings.NODE_ID,
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process heartbeat: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process heartbeat: {str(e)}"
        )

@router.post("/consensus/leader", response_model=Dict)
async def announce_leader(
    announcement: Dict,
    db: AsyncSession = Depends(get_db)
):
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

        total_nodes = len(node_metrics) or 1
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