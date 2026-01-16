import asyncio
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from enum import Enum

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from app.database import get_db
from app.models import DistributedTransaction, TransactionLog, DbSession
# from app.schemas import TransactionParticipant
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.locks import DistributedLock, TransactionLocks
from app.utils.clocks import get_clock_manager

logger = setup_logger(__name__)
clock_manager = get_clock_manager()  # Assume initialized in startup

class TransactionStatus(Enum):
    INIT = "INIT"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"

class TwoPhaseCommit:
    """Two-Phase Commit protocol implementation"""

    @staticmethod
    async def coordinate_transaction(
        xid: str,
        operation_data: Dict[str, Any],
        db: AsyncSession
    ) -> bool:
        """Coordinate a distributed transaction as coordinator (called from core /transaction/begin background)"""
        try:
            logger.info(f"Starting 2PC for tx {xid}")

            prepare_success = await TwoPhaseCommit._prepare_phase(xid, operation_data, db)

            if not prepare_success:
                await TwoPhaseCommit._abort_transaction(xid, db)
                return False

            commit_success = await TwoPhaseCommit._commit_phase(xid, db)

            clock_manager.create_event({"type": "tx_coordinated", "xid": xid, "success": commit_success})

            return commit_success
        except Exception as e:
            logger.error(f"Coordination failed for {xid}: {e}")
            await TwoPhaseCommit._abort_transaction(xid, db)
            return False

    @staticmethod
    async def _prepare_phase(xid: str, operation_data: Dict, db: AsyncSession) -> bool:
        """Execute Phase 1: Prepare"""
        async with DistributedLock("tx_prepare", xid) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise ValueError(f"Tx {xid} prepare locked")

            result = await db.execute(
                select(DistributedTransaction).where(DistributedTransaction.xid == xid)
            )
            transaction = result.scalar_one_or_none()

            if not transaction:
                raise ValueError(f"Transaction {xid} not found")

            await db.execute(
                update(DistributedTransaction)
                .where(DistributedTransaction.xid == xid)
                .values(status=TransactionStatus.PREPARING.value, prepared_at=datetime.utcnow())
            )
            await db.commit()

            participants = transaction.participants
            prepare_tasks = [
                TwoPhaseCommit._request_prepare(p["node_id"], p["url"], xid, operation_data)
                for p in participants
            ]

            results = await asyncio.gather(*prepare_tasks, return_exceptions=True)

            prepare_ok = 0
            prepare_fail = 0
            for i, res in enumerate(results):
                p = participants[i]
                if isinstance(res, Exception) or not res or res.get("vote") != "PREPARE_OK":
                    p["vote"] = "PREPARE_FAIL"
                    prepare_fail += 1
                else:
                    p["vote"] = "PREPARE_OK"
                    prepare_ok += 1

            new_status = TransactionStatus.PREPARED.value if prepare_fail == 0 else TransactionStatus.ABORTING.value

            await db.execute(
                update(DistributedTransaction)
                .where(DistributedTransaction.xid == xid)
                .values(participants=participants, status=new_status)
            )
            await db.commit()

            clock_manager.create_event({"type": "tx_prepare_phase", "xid": xid, "votes_ok": prepare_ok})

            return prepare_fail == 0

    @staticmethod
    async def _commit_phase(xid: str, db: AsyncSession) -> bool:
        """Execute Phase 2: Commit"""
        async with DistributedLock("tx_commit", xid) as lock:
            if not await lock.acquire(timeout_ms=3000):
                raise ValueError(f"Tx {xid} commit locked")

            result = await db.execute(
                select(DistributedTransaction).where(DistributedTransaction.xid == xid)
            )
            transaction = result.scalar_one_or_none()

            if not transaction:
                raise ValueError(f"Transaction {xid} not found")

            await db.execute(
                update(DistributedTransaction)
                .where(DistributedTransaction.xid == xid)
                .values(status=TransactionStatus.COMMITTING.value)
            )
            await db.commit()

            participants = transaction.participants
            commit_tasks = []
            commit_indices = []
            for i, p in enumerate(participants):
                if p.get("vote") == "PREPARE_OK":
                    commit_tasks.append(TwoPhaseCommit._request_commit(p["node_id"], p["url"], xid))
                    commit_indices.append(i)

            results = await asyncio.gather(*commit_tasks, return_exceptions=True)

            commit_ok = 0
            commit_fail = 0
            for res_idx, res in enumerate(results):
                i = commit_indices[res_idx]
                p = participants[i]
                if isinstance(res, Exception) or not res or not res.get("success"):
                    p["decided"] = "ABORT"
                    commit_fail += 1
                else:
                    p["decided"] = "COMMIT"
                    commit_ok += 1

            final_status = TransactionStatus.COMMITTED.value if commit_fail == 0 else TransactionStatus.ABORTED.value

            await db.execute(
                update(DistributedTransaction)
                .where(DistributedTransaction.xid == xid)
                .values(participants=participants, status=final_status, decided_at=datetime.utcnow())
            )
            await db.commit()

            clock_manager.create_event({"type": "tx_commit_phase", "xid": xid, "commits_ok": commit_ok})

            return commit_fail == 0

    @staticmethod
    async def _abort_transaction(xid: str, db: AsyncSession):
        """Abort a transaction"""
        try:
            result = await db.execute(
                select(DistributedTransaction).where(DistributedTransaction.xid == xid)
            )
            transaction = result.scalar_one_or_none()

            if transaction:
                participants = transaction.participants
                abort_tasks = [
                    TwoPhaseCommit._request_abort(p["node_id"], p["url"], xid)
                    for p in participants if p.get("vote") == "PREPARE_OK"
                ]

                await asyncio.gather(*abort_tasks, return_exceptions=True)

                await db.execute(
                    update(DistributedTransaction)
                    .where(DistributedTransaction.xid == xid)
                    .values(status=TransactionStatus.ABORTED.value, decided_at=datetime.utcnow())
                )
                await db.commit()

                logger.info(f"Tx {xid} aborted")
                clock_manager.create_event({"type": "tx_aborted", "xid": xid})
        except Exception as e:
            logger.error(f"Abort failed for {xid}: {e}")

    @staticmethod
    async def _request_prepare(node_id: str, url: str, xid: str, operation_data: Dict) -> Optional[Dict]:
        """Request participant prepare"""
        async with NetworkClient() as client:
            return await client.call_node(node_id, f"/transaction/{xid}/prepare", data={"xid": xid, "operation_data": operation_data})

    @staticmethod
    async def _request_commit(node_id: str, url: str, xid: str) -> Optional[Dict]:
        """Request participant commit"""
        async with NetworkClient() as client:
            return await client.call_node(node_id, f"/transaction/{xid}/commit", data={"xid": xid})

    @staticmethod
    async def _request_abort(node_id: str, url: str, xid: str) -> Optional[Dict]:
        """Request participant abort"""
        async with NetworkClient() as client:
            return await client.call_node(node_id, f"/transaction/{xid}/abort", data={"xid": xid})

    # Participant-side methods

    @staticmethod
    async def prepare_local(xid: str, transaction_info: Dict, db: AsyncSession) -> str:
        """Participant: Prepare local transaction"""
        async with TransactionLocks.lock_resources(db, xid, transaction_info.get("resources", {})):
            operation_data = transaction_info.get("operation_data")
            log = TransactionLog(
                transaction_id=xid,
                node_id=settings.NODE_ID,
                operation_type="2PC_PREPARE",
                old_state=operation_data.get("old_state"),
                new_state=operation_data.get("new_state")
            )
            db.add(log)
            await db.commit()

            vote = "PREPARE_OK" if random.random() > 0.1 else "PREPARE_FAIL"  # Simulate 10% failure

            clock_manager.create_event({"type": "local_prepare", "xid": xid, "vote": vote})

            return vote

    @staticmethod
    async def commit_local(xid: str, db: AsyncSession) -> bool:
        """Participant: Commit local transaction"""
        try:
            result = await db.execute(
                select(TransactionLog)
                .where(TransactionLog.transaction_id == xid)
                .where(TransactionLog.node_id == settings.NODE_ID)
            )
            log = result.scalar_one_or_none()
            if not log:
                raise ValueError(f"No prepare log for {xid}")

            # Apply changes (example: update session status if applicable)
            if log.operation_type == "SESSION_UPDATE":
                await db.execute(
                    update(DbSession)
                    .where(DbSession.session_id == log.record_id)
                    .values(status=log.new_state.get("status"))
                )

            log.committed = True
            await db.commit()

            clock_manager.create_event({"type": "local_commit", "xid": xid})

            return True
        except Exception as e:
            logger.error(f"Local commit failed for {xid}: {e}")
            return False

    @staticmethod
    async def abort_local(xid: str, db: AsyncSession) -> bool:
        """Participant: Abort local transaction"""
        try:
            result = await db.execute(
                select(TransactionLog)
                .where(TransactionLog.transaction_id == xid)
                .where(TransactionLog.node_id == settings.NODE_ID)
            )
            log = result.scalar_one_or_none()
            if log:
                # Rollback changes (example: revert session status)
                if log.operation_type == "SESSION_UPDATE":
                    await db.execute(
                        update(DbSession)
                        .where(DbSession.session_id == log.record_id)
                        .values(status=log.old_state.get("status"))
                    )
                await db.delete(log)
                await db.commit()

            clock_manager.create_event({"type": "local_abort", "xid": xid})

            return True
        except Exception as e:
            logger.error(f"Local abort failed for {xid}: {e}")
            return False

    @staticmethod
    async def recover_incomplete_transactions(db: AsyncSession):
        """Recover incomplete transactions after node restart (called in startup if needed)"""
        try:
            result = await db.execute(
                select(TransactionLog)
                .where(TransactionLog.node_id == settings.NODE_ID)
                .where(TransactionLog.committed.is_(None))
            )
            incomplete = result.scalars().all()

            for log in incomplete:
                logger.warning(f"Recovering incomplete tx {log.transaction_id}")
                if datetime.utcnow() - log.log_timestamp > timedelta(minutes=5):
                    await TwoPhaseCommit.abort_local(log.transaction_id, db)
                else:
                    # Query coordinator for decision (simulated - in real: network call)
                    await TwoPhaseCommit.commit_local(log.transaction_id, db)  # Assume commit for MVP

            clock_manager.create_event({"type": "tx_recovery_run"})
        except Exception as e:
            logger.error(f"Recovery failed: {e}")