import asyncio
import random
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select, update
from app.models import ConsensusState
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.network import NetworkClient
from app.utils.locks import DistributedLock
from app.utils.clocks import get_clock_manager
from app.database import AsyncSessionLocal

logger = setup_logger(__name__)
clock_manager = get_clock_manager()

class NodeRole(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class ConsensusService:
    def __init__(self):
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.role: NodeRole = NodeRole.FOLLOWER
        self.leader_id: Optional[str] = None
        self.last_heartbeat: datetime = datetime.utcnow()
        self.election_timeout: float = random.uniform(1.5, 3.0)
        self.heartbeat_interval: float = settings.HEARTBEAT_INTERVAL_MS / 1000.0

    async def start(self, db: AsyncSession):
        logger.info(f"Starting consensus for {settings.NODE_ID}")
        await self._load_state(db)
        asyncio.create_task(self._run_consensus_loop())

    async def _load_state(self, db: AsyncSession):
        try:
            result = await db.execute(
                select(ConsensusState).where(ConsensusState.node_id == settings.NODE_ID)
            )
            state = result.scalar_one_or_none()
            if state:
                self.current_term = state.current_term
                self.voted_for = state.voted_for
                self.role = NodeRole(state.status)
                self.leader_id = state.leader_id
                self.last_heartbeat = state.last_heartbeat
            else:
                await db.execute(
                    insert(ConsensusState).values(
                        node_id=settings.NODE_ID,
                        current_term=0,
                        voted_for=None,
                        leader_id=None,
                        last_heartbeat=datetime.utcnow(),
                        status=NodeRole.FOLLOWER.value
                    )
                )
                await db.commit()

            logger.info(f"Loaded state: term={self.current_term}, role={self.role}")
            clock_manager.create_event({"type": "consensus_loaded", "term": self.current_term})
        except Exception as e:
            logger.error(f"State load failed: {e}")

    async def _save_state(self, db: AsyncSession):
        try:
            await db.execute(
                update(ConsensusState)
                .where(ConsensusState.node_id == settings.NODE_ID)
                .values(
                    current_term=self.current_term,
                    voted_for=self.voted_for,
                    leader_id=self.leader_id,
                    last_heartbeat=self.last_heartbeat,
                    status=self.role.value
                )
            )
            await db.commit()
        except Exception as e:
            logger.error(f"State save failed: {e}")

    async def _run_consensus_loop(self):
        while True:
            try:
                async with AsyncSessionLocal() as db:
                    if self.role == NodeRole.FOLLOWER:
                        await self._follower_loop(db)
                    elif self.role == NodeRole.CANDIDATE:
                        await self._candidate_loop(db)
                    elif self.role == NodeRole.LEADER:
                        await self._leader_loop(db)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Consensus loop error: {e}")
                await asyncio.sleep(1)

    async def _follower_loop(self, db: AsyncSession):
        time_since = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        if time_since > self.election_timeout:
            logger.info(f"No heartbeat ({time_since:.1f}s). Starting election.")
            self.role = NodeRole.CANDIDATE
            self.current_term += 1
            self.voted_for = settings.NODE_ID
            await self._save_state(db)
            clock_manager.create_event({"type": "election_started", "term": self.current_term})

    async def _candidate_loop(self, db: AsyncSession):
        async with DistributedLock("election", str(self.current_term)) as lock:
            if not await lock.acquire(timeout_ms=3000):
                return

            logger.info(f"Election for term {self.current_term}")
            votes = 1
            total = len(settings.NODE_URLS)

            async with NetworkClient() as client:
                tasks = [self._request_vote(nid, client) for nid in settings.NODE_URLS if nid != settings.NODE_ID]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if not isinstance(res, Exception) and res and res.get("vote_granted"):
                        votes += 1

            if votes > total / 2:
                logger.info(f"Won election term {self.current_term} with {votes}/{total} votes")
                self.role = NodeRole.LEADER
                self.leader_id = settings.NODE_ID
                await self._save_state(db)
                await self._announce_leadership()
                clock_manager.create_event({"type": "election_won", "term": self.current_term})
            else:
                logger.info(f"Lost election term {self.current_term}")
                self.role = NodeRole.FOLLOWER
                await self._save_state(db)
                clock_manager.create_event({"type": "election_lost", "term": self.current_term})

    async def _leader_loop(self, db: AsyncSession):
        async with NetworkClient() as client:
            tasks = [self._send_heartbeat(nid, client) for nid in settings.NODE_URLS if nid != settings.NODE_ID]
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(self.heartbeat_interval)

    async def _request_vote(self, node_id: str, client: NetworkClient) -> Optional[Dict]:
        try:
            return await client.call_node(
                node_id,
                "/consensus/vote",
                data={"term": self.current_term, "candidate_id": settings.NODE_ID}
            )
        except Exception as e:
            logger.warning(f"Vote request to {node_id} failed: {e}")
            return None

    async def _send_heartbeat(self, node_id: str, client: NetworkClient):
        try:
            await client.call_node(
                node_id,
                "/consensus/heartbeat",
                data={"term": self.current_term, "leader_id": settings.NODE_ID}
            )
        except Exception as e:
            logger.warning(f"Heartbeat to {node_id} failed: {e}")

    async def _announce_leadership(self):
        async with NetworkClient() as client:
            tasks = [
                client.call_node(
                    nid,
                    "/consensus/leader",
                    data={"term": self.current_term, "leader_id": settings.NODE_ID}
                )
                for nid in settings.NODE_URLS if nid != settings.NODE_ID
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def should_grant_vote(term: int, candidate_id: str, db: AsyncSession) -> bool:
        try:
            result = await db.execute(
                select(ConsensusState).where(ConsensusState.node_id == settings.NODE_ID)
            )
            state = result.scalar_one_or_none()

            if not state:
                await db.execute(
                    insert(ConsensusState).values(
                        node_id=settings.NODE_ID,
                        current_term=term,
                        voted_for=candidate_id,
                        status=NodeRole.FOLLOWER.value
                    )
                )
                await db.commit()
                return True

            if term > state.current_term:
                await db.execute(
                    update(ConsensusState)
                    .where(ConsensusState.node_id == settings.NODE_ID)
                    .values(current_term=term, voted_for=candidate_id, leader_id=None)
                )
                await db.commit()
                return True
            elif term == state.current_term and (state.voted_for is None or state.voted_for == candidate_id):
                await db.execute(
                    update(ConsensusState)
                    .where(ConsensusState.node_id == settings.NODE_ID)
                    .values(voted_for=candidate_id)
                )
                await db.commit()
                return True

            return False
        except Exception as e:
            logger.error(f"Vote decision failed: {e}")
            return False

    @staticmethod
    async def update_leader(leader_id: str, term: int, db: AsyncSession):
        try:
            await db.execute(
                update(ConsensusState)
                .where(ConsensusState.node_id == settings.NODE_ID)
                .values(leader_id=leader_id, current_term=term, voted_for=None)
            )
            await db.commit()
            logger.info(f"Acknowledged {leader_id} as leader for term {term}")
            clock_manager.create_event({"type": "leader_updated", "leader_id": leader_id})
        except Exception as e:
            logger.error(f"Update leader failed: {e}")

    @staticmethod
    async def get_leader_info(db: AsyncSession) -> Optional[Dict]:
        try:
            result = await db.execute(
                select(ConsensusState.leader_id, ConsensusState.current_term)
                .where(ConsensusState.node_id == settings.NODE_ID)
            )
            info = result.fetchone()
            return {"leader_id": info.leader_id, "current_term": info.current_term} if info else None
        except Exception as e:
            logger.error(f"Get leader failed: {e}")
            return None

    @staticmethod
    async def recover(db: AsyncSession):
        try:
            await db.execute(
                update(ConsensusState)
                .where(ConsensusState.node_id == settings.NODE_ID)
                .values(status=NodeRole.FOLLOWER.value, voted_for=None)
            )
            await db.commit()
            logger.info("Consensus recovered to FOLLOWER")
            clock_manager.create_event({"type": "consensus_recovered"})
        except Exception as e:
            logger.error(f"Consensus recovery failed: {e}")

_consensus_service: Optional[ConsensusService] = None

def get_consensus_service() -> ConsensusService:
    global _consensus_service
    if _consensus_service is None:
        _consensus_service = ConsensusService()
    return _consensus_service

async def start_consensus(db: AsyncSession):
    service = get_consensus_service()
    await service.start(db)