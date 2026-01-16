import asyncio
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy import text
from app.database import AsyncSession, AsyncSessionLocal
from app.config import settings
from app.utils.logger import setup_logger

logger = setup_logger(__name__)

class DistributedLock:
    """Enhanced distributed lock with timeout and retry"""
    
    def __init__(self, resource_type: str, resource_id: str):
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.lock_key = f"{resource_type}:{resource_id}"
        self.acquired_at: Optional[datetime] = None
        self.owner_node: Optional[str] = None
    
    async def acquire(
        self, 
        timeout_ms: Optional[int] = None,
        retry_interval_ms: int = 100
    ) -> bool:
        """
        Acquire distributed lock with timeout and retry
        Returns True if lock acquired
        """
        timeout = timeout_ms or settings.LOCK_TIMEOUT_MS
        max_retries = timeout // retry_interval_ms if retry_interval_ms > 0 else 1
        retry_interval = retry_interval_ms / 1000.0
        
        for attempt in range(max_retries):
            try:
                async with AsyncSessionLocal() as session:
                    # Try to acquire lock using advisory lock
                    result = await session.execute(
                        text(
                            """
                            SELECT pg_try_advisory_xact_lock(hashtext(:lock_key))
                            """
                        ),
                        {"lock_key": self.lock_key}
                    )
                    acquired = result.scalar()
                    
                    if acquired:
                        # Record lock acquisition
                        await session.execute(
                            text(
                                """
                                INSERT INTO shared_schema.locks 
                                (resource_type, resource_id, node_id, acquired_at)
                                VALUES (:rtype, :rid, :node, NOW())
                                ON CONFLICT (resource_type, resource_id) 
                                DO UPDATE SET node_id = :node, acquired_at = NOW()
                                """
                            ),
                            {
                                "rtype": self.resource_type,
                                "rid": self.resource_id,
                                "node": settings.NODE_ID
                            }
                        )
                        await session.commit()
                        
                        self.acquired_at = datetime.utcnow()
                        self.owner_node = settings.NODE_ID
                        logger.debug(f"Lock acquired: {self.lock_key} by {settings.NODE_ID}")
                        return True
                    else:
                        # Check if lock is stale (held for too long)
                        await self._cleanup_stale_locks(session)
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_interval)
                            continue
                        
            except Exception as e:
                logger.error(f"Lock acquisition error: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_interval)
                    continue
        
        logger.warning(f"Failed to acquire lock: {self.lock_key}")
        return False
    
    async def release(self) -> None:
        """Release the distributed lock"""
        try:
            async with AsyncSessionLocal() as session:
                # Remove lock record
                await session.execute(
                    text(
                        """
                        DELETE FROM shared_schema.locks 
                        WHERE resource_type = :rtype AND resource_id = :rid AND node_id = :node
                        """
                    ),
                    {
                        "rtype": self.resource_type,
                        "rid": self.resource_id,
                        "node": settings.NODE_ID
                    }
                )
                await session.commit()
                
                # Advisory lock is released automatically on transaction end
                self.acquired_at = None
                self.owner_node = None
                logger.debug(f"Lock released: {self.lock_key}")
                
        except Exception as e:
            logger.error(f"Lock release error: {e}")
    
    async def _cleanup_stale_locks(self, session: AsyncSession) -> None:
        """Clean up locks held for too long"""
        try:
            # Delete locks older than 2x heartbeat timeout
            timeout_seconds = (settings.HEARTBEAT_TIMEOUT_MS * 2) / 1000
            
            await session.execute(
                text(
                    """
                    DELETE FROM shared_schema.locks 
                    WHERE acquired_at < NOW() - INTERVAL ':timeout seconds'
                    """
                ),
                {"timeout": timeout_seconds}
            )
        except Exception as e:
            logger.warning(f"Failed to cleanup stale locks: {e}")
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

# Transaction lock manager for 2PC
class TransactionLocks:
    """Manage locks for distributed transactions"""
    
    @staticmethod
    async def lock_resources(
        session: AsyncSession,
        transaction_id: str,
        resources: Dict[str, List[str]]  # {"subscribers": ["sub1", "sub2"], "sessions": ["sess1"]}
    ) -> bool:
        """
        Lock multiple resources for a transaction
        Returns True if all locks acquired
        """
        try:
            for table_name, record_ids in resources.items():
                for record_id in record_ids:
                    lock_key = f"{table_name}:{record_id}:{transaction_id}"
                    
                    result = await session.execute(
                        text(
                            """
                            SELECT pg_try_advisory_xact_lock(hashtext(:lock_key))
                            """
                        ),
                        {"lock_key": lock_key}
                    )
                    
                    if not result.scalar():
                        logger.warning(f"Failed to lock {table_name}:{record_id} for tx {transaction_id}")
                        return False
            
            logger.info(f"All resources locked for transaction {transaction_id}")
            return True
            
        except Exception as e:
            logger.error(f"Transaction lock error: {e}")
            return False

# Create locks table if not exists
async def ensure_locks_table() -> None:
    """Ensure locks table exists in shared schema"""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS shared_schema.locks (
                        resource_type VARCHAR(50) NOT NULL,
                        resource_id VARCHAR(100) NOT NULL,
                        node_id VARCHAR(50) NOT NULL,
                        acquired_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (resource_type, resource_id)
                    )
                    """
                )
            )
            await session.commit()
            logger.info("Locks table ensured in shared schema")
    except Exception as e:
        logger.error(f"Failed to create locks table: {e}")