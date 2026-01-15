from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import asyncio
from app.config import settings

class Base(DeclarativeBase):
    pass

async_engine = create_async_engine(
    str(settings.DATABASE_URL),
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=20,
    max_overflow=10,
    connect_args={
        "server_settings": {
            "jit": "off",
            "search_path": f"{settings.node_schema},shared_schema,public"
        }
    }
)

sync_engine = create_engine(
    settings.DATABASE_URL_SYNC,
    pool_pre_ping=True,
    pool_recycle=3600,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

SCHEMAS = {
    "edge": "edge_schema",
    "core": "core_schema",
    "cloud": "cloud_schema",
    "monitoring": "monitoring_schema",
    "shared": "shared_schema",
}

async def get_db(schema: Optional[str] = None) -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency: yields session with correct search_path.
    Caller must commit/rollback.
    """
    schema_to_use = schema or settings.node_schema

    async with AsyncSessionLocal() as session:
        try:
            await session.execute(
                text(f"SET search_path TO {schema_to_use},shared_schema,public")
            )
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

async def set_session_schema(session: AsyncSession, schema: str) -> None:
    await session.execute(
        text(f"SET search_path TO {schema},shared_schema,public")
    )

async def replicate_across_nodes(sql_statements: dict[str, str]) -> None:
    """
    Execute same or different statements across multiple schemas/nodes.
    Example: sql_statements = {"edge_1": "INSERT ...", "cloud_1": "INSERT ..."}
    """
    tasks = []
    for target, stmt in sql_statements.items():
        target_type = target.split("_")[0] if "_" in target else target
        schema = SCHEMAS.get(target_type, settings.node_schema)
        tasks.append(_execute_with_retry(stmt, schema))
    await asyncio.gather(*tasks, return_exceptions=True)

async def _execute_with_retry(stmt: str, schema: str, retries: int = 3) -> None:
    backoff = 0.5
    for attempt in range(retries):
        async with AsyncSessionLocal() as session:
            try:
                await set_session_schema(session, schema)
                await session.execute(text(stmt))
                await session.commit()
                return
            except OperationalError as e:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(backoff * (2 ** attempt))
            except Exception:
                await session.rollback()
                raise

async def health_check() -> bool:
    try:
        with sync_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False