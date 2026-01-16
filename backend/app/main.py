from app.database import Base
Base.metadata.clear()

# Initialize clock manager EARLY
from app.utils.clocks import init_clock_manager
from app.config import settings
import json
from pathlib import Path

# Load all node IDs from ports_config.json
ports_path = Path("ports_config.json")
if ports_path.exists():
    with ports_path.open() as f:
        config = json.load(f)
    all_nodes = list(config.get("nodes", {}).keys())
else:
    all_nodes = [settings.NODE_ID]  # fallback

init_clock_manager(node_id=settings.NODE_ID, all_nodes=all_nodes)

from app.services.load import LoadBalancer
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
from datetime import datetime
import os
import sys
from sqlalchemy.ext.asyncio import AsyncSession

from app.routers import edge, core, cloud, fault, metrics
from app.config import settings
from app.database import health_check, get_db, AsyncSessionLocal
from app.utils.logger import setup_logger
from app.utils.heartbeat import start_heartbeat
from app.services.metrics import MetricsService
from app.services.consensus import ConsensusService, start_consensus
from app.services.fault import FaultService
from app.services.replication import ReplicationService

logger = setup_logger(__name__)

app = FastAPI(
    title="Carrier-Grade Telecom Distributed System",
    description="Edge-Core-Cloud Distributed Telecommunication System",
    version="1.0.0",
    docs_url="/docs" if settings.NODE_ID == "gateway" else None,
    redoc_url="/redoc" if settings.NODE_ID == "gateway" else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.security import APIKeyHeader
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key"
        )
    return api_key

@app.get("/", tags=["health"])
async def root():
    return {
        "service": "Carrier-Grade Telecom System",
        "node_id": settings.NODE_ID,
        "node_type": settings.NODE_TYPE,
        "port": settings.NODE_PORT,
        "status": "operational",
        "version": "1.0.0"
    }

@app.get("/health", tags=["health"])
async def health(db: AsyncSession = Depends(get_db)):
    db_healthy = await health_check()
    if not db_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed"
        )

    leader_info = await ConsensusService.get_leader_info(db)
    return {
        "status": "healthy",
        "node_id": settings.NODE_ID,
        "database": "connected",
        "current_leader": leader_info.get("leader_id") if leader_info else "No leader elected",
        "timestamp": datetime.utcnow().isoformat()
    }

if settings.NODE_TYPE == "edge":
    app.include_router(edge.router, prefix="/edge", tags=["edge"], dependencies=[Depends(verify_api_key)])
elif settings.NODE_TYPE == "core":
    app.include_router(core.router, prefix="/core", tags=["core"], dependencies=[Depends(verify_api_key)])
elif settings.NODE_TYPE == "cloud":
    app.include_router(cloud.router, prefix="/cloud", tags=["cloud"], dependencies=[Depends(verify_api_key)])

app.include_router(fault.router, prefix="/fault", tags=["fault"], dependencies=[Depends(verify_api_key)])

app.include_router(metrics.router, prefix="/metrics", tags=["metrics"], dependencies=[Depends(verify_api_key)])

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "node_id": settings.NODE_ID}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "node_id": settings.NODE_ID}
    )

@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting {settings.NODE_TYPE} node {settings.NODE_ID} on port {settings.NODE_PORT}")

    async with AsyncSessionLocal() as db:
        if settings.NODE_TYPE == "cloud":
            await ReplicationService.perform_full_replication(db)

        if settings.NODE_TYPE == "core":
            await start_consensus(db)

    asyncio.create_task(start_heartbeat())
    asyncio.create_task(FaultService.detect_failures())
    asyncio.create_task(FaultService.auto_recover_failures())
    asyncio.create_task(LoadBalancer.auto_balance_loop())

    if settings.NODE_ID == "monitoring":
        asyncio.create_task(MetricsService.collect_system_metrics())

    logger.info("Node startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info(f"Shutting down {settings.NODE_ID}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.NODE_PORT,
        reload=False,
        log_level="info",
        workers=4
    )