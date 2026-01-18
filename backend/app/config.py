from pathlib import Path
import json
from dotenv import load_dotenv

from pydantic import PostgresDsn, Field
from pydantic_settings import BaseSettings
from pydantic.functional_validators import field_validator

load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: PostgresDsn
    DATABASE_URL_SYNC: str

    NODE_ID: str
    NODE_TYPE: str
    NODE_PORT: int
    NODE_REGION: str = "global"

    CONSENSUS_TIMEOUT_MS: int = 3000
    HEARTBEAT_INTERVAL_MS: int = 2000
    HEARTBEAT_TIMEOUT_MS: int = 15000
    TWO_PHASE_TIMEOUT_MS: int = 10000
    LOCK_TIMEOUT_MS: int = 5000
    MAX_CONCURRENT_TRANSACTIONS: int = 100

    MAX_EDGE_LATENCY_MS: int = 100
    MAX_CORE_LATENCY_MS: int = 200
    MAX_CLOUD_LATENCY_MS: int = 500
    CPU_THRESHOLD_PERCENT: int = 80
    MEMORY_THRESHOLD_PERCENT: int = 85
    MIGRATION_THRESHOLD_PERCENT: int = 70

    SIMULATION_ENABLED: bool = False
    TRAFFIC_PATTERN: str = "bursty"
    USERS_PER_NODE: int = 1000
    CALL_RATE_PER_SECOND: int = 10

    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/node.log"
    METRICS_FILE: str = "logs/metrics.csv"

    API_KEY: str
    JWT_SECRET: str

    NODE_URLS: dict = Field(default_factory=dict)
    LOAD_BALANCE_INTERVAL_SECONDS: int = Field(30, ge=10, description="How often to run auto-balance (seconds)")

    @property
    def node_schema(self) -> str:
        return {
            "edge": "edge_schema",
            "core": "core_schema",
            "cloud": "cloud_schema",
        }.get(self.NODE_TYPE, "public")

    @field_validator("NODE_TYPE")
    @classmethod
    def validate_node_type(cls, v: str) -> str:
        allowed = {"edge", "core", "cloud", "monitoring", "gateway"}
        if v not in allowed:
            raise ValueError(f"NODE_TYPE must be one of: {', '.join(allowed)}")
        return v

    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
    }

settings = Settings()

# Load ports config
def load_ports_config() -> dict:
    path = Path("ports_config.json")
    if path.exists():
        return json.loads(path.read_text())
    # Minimal fallback (do not hardcode secrets)
    return {"nodes": {}, "services": {}}

ports_data = load_ports_config()
settings.NODE_URLS = {k: v["url"] for k, v in ports_data.get("nodes", {}).items()}