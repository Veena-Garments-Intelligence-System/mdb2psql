from pydantic_settings import BaseSettings, SettingsConfigDict

import os

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Postgres
    POSTGRES_URI: str = "postgresql://postgres:postgres@localhost:5432/mdb_sync"

    @property
    def postgres_url(self) -> str:
        # Detect if running inside Docker
        is_docker = os.path.exists("/.dockerenv") or os.environ.get("IS_DOCKER", "false") == "true"
        
        uri = self.POSTGRES_URI
        if uri.startswith("postgresql+asyncpg://"):
            uri = uri.replace("postgresql+asyncpg://", "postgresql://", 1)

        if is_docker:
            # Inside Docker container, 127.0.0.1/localhost refers to the container itself.
            # Map loopback addresses to the docker database service/alias (e.g. postgres or host.docker.internal).
            docker_host = os.environ.get("POSTGRES_HOST", "postgres")
            uri = uri.replace("@127.0.0.1", f"@{docker_host}")
            uri = uri.replace("@localhost", f"@{docker_host}")
            return uri
        else:
            # For local tools / CLI execution on host machine, swap container hostnames with localhost
            uri = uri.replace("@vgis-postgres", "@localhost")
            uri = uri.replace("@graphora-postgres", "@localhost")
            uri = uri.replace("@postgres", "@localhost")
            return uri.replace("@host.docker.internal", "@localhost")

    # MDB
    MDB_PATH: str = "./data/raw/Billing.mdb"
    MDB_DRIVER: str = "{Microsoft Access Driver (*.mdb, *.accdb)}"

    @property
    def mdb_connection_string(self) -> str:
        return f"DRIVER={self.MDB_DRIVER};DBQ={self.MDB_PATH};"

    # Sync
    SYNC_INTERVAL_SECONDS: int = 3600
    RECONCILIATION_WINDOW_ROWS: int = 5000
    RECONCILIATION_CHUNK_LIMIT: int = 5000
    SOURCE_SYSTEM: str = "BILLING_MDB"
    
    # Locking
    LOCK_RETRY_INTERVAL_SECONDS: int = 30
    LOCK_MAX_WAIT_MINUTES: int = 60 # Allow waiting for long intelligence runs
    
    # DB Connection Pool configuration
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 40
    DB_POOL_RECYCLE: int = 1800
    DB_POOL_TIMEOUT: int = 30

    # Startup Resilience settings (Requirement 1 & 12)
    DB_STARTUP_RETRY_LIMIT: int | None = None  # None for infinite retries
    DB_STARTUP_MIN_BACKOFF: float = 60.0       # Start retry at 60s as requested
    DB_STARTUP_MAX_BACKOFF: float = 90.0       # Max retry at 90s as requested
    DB_STARTUP_BACKOFF_FACTOR: float = 1.1

    # Loop Resilience settings
    LOOP_RETRY_MIN_BACKOFF: float = 60.0
    LOOP_RETRY_MAX_BACKOFF: float = 90.0

    # Circuit Breaker configurations
    CB_FAILURE_THRESHOLD: int = 5
    CB_RECOVERY_TIMEOUT: int = 30  # seconds cooldown
    CB_SUCCESS_THRESHOLD: int = 2

    # Health / Metrics HTTP Server configuration
    HEALTH_HOST: str = "0.0.0.0"
    HEALTH_PORT: int = 8000

    # Alerting configurations
    ALERT_LAG_THRESHOLD_MINUTES: int = 120
    SLACK_WEBHOOK_URL: str | None = None
    WEBHOOK_URL: str | None = None

    # Logging
    LOG_LEVEL: str = "info"
    LOG_FORMAT: str = "auto"  # "auto", "json", or "console"

settings = Settings()

