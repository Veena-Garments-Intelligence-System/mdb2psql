import structlog
import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from src.mdb_sync.config import settings

def configure_logging():
    # Use standard logging as the backend for structlog
    log_level = settings.LOG_LEVEL.upper()
    
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Detect if we should use console or JSON rendering
    use_json = settings.LOG_FORMAT == "json"
    if settings.LOG_FORMAT == "auto":
        use_json = not sys.stdout.isatty()
    
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if use_json:
        # Production: structured JSON
        processors = shared_processors + [
            structlog.processors.JSONRenderer()
        ]
    else:
        # Development: pretty-print to console
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True)
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(console_handler)

    # 2. File Handler (Rotating)
    # Note: Rotation here is by size, but we will also implement age-based cleanup in the scheduler
    file_handler = RotatingFileHandler(
        "logs/app.log", 
        maxBytes=10*1024*1024, # 10MB
        backupCount=10
    )
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(file_handler)

    # Silence noisy third-party loggers
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("pyodbc").setLevel(logging.WARNING)

def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Returns a module-specific logger."""
    return structlog.get_logger(name)

# Default logger for root-level or legacy imports
logger = structlog.get_logger("mdb_sync")
