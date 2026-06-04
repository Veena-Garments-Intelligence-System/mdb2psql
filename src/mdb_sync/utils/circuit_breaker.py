import time
import threading
from enum import Enum
from src.mdb_sync.config import settings
from src.mdb_sync.logging_config import get_logger

logger = get_logger(__name__)

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 30, success_threshold: int = 2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = time.time()
        self._lock = threading.Lock()

    def __call__(self, func, *args, **kwargs):
        with self._lock:
            self._check_state()
            if self.state == CircuitState.OPEN:
                raise CircuitBreakerOpenException(f"Circuit breaker '{self.name}' is OPEN")

        try:
            result = func(*args, **kwargs)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= self.success_threshold:
                        self.state = CircuitState.CLOSED
                        self.failure_count = 0
                        self.success_count = 0
                        self.last_state_change = time.time()
                        logger.info("Circuit breaker closed", name=self.name)
            return result
        except Exception as e:
            with self._lock:
                self.failure_count += 1
                logger.warning("Circuit breaker operation failure", name=self.name, failure_count=self.failure_count, error=str(e))
                if self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
                    if self.failure_count >= self.failure_threshold or self.state == CircuitState.HALF_OPEN:
                        self.state = CircuitState.OPEN
                        self.last_state_change = time.time()
                        logger.error("Circuit breaker opened", name=self.name, failure_count=self.failure_count)
            raise e

    def _check_state(self):
        if self.state == CircuitState.OPEN:
            elapsed = time.time() - self.last_state_change
            if elapsed >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                self.failure_count = 0
                self.last_state_change = time.time()
                logger.info("Circuit breaker entered half-open state", name=self.name)

    def is_available(self) -> bool:
        with self._lock:
            self._check_state()
            return self.state != CircuitState.OPEN

postgres_breaker = CircuitBreaker(
    name="PostgreSQL",
    failure_threshold=settings.CB_FAILURE_THRESHOLD,
    recovery_timeout=settings.CB_RECOVERY_TIMEOUT,
    success_threshold=settings.CB_SUCCESS_THRESHOLD
)

mdb_breaker = CircuitBreaker(
    name="MDB",
    failure_threshold=settings.CB_FAILURE_THRESHOLD,
    recovery_timeout=settings.CB_RECOVERY_TIMEOUT,
    success_threshold=settings.CB_SUCCESS_THRESHOLD
)
