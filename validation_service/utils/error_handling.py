"""
Error Handling Utilities
========================

Provides robust error handling with retries, circuit breakers, and fallbacks.
"""

import asyncio
import logging
from typing import Callable, TypeVar, Any, Optional
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ErrorCategory(Enum):
    """Error categories for classification and handling"""

    TRANSIENT = "transient"  # Temporary errors, retry
    PERMANENT = "permanent"  # Don't retry
    RESOURCE = "resource"  # Resource exhaustion
    VALIDATION = "validation"  # Input validation
    TIMEOUT = "timeout"  # Operation timeout
    UNKNOWN = "unknown"  # Unclassified


@dataclass
class ErrorContext:
    """Context information for error handling"""

    operation: str
    category: ErrorCategory
    error: Exception
    timestamp: datetime
    attempt: int
    max_attempts: int
    retryable: bool

    def to_dict(self) -> dict:
        return {
            "operation": self.operation,
            "category": self.category.value,
            "error_type": type(self.error).__name__,
            "error_message": str(self.error),
            "timestamp": self.timestamp.isoformat(),
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "retryable": self.retryable,
        }


class RetryableError(Exception):
    """Base class for errors that should trigger retry"""

    pass


class ResourceExhaustedError(RetryableError):
    """GPU memory, disk space, etc."""

    pass


class TransientError(RetryableError):
    """Temporary network, I/O errors"""

    pass


class ValidationError(Exception):
    """Input validation errors - don't retry"""

    pass


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    Prevents cascading failures by failing fast when error rate is high.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_duration: float = 60.0,
        expected_exception: type = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.timeout_duration = timeout_duration
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "closed"  # closed, open, half_open

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function with circuit breaker protection"""
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half_open"
            else:
                raise Exception(
                    f"Circuit breaker is OPEN. Too many failures. "
                    f"Retry after {self.timeout_duration}s"
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        return datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout_duration)

    def _on_success(self):
        """Reset on successful call"""
        self.failure_count = 0
        self.state = "closed"

    def _on_failure(self):
        """Increment failure count and open circuit if threshold exceeded"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


def with_retries(
    max_attempts: int = 3,
    initial_wait: float = 1.0,
    max_wait: float = 10.0,
    exponential_base: int = 2,
):
    """
    Decorator for automatic retries with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        initial_wait: Initial wait time in seconds
        max_wait: Maximum wait time between retries
        exponential_base: Base for exponential backoff
    """

    def decorator(func: Callable) -> Callable:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=initial_wait, max=max_wait, exp_base=exponential_base),
            retry=retry_if_exception_type(RetryableError),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Classify error and convert to retryable if appropriate
                if _is_retryable(e):
                    raise RetryableError(f"Retryable error: {e}") from e
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if _is_retryable(e):
                    raise RetryableError(f"Retryable error: {e}") from e
                raise

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def _is_retryable(error: Exception) -> bool:
    """Determine if an error is retryable based on type and message"""
    retryable_messages = [
        "timeout",
        "connection refused",
        "connection reset",
        "temporary failure",
        "resource temporarily unavailable",
        "cuda out of memory",
    ]

    error_str = str(error).lower()
    return any(msg in error_str for msg in retryable_messages)


def classify_error(error: Exception) -> ErrorCategory:
    """Classify error into category for appropriate handling"""
    error_str = str(error).lower()
    error_type = type(error).__name__.lower()

    if "timeout" in error_str or "timeout" in error_type:
        return ErrorCategory.TIMEOUT
    elif "validation" in error_type or "invalid" in error_str:
        return ErrorCategory.VALIDATION
    elif "memory" in error_str or "resource" in error_str:
        return ErrorCategory.RESOURCE
    elif _is_retryable(error):
        return ErrorCategory.TRANSIENT
    elif isinstance(error, (ValueError, TypeError, AttributeError)):
        return ErrorCategory.PERMANENT
    else:
        return ErrorCategory.UNKNOWN


async def with_timeout(coro, timeout_seconds: float, operation_name: str = "Operation"):
    """
    Execute coroutine with timeout.

    Args:
        coro: Coroutine to execute
        timeout_seconds: Timeout in seconds
        operation_name: Name for logging

    Returns:
        Result of coroutine

    Raises:
        TimeoutError: If operation exceeds timeout
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        logger.error(f"{operation_name} timed out after {timeout_seconds}s")
        raise TimeoutError(f"{operation_name} exceeded {timeout_seconds}s timeout")


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division with default for zero denominator"""
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ValueError):
        return default


class GracefulDegradation:
    """
    Provides fallback mechanisms when primary operations fail.
    """

    @staticmethod
    async def with_fallback(
        primary: Callable,
        fallback: Callable,
        fallback_on: tuple = (Exception,),
        operation_name: str = "Operation",
    ):
        """
        Try primary operation, fall back to secondary if it fails.

        Args:
            primary: Primary operation to attempt
            fallback: Fallback operation if primary fails
            fallback_on: Exception types that trigger fallback
            operation_name: Name for logging

        Returns:
            Result from primary or fallback operation
        """
        try:
            if asyncio.iscoroutinefunction(primary):
                return await primary()
            return primary()
        except fallback_on as e:
            logger.warning(f"{operation_name} failed, using fallback: {e}")
            try:
                if asyncio.iscoroutinefunction(fallback):
                    return await fallback()
                return fallback()
            except Exception as fallback_error:
                logger.error(f"Fallback also failed for {operation_name}: {fallback_error}")
                raise

    @staticmethod
    def with_default(func: Callable, default_value: Any, exceptions: tuple = (Exception,)):
        """
        Execute function and return default value on specified exceptions.

        Args:
            func: Function to execute
            default_value: Value to return on exception
            exceptions: Exception types to catch

        Returns:
            Function result or default value
        """
        try:
            return func()
        except exceptions as e:
            logger.warning(f"Operation failed, returning default: {e}")
            return default_value
