"""steadypy: reliable primitives for production asyncio services."""

from __future__ import annotations

import logging

from steadypy.circuit_breaker import CircuitBreaker, CircuitState, CircuitStats
from steadypy.clock import Clock, SystemClock
from steadypy.concurrency import BoundedStream, bounded_map, bounded_stream
from steadypy.exceptions import CircuitOpenError, SteadyError
from steadypy.rate_limit import RateLimiter
from steadypy.retry import RetryAttempt, async_retry, retry
from steadypy.supervisor import RestartMode, SupervisedTask, TaskState, TaskSupervisor

__version__ = "0.1.0"

__all__ = [
    "BoundedStream",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "CircuitStats",
    "Clock",
    "RateLimiter",
    "RestartMode",
    "RetryAttempt",
    "SteadyError",
    "SupervisedTask",
    "SystemClock",
    "TaskState",
    "TaskSupervisor",
    "__version__",
    "async_retry",
    "bounded_map",
    "bounded_stream",
    "retry",
]

# Library convention: never configure logging for the application, but make
# sure "No handlers could be found" noise never appears either.
logging.getLogger("steadypy").addHandler(logging.NullHandler())
