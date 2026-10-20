"""steadypy: reliable primitives for production asyncio services."""

from __future__ import annotations

import logging

from steadypy.clock import Clock, SystemClock
from steadypy.exceptions import SteadyError

__version__ = "0.1.0"

__all__ = [
    "Clock",
    "SteadyError",
    "SystemClock",
    "__version__",
]

# Library convention: never configure logging for the application, but make
# sure "No handlers could be found" noise never appears either.
logging.getLogger("steadypy").addHandler(logging.NullHandler())
