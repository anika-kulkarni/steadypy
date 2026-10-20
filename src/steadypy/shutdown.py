"""Coordinating a graceful shutdown, optionally triggered by Unix signals.

Example::

    shutdown = ShutdownSignal()
    shutdown.install_signal_handlers()      # SIGINT and SIGTERM trigger it
    try:
        async with TaskSupervisor() as supervisor:
            supervisor.start(worker, restart=True)
            reason = await shutdown.wait()
            supervisor.stop()
    finally:
        shutdown.remove_signal_handlers()

Nothing here depends on signals: :meth:`ShutdownSignal.trigger` works the same
on every platform, from an admin endpoint, a test, or a failed health check.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import signal
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from types import FrameType
from typing import Any

__all__ = ["ShutdownSignal"]

logger = logging.getLogger("steadypy.shutdown")


@dataclass(slots=True)
class _InstalledHandler:
    loop: asyncio.AbstractEventLoop
    previous: Any
    via_loop: bool


class ShutdownSignal:
    """A one-shot, awaitable "time to shut down" flag.

    Any number of tasks can :meth:`wait` for it; the first :meth:`trigger`
    releases all of them and records why. Later triggers are ignored, so the
    first reason is the one that is kept.

    Use it from a single event loop. To trigger it from another thread, use
    ``loop.call_soon_threadsafe(shutdown.trigger, reason)``.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._reason: str | None = None
        self._installed: dict[signal.Signals, _InstalledHandler] = {}

    def is_set(self) -> bool:
        """Return ``True`` once shutdown has been triggered."""
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        """Why shutdown was triggered, or ``None`` if it has not been."""
        return self._reason

    def trigger(self, reason: str = "requested") -> bool:
        """Request shutdown and wake every waiter.

        Returns ``True`` if this call triggered it, ``False`` if it was
        already triggered.
        """
        if self._event.is_set():
            return False
        self._reason = reason
        self._event.set()
        logger.info("shutdown requested: %s", reason)
        return True

    async def wait(self) -> str:
        """Wait until shutdown is triggered and return the reason."""
        await self._event.wait()
        assert self._reason is not None
        return self._reason

    def install_signal_handlers(
        self, signals: Iterable[int] | None = None
    ) -> tuple[signal.Signals, ...]:
        """Trigger shutdown when one of ``signals`` arrives.

        Defaults to ``SIGINT`` and ``SIGTERM``. Uses
        :meth:`asyncio.loop.add_signal_handler` where the event loop supports
        it (Unix) and falls back to :func:`signal.signal` otherwise (for
        example, the Windows proactor loop). Must be called from the main
        thread while the event loop is running. Signals that already have a
        handler from this object are skipped.

        A handler set with :func:`signal.signal` is replaced and restored by
        :meth:`remove_signal_handlers`. A handler registered with
        ``loop.add_signal_handler()`` cannot be restored once replaced, so
        installing over one is refused instead. That includes another
        ``ShutdownSignal``'s handlers: an event loop allows one handler per
        signal.

        Returns:
            The signals for which a handler was installed by this call.

        Raises:
            RuntimeError: If called off the main thread, or if one of the
                signals already has a handler registered with the event loop.
                Nothing is installed in that case.
        """
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("signal handlers can only be installed from the main thread")
        loop = asyncio.get_running_loop()
        chosen = (signal.SIGINT, signal.SIGTERM) if signals is None else signals
        wanted = [s for s in dict.fromkeys(map(signal.Signals, chosen)) if s not in self._installed]
        taken = [s.name for s in wanted if _owned_by_event_loop(signal.getsignal(s))]
        if taken:
            raise RuntimeError(
                f"{', '.join(taken)} already handled via loop.add_signal_handler(); "
                "installing would replace that handler for good. Remove it first, or "
                "call this ShutdownSignal's trigger() from it."
            )
        installed: list[signal.Signals] = []
        for sig in wanted:
            previous = signal.getsignal(sig)
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
                via_loop = True
            except NotImplementedError:
                signal.signal(sig, functools.partial(self._on_raw_signal, loop))
                via_loop = False
            self._installed[sig] = _InstalledHandler(loop, previous, via_loop)
            installed.append(sig)
        return tuple(installed)

    def remove_signal_handlers(self) -> None:
        """Remove the handlers installed by this object and restore the previous ones.

        Calling it again, or when nothing is installed, does nothing.
        """
        while self._installed:
            sig, entry = self._installed.popitem()
            if entry.via_loop and not entry.loop.is_closed():
                entry.loop.remove_signal_handler(sig)
            if entry.previous is not None:
                signal.signal(sig, entry.previous)

    def __repr__(self) -> str:
        state = f"triggered ({self._reason})" if self._event.is_set() else "waiting"
        return f"<ShutdownSignal {state}>"

    def _on_signal(self, sig: signal.Signals) -> None:
        self.trigger(f"received {sig.name}")

    def _on_raw_signal(
        self, loop: asyncio.AbstractEventLoop, signum: int, frame: FrameType | None
    ) -> None:
        # Runs in the main thread between bytecodes, possibly while the loop is
        # busy: hand the work to the loop instead of touching state here.
        if not loop.is_closed():
            loop.call_soon_threadsafe(self._on_signal, signal.Signals(signum))


def _owned_by_event_loop(handler: object) -> bool:
    """Whether ``handler`` is the placeholder that ``loop.add_signal_handler()`` installs.

    The event loop keeps the real callback to itself, so it can be neither
    read back nor restored once replaced.
    """
    return getattr(handler, "__name__", None) == "_sighandler_noop" and str(
        getattr(handler, "__module__", "")
    ).startswith("asyncio")
