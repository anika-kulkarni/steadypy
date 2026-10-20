"""Supervision for long-running asyncio tasks.

:class:`TaskSupervisor` is a structured-concurrency container in the spirit of
:class:`asyncio.TaskGroup`: every task it starts is finished (completed,
failed or cancelled) by the time its ``async with`` block exits. On top of
that it adds the policies long-running services keep re-implementing: named
tasks, restart-on-failure with exponential backoff and limits, a choice
between fail-fast and isolated failure handling, and live state inspection.

Example::

    async with TaskSupervisor() as supervisor:
        supervisor.start(consume_queue, name="consumer", restart=True, max_restarts=5)
        supervisor.start(flush_metrics, name="metrics", restart=True)
        await shutdown.wait()
        supervisor.stop()  # cancel the children; the block exits once they finish
"""

from __future__ import annotations

import asyncio
import enum
import functools
import inspect
import logging
import sys
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from types import MappingProxyType, TracebackType
from typing import Any, Generic, Literal, Self, TypeVar, TypeVarTuple, Unpack, cast, overload

from steadypy._backoff import exponential_delay
from steadypy.clock import Clock, resolve_clock

__all__ = ["RestartMode", "SupervisedTask", "TaskState", "TaskSupervisor"]

logger = logging.getLogger("steadypy.supervisor")

T = TypeVar("T")
Ts = TypeVarTuple("Ts")

_DEFAULT_MAX_RESTART_DELAY = 30.0

# Re-arming a pending cancellation (uncancel() + cancel()) is only safe from
# Python 3.13, where uncancel() also clears the cancellation once no requests
# remain. On 3.11 and 3.12 the re-armed request would survive an enclosing
# asyncio.timeout() withdrawing it, and hit the next await as a stray
# CancelledError. There, as TaskGroup does, the pending request is absorbed.
_REARM_PENDING_CANCELLATION = sys.version_info >= (3, 13)

RestartMode = Literal["never", "on-failure", "always"]
"""When a supervised task is restarted.

* ``"never"`` (or ``False``): run once.
* ``"on-failure"`` (or ``True``): restart when the task raises an exception.
* ``"always"``: restart when it raises *or* returns; for tasks that are meant
  to run for the whole life of the service.

Cancellation never triggers a restart.
"""


class TaskState(enum.Enum):
    """Lifecycle state of a :class:`SupervisedTask`."""

    RUNNING = "running"
    """The task's coroutine is executing."""

    RESTARTING = "restarting"
    """The previous run ended and the task is waiting out its restart backoff."""

    COMPLETED = "completed"
    """The task returned and will not run again."""

    FAILED = "failed"
    """The task raised and will not run again."""

    CANCELLED = "cancelled"
    """The task was cancelled."""


_FINAL_STATES = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED})


def _restart_mode(value: bool | RestartMode) -> RestartMode:
    if value is True:
        return "on-failure"
    if value is False:
        return "never"
    if value in ("never", "on-failure", "always"):
        return value
    raise ValueError(f"restart must be a bool, 'never', 'on-failure' or 'always', not {value!r}")


class _OneShot(Generic[T]):
    """Factory for a task given as a coroutine object, which can only run once."""

    __slots__ = ("_coro",)

    def __init__(self, coro: Coroutine[Any, Any, T]) -> None:
        self._coro: Coroutine[Any, Any, T] | None = coro

    def __call__(self) -> Awaitable[T]:
        coro = self._coro
        if coro is None:  # pragma: no cover - restarts are refused for coroutine objects
            raise RuntimeError("a coroutine object cannot be awaited twice")
        self._coro = None
        return coro

    def discard(self) -> None:
        """Close the coroutine if it never started (its task was cancelled first)."""
        if self._coro is not None:
            self._coro.close()
            self._coro = None


class SupervisedTask(Generic[T]):
    """A handle to a task started with :meth:`TaskSupervisor.start`.

    The handle describes the supervised *unit*, not one particular run of its
    coroutine, so it stays valid across restarts. Its query methods mirror
    :class:`asyncio.Task` (``done()``, ``result()``, ``exception()``,
    ``cancel()``).
    """

    def __init__(
        self,
        *,
        name: str,
        factory: Callable[[], Awaitable[T]],
        restart: RestartMode,
        max_restarts: int | None,
        restart_window: float | None,
        restart_delay: float,
        max_restart_delay: float,
    ) -> None:
        self._name = name
        self._factory: Callable[[], Awaitable[T]] | None = factory
        self._restart = restart
        self._max_restarts = max_restarts
        self._restart_window = restart_window
        self._restart_delay = restart_delay
        self._max_restart_delay = max_restart_delay
        self._state = TaskState.RUNNING
        self._restarts = 0
        self._failures = 0
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._last_exception: BaseException | None = None
        self._recent_restarts: deque[float] = deque()
        self._task: asyncio.Task[None] | None = None
        self._cancel_requested = False

    @property
    def name(self) -> str:
        """The task's name, unique among the supervisor's live tasks."""
        return self._name

    @property
    def state(self) -> TaskState:
        """The current :class:`TaskState`."""
        self._sync()
        return self._state

    @property
    def restart_mode(self) -> RestartMode:
        """The task's restart policy."""
        return self._restart

    @property
    def restarts(self) -> int:
        """How many times the task has been restarted so far."""
        return self._restarts

    @property
    def failures(self) -> int:
        """How many runs ended by raising, including runs that were restarted."""
        return self._failures

    @property
    def last_exception(self) -> BaseException | None:
        """The exception from the most recent failed run, even if it was restarted."""
        return self._last_exception

    def done(self) -> bool:
        """Return ``True`` once the task has completed, failed or been cancelled."""
        self._sync()
        return self._state in _FINAL_STATES

    def result(self) -> T:
        """Return the task's result.

        Raises the task's exception if it failed, :class:`asyncio.CancelledError`
        if it was cancelled, and :class:`asyncio.InvalidStateError` if it is
        still running or restarting.
        """
        self._sync()
        if self._state is TaskState.COMPLETED:
            return cast(T, self._result)
        if self._state is TaskState.FAILED:
            assert self._exception is not None
            raise self._exception
        if self._state is TaskState.CANCELLED:
            raise asyncio.CancelledError(f"supervised task {self._name!r} was cancelled")
        raise asyncio.InvalidStateError(f"supervised task {self._name!r} is not done")

    def exception(self) -> BaseException | None:
        """Return the exception that ended the task, or ``None`` if it completed.

        Raises like :meth:`asyncio.Task.exception` when the task was cancelled
        or is not done yet.
        """
        self._sync()
        if self._state is TaskState.CANCELLED:
            raise asyncio.CancelledError(f"supervised task {self._name!r} was cancelled")
        if not self.done():
            raise asyncio.InvalidStateError(f"supervised task {self._name!r} is not done")
        return self._exception

    def cancel(self) -> bool:
        """Cancel this task (whether running or waiting to restart).

        A cancelled task is never restarted and is not reported as a failure.
        Returns ``False`` if the task was already done.
        """
        if self.done() or self._task is None:
            return False
        self._cancel_requested = True
        return self._task.cancel()

    async def wait(self) -> None:
        """Wait until the task is done, without raising its exception.

        Cancelling the caller does not cancel the task.
        """
        task = self._task
        if task is not None and not task.done():
            await asyncio.wait({task})

    def _reserve_restart(self, now: float) -> float | None:
        """Account for one more restart; return its delay, or ``None`` if exhausted."""
        if self._restart_window is None:
            recent = self._restarts
        else:
            horizon = now - self._restart_window
            while self._recent_restarts and self._recent_restarts[0] <= horizon:
                self._recent_restarts.popleft()
            recent = len(self._recent_restarts)
        if self._max_restarts is not None and recent >= self._max_restarts:
            return None
        if self._restart_window is not None:
            self._recent_restarts.append(now)
        return exponential_delay(recent, base=self._restart_delay, maximum=self._max_restart_delay)

    def _finish(
        self,
        state: TaskState,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
    ) -> None:
        self._state = state
        self._result = result
        self._exception = exception
        self._release()

    def _release(self) -> None:
        """Drop the callable and its arguments; the task will not run again."""
        factory, self._factory = self._factory, None
        if isinstance(factory, _OneShot):
            # A task cancelled before its first step never awaited the coroutine
            # it was given; close it so it is not reported as never awaited.
            factory.discard()

    def _sync(self) -> None:
        # A task cancelled before its first step finishes one event-loop
        # iteration before the supervisor's done callback records it. Report
        # it as cancelled in the meantime instead of as still running.
        task = self._task
        if self._state not in _FINAL_STATES and task is not None and task.cancelled():
            self._state = TaskState.CANCELLED
            self._release()

    def __repr__(self) -> str:
        return (
            f"<SupervisedTask {self._name!r} state={self.state.value} "
            f"restarts={self._restarts} failures={self._failures}>"
        )


class TaskSupervisor:
    """Run, watch, restart and cleanly cancel a set of long-running tasks.

    Use it as an async context manager and start tasks with :meth:`start`.
    Like :class:`asyncio.TaskGroup`, exiting the ``async with`` block waits
    for every task to finish; call :meth:`stop` to cancel them instead. No
    task ever outlives the block.

    Failures:
        A task *fails for good* when it raises and is not going to be
        restarted (it has no restart policy, its restart limit is reached, or
        the supervisor is stopping). Exceptions from runs that were restarted
        are not failures; they are logged and available on the handle.

        With ``fail_fast=True`` (the default, matching ``TaskGroup``), the
        first failure cancels every other task *and* the body of the
        ``async with`` block. With ``fail_fast=False``, the failure is recorded
        and the other tasks keep running. Either way, all failures are raised
        together as an :class:`ExceptionGroup` when the block exits, each one
        annotated with the name of the task that raised it.

    Cancellation:
        If the task running the ``async with`` block is cancelled, the
        supervisor cancels its children, waits for them to finish their
        cleanup, and re-raises :class:`asyncio.CancelledError`. The
        cancellation the supervisor uses internally to interrupt the body in
        fail-fast mode is withdrawn with :meth:`asyncio.Task.uncancel` and
        never escapes, so it is safe to use inside :func:`asyncio.timeout`.

        As with :class:`asyncio.TaskGroup`, failures take priority: if tasks
        failed, the ``ExceptionGroup`` is raised instead of ``CancelledError``.
        On Python 3.13 and later the cancellation request is kept pending, as
        ``TaskGroup`` does there, so it is delivered at the next ``await``. On
        3.11 and 3.12 it is absorbed, again matching ``TaskGroup``.

        A task that ends with a ``CancelledError`` although nobody cancelled
        it (it awaited a future that something else cancelled) counts as a
        failed run when it has a restart policy, so it is restarted instead of
        stopping silently.

    ``KeyboardInterrupt`` and ``SystemExit``, whether raised by a task or by
    the body, always stop the supervisor and are re-raised on their own,
    never wrapped in a group.

    Memory:
        By default :attr:`tasks` keeps a handle for every task started,
        finished ones included, so their final state can be inspected. For an
        unbounded stream of short-lived tasks (one per connection or message)
        pass ``retain_finished=False`` so that finished tasks are dropped.
        Failures are still collected in :attr:`errors` until the block exits,
        so handle expected exceptions inside such tasks.

    Args:
        fail_fast: Whether one task failing for good cancels everything else.
        restart_delay: Default delay before a task's first restart. Each
            further restart doubles it.
        max_restart_delay: Upper bound for the restart delay. Defaults to 30
            seconds, or to ``restart_delay`` if that is larger.
        retain_finished: Whether :attr:`tasks` keeps finished tasks.
        name: Optional name for log messages and ``repr``.
        clock: Time source for restart backoff; see :mod:`steadypy.testing`.
    """

    def __init__(
        self,
        *,
        fail_fast: bool = True,
        restart_delay: float = 0.5,
        max_restart_delay: float | None = None,
        retain_finished: bool = True,
        name: str | None = None,
        clock: Clock | None = None,
    ) -> None:
        if max_restart_delay is None:
            max_restart_delay = max(_DEFAULT_MAX_RESTART_DELAY, restart_delay)
        _check_delays(restart_delay, max_restart_delay)
        self._fail_fast = fail_fast
        self._retain_finished = retain_finished
        self._restart_delay = restart_delay
        self._max_restart_delay = max_restart_delay
        self._name = name
        self._clock = resolve_clock(clock)
        self._tasks: dict[str, SupervisedTask[Any]] = {}
        self._active: set[asyncio.Task[None]] = set()
        self._errors: list[BaseException] = []
        self._base_error: BaseException | None = None
        self._parent_task: asyncio.Task[Any] | None = None
        self._entered = False
        self._exiting = False
        self._closed = False
        self._stopping = False
        self._parent_cancel_requested = False
        self._done_waiter: asyncio.Future[None] | None = None

    # -- context manager -------------------------------------------------

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("a TaskSupervisor can only be entered once; create a new one")
        parent = asyncio.current_task()
        if parent is None:  # pragma: no cover - only reachable outside a task
            raise RuntimeError("TaskSupervisor must be used from inside an asyncio task")
        self._entered = True
        self._parent_task = parent
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        parent = self._parent_task
        assert parent is not None
        self._exiting = True
        propagate_cancel = exc if isinstance(exc, asyncio.CancelledError) else None
        if isinstance(exc, (KeyboardInterrupt, SystemExit)) and self._base_error is None:
            self._base_error = exc
        if exc is not None:
            self._abort()

        loop = asyncio.get_running_loop()
        while self._active:
            waiter: asyncio.Future[None] = loop.create_future()
            self._done_waiter = waiter
            try:
                await waiter
            except asyncio.CancelledError as cancel:
                # Cancelled while waiting for the children: cancel them as well,
                # but keep waiting so that none of them outlives the supervisor.
                if propagate_cancel is None:
                    propagate_cancel = cancel
                self._abort()
            finally:
                self._done_waiter = None
        self._closed = True

        if self._parent_cancel_requested and parent.uncancel() == 0:
            # The only cancellation pending on the parent is the one we issued
            # to interrupt the body after a failure. It must not escape.
            propagate_cancel = None

        if self._base_error is None and not self._errors:
            if propagate_cancel is not None:
                raise propagate_cancel
            return  # any other exception from the body propagates unchanged

        if _REARM_PENDING_CANCELLATION and parent.cancelling():
            # Failures take priority over a pending cancellation, but the
            # cancellation must not be lost: re-arm it so that it is delivered
            # at the parent's next await, as TaskGroup does since 3.13.
            parent.uncancel()
            parent.cancel()
        if self._base_error is not None:
            raise self._base_error
        errors = list(self._errors)
        if exc is not None and not isinstance(exc, asyncio.CancelledError):
            errors.insert(0, exc)
        raise BaseExceptionGroup("unhandled errors in TaskSupervisor", errors)

    # -- starting and stopping tasks ---------------------------------------

    @overload
    def start(
        self,
        target: Coroutine[Any, Any, T],
        /,
        *,
        name: str | None = None,
        restart: bool | RestartMode = False,
        max_restarts: int | None = None,
        restart_window: float | None = None,
        restart_delay: float | None = None,
        max_restart_delay: float | None = None,
    ) -> SupervisedTask[T]: ...

    @overload
    def start(
        self,
        target: Callable[[Unpack[Ts]], Awaitable[T]],
        /,
        *args: *Ts,
        name: str | None = None,
        restart: bool | RestartMode = False,
        max_restarts: int | None = None,
        restart_window: float | None = None,
        restart_delay: float | None = None,
        max_restart_delay: float | None = None,
    ) -> SupervisedTask[T]: ...

    # mypy cannot yet check an overload implementation against a signature that
    # uses a TypeVarTuple (*args: *Ts); call sites are still fully checked.
    def start(  # type: ignore[misc]
        self,
        target: Any,
        /,
        *args: Any,
        name: str | None = None,
        restart: bool | RestartMode = False,
        max_restarts: int | None = None,
        restart_window: float | None = None,
        restart_delay: float | None = None,
        max_restart_delay: float | None = None,
    ) -> SupervisedTask[Any]:
        """Start a supervised task and return its :class:`SupervisedTask` handle.

        Args:
            target: Either a coroutine object (``worker()``), which runs once,
                or a callable returning an awaitable (``worker``), which is
                called with ``*args`` for every run. Restartable tasks need a
                callable, because a coroutine object can only be awaited once.
            *args: Positional arguments for a callable ``target``.
            name: Name for the task; defaults to the target's qualified name,
                suffixed with ``-2``, ``-3``... if that name is taken. Also used
                as the underlying :class:`asyncio.Task`'s name.
            restart: The task's :data:`RestartMode`.
            max_restarts: Maximum number of restarts, or ``None`` for no limit.
                Once it is reached, the next exit is final.
            restart_window: If given, only restarts within the last
                ``restart_window`` seconds count toward ``max_restarts`` and
                toward the backoff exponent, so a task that has been stable for
                a while starts again from the shortest delay.
            restart_delay: Overrides the supervisor's ``restart_delay``.
            max_restart_delay: Overrides the supervisor's ``max_restart_delay``.
                When only ``restart_delay`` is overridden, the cap is raised to
                it if necessary.

        Raises:
            RuntimeError: If the supervisor is not entered, is stopping, or has
                already exited.
            TypeError: If ``target`` is not a coroutine or callable, or a
                coroutine object is combined with ``*args`` or a restart policy.
            ValueError: If ``name`` belongs to a task that is still live, or a
                numeric argument is out of range.
        """
        try:
            if not self._entered:
                raise RuntimeError("TaskSupervisor.start() called outside of 'async with'")
            if self._closed:
                raise RuntimeError("TaskSupervisor has already exited")
            if self._stopping:
                raise RuntimeError("TaskSupervisor is stopping; no new tasks can be started")
            mode = _restart_mode(restart)
            if max_restarts is not None and max_restarts < 0:
                raise ValueError("max_restarts must be >= 0 or None")
            if restart_window is not None and restart_window <= 0:
                raise ValueError("restart_window must be > 0 or None")
            delay = self._restart_delay if restart_delay is None else restart_delay
            if max_restart_delay is None:
                max_delay = max(self._max_restart_delay, delay)
            else:
                max_delay = max_restart_delay
            _check_delays(delay, max_delay)

            factory: Callable[[], Awaitable[Any]]
            if inspect.iscoroutine(target):
                if args:
                    raise TypeError(
                        "positional arguments can only be passed with a callable target"
                    )
                if mode != "never":
                    raise TypeError(
                        "a coroutine object runs only once, so it cannot be restarted; pass "
                        "the coroutine function instead: start(worker, restart=True), "
                        "not start(worker(), restart=True)"
                    )
                factory = _OneShot(target)
                default_name = target.__qualname__
            elif callable(target):
                factory = functools.partial(target, *args)
                default_name = getattr(target, "__qualname__", None) or type(target).__qualname__
            else:
                raise TypeError(
                    "target must be a coroutine or a callable returning an awaitable, "
                    f"not {type(target).__name__}"
                )
            task_name = self._claim_name(name, default_name)
        except BaseException:
            if inspect.iscoroutine(target):
                target.close()  # avoid a "coroutine was never awaited" warning
            raise

        handle: SupervisedTask[Any] = SupervisedTask(
            name=task_name,
            factory=factory,
            restart=mode,
            max_restarts=max_restarts,
            restart_window=restart_window,
            restart_delay=delay,
            max_restart_delay=max_delay,
        )
        self._tasks[task_name] = handle
        task = asyncio.get_running_loop().create_task(self._run(handle), name=task_name)
        handle._task = task
        self._active.add(task)
        task.add_done_callback(functools.partial(self._on_task_done, handle))
        return handle

    def stop(self) -> None:
        """Cancel every task and prevent restarts and new tasks.

        Returns immediately; the ``async with`` block's exit waits for the
        tasks to finish their cleanup. Tasks cancelled this way are not
        failures. Calling ``stop()`` more than once is harmless.
        """
        if not self._entered:
            raise RuntimeError("TaskSupervisor.stop() called outside of 'async with'")
        self._abort()

    # -- inspection -----------------------------------------------------------

    @property
    def name(self) -> str | None:
        """The supervisor's name, if it was given one."""
        return self._name

    @property
    def tasks(self) -> Mapping[str, SupervisedTask[Any]]:
        """A read-only snapshot of the supervisor's tasks, keyed by name.

        Includes finished tasks unless the supervisor was created with
        ``retain_finished=False``.
        """
        return MappingProxyType(dict(self._tasks))

    @property
    def errors(self) -> tuple[BaseException, ...]:
        """Exceptions from tasks that have failed for good so far."""
        return tuple(self._errors)

    @property
    def stopping(self) -> bool:
        """``True`` once :meth:`stop` was called or a fail-fast failure occurred."""
        return self._stopping

    def __repr__(self) -> str:
        label = f" {self._name!r}" if self._name else ""
        if self._closed:
            status = "closed"
        elif self._stopping:
            status = "stopping"
        elif self._entered:
            status = "running"
        else:
            status = "new"
        return (
            f"<TaskSupervisor{label} {status} tasks={len(self._active)} errors={len(self._errors)}>"
        )

    # -- internals ----------------------------------------------------------

    def _claim_name(self, name: str | None, default: str) -> str:
        if name is not None:
            if not isinstance(name, str) or not name:
                raise ValueError("name must be a non-empty string")
            existing = self._tasks.get(name)
            if existing is not None and not existing.done():
                raise ValueError(f"a supervised task named {name!r} is already running")
            return name
        candidate, suffix = default, 1
        while candidate in self._tasks:
            suffix += 1
            candidate = f"{default}-{suffix}"
        return candidate

    async def _run(self, handle: SupervisedTask[Any]) -> None:
        try:
            await self._run_until_final(handle)
        except asyncio.CancelledError:
            handle._state = TaskState.CANCELLED
            raise

    async def _run_until_final(self, handle: SupervisedTask[Any]) -> None:
        task = asyncio.current_task()
        assert task is not None
        while True:
            handle._state = TaskState.RUNNING
            result: Any = None
            failure: BaseException | None = None
            factory = handle._factory
            assert factory is not None
            try:
                awaitable = factory()
                if not inspect.isawaitable(awaitable):
                    raise TypeError(
                        f"target of supervised task {handle.name!r} returned "
                        f"{type(awaitable).__name__}, not an awaitable"
                    )
                result = await awaitable
            except asyncio.CancelledError as exc:
                if handle._restart == "never" or self._cancellation_requested(handle, task):
                    raise
                # Nobody asked this task to stop: something it awaited was
                # cancelled elsewhere. Let the restart policy handle it rather
                # than letting the task end silently.
                failure = exc
            except Exception as exc:
                failure = exc
            except GeneratorExit:
                raise  # the coroutine is being closed; not a failure
            except BaseException as exc:
                # KeyboardInterrupt, SystemExit and the like are never restarted.
                handle._failures += 1
                handle._last_exception = exc
                handle._finish(TaskState.FAILED, exception=exc)
                return

            if failure is not None:
                handle._failures += 1
                handle._last_exception = failure
            if not self._wants_restart(handle, task, failed=failure is not None):
                self._finish_run(handle, result, failure)
                return
            delay = handle._reserve_restart(self._clock.monotonic())
            if delay is None:
                logger.warning(
                    "supervised task %r reached its restart limit (%d) and will not be restarted",
                    handle.name,
                    handle._max_restarts,
                )
                self._finish_run(handle, result, failure)
                return
            handle._state = TaskState.RESTARTING
            if isinstance(failure, asyncio.CancelledError):
                outcome = "was stopped by a CancelledError that nobody requested"
            elif failure is not None:
                outcome = f"failed with {failure!r}"
            else:
                outcome = "returned"
            logger.warning(
                "supervised task %r %s; restarting in %.3gs (restart #%d)",
                handle.name,
                outcome,
                delay,
                handle._restarts + 1,
            )
            await self._clock.sleep(delay)
            handle._restarts += 1

    def _cancellation_requested(
        self, handle: SupervisedTask[Any], task: asyncio.Task[None]
    ) -> bool:
        return self._stopping or handle._cancel_requested or task.cancelling() > 0

    def _wants_restart(
        self, handle: SupervisedTask[Any], task: asyncio.Task[None], *, failed: bool
    ) -> bool:
        if handle._restart == "never" or self._cancellation_requested(handle, task):
            # This also covers a cancellation request that the task swallowed:
            # honour the request rather than starting the task again.
            return False
        return failed or handle._restart == "always"

    @staticmethod
    def _finish_run(
        handle: SupervisedTask[Any], result: Any, failure: BaseException | None
    ) -> None:
        if isinstance(failure, asyncio.CancelledError):
            raise failure  # out of restarts after stray cancellations: end as cancelled
        if failure is not None:
            handle._finish(TaskState.FAILED, exception=failure)
        else:
            handle._finish(TaskState.COMPLETED, result=result)

    def _on_task_done(self, handle: SupervisedTask[Any], task: asyncio.Task[None]) -> None:
        self._active.discard(task)
        if task.cancelled():
            # Also covers a task cancelled before its coroutine ever started.
            handle._state = TaskState.CANCELLED
        elif (exc := task.exception()) is not None:  # pragma: no cover - defensive
            handle._finish(TaskState.FAILED, exception=exc)
        handle._release()
        if handle._state is TaskState.FAILED:
            self._record_failure(handle)
        if not self._retain_finished and self._tasks.get(handle.name) is handle:
            del self._tasks[handle.name]
        waiter = self._done_waiter
        if waiter is not None and not self._active and not waiter.done():
            waiter.set_result(None)

    def _record_failure(self, handle: SupervisedTask[Any]) -> None:
        exc = handle._exception
        assert exc is not None
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            if self._base_error is None:
                self._base_error = exc
            self._abort(cancel_parent=True)
            return
        exc.add_note(f"raised by supervised task {handle.name!r}")
        self._errors.append(exc)
        logger.error("supervised task %r failed: %r", handle.name, exc, exc_info=exc)
        if self._fail_fast:
            self._abort(cancel_parent=True)

    def _abort(self, *, cancel_parent: bool = False) -> None:
        self._stopping = True
        for task in self._active:
            if not task.done():
                task.cancel()
        if cancel_parent and not self._exiting and not self._parent_cancel_requested:
            parent = self._parent_task
            if parent is not None and not parent.done():
                self._parent_cancel_requested = True
                parent.cancel()


def _check_delays(delay: float, max_delay: float) -> None:
    if delay < 0:
        raise ValueError("restart_delay must be >= 0")
    if max_delay < delay:
        raise ValueError("max_restart_delay must be >= restart_delay")
