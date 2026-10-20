"""A small service that starts, reports health, and shuts down cleanly.

    python examples/graceful_app.py            # runs until Ctrl-C (or SIGTERM)
    python examples/graceful_app.py 5          # or stops by itself after 5 seconds

It combines:

* ``ShutdownSignal``: SIGINT/SIGTERM (or a timer) request a shutdown;
* ``TaskSupervisor``: runs the workers and restarts the flaky one;
* ``HealthRegistry``: each worker heartbeats; a reporter prints the status;
* ``CircuitBreaker``: protects calls to a dependency that has an outage.

On shutdown the supervisor cancels every worker and waits for their cleanup,
and the whole shutdown is bounded by a grace period.
"""

from __future__ import annotations

import asyncio
import random
import sys

from steadypy import (
    CircuitBreaker,
    CircuitOpenError,
    HealthRegistry,
    ShutdownSignal,
    TaskSupervisor,
)

SHUTDOWN_GRACE_SECONDS = 5


class Dependency:
    """A downstream service that is unavailable between t=1.5s and t=3s."""

    def __init__(self) -> None:
        self._started = asyncio.get_running_loop().time()

    async def enrich(self, job: int) -> str:
        await asyncio.sleep(0.02)
        elapsed = asyncio.get_running_loop().time() - self._started
        if 1.5 <= elapsed < 3.0:
            raise ConnectionError("dependency unavailable")
        return f"job-{job}:enriched"


async def job_worker(health: HealthRegistry, breaker: CircuitBreaker, dep: Dependency) -> None:
    job = 0
    while True:
        job += 1
        try:
            result = await breaker.call(dep.enrich, job)
        except CircuitOpenError:
            result = f"job-{job}:skipped (circuit open)"
        except ConnectionError:
            result = f"job-{job}:failed"
        health.heartbeat("jobs", details={"last": result})
        await asyncio.sleep(0.25)


async def cache_refresher(health: HealthRegistry) -> None:
    rng = random.Random(3)
    while True:
        await asyncio.sleep(0.3)
        if rng.random() < 0.2:
            raise TimeoutError("cache refresh timed out")
        health.heartbeat("cache")


async def health_reporter(health: HealthRegistry, breaker: CircuitBreaker) -> None:
    while True:
        await asyncio.sleep(1)
        report = health.status()
        parts = [f"{n}={'ok' if c.healthy else c.reason}" for n, c in report.components.items()]
        print(f"health: healthy={report.healthy} {' '.join(parts)} breaker={breaker.state.value}")


async def main(run_for: float | None) -> None:
    shutdown = ShutdownSignal()
    shutdown.install_signal_handlers()
    if run_for is not None:
        asyncio.get_running_loop().call_later(run_for, shutdown.trigger, "timer expired")

    health = HealthRegistry(default_timeout=2)
    health.register("jobs")
    health.register("cache", critical=False)
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1, name="dependency")
    dependency = Dependency()

    try:
        # No deadline while running; one is set the moment shutdown begins, so
        # workers that take too long to clean up are cancelled again.
        async with asyncio.timeout(None) as shutdown_deadline:
            async with TaskSupervisor(restart_delay=0.1) as supervisor:
                supervisor.start(job_worker, health, breaker, dependency, restart=True)
                supervisor.start(
                    cache_refresher, health, restart=True, max_restarts=20, restart_window=60
                )
                supervisor.start(health_reporter, health, breaker)
                print("service running; press Ctrl-C to stop")

                reason = await shutdown.wait()
                print(f"shutting down ({reason})...")
                loop = asyncio.get_running_loop()
                shutdown_deadline.reschedule(loop.time() + SHUTDOWN_GRACE_SECONDS)
                supervisor.stop()
    finally:
        shutdown.remove_signal_handlers()

    print("stopped cleanly:")
    for name, task in supervisor.tasks.items():
        print(f"  {name:<16} {task.state.value:<10} restarts={task.restarts}")


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else None))
