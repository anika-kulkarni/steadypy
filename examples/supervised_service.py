"""Several long-running workers managed by a TaskSupervisor.

    python examples/supervised_service.py [seconds]

The service runs a job producer, two consumers, a connection that keeps
dropping, and a stats reporter. The flaky connection is restarted with
exponential backoff and gives up after too many restarts in a short window.
Isolated failure handling (``fail_fast=False``) keeps the other workers
running after it gives up. After a few seconds the service stops, and the
supervisor cancels every worker and waits for their cleanup.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import sys

from steadypy import TaskSupervisor


async def producer(queue: asyncio.Queue[int]) -> None:
    for job in itertools.count(1):
        await queue.put(job)
        await asyncio.sleep(0.05)


async def consumer(queue: asyncio.Queue[int], done: list[int]) -> None:
    while True:
        job = await queue.get()
        try:
            await asyncio.sleep(0.08)  # do the work
            done.append(job)
        finally:
            queue.task_done()


class FlakyConnection:
    """Pretends to hold a connection that drops every half second."""

    def __init__(self) -> None:
        self.connects = 0

    async def run(self) -> None:
        self.connects += 1
        print(f"  connection: connected (attempt {self.connects})")
        await asyncio.sleep(0.5)
        raise ConnectionResetError("peer closed the connection")


async def reporter(queue: asyncio.Queue[int], done: list[int]) -> None:
    while True:
        await asyncio.sleep(1)
        print(f"  stats: {len(done)} jobs done, {queue.qsize()} queued")


async def main(duration: float) -> None:
    logging.basicConfig(level=logging.WARNING, format="  log: %(message)s", stream=sys.stdout)
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=20)
    done: list[int] = []
    connection = FlakyConnection()

    try:
        async with TaskSupervisor(fail_fast=False, restart_delay=0.2) as supervisor:
            supervisor.start(producer, queue, name="producer", restart=True)
            for n in (1, 2):
                supervisor.start(consumer, queue, done, name=f"consumer-{n}", restart=True)
            supervisor.start(
                connection.run,
                name="connection",
                restart=True,
                max_restarts=3,
                restart_window=10,
            )
            supervisor.start(reporter, queue, done, name="reporter")

            await asyncio.sleep(duration)
            print("stopping...")
            supervisor.stop()
    except* ConnectionResetError as group:
        print(f"the connection gave up for good: {group.exceptions[0]!r}")

    print("final task states:")
    for name, task in supervisor.tasks.items():
        print(f"  {name:<12} {task.state.value:<10} restarts={task.restarts}")


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else 5.0))
