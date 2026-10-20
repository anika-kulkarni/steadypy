"""Poll a flaky API with retries, per-attempt timeouts, and a rate limit.

    python examples/resilient_worker.py

Nothing here touches the network: ``FlakyWeatherAPI`` stands in for an HTTP
service that rejects about a quarter of requests and occasionally hangs.
The worker:

* stays under the provider's quota with a ``RateLimiter``,
* bounds every attempt with ``asyncio.timeout``,
* retries transient errors and timeouts with jittered backoff,
* fetches many stations at once with ``bounded_map``, without letting one
  failing station abort the whole batch.
"""

from __future__ import annotations

import asyncio
import random

from steadypy import RateLimiter, RetryAttempt, bounded_map, retry


class TransientAPIError(Exception):
    """A 5xx-style error: worth retrying."""


class FlakyWeatherAPI:
    def __init__(self, seed: int = 7) -> None:
        self._rng = random.Random(seed)
        self.requests = 0

    async def temperature(self, station: str) -> float:
        self.requests += 1
        await asyncio.sleep(self._rng.uniform(0.01, 0.03))  # network latency
        roll = self._rng.random()
        if roll < 0.25:
            raise TransientAPIError(f"503 Service Unavailable ({station})")
        if roll < 0.30:
            await asyncio.sleep(60)  # a request that hangs
        return round(self._rng.uniform(-5, 30), 1)


async def main() -> None:
    api = FlakyWeatherAPI()
    limiter = RateLimiter(rate=40, per=1.0)  # the provider allows 40 requests/s

    def report_retry(attempt: RetryAttempt) -> None:
        print(f"    retry #{attempt.attempt}: {attempt.exception!r}, waiting {attempt.delay:.2f}s")

    async def fetch_once(station: str) -> float:
        async with limiter, asyncio.timeout(0.25):
            return await api.temperature(station)

    async def fetch(station: str) -> float:
        return await retry(
            lambda: fetch_once(station),
            attempts=4,
            base_delay=0.05,
            max_delay=0.5,
            retry_on=(TransientAPIError, TimeoutError),
            on_retry=report_retry,
        )

    stations = [f"station-{n:02d}" for n in range(16)]
    for cycle in (1, 2):
        print(f"cycle {cycle}: polling {len(stations)} stations")
        readings = await bounded_map(fetch, stations, concurrency=6, return_exceptions=True)
        failed = [s for s, r in zip(stations, readings, strict=True) if isinstance(r, Exception)]
        print(
            f"cycle {cycle}: {len(stations) - len(failed)} readings, "
            f"{len(failed)} gave up {failed}, {api.requests} requests in total\n"
        )


if __name__ == "__main__":
    asyncio.run(main())
