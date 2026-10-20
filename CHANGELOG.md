# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).
Until 1.0, minor versions may contain breaking changes; they will always be
listed here.

## [Unreleased]

## [0.1.0] - 2026-10-20

First tagged version. It is available from GitHub (`v0.1.0`) and has not been
published to PyPI.

### Added

- `TaskSupervisor` and `SupervisedTask`: structured supervision of
  long-running tasks with restart policies (`"on-failure"`, `"always"`),
  exponential restart backoff, restart limits with an optional sliding
  window, fail-fast or isolated failure handling, state inspection, and
  `retain_finished=False` for unbounded streams of short-lived tasks.
- `retry()` and `@async_retry`: retries with capped exponential backoff,
  jitter, `retry_on` / `retry_if` filtering, a total deadline and `on_retry`
  callbacks. Cancellation is never retried.
- `CircuitBreaker`: closed / open / half-open with bounded concurrent
  probes that expire after `recovery_timeout`, generation-checked state
  transitions, statistics, `reset()`, and `guard()` for blocks that may be
  exited in a different task.
- `bounded_map()` and `bounded_stream()`: lazily consumed, bounded
  concurrency over iterables and async iterables, in input or completion
  order.
- `RateLimiter`: an async token bucket with bursts, FIFO waiters and
  cancellation-safe waiting.
- `HealthRegistry`: heartbeat-based component health with critical and
  non-critical components and JSON-ready reports.
- `ShutdownSignal`: an awaitable shutdown flag with SIGINT/SIGTERM handling
  and restoration of previous handlers.
- `steadypy.testing.FakeClock` for deterministic tests of time-dependent code.

[Unreleased]: https://github.com/anika-kulkarni/steadypy/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/anika-kulkarni/steadypy/releases/tag/v0.1.0
