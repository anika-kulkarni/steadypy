"""Heartbeat-based health tracking for long-running services.

Components register with a timeout and send heartbeats while they make
progress. A component that has not sent one within its timeout is unhealthy.

Example::

    health = HealthRegistry(default_timeout=30)
    health.register("database")
    health.register("cache", critical=False)

    health.heartbeat("database", details={"pool_size": 10})

    report = health.status()
    report.healthy          # all critical components healthy?
    report.to_dict()        # JSON-ready, for whatever endpoint you already serve

This module deliberately stops at the data: it does not run an HTTP server.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from steadypy.clock import Clock, resolve_clock

__all__ = ["ComponentHealth", "HealthRegistry", "HealthReport"]

_EMPTY: Mapping[str, Any] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    """The health of one component at the moment :meth:`HealthRegistry.status` ran.

    Attributes:
        name: The component's name.
        healthy: Whether the component is currently healthy.
        critical: Whether the component counts toward overall health.
        timeout: Seconds a heartbeat stays valid.
        last_heartbeat: Wall-clock (UTC) time of the last heartbeat, if any.
            For display only; staleness is measured with a monotonic clock.
        seconds_since_heartbeat: Age of the last heartbeat, if any.
        reason: Why the component is unhealthy, or ``None`` when healthy.
        details: Free-form details from the latest heartbeat or failure report.
    """

    name: str
    healthy: bool
    critical: bool
    timeout: float
    last_heartbeat: datetime | None
    seconds_since_heartbeat: float | None
    reason: str | None
    details: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict (assuming the details are)."""
        return {
            "healthy": self.healthy,
            "critical": self.critical,
            "timeout": self.timeout,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "seconds_since_heartbeat": self.seconds_since_heartbeat,
            "reason": self.reason,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class HealthReport:
    """A point-in-time health report for a whole registry.

    Attributes:
        healthy: ``True`` when every critical component is healthy (and so
            also when no critical components are registered).
        components: Per-component health, keyed by name.
    """

    healthy: bool
    components: Mapping[str, ComponentHealth]

    @property
    def degraded(self) -> bool:
        """Healthy overall, but at least one non-critical component is not."""
        return self.healthy and any(not c.healthy for c in self.components.values())

    @property
    def unhealthy(self) -> tuple[str, ...]:
        """Names of the components that are currently unhealthy."""
        return tuple(name for name, c in self.components.items() if not c.healthy)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict (assuming the details are)."""
        return {
            "healthy": self.healthy,
            "degraded": self.degraded,
            "components": {name: c.to_dict() for name, c in self.components.items()},
        }


@dataclass(slots=True)
class _Component:
    timeout: float
    critical: bool
    details: Mapping[str, Any]
    beat_at: float | None = None
    beat_wall: datetime | None = None
    failure: str | None = None


class HealthRegistry:
    """Tracks heartbeats from named components.

    A component is healthy when it has sent a heartbeat within its timeout
    and has not been marked unhealthy since. Until its first heartbeat it is
    unhealthy, which makes the registry usable for readiness checks: a
    service is not ready until each component has reported in.

    Args:
        default_timeout: Timeout for components registered without one.
        clock: Time source for staleness; see :mod:`steadypy.testing`.
    """

    def __init__(self, *, default_timeout: float = 30.0, clock: Clock | None = None) -> None:
        _check_timeout(default_timeout)
        self._default_timeout = default_timeout
        self._clock = resolve_clock(clock)
        self._components: dict[str, _Component] = {}

    def register(
        self,
        name: str,
        *,
        timeout: float | None = None,
        critical: bool = True,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """Start tracking a component.

        Args:
            name: Unique component name.
            timeout: Seconds a heartbeat stays valid; defaults to the
                registry's ``default_timeout``.
            critical: Whether this component counts toward overall health.
                Non-critical components can only make the report *degraded*.
            details: Initial details to report.

        Raises:
            ValueError: If ``name`` is already registered or ``timeout`` is not
                positive.
        """
        if name in self._components:
            raise ValueError(f"component {name!r} is already registered")
        timeout = self._default_timeout if timeout is None else timeout
        _check_timeout(timeout)
        self._components[name] = _Component(timeout, critical, _freeze(details))

    def unregister(self, name: str) -> None:
        """Stop tracking a component."""
        self._get(name)
        del self._components[name]

    def heartbeat(self, name: str, *, details: Mapping[str, Any] | None = None) -> None:
        """Record that ``name`` is alive and making progress.

        Clears any :meth:`mark_unhealthy` failure. ``details``, if given,
        replace the component's previous details.
        """
        component = self._get(name)
        component.beat_at = self._clock.monotonic()
        component.beat_wall = datetime.now(UTC)
        component.failure = None
        if details is not None:
            component.details = _freeze(details)

    def mark_unhealthy(
        self, name: str, reason: str, *, details: Mapping[str, Any] | None = None
    ) -> None:
        """Mark ``name`` unhealthy right away, until its next heartbeat."""
        component = self._get(name)
        component.failure = reason
        if details is not None:
            component.details = _freeze(details)

    def check(self, name: str) -> ComponentHealth:
        """Return the current health of one component."""
        return self._evaluate(name, self._get(name), self._clock.monotonic())

    def status(self) -> HealthReport:
        """Return a :class:`HealthReport` for every registered component."""
        now = self._clock.monotonic()
        components = {
            name: self._evaluate(name, component, now)
            for name, component in self._components.items()
        }
        healthy = all(c.healthy for c in components.values() if c.critical)
        return HealthReport(healthy=healthy, components=MappingProxyType(components))

    def is_healthy(self) -> bool:
        """Shortcut for ``status().healthy``."""
        return self.status().healthy

    @property
    def names(self) -> tuple[str, ...]:
        """Registered component names, in registration order."""
        return tuple(self._components)

    def __contains__(self, name: object) -> bool:
        return name in self._components

    def __len__(self) -> int:
        return len(self._components)

    def __repr__(self) -> str:
        return f"<HealthRegistry components={len(self._components)}>"

    def _get(self, name: str) -> _Component:
        try:
            return self._components[name]
        except KeyError:
            raise KeyError(f"unknown component {name!r}; register it first") from None

    @staticmethod
    def _evaluate(name: str, component: _Component, now: float) -> ComponentHealth:
        age = None if component.beat_at is None else max(0.0, now - component.beat_at)
        if component.failure is not None:
            reason: str | None = component.failure
        elif age is None:
            reason = "no heartbeat received yet"
        elif age > component.timeout:
            reason = f"no heartbeat for {age:.3g}s (timeout {component.timeout:g}s)"
        else:
            reason = None
        return ComponentHealth(
            name=name,
            healthy=reason is None,
            critical=component.critical,
            timeout=component.timeout,
            last_heartbeat=component.beat_wall,
            seconds_since_heartbeat=age,
            reason=reason,
            details=component.details,
        )


def _freeze(details: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return _EMPTY if not details else MappingProxyType(dict(details))


def _check_timeout(timeout: float) -> None:
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
