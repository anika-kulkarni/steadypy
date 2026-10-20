from __future__ import annotations

import json
from datetime import UTC

import pytest

from steadypy import HealthRegistry
from steadypy.testing import FakeClock


@pytest.fixture
def health(clock: FakeClock) -> HealthRegistry:
    return HealthRegistry(default_timeout=10, clock=clock)


def test_component_is_unhealthy_until_its_first_heartbeat(health: HealthRegistry) -> None:
    health.register("db")
    status = health.check("db")
    assert not status.healthy
    assert status.reason == "no heartbeat received yet"
    assert status.last_heartbeat is None
    assert status.seconds_since_heartbeat is None
    assert not health.is_healthy()


async def test_heartbeat_is_valid_for_exactly_the_timeout(
    health: HealthRegistry, clock: FakeClock
) -> None:
    health.register("db")
    health.heartbeat("db")
    await clock.advance(10)
    assert health.check("db").healthy
    await clock.advance(0.5)
    status = health.check("db")
    assert not status.healthy
    assert status.seconds_since_heartbeat == pytest.approx(10.5)
    assert status.reason == "no heartbeat for 10.5s (timeout 10s)"
    health.heartbeat("db")
    assert health.check("db").healthy


async def test_per_component_timeouts(health: HealthRegistry, clock: FakeClock) -> None:
    health.register("fast", timeout=1)
    health.register("slow")
    health.heartbeat("fast")
    health.heartbeat("slow")
    await clock.advance(2)
    report = health.status()
    assert report.unhealthy == ("fast",)
    assert report.components["slow"].timeout == 10


def test_heartbeat_details_replace_previous_details(health: HealthRegistry) -> None:
    source = {"queue_depth": 3}
    health.register("worker", details={"version": "1.2"})
    assert dict(health.check("worker").details) == {"version": "1.2"}
    health.heartbeat("worker", details=source)
    source["queue_depth"] = 99  # the registry keeps its own copy
    assert dict(health.check("worker").details) == {"queue_depth": 3}
    health.heartbeat("worker")  # no details: keep the previous ones
    assert dict(health.check("worker").details) == {"queue_depth": 3}
    with pytest.raises(TypeError):
        health.check("worker").details["queue_depth"] = 0  # type: ignore[index]


def test_mark_unhealthy_lasts_until_the_next_heartbeat(health: HealthRegistry) -> None:
    health.register("db")
    health.heartbeat("db")
    health.mark_unhealthy("db", "connection refused", details={"host": "db-1"})
    status = health.check("db")
    assert not status.healthy
    assert status.reason == "connection refused"
    assert dict(status.details) == {"host": "db-1"}
    health.heartbeat("db")
    assert health.check("db").healthy


def test_only_critical_components_decide_overall_health(health: HealthRegistry) -> None:
    health.register("db")
    health.register("cache", critical=False)
    health.heartbeat("db")
    report = health.status()
    assert report.healthy
    assert report.degraded
    assert report.unhealthy == ("cache",)
    health.mark_unhealthy("db", "down")
    report = health.status()
    assert not report.healthy
    assert not report.degraded


def test_empty_registry_is_healthy(health: HealthRegistry) -> None:
    report = health.status()
    assert report.healthy
    assert not report.degraded
    assert report.to_dict() == {"healthy": True, "degraded": False, "components": {}}


def test_report_serializes_to_json(health: HealthRegistry) -> None:
    health.register("db", details={"pool": 5})
    health.heartbeat("db")
    payload = json.loads(json.dumps(health.status().to_dict()))
    component = payload["components"]["db"]
    assert payload["healthy"] is True
    assert component["healthy"] is True
    assert component["critical"] is True
    assert component["details"] == {"pool": 5}
    assert component["reason"] is None
    last = health.check("db").last_heartbeat
    assert last is not None
    assert last.tzinfo is UTC
    assert component["last_heartbeat"] == last.isoformat()


def test_registration_and_lookup_errors(health: HealthRegistry) -> None:
    health.register("db")
    with pytest.raises(ValueError, match="already registered"):
        health.register("db")
    with pytest.raises(ValueError, match="timeout"):
        health.register("bad", timeout=0)
    for operation in (
        lambda: health.heartbeat("nope"),
        lambda: health.mark_unhealthy("nope", "x"),
        lambda: health.check("nope"),
        lambda: health.unregister("nope"),
    ):
        with pytest.raises(KeyError, match="unknown component 'nope'"):
            operation()
    with pytest.raises(ValueError, match="timeout"):
        HealthRegistry(default_timeout=-1)


def test_unregister_and_container_protocol(health: HealthRegistry) -> None:
    health.register("a")
    health.register("b")
    assert health.names == ("a", "b")
    assert "a" in health
    assert len(health) == 2
    health.unregister("a")
    assert "a" not in health
    assert health.names == ("b",)
    assert repr(health) == "<HealthRegistry components=1>"
