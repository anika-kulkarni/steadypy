from __future__ import annotations

import pytest

from steadypy.testing import FakeClock


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
