"""Concurrency tests for the CHS rate limiter.

CHS publishes 3 requests/second and 30 requests/minute and answers HTTP 429
past either. Deciding whether to wait and then recording the request is a
check-then-act sequence, so the limiter must serialize admission: without
it, concurrent callers all observe the same under-cap history and fire
together. Measured with four threads against an unlocked implementation:
11 admissions in one second and 39 in one minute.

These run against the rolling one-second window only, so they stay fast.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ofs_skill.obs_retrieval import chs_utils
from ofs_skill.obs_retrieval.chs_utils import (
    _MAX_PER_MINUTE,
    _MAX_PER_SECOND,
    RateLimiter,
)


class _FakeClock:
    """Deterministic stand-in for the time module.

    The per-minute cap is the one that actually governs a run, but testing
    it in real time costs a minute per assertion. Driving the limiter with
    a virtual clock makes it instant and exact.
    """

    def __init__(self, start=1000.0):
        self.now = start

    def time(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0, f'negative sleep: {seconds}'
        self.now += seconds


def _admit(limiter, count, workers):
    """Run ``count`` admissions across ``workers`` threads; return stamps."""
    stamps: list[float] = []
    lock = threading.Lock()

    def one(_):
        limiter.wait()
        with lock:
            stamps.append(time.time())

    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(one, range(count)))

    return sorted(stamps)


def _worst_window(stamps, span):
    """Largest number of stamps falling in any half-open window of `span`."""
    return max(
        sum(1 for s in stamps if start <= s < start + span)
        for start in stamps
    )


@pytest.mark.parametrize('workers', [1, 2, 4, 8])
def test_per_second_cap_holds_under_concurrency(workers):
    """No one-second window may contain more than the published burst."""
    stamps = _admit(RateLimiter(), 12, workers)

    assert _worst_window(stamps, 1.0) <= _MAX_PER_SECOND


def test_concurrent_callers_are_not_admitted_together():
    """The unlocked version released whole batches early.

    9 admissions capped at 3/sec need two full second-boundaries, so the
    span must exceed 2s. The unlocked implementation completed the same 9
    inside 1.0s, because after sleeping it appended without re-checking.
    """
    limiter = RateLimiter()
    stamps = _admit(limiter, 9, 9)

    assert stamps[-1] - stamps[0] >= 1.9


def test_admission_is_recorded_once_per_call():
    """Each wait() records exactly one request in both rolling windows."""
    limiter = RateLimiter()
    _admit(limiter, 3, 1)

    assert len(limiter.second_history) == 3
    assert len(limiter.minute_history) == 3


class TestPerMinuteCap:
    """The 30/min cap is what throttles a real run; test it directly.

    With 3 CHS workers and ~8 requests per station the per-second cap is
    rarely reached, so a regression that removed or mis-tuned the minute
    window would not be caught by the burst tests above.
    """

    @staticmethod
    def _seeded_limiter(monkeypatch):
        clock = _FakeClock()
        monkeypatch.setattr(chs_utils, 'time', clock)
        limiter = RateLimiter()
        # Fill the minute window without ever touching the 3/sec cap.
        for _ in range(_MAX_PER_MINUTE):
            limiter.wait()
            clock.now += 1.0
        return limiter, clock

    def test_admission_blocks_once_the_minute_window_is_full(
            self, monkeypatch):
        """The 31st request waits for the oldest to age out of 60s."""
        limiter, clock = self._seeded_limiter(monkeypatch)

        start = clock.now
        limiter.wait()

        # 30 admissions occupy t..t+29, so at t+30 the oldest is 30s old
        # and the caller must wait the remaining 30s.
        assert clock.now - start >= 30.0

    def test_minute_window_is_sixty_seconds(self, monkeypatch):
        """A mis-tuned purge window would let the cap be exceeded."""
        limiter, clock = self._seeded_limiter(monkeypatch)

        # Nothing has aged out yet, so the window still holds all 30.
        assert len(limiter.minute_history) == _MAX_PER_MINUTE

        limiter.wait()

        # After the wait exactly one slot freed up and was reused.
        assert len(limiter.minute_history) <= _MAX_PER_MINUTE

    def test_no_wait_once_the_window_has_drained(self, monkeypatch):
        """A full window must not throttle forever."""
        limiter, clock = self._seeded_limiter(monkeypatch)

        clock.now += 120.0
        start = clock.now
        limiter.wait()

        assert clock.now == start
