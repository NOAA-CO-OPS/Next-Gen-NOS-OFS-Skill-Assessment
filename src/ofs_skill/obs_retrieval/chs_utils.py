"""
Shared Canadian Hydrographic Service (CHS) HTTP utilities.

Enforces rolling rate limits across all CHS API interactions, matching the
limits CHS publishes at
https://tides.gc.ca/en/web-services-offered-canadian-hydrographic-service:
    - Max 3 requests per second
    - Max 30 requests per minute

Exceeding either returns HTTP 429.
"""

import re
import threading
import time
from collections import deque
from typing import Any

import requests

# Authoritative CHS API Base URLs
CHS_IWLS_BASE_URL = 'https://api-iwls.dfo-mpo.gc.ca/api/v1'
CHS_SINE_BASE_URL = 'https://api-sine.dfo-mpo.gc.ca/api/v1'

# Published CHS limits (see module docstring).
_MAX_PER_SECOND = 3
_MAX_PER_MINUTE = 30

# time.sleep can wake fractionally early, which would otherwise leave the
# window still full and force an extra loop iteration; nudge past the edge.
_SLEEP_EPSILON = 0.005

# Transient-failure retry. searvey's fetch_chs_station retried transport
# errors via tenacity (10 attempts / 90s, 2-10s random wait); calling the
# API directly means reproducing that, otherwise a single connection reset
# silently costs a whole chunk of a station's record.
_RETRY_ATTEMPTS = 4
_RETRY_BASE_DELAY = 2.0
_RETRY_MAX_DELAY = 16.0
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

# CHS backend IDs are 24-character MongoDB ObjectIds (hexadecimal)
_CHS_OBJECT_ID_RE = re.compile(r'^[0-9a-fA-F]{24}$')

def is_chs_uuid(val: Any) -> bool:
    """Return True if val matches a 24-character CHS backend ObjectId."""
    return bool(isinstance(val, str) and _CHS_OBJECT_ID_RE.match(val))

class RateLimiter:
    """Enforces dual rolling rate limits: 3/sec and 30/min.

    Admission is serialized by a lock. Deciding whether to wait and then
    recording the request is a check-then-act sequence, so without the lock
    concurrent callers all observe the same under-cap history, all decline
    to wait, and all fire together. Measured with four threads against the
    unlocked version: 11 requests in a one-second window (cap 3) and 39 in
    a minute (cap 30), which is what CHS answers with HTTP 429.

    The lock is held across the sleep, so each caller reserves its slot in
    turn. It is *not* held during the request itself -- ``chs_get`` returns
    from ``wait`` before issuing the GET -- so transfers still overlap
    across worker threads. Only admission is serialized.
    """

    def __init__(self):
        self.second_history = deque()
        self.minute_history = deque()
        self._lock = threading.Lock()

    def _purge(self, now):
        """Drop timestamps that have aged out of both rolling windows."""
        while self.second_history and now - self.second_history[0] >= 1.0:
            self.second_history.popleft()
        while self.minute_history and now - self.minute_history[0] >= 60.0:
            self.minute_history.popleft()

    def wait(self):
        with self._lock:
            while True:
                now = time.time()
                self._purge(now)

                wait_time = 0.0
                if len(self.second_history) >= _MAX_PER_SECOND:
                    wait_time = max(
                        wait_time, 1.0 - (now - self.second_history[0]))
                if len(self.minute_history) >= _MAX_PER_MINUTE:
                    wait_time = max(
                        wait_time, 60.0 - (now - self.minute_history[0]))

                # Re-check after sleeping rather than admitting straight
                # away: time.sleep can return fractionally early, leaving
                # the window still full, and the previous code appended
                # regardless. That admitted a 4th request inside a
                # one-second window whose cap is 3.
                if wait_time <= 0:
                    break
                time.sleep(wait_time + _SLEEP_EPSILON)

            self.second_history.append(now)
            self.minute_history.append(now)


# Global module singleton
chs_rate_limiter = RateLimiter()


def _retry_delay(attempt: int, response: requests.Response | None) -> float:
    """Backoff before the next attempt, honouring Retry-After on a 429."""
    if response is not None:
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return min(float(retry_after), _RETRY_MAX_DELAY)
            except ValueError:
                pass
    return min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)


def chs_get(url: str, **kwargs: Any) -> requests.Response:
    """Issue a strictly rate-limited GET request to the CHS API.

    Retries transport errors and retryable statuses with backoff. Every
    attempt passes through the rate limiter, so retries are counted against
    the published budget rather than bypassing it. The last response is
    returned even when it is still a retryable status, leaving the caller's
    existing raise_for_status handling in charge of reporting it.
    """
    last_response: requests.Response | None = None
    last_exc: Exception | None = None

    for attempt in range(_RETRY_ATTEMPTS):
        chs_rate_limiter.wait()
        try:
            response = requests.get(url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc, last_response = exc, None
        else:
            if response.status_code not in _RETRY_STATUSES:
                return response
            last_exc, last_response = None, response

        if attempt < _RETRY_ATTEMPTS - 1:
            time.sleep(_retry_delay(attempt, last_response))

    if last_response is not None:
        return last_response
    raise last_exc  # type: ignore[misc]
