"""
Shared Canadian Hydrographic Service (CHS) HTTP utilities.

Enforces rolling rate limits across all CHS API interactions:
    - Max 5 requests per second
    - Max 30 requests per minute
"""

import re
import time
from collections import deque
from typing import Any

import requests

# Authoritative CHS API Base URLs
CHS_IWLS_BASE_URL = 'https://api-iwls.dfo-mpo.gc.ca/api/v1'
CHS_SINE_BASE_URL = 'https://api-sine.dfo-mpo.gc.ca/api/v1'

# CHS backend IDs are 24-character MongoDB ObjectIds (hexadecimal)
_CHS_OBJECT_ID_RE = re.compile(r'^[0-9a-fA-F]{24}$')

def is_chs_uuid(val: Any) -> bool:
    """Return True if val matches a 24-character CHS backend ObjectId."""
    return bool(isinstance(val, str) and _CHS_OBJECT_ID_RE.match(val))

class RateLimiter:
    """Enforces dual rolling rate limits: 5/sec and 30/min."""

    def __init__(self):
        self.second_history = deque()
        self.minute_history = deque()

    def wait(self):
        now = time.time()

        # Purge timestamps older than 1 second and 60 seconds
        while self.second_history and now - self.second_history[0] >= 1.0:
            self.second_history.popleft()
        while self.minute_history and now - self.minute_history[0] >= 60.0:
            self.minute_history.popleft()

        wait_time = 0.0
        if len(self.second_history) >= 5:
            wait_time = max(wait_time, 1.0 - (now - self.second_history[0]))
        if len(self.minute_history) >= 30:
            wait_time = max(wait_time, 60.0 - (now - self.minute_history[0]))

        if wait_time > 0:
            time.sleep(wait_time)
            now = time.time()
            while self.second_history and now - self.second_history[0] >= 1.0:
                self.second_history.popleft()
            while self.minute_history and now - self.minute_history[0] >= 60.0:
                self.minute_history.popleft()

        self.second_history.append(now)
        self.minute_history.append(now)


# Global module singleton
chs_rate_limiter = RateLimiter()


def chs_get(url: str, **kwargs: Any) -> requests.Response:
    """Issue a strictly rate-limited GET request to the CHS API."""
    chs_rate_limiter.wait()
    return requests.get(url, **kwargs)
