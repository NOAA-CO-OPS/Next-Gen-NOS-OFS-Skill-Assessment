"""Resilience layer for ``coastalmodeling_vdatum.vdatum.convert``.

The MLLW <-> NAVD88 / IGLD85 / xgeoid20b pipelines reference a remote
geotiff served from
``https://noaa-nos-stofs2d-pds.s3.amazonaws.com/_archive/coastalmodeling-vdatum/``.
PROJ fetches the grid header via HTTP at pipeline-build time. Two things
make this brittle when called from many worker threads at once:

1. PROJ's SQLite grid cache is not safe for *concurrent first-time
   fetches* of the same URL. If N threads hit an empty cache at the
   same moment they collide and all N raise ``ProjError`` (Error 1029).
   Once the cache holds the URL, subsequent threaded calls are fine.
2. Even on a warm cache, transient network hiccups (TLS, DNS, S3 5xx)
   can briefly break a build.

This module wraps ``vdatum.convert`` with:

* a per-(vd_from, vd_to) **single-threaded prime** on first use, so the
  grid cache is populated before any concurrent threads race for it;
* eager ``pyproj.network.set_network_enabled(True)`` on the calling
  thread so worker contexts have network=True regardless of the
  ``PROJ_NETWORK`` env var;
* full-jitter exponential backoff on ``ProjError``;
* a single fallback retry with ``online=False`` so cached grids resolve
  without another network round-trip.

The wrapper preserves ``vdatum.convert``'s return shape: ``(lat, lon, z)``.
"""
from __future__ import annotations

import logging
import random
import threading
import time

import pyproj
import pyproj.exceptions
from coastalmodeling_vdatum import vdatum

_logger = logging.getLogger(__name__)

# Enable network at import time so threads spawned later inherit
# network=True via the pyproj global ``_NETWORK_ENABLED`` consulted by
# ``pyproj_context_initialize``.  Workers that build a Transformer
# *without* having called ``set_network_enabled`` first would otherwise
# pick up the import-time default (driven by the PROJ_NETWORK env var,
# which the user may not have set).
try:
    pyproj.network.set_network_enabled(active=True)
except Exception:  # pragma: no cover - shouldn't happen on a healthy install
    _logger.exception('Could not enable PROJ network at import time')

_RETRY_ATTEMPTS = 4
_RETRY_BASE_SECONDS = 1.5
_RETRY_CAP_SECONDS = 12.0

# Per-(vd_from, vd_to) prime state.  ``_PRIME_LOCK`` guards
# ``_PRIMED_PAIRS``; on first use of a pair, the holding thread runs a
# single throwaway ``vdatum.convert`` to populate PROJ's grid cache so
# later concurrent calls don't race for the same uninitialized URL grid.
_PRIME_LOCK = threading.Lock()
_PRIMED_PAIRS: set[tuple[str, str]] = set()
# Coordinates that lie inside both MLLW <-> NAVD88 and xgeoid20b <-> *
# coverage areas; primarily used so the prime call returns a finite
# value rather than ``inf``.  The exact value doesn't matter -- the
# call's purpose is to populate PROJ's grid cache.
_PRIME_LAT = 36.94
_PRIME_LON = -76.33


def _sleep_with_backoff(attempt: int) -> None:
    backoff = min(_RETRY_CAP_SECONDS,
                  _RETRY_BASE_SECONDS * (2 ** attempt))
    time.sleep(random.uniform(0, backoff))


def _prime_pair(vd_from: str, vd_to: str,
                logger: logging.Logger) -> None:
    """Single-threaded grid-cache warm-up for one datum pair.

    PROJ's SQLite cache is not safe for concurrent first-time fetches of
    the same URL grid.  Holding a process-wide lock on the first call
    for each (vd_from, vd_to) pair guarantees that exactly one thread
    populates the cache while all others block, after which they all
    proceed with a warm cache and no longer race.
    """
    pair = (vd_from, vd_to)
    if pair in _PRIMED_PAIRS:
        return
    with _PRIME_LOCK:
        if pair in _PRIMED_PAIRS:
            return
        try:
            vdatum.convert(vd_from, vd_to,
                           _PRIME_LAT, _PRIME_LON, 0.0,
                           online=True)
            _PRIMED_PAIRS.add(pair)
            logger.debug('Primed PROJ grid cache for %s->%s',
                         vd_from, vd_to)
        except pyproj.exceptions.ProjError as exc:
            # Don't trap forever -- still mark the pair as primed so
            # later callers can attempt their own retry path. They will
            # hit the same network failure but at least the lock is
            # released for everyone.
            _PRIMED_PAIRS.add(pair)
            logger.warning(
                'PROJ grid prime failed for %s->%s; subsequent calls '
                'will retry on their own. Underlying error: %s',
                vd_from, vd_to, exc)


def convert(vd_from, vd_to, lat, lon, z, *, epoch=None,
            station_id: str | None = None,
            logger: logging.Logger | None = None):
    """Resilient drop-in for ``vdatum.convert``.

    Returns ``(lat, lon, z)`` on success.  Raises the last ``ProjError``
    if every retry fails so the caller can decide what to do (log and
    skip the station, fall back to a default offset, etc.).
    """
    log = logger or _logger
    last_exc: BaseException | None = None

    # PROJ contexts are per-thread.  ``pyproj.network.set_network_enabled``
    # only flips the *calling thread's* context.  Setting it explicitly
    # before the first vdatum.convert call ensures network is enabled on
    # this thread's PROJ context regardless of how it was initialized.
    try:
        pyproj.network.set_network_enabled(active=True)
    except Exception:
        pass

    # Serialize the first call for each datum pair to populate PROJ's
    # SQLite grid cache without racing against sibling worker threads.
    _prime_pair(vd_from, vd_to, log)

    for attempt in range(_RETRY_ATTEMPTS):
        try:
            return vdatum.convert(vd_from, vd_to, lat, lon, z,
                                  online=True, epoch=epoch)
        except pyproj.exceptions.ProjError as exc:
            last_exc = exc
            log.warning(
                'vdatum.convert ProjError (attempt %d/%d) for %s->%s'
                '%s: %s',
                attempt + 1, _RETRY_ATTEMPTS, vd_from, vd_to,
                f' station {station_id}' if station_id else '',
                exc,
            )
            if attempt < _RETRY_ATTEMPTS - 1:
                _sleep_with_backoff(attempt)

    # One last attempt with online=False -- the grid may already be in
    # PROJ's user-writable cache from a prior successful fetch.
    try:
        log.warning(
            'vdatum.convert online=True exhausted; falling back to '
            'online=False (cached grids only)%s',
            f' for station {station_id}' if station_id else '')
        return vdatum.convert(vd_from, vd_to, lat, lon, z,
                              online=False, epoch=epoch)
    except pyproj.exceptions.ProjError as exc:
        log.error(
            'vdatum.convert permanently failed for %s->%s%s. Set '
            'PROJ_NETWORK=ON, run `projsync --user-writable-directory '
            '--all`, or set local_vdatum=true in conf/ofs_dps.conf. '
            'Underlying error: %s',
            vd_from, vd_to,
            f' station {station_id}' if station_id else '',
            exc,
        )
        raise exc from last_exc
