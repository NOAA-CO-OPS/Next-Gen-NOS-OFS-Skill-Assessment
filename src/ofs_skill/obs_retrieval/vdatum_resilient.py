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
3. Every pipeline this package builds also references the GEOID18 grid
   ``us_noaa_g2018u0.tif`` by *bare filename*, which PROJ resolves from
   local disk and only then from cdn.proj.org -- a different host from
   the NOAA bucket, and one many operational networks block. If that
   grid is absent the failure is permanent rather than transient
   (issues #127, #216, #295).

PROJ does not let us tell 2 and 3 apart from the exception: an
unreachable bucket and an absent GEOID18 grid both surface as
``Error 1029 (File not found or invalid)``, verbatim. So the retry
policy stays uniform -- retrying is cheap and is the only thing that
helps case 2 -- and the disk is consulted only to decide *what to tell
the operator* once every attempt has failed.

This module wraps ``vdatum.convert`` with:

* a per-(vd_from, vd_to) **single-threaded prime** on first use, so the
  grid cache is populated before any concurrent threads race for it;
* eager ``pyproj.network.set_network_enabled(True)`` on the calling
  thread so worker contexts have network=True regardless of the
  ``PROJ_NETWORK`` env var;
* full-jitter exponential backoff on ``ProjError``;
* a single fallback retry with ``online=False`` so cached grids resolve
  without another network round-trip;
* a permanent-failure message that names the actual remedy, chosen by
  looking for the GEOID18 grid on disk rather than by reading the
  exception text.

The wrapper preserves ``vdatum.convert``'s return shape: ``(lat, lon, z)``.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
from pathlib import Path

import pyproj
import pyproj.datadir
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

# Vertical datums ``coastalmodeling_vdatum`` knows how to build pipelines
# for.  We validate against this set up front because the underlying
# ``vdatum.convert`` has an operator-precedence bug in its own guard
# (``if vd_from and vd_to not in [...]``) that only validates ``vd_to`` --
# an invalid ``vd_from`` slips through and later raises a cryptic
# ``UnboundLocalError`` ('h_g') from an unguarded if/elif chain instead of
# a clean error.  Membership here does not guarantee a *pair* is
# convertible (there is no overlap between Great-Lakes and tidal datums,
# e.g. ``igld85`` <-> ``mllw``); that case is caught at call time.
SUPPORTED_DATUMS = frozenset({
    'xgeoid20b', 'navd88', 'mllw', 'mlw', 'mhhw', 'mhw', 'lmsl',
    'igld85', 'lwd',
})

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


# ---------------------------------------------------------------------------
# GEOID18 grid discovery (issues #127, #216, #295)
#
# ``coastalmodeling_vdatum`` names seven of its eight grids by absolute
# https:// URL on the NOAA bucket, which PROJ streams on demand. The
# eighth, GEOID18, is named by bare filename, so PROJ looks for it in its
# own data directories and only then falls back to cdn.proj.org -- a
# different host. Every datum pair the package supports routes through
# it, so when it is missing nothing converts at all.
#
# This is used purely to pick the right remediation text. It is NOT a
# test of whether the host can convert: PROJ also serves grids out of its
# network cache (``cache.db``), which never contains a ``.tif``, so a
# host with no grid on disk can convert perfectly well.
# ---------------------------------------------------------------------------

# Grid PROJ resolves by bare filename (GEOID18, ~15 MB).
GEOID18_GRID = 'us_noaa_g2018u0.tif'

# Host serving the remaining coastalmodeling-vdatum grids by URL.
VDATUM_GRID_HOST = 'noaa-nos-stofs2d-pds.s3.amazonaws.com'


def _proj_data_dirs() -> list[Path]:
    """Every directory PROJ searches for a grid named by bare filename.

    ``get_data_dir`` can return several directories joined by the
    platform path separator (that is how ``PROJ_DATA`` is allowed to be
    set), so the result is split rather than used whole.
    """
    dirs: list[Path] = []
    for getter in (pyproj.datadir.get_data_dir,
                   pyproj.datadir.get_user_data_dir):
        try:
            raw = getter()
        except Exception:  # pragma: no cover - broken PROJ install only
            continue
        if not raw:
            continue
        for part in str(raw).split(os.pathsep):
            if part:
                dirs.append(Path(part))
    return dirs


def find_geoid18_grid() -> Path | None:
    """Return the on-disk path of the GEOID18 grid, or None if absent."""
    for directory in _proj_data_dirs():
        candidate = directory / GEOID18_GRID
        try:
            if candidate.is_file():
                return candidate
        except OSError:  # pragma: no cover - unreadable mount only
            continue
    return None


def grid_remediation() -> str:
    """Tell the operator what to fix, based on what is actually on disk.

    The exception text cannot distinguish the two faults -- PROJ reports
    both an absent GEOID18 grid and an unreachable NOAA bucket as
    ``Error 1029 (File not found or invalid)`` -- but the filesystem can.
    """
    grid_path = find_geoid18_grid()
    if grid_path is None:
        searched = ', '.join(str(d) for d in _proj_data_dirs()) or '<none>'
        return (
            f'The GEOID18 grid {GEOID18_GRID}, which every vertical datum '
            f'pipeline needs and which PROJ resolves by bare filename, is '
            f'not in any PROJ data directory (searched: {searched}). Run '
            f'`make proj-grids` once to download it into the environment, '
            f'then re-run. If PROJ is instead serving that grid from its '
            f'network cache, the fault is outbound HTTPS: this host needs '
            f'access to cdn.proj.org and to https://{VDATUM_GRID_HOST}.'
        )
    return (
        f'The GEOID18 grid is present at {grid_path}, so this is not a '
        f'missing download. The remaining vertical datum grids are '
        f'streamed from https://{VDATUM_GRID_HOST}, so this host needs '
        f'outbound HTTPS access to that bucket.'
    )


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
        except UnboundLocalError:
            # The datum pair is in-vocabulary but has no conversion
            # pipeline (e.g. a Great-Lakes datum to a tidal datum). The
            # underlying vdatum.convert leaves its grid variables unbound
            # and raises UnboundLocalError. Mark primed and let the real
            # call surface a clean error to the caller.
            _PRIMED_PAIRS.add(pair)


def convert(vd_from, vd_to, lat, lon, z, *, epoch=None,
            station_id: str | None = None,
            logger: logging.Logger | None = None):
    """Resilient drop-in for ``vdatum.convert``.

    Returns ``(lat, lon, z)`` on success.  Raises the last ``ProjError``
    if every retry fails so the caller can decide what to do (log and
    skip the station, fall back to a default offset, etc.).  Raises
    ``ValueError`` for an unsupported datum or an in-vocabulary pair that
    has no conversion pipeline (e.g. a Great-Lakes datum to a tidal
    datum), so callers get a clean error instead of the dependency's
    cryptic ``UnboundLocalError``.
    """
    log = logger or _logger
    last_exc: BaseException | None = None

    # Validate the datum vocabulary up front.  ``vdatum.convert``'s own
    # guard fails to validate ``vd_from`` (operator-precedence bug), so an
    # unknown source datum would otherwise reach an unguarded if/elif
    # chain and raise a confusing ``UnboundLocalError`` ('h_g').  Raise a
    # clear ValueError instead so callers can skip the station cleanly.
    if vd_from not in SUPPORTED_DATUMS or vd_to not in SUPPORTED_DATUMS:
        raise ValueError(
            f'Unsupported vertical datum conversion {vd_from!r}->{vd_to!r}. '
            f'Supported datums: {sorted(SUPPORTED_DATUMS)}')

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
        except UnboundLocalError as exc:
            # In-vocabulary pair with no conversion pipeline (e.g. a
            # Great-Lakes datum to a tidal datum). This is deterministic,
            # so don't retry -- raise a clear error for the caller.
            raise ValueError(
                f'No vertical datum conversion path from {vd_from!r} to '
                f'{vd_to!r}.') from exc
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
            'vdatum.convert permanently failed for %s->%s%s. Check that '
            'PROJ_NETWORK=ON. %s Underlying error: %s',
            vd_from, vd_to,
            f' station {station_id}' if station_id else '',
            grid_remediation(), exc,
        )
        raise exc from last_exc


# ---------------------------------------------------------------------------
# PROJ vertical-datum grid preflight (issues #127, #216, #295)
#
# coastalmodeling-vdatum names seven of its eight grids by absolute
# https:// URL on the NOAA bucket, which PROJ streams on demand. The
# eighth, GEOID18, is named by *bare filename*, so PROJ resolves it from
# its local data directories and only then falls back to cdn.proj.org --
# a different host. The conda environment ships no .tif grids, so on a
# host that cannot reach cdn.proj.org every conversion raises ProjError
# 1029 and the affected stations are silently dropped.
#
# The gate is the conversion itself, not the presence of a file. A host
# can convert with no grid on disk at all (PROJ serves it out of the
# network cache in cache.db, which never holds a .tif), so gating on the
# disk would kill runs that work. The disk is consulted only afterwards,
# to choose the remediation text -- see ``grid_remediation``.
# ---------------------------------------------------------------------------

# Probe coordinates (Sewells Point, VA). They sit inside the NAVD88 and
# MLLW coverage areas, so a healthy install returns a finite value here.
# The exact position does not matter -- the call exists to prove the
# pipeline can be built at all.
PREFLIGHT_LAT = 36.94
PREFLIGHT_LON = -76.33

# The pair probed. Every pair coastalmodeling-vdatum supports is built as
# "GEOID18 grid -> ITRF2020 helmert chain -> one NOAA-bucket grid"
# (verified against ``vdatum.inputs`` for navd88->mllw, navd88->lwd,
# navd88->igld85, navd88->lmsl and xgeoid20b->navd88), so this one pair
# exercises the grid resolution that every other pair depends on.
PREFLIGHT_FROM = 'navd88'
PREFLIGHT_TO = 'mllw'

# Great Lakes OFS. Their model-side datum offsets are fixed arithmetic
# and their CO-OPS observations take an arithmetic IGLD85/LWD branch, so
# a broken PROJ install still yields a usable Great Lakes assessment --
# it costs only USGS stations that report NAVD88. Those runs are warned,
# not aborted.
GREAT_LAKES_OFS = ('leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs')


def validate_proj_vdatum_grids(prop, logger: logging.Logger) -> None:
    """Abort early if this host cannot perform vertical datum conversions.

    Without this gate a broken PROJ grid setup surfaces only once per
    station, hours into the run, as an INFO-level "data not found"
    line -- and the run still reports success while quietly omitting
    every station that needed a conversion.

    The check deliberately does not look at ``prop.datum``. The
    observation side converts on the datum each *station* reports, not on
    the datum the run requests: a default ``-d MLLW`` run on cbofs still
    converts every NAVD88 USGS gauge to MLLW
    (``write_obs_ctlfile._process_usgs_station``), which is precisely the
    conversion that dropped 192 stations. A run in the OFS native datum
    is therefore not exempt.
    """
    ofs = str(getattr(prop, 'ofs', '') or '')
    try:
        convert(
            PREFLIGHT_FROM, PREFLIGHT_TO, PREFLIGHT_LAT, PREFLIGHT_LON, 0.0,
            epoch=None, station_id='preflight', logger=logger)
    except Exception as exc:
        # ``convert`` has already logged the failure at ERROR, including
        # the full PROJ pipeline text. Repeating it here would print the
        # same twenty lines twice and bury the one thing the operator
        # needs, so this message carries only the remediation.
        detail = (
            f'PROJ cannot build the {PREFLIGHT_FROM} -> {PREFLIGHT_TO} '
            f'vertical datum pipeline on this host, so every observation '
            f'needing a datum conversion will be dropped from the '
            f'assessment. {grid_remediation()}'
        )
        if ofs.lower() in GREAT_LAKES_OFS:
            logger.error(
                '%s %s uses fixed Great Lakes datum offsets for its model '
                'data and its CO-OPS observations, so the run continues -- '
                'but any USGS gauge reporting NAVD88 will be missing.',
                detail, ofs)
            return
        logger.error('%s Abort!', detail)
        raise SystemExit(1) from exc

    logger.info('PROJ vertical datum conversion verified (%s -> %s).',
                PREFLIGHT_FROM, PREFLIGHT_TO)
