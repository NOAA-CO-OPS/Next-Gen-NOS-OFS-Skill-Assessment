"""
Retrieve time series observations from NOAA Tides and Currents (CO-OPS) API.

This module provides functions to retrieve tidal observations, water level,
temperature, salinity, currents, and other oceanographic data from NOAA CO-OPS
stations, as well as tidal predictions and nearest station finding capabilities.

The retrieval is performed in 30-day chunks to handle API limitations. Every
CO-OPS HTTP call is gated by a module-level semaphore (see
``_rate_limited_get``) so upstream parallelization cannot exceed a safe
concurrent-request ceiling, and transient errors are handled with
full-jitter exponential backoff (see ``_get_with_retry``). Water temperature
and salinity additionally fall back to a ``PHYSOCEAN`` backup URL when the
primary endpoint exhausts retries.

For ``variable == 'currents'`` the module fans out to one datagetter call per
ADCP bin (``&bin=N``) and returns a ``dict[int, DataFrame]`` keyed by bin
number — see :func:`_retrieve_currents_all_bins` and the wiki page
"CO-OPS ADCP current processing" for the full flow.
"""

import json
import math
import random
import threading
import time
from datetime import datetime, timedelta
from logging import Logger
from typing import Any, Optional, Union
from urllib.error import HTTPError

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from ofs_skill.obs_retrieval import t_and_c_properties, utils

# ---------------------------------------------------------------------------
# Module-level HTTP session with connection pooling (Task 2)
# ---------------------------------------------------------------------------
_session = None  # pylint: disable=invalid-name


def _get_session():
    """Lazily create and return a shared requests.Session with connection pooling."""
    global _session  # pylint: disable=invalid-name,global-statement
    if _session is None:
        _session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        _session.mount('https://', adapter)
        _session.mount('http://', adapter)
    return _session


# ---------------------------------------------------------------------------
# Global CO-OPS concurrency cap + pacing (issue #98)
# ---------------------------------------------------------------------------
# CO-OPS throttles aggressively when many historical datagetter requests
# come from one IP at once. Two measures guard against this:
#
#   1. Semaphore caps concurrent in-flight CO-OPS GETs process-wide,
#      decoupling HTTP pressure from upstream ThreadPoolExecutor sizing
#      (``write_obs_ctlfile`` otherwise fans out 4 vars x 6 stations = 24
#      concurrent workers).
#   2. Minimum inter-request gap paces the START of each GET so the
#      sustained request rate stays below CO-OPS's per-IP threshold.
#      This is what lets long historical windows retrieve 100% of
#      chunks; relying on backoff alone to recover AFTER a 403 doesn't
#      work when CO-OPS's throttle window outlasts the retry budget.
#
# Tuned against a 14-month stofs_3d_atl run where the earlier settings
# (concurrency=4, no pacing) still dropped ~1.6% of chunks to exhaustion.
_COOPS_CONCURRENCY_LIMIT = 2
_COOPS_MIN_REQUEST_GAP_SEC = 0.5
_coops_request_semaphore = threading.Semaphore(_COOPS_CONCURRENCY_LIMIT)
_coops_pacing_lock = threading.Lock()
_coops_last_request_time = 0.0  # pylint: disable=invalid-name


def _rate_limited_get(url: str, timeout: int = 120) -> requests.Response:
    """GET ``url`` through the shared session, gated by the CO-OPS semaphore
    and paced by a minimum inter-request gap.

    Concurrency semaphore caps in-flight requests across all threads;
    pacing lock serializes request STARTS so no two GETs fire within
    ``_COOPS_MIN_REQUEST_GAP_SEC``. Retry backoff sleeps outside both
    locks so waiting threads do not starve the pool.
    """
    global _coops_last_request_time  # pylint: disable=invalid-name,global-statement
    with _coops_request_semaphore:
        with _coops_pacing_lock:
            now = time.monotonic()
            wait = _COOPS_MIN_REQUEST_GAP_SEC - (now - _coops_last_request_time)
            if wait > 0:
                time.sleep(wait)
            _coops_last_request_time = time.monotonic()
        return _get_session().get(url, timeout=timeout)


# ---------------------------------------------------------------------------
# Module-level cache for station depth metadata (Task 3)
# ---------------------------------------------------------------------------
_depth_cache: dict[str, Any] = {}  # station_id -> depth_data

# Cache station-level metadata (``height_from_bottom`` etc.) keyed by
# station_id. Populated via ``get_station_info``.
_station_info_cache: dict[str, Optional[dict]] = {}

# Cache deployment-level metadata (``orientation``, ``sensor_depth`` etc.)
# keyed by station_id. Populated via ``get_station_deployment``.
# The station endpoint (``stations/{id}.json``) only returns a
# ``deployments: {self: <url>}`` pointer; the actual deployment fields
# live one level deeper at ``stations/{id}/deployments.json``.
_station_deployment_cache: dict[str, Optional[dict]] = {}

# Canonical ADCP mounting symbols. Single source of truth — every
# emitter, parser, label formatter, and run counter funnels through
# this set so a future MDAPI vocabulary addition can be supported by
# editing one constant rather than chasing hard-coded literals.
ADCP_MOUNTING_SYMBOLS: frozenset[str] = frozenset({
    'side', 'up', 'down', 'unknown',
})

# Process-level counter of ADCP mounting types resolved during a run.
# Incremented inside ``_retrieve_currents_all_bins`` so the end-of-run
# log can summarise the population (analogous to the CO-OPS retrieval
# summary added in PR #109). Reset by the public ``reset_run_counters``
# entrypoint at the start of each pipeline run.
_adcp_mounting_counter: dict[str, int] = {
    sym: 0 for sym in ADCP_MOUNTING_SYMBOLS
}
_adcp_mounting_counter_lock = threading.Lock()


def reset_run_counters() -> None:
    """Reset module-level run counters before a fresh skill-assessment run.

    Currently only resets the ADCP mounting-type tally. Idempotent and
    safe to call from any thread (counters are guarded by a lock).
    """
    with _adcp_mounting_counter_lock:
        for key in _adcp_mounting_counter:
            _adcp_mounting_counter[key] = 0


def canonicalize_mounting_symbol(value: Any) -> str:
    """Coerce any incoming string to a canonical ADCP mounting symbol.

    Returns one of ``side``/``up``/``down``/``unknown`` from
    :data:`ADCP_MOUNTING_SYMBOLS`. Accepts the canonical MDAPI
    strings exactly, the free-form synonyms documented for the MDAPI
    ``orientation`` field (``horizontal``, ``upward``, ``downward``,
    and the single-letter forms ``h``/``u``/``d``), and the
    longer-form display labels users tend to write in CSV overrides
    (``side-looking``, ``upward-looking``, ``downward-looking``).
    Anything else — including ``None``, blanks, and non-string inputs
    — resolves to ``'unknown'``.

    Centralised here so emitters, parsers, label formatters, the user
    CSV override path, and the run counter share one normalisation
    path. Callers that already hold a deployment_info dict should use
    :func:`resolve_mounting_type` instead.
    """
    if value is None:
        return 'unknown'
    token = str(value).strip().lower()
    if token in ('side', 'h') or token.startswith('side-') \
            or token.startswith('horiz'):
        return 'side'
    if token in ('up', 'u') or token.startswith('upward'):
        return 'up'
    if token in ('down', 'd') or token.startswith('downward'):
        return 'down'
    return 'unknown'


def _record_mounting(mounting_type: str) -> None:
    """Bump the run-level counter for the given mounting type.

    Counts *station retrievals*, not unique stations — if the same
    station is processed twice in one run (e.g. by retry logic above
    this layer), it is counted twice. The summary log line is a
    soft-information channel; absolute uniqueness is not required.
    """
    key = canonicalize_mounting_symbol(mounting_type)
    with _adcp_mounting_counter_lock:
        _adcp_mounting_counter[key] += 1


def emit_adcp_mounting_summary(logger: Logger) -> None:
    """Log a single end-of-run line tallying ADCP mounting types."""
    with _adcp_mounting_counter_lock:
        snapshot = dict(_adcp_mounting_counter)
    total = sum(snapshot.values())
    if total == 0:
        return
    level = logger.warning if snapshot['unknown'] > 0 else logger.info
    level(
        'ADCP mounting summary: %d total (%d side, %d up, %d down, '
        '%d unknown).',
        total, snapshot['side'], snapshot['up'], snapshot['down'],
        snapshot['unknown'],
    )


def get_station_depth(station_id, mdapi_url, logger):
    """Fetch station depth/bins metadata, returning cached result when available.

    Parameters
    ----------
    station_id : str
        CO-OPS station identifier.
    mdapi_url : str
        Base URL for the CO-OPS metadata API.
    logger : Logger
        Logger instance.

    Returns
    -------
    dict or None
        Parsed JSON response from the bins endpoint, or None on failure.
    """
    if station_id in _depth_cache:
        logger.info(
            'Using cached depth metadata for station %s.', station_id)
        return _depth_cache[station_id]

    url = f'{mdapi_url}/webapi/stations/{station_id}/bins.json?units=metric'
    try:
        response = _rate_limited_get(url, timeout=120)
        response.raise_for_status()
        depth_data = response.json()
        logger.info(
            'CO-OPS depth retrieval complete for station %s.', station_id)
    except requests.exceptions.RequestException as ex:
        logger.error(
            'CO-OPS depth metadata retrieval failed for station %s: %s',
            station_id, ex)
        # Don't pin a None failure — a transient 5xx on the bins
        # endpoint must not silently downgrade every subsequent caller
        # for the rest of the process. Side-looking depth resolution
        # depends on real_time_bin from this payload, so a cached None
        # would mis-route a station to the legacy fallback.
        return None

    _depth_cache[station_id] = depth_data
    return depth_data


# CO-OPS occasionally rate-limits or returns transient 5xx when a long
# historical window forces many 30-day chunk requests per station; retry
# a small number of times with full-jitter exponential backoff before
# giving up. Kept in sync with the per-bin retry helper on
# issue-87-currents-bins so both paths share the same backoff profile.
_RETRY_STATUSES = (403, 408, 429, 500, 502, 503, 504)
# Observed with stofs_3d_atl + 14-month historical window: CO-OPS throttles
# hard enough that unpaced requests still drop chunks even with backoff.
# The pacing gap above prevents most 403s from ever firing; these retry
# settings are the safety net for the few that still slip through.
# Worst-case total wait per chunk with full-jitter and cap=120s:
# 10+20+40+80 + 5*120 = 750s = 12.5min, expected ~375s.
_RETRY_MAX_ATTEMPTS = 10
_RETRY_BASE_DELAY = 10.0
# Cap per-attempt delay so late retries don't run away to 20+ minute waits
# (base * 2^9 = ~85min otherwise at attempt 10). 120s is long enough to
# clear typical CO-OPS throttle windows, short enough to keep total
# wall-clock bounded when an individual chunk is having a bad day.
_RETRY_DELAY_CAP = 120.0


def _backoff_delay(attempt: int) -> float:
    """Return seconds to sleep before the next retry attempt.

    Full-jitter exponential backoff capped at ``_RETRY_DELAY_CAP``:
    ``uniform(0, min(base * 2^(attempt-1), cap))``. Full jitter
    decorrelates retry timing across threads that hit the limiter
    together; the cap keeps late-attempt waits bounded.
    """
    max_delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                    _RETRY_DELAY_CAP)
    return random.uniform(0, max_delay)


def _get_with_retry(
    url: str,
    station_id: str,
    context: str,
    logger: Logger,
) -> Optional[dict]:
    """GET ``url`` with retries for transient CO-OPS errors.

    Retries only on network-level failures (ConnectionError, Timeout) or
    HTTP status codes in ``_RETRY_STATUSES``. Permanent HTTP errors such
    as 400 (bad station/date combo - CO-OPS "no data") or 404 fail
    immediately without burning the retry budget.

    ``context`` is a free-form label (variable name, bin number, etc.)
    included in log messages so parallel workers can be disambiguated.

    Returns the parsed JSON payload on success, ``None`` when the call
    hits a non-retryable error or all retries are exhausted.
    """
    last_exc = None
    for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
        try:
            response = _rate_limited_get(url, timeout=120)
        except (requests.exceptions.SSLError,
                requests.exceptions.InvalidURL,
                requests.exceptions.MissingSchema,
                requests.exceptions.InvalidSchema) as ex:
            # SSL validation or URL-construction failures indicate a
            # configuration problem (or MITM risk) rather than "no
            # data". Escalate to ERROR so operators notice; still
            # return None so retrieval soft-fails instead of crashing
            # the whole run. NOTE: SSLError subclasses ConnectionError,
            # so this branch MUST come before the ConnectionError catch
            # below or it will be masked.
            logger.error(
                'CO-OPS %s station=%s SSL/URL error (not retried): %s',
                context, station_id, ex)
            return None
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as ex:
            last_exc = ex
            if attempt < _RETRY_MAX_ATTEMPTS:
                delay = _backoff_delay(attempt)
                logger.warning(
                    'CO-OPS %s station=%s network error (attempt %d/%d): '
                    '%s; retrying in %.1fs', context, station_id,
                    attempt, _RETRY_MAX_ATTEMPTS, ex, delay)
                time.sleep(delay)
                continue
            break
        except requests.exceptions.RequestException as ex:
            logger.warning(
                'CO-OPS %s station=%s non-retryable request error: %s',
                context, station_id, ex)
            return None

        status = response.status_code
        if status in _RETRY_STATUSES:
            if attempt < _RETRY_MAX_ATTEMPTS:
                delay = _backoff_delay(attempt)
                logger.warning(
                    'CO-OPS %s station=%s HTTP %d (attempt %d/%d); '
                    'retrying in %.1fs', context, station_id, status,
                    attempt, _RETRY_MAX_ATTEMPTS, delay)
                time.sleep(delay)
                continue
            logger.error(
                'CO-OPS %s station=%s HTTP %d - retries exhausted '
                'after %d attempts; dropping chunk',
                context, station_id, status, _RETRY_MAX_ATTEMPTS)
            return None

        if status >= 400:
            # Non-retryable 4xx/5xx (400, 401, 404, etc.) - fail fast.
            # CO-OPS returns 400 "No data was found" for stations with
            # no observations in the requested window; retrying won't
            # change that. Logged at WARNING because the chunk is
            # dropped silently and operators should notice.
            logger.warning(
                'CO-OPS %s station=%s HTTP %d - not retrying '
                '(dropping chunk)', context, station_id, status)
            return None

        try:
            return response.json()
        except ValueError as ex:
            logger.warning(
                'CO-OPS %s station=%s returned non-JSON body: %s',
                context, station_id, ex)
            return None

    logger.error(
        'CO-OPS %s retrieval failed for station %s after %d attempts: %s',
        context, station_id, _RETRY_MAX_ATTEMPTS, last_exc)
    return None


def _fetch_with_backup(
    primary_url: str,
    backup_url: Optional[str],
    station_id: str,
    variable: str,
    logger: Logger,
) -> tuple[Optional[dict], bool]:
    """Fetch the primary URL with retry; fall back to ``backup_url`` when set.

    The water_temperature / salinity backup URL points at the
    ``NOS.COOPS.TAC.PHYSOCEAN`` endpoint with ``interval=6``, which
    returns HOURLY observations rather than the primary's 6-minute
    cadence. Mixing the two resolutions silently degrades skill metrics
    that assume uniform sampling, so log a WARNING whenever the backup
    produces data so operators notice the resolution change.

    Returns
    -------
    tuple[Optional[dict], bool]
        ``(obs, used_backup)`` where ``obs`` is the parsed JSON payload
        (or ``None`` when both endpoints fail / no backup is configured)
        and ``used_backup`` is ``True`` only when the hourly backup URL
        actually produced the returned payload. Callers use the flag to
        count cadence-mixed chunks in end-of-run summaries.
    """
    obs = _get_with_retry(primary_url, station_id, variable, logger)
    if obs is not None:
        logger.info(
            'CO-OPS station %s contacted for %s retrieval.',
            station_id, variable)
        return obs, False

    if backup_url is None:
        return None, False

    obs = _get_with_retry(backup_url, station_id, variable, logger)
    if obs is None:
        return None, False
    logger.warning(
        'CO-OPS backup endpoint used for station %s %s - this chunk '
        'returns hourly samples (interval=6) rather than the primary 6-min '
        'cadence; downstream skill metrics may be affected.',
        station_id, variable)
    return obs, True


def get_station_info(
    station_id: str, mdapi_url: str, logger: Logger,
) -> Optional[dict]:
    """Fetch station-level metadata (cached) from the MDAPI station endpoint.

    Returns the first entry of ``stations`` (fields include
    ``height_from_bottom``, ``center_bin_1_dist``, ``lat``, ``lng``) or
    ``None`` on error / missing station.
    """
    if station_id in _station_info_cache:
        return _station_info_cache[station_id]

    url = f'{mdapi_url}/webapi/stations/{station_id}.json?units=metric'
    try:
        response = _rate_limited_get(url, timeout=120)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.RequestException as ex:
        logger.warning(
            'CO-OPS station metadata retrieval failed for %s: %s',
            station_id, ex)
        _station_info_cache[station_id] = None
        return None

    stations = payload.get('stations') or []
    info = stations[0] if stations else None
    _station_info_cache[station_id] = info
    return info


def get_station_deployment(
    station_id: str, mdapi_url: str, logger: Logger,
) -> Optional[dict]:
    """Fetch deployment-level metadata (cached) from MDAPI.

    The station-level endpoint (``stations/{id}.json``) only returns a
    ``deployments: {self: <url>}`` pointer — the actual fields live one
    level deeper at ``stations/{id}/deployments.json``. This payload is
    the only source of truth for ``orientation`` (``side``/``up``/
    ``down``) and ``sensor_depth``; both are absent from the station
    endpoint.

    Returns the full top-level dict (with keys including ``orientation``,
    ``sensor_depth``, ``measured_depth``, ``height_from_bottom``, and a
    nested ``deployments`` list of historical deployments), or ``None``
    on HTTP error / unrecognized payload shape.

    Failures are NOT cached — a transient 5xx must not silently
    downgrade every subsequent caller for the rest of the process.
    """
    if station_id in _station_deployment_cache:
        return _station_deployment_cache[station_id]

    url = (
        f'{mdapi_url}/webapi/stations/{station_id}/deployments.json'
        '?units=metric'
    )
    payload = _get_with_retry(
        url, station_id, 'deployment-metadata', logger)
    if payload is None or not isinstance(payload, dict):
        return None

    _station_deployment_cache[station_id] = payload
    return payload


def resolve_mounting_type(
    deployment_info: Optional[dict],
) -> str:
    """Canonicalise the MDAPI deployment ``orientation`` to a known symbol.

    Thin wrapper around :func:`canonicalize_mounting_symbol`. Returns
    one of :data:`ADCP_MOUNTING_SYMBOLS`. The MDAPI free-form values
    seen in production (surveyed across all 88 CO-OPS currents
    stations) are exactly ``side``, ``up``, ``down``; the canonicaliser
    also accepts historical synonyms (``horizontal``, ``upward``,
    ``downward`` and their single-letter forms) so a future MDAPI
    vocabulary drift back to long-form labels does not silently flip
    everything to ``unknown``.
    """
    if deployment_info is None:
        return 'unknown'
    return canonicalize_mounting_symbol(deployment_info.get('orientation'))


def _resolve_side_real_time_bin(
    bins_payload: Optional[dict],
    deployment_info: Optional[dict],
) -> Optional[int]:
    """Return the bin number to use for a side-looking ADCP, or ``None``.

    Priority order, validated against the live MDAPI survey (issue #140):

    1. Top-level ``real_time_bin`` on ``stations/{id}/bins.json``
       (populated for all 40 surveyed side-looking stations).
    2. ``deployments[*].real_time_bin`` from the deployments endpoint —
       prefers the active deployment (``retrieved == ''``) over older
       historical ones, since the deployments array is forward-ordered
       in time and the [0] entry can be a 20-year-old install.
       Currently null on every surveyed station; kept as a defensive
       fallback for future MDAPI shapes.
    """
    rtb = _coerce_int(
        bins_payload.get('real_time_bin') if bins_payload else None)
    if rtb is not None:
        return rtb
    if deployment_info is None:
        return None
    inner = deployment_info.get('deployments') or []
    active = next(
        (
            d for d in inner
            if isinstance(d, dict) and (d.get('retrieved') or '') == ''
        ),
        None,
    )
    candidates = [active] if active is not None else list(inner)
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        rtb = _coerce_int(entry.get('real_time_bin'))
        if rtb is not None:
            return rtb
    return None


def _coerce_int(value: Any) -> Optional[int]:
    """Best-effort int coercion that returns ``None`` instead of raising."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_side_sensor_depth(
    deployment_info: Optional[dict],
) -> Optional[float]:
    """Return the side-looking ADCP's sensor_depth (m, positive-down).

    Prefers the explicit ``sensor_depth`` field; falls back to
    ``measured_depth - height_from_bottom`` when only the deeper
    bathymetric fields are populated. Returns ``None`` when neither
    formulation yields a physically valid depth — letting the model-side
    bathymetry-based resolver in ``_resolve_side_looking_depths`` take
    over rather than emitting a negative obs depth that would silently
    invert the model-layer pairing in ``index_nearest_depth``.

    Two MDAPI side stations carry negative ``measured_depth``
    (``hb0401`` -7.2, ``nl0101`` -13.4 — apparently a data-entry bug;
    surveyed 2026-05-06). Both also expose valid positive
    ``sensor_depth``, so this is defensive against a future station
    that lands in the fallback branch with a sign-flipped value.
    """
    if deployment_info is None:
        return None
    sd_raw = deployment_info.get('sensor_depth')
    if sd_raw is not None:
        try:
            sd_val = float(sd_raw)
        except (TypeError, ValueError):
            sd_val = None
        if sd_val is not None and sd_val >= 0:
            return sd_val
    md_raw = (
        deployment_info.get('measured_depth')
        or deployment_info.get('depth')
    )
    hfb_raw = deployment_info.get('height_from_bottom')
    if md_raw is not None and hfb_raw is not None:
        try:
            md_val = float(md_raw)
            hfb_val = float(hfb_raw)
        except (TypeError, ValueError):
            return None
        if md_val > hfb_val >= 0:
            return md_val - hfb_val
    return None


# Backward-compat aliases: these used to be leading-underscore "private"
# symbols but are imported by ``plotting_functions.py`` across module
# boundaries, so keep the old names working for any external caller.
_get_station_depth = get_station_depth
_get_station_info = get_station_info
_get_station_deployment = get_station_deployment


def retrieve_t_and_c_station(
    retrieve_input: Any,
    logger: Logger,
    only_bins: Optional[set[int]] = None,
    config_file=None,
) -> Optional[Union[pd.DataFrame, dict[int, pd.DataFrame]]]:
    """
    Retrieve time series observations from NOAA Tides and Currents station.

    This function loops between the start and end date, gathering data
    in 30-day pieces. If the last 30-day period does not end exactly at
    the end date (which is very likely), data will be masked between
    start_dt_0 and end_dt_0.

    Args:
        retrieve_input: Object with attributes:
            - station: Station ID
            - start_date: Start date in YYYYMMDD format
            - end_date: End date in YYYYMMDD format
            - variable: Variable type ('water_level', 'water_temperature',
                       'currents', 'salinity', 'wind', 'air_pressure')
            - datum: Vertical datum (for water_level)
        logger: Logger instance for logging messages
        only_bins: Optional set of ADCP bin numbers to restrict the
            currents retrieval to. Ignored for non-currents variables.
            When supplied, the datagetter is only called for those
            bins — used by ``write_obs_ctlfile`` to honour the user
            currents-bins CSV without fetching every bin.

    Returns:
        For ``variable == 'currents'``:
            ``dict[int, pd.DataFrame]`` keyed by ADCP bin number. Each
            DataFrame carries per-bin depth via ``df.attrs['depth']`` (also
            populated in the ``DEP01`` column) plus ``df.attrs['bin']``,
            ``df.attrs['orientation']``, and ``df.attrs['height_from_bottom']``
            (the last used by the model-CTL writer to resolve depth for
            side-looking PICS ADCPs). If the bins endpoint is unavailable
            the retrieval falls back to a single ``{1: DataFrame}`` using
            the ``real_time_bin`` depth (legacy behavior).
        For other variables:
            DataFrame with columns DateTime, DEP01, OBS (and DIR for wind).
        ``None`` if no data retrieved.

    Raises:
        HTTPError: If API request fails after retries
    """
    variable = retrieve_input.variable

    t_c = t_and_c_properties.TidesandCurrentsProperties()

    # Retrieve url from config file
    url_params = utils.Utils(config_file).read_config_section('urls', logger)
    t_c.mdapi_url = url_params['co_ops_mdapi_base_url']
    t_c.api_url = url_params['co_ops_api_base_url']

    t_c.start_dt_0 = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    # Currents uses a per-bin retrieval loop against a different endpoint
    # shape (``&bin=N``). Keep it out of the generic chunked loop below.
    if variable == 'currents':
        return _retrieve_currents_all_bins(
            station_id=str(retrieve_input.station),
            start_date=retrieve_input.start_date,
            end_date=retrieve_input.end_date,
            api_url=t_c.api_url,
            mdapi_url=t_c.mdapi_url,
            logger=logger,
            only_bins=only_bins,
        )

    t_c.start_dt = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var, t_c.total_dir = [], [], []

    # Chunk accounting for end-of-run summary (issue: silent data loss
    # and hourly-backup usage were only visible via scattered WARNING
    # lines before). Counters are grep'd from operator logs via the
    # 'CO-OPS retrieval summary' prefix emitted below.
    chunks_attempted = 0
    chunks_dropped = 0
    chunks_hourly_backup = 0

    while t_c.start_dt <= t_c.end_dt:
        date_i = (
            t_c.start_dt.strftime('%Y') +
            t_c.start_dt.strftime('%m') +
            t_c.start_dt.strftime('%d')
        )
        date_f = (
            (t_c.start_dt + t_c.delta).strftime('%Y')
            + (t_c.start_dt + t_c.delta).strftime('%m')
            + (t_c.start_dt + t_c.delta).strftime('%d')
        )

        if variable == 'water_level':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product={variable}&datum='
                f'{retrieve_input.datum}&time_zone=gmt&units='
                f'metric&format=json'
            )

        elif variable == 'water_temperature':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product='
                f'{variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

            t_c.station_url_2 = (
                f'{t_c.api_url}/datagetter?product='
                f'{variable}&application='
                f'NOS.COOPS.TAC.PHYSOCEAN&begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&time_zone=GMT&units='
                f'metric&interval=6&format=json'
            )

        elif variable == 'salinity':

            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product='
                f'{variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

            t_c.station_url_2 = (
                f'{t_c.api_url}/datagetter?product='
                f'{variable}&application='
                f'NOS.COOPS.TAC.PHYSOCEAN&begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&time_zone=GMT&units='
                f'metric&interval=6&format=json'
            )

        backup_url = (t_c.station_url_2
                      if variable in {'water_temperature', 'salinity'}
                      else None)
        chunks_attempted += 1
        obs, used_backup = _fetch_with_backup(
            t_c.station_url, backup_url, str(retrieve_input.station),
            variable, logger)
        if used_backup:
            chunks_hourly_backup += 1
        if obs is None:
            chunks_dropped += 1
            t_c.start_dt += t_c.delta
            continue

        t_c.date, t_c.var, t_c.drt = [], [], []
        if 'data' in obs.keys():
            for i in range(len(obs['data'])):
                row = obs['data'][i]

                if variable in {'water_level',
                                'water_temperature',
                                'air_pressure'}:
                    t_c.date.append(row['t'])
                    t_c.var.append(row['v'])

                elif variable == 'salinity':
                    t_c.date.append(row['t'])
                    t_c.var.append(row['s'])

                elif variable == 'wind':
                    t_c.date.append(row['t'])
                    t_c.var.append(row['s'])
                    t_c.drt.append(row['d'])

            t_c.total_date.append(t_c.date)
            t_c.total_var.append(t_c.var)
            if variable == 'wind':
                t_c.total_dir.append(t_c.drt)

        t_c.start_dt += t_c.delta

    # End-of-run summary so operators notice silent chunk drops or
    # cadence mixing without scanning every WARNING. Severity escalates
    # when any data was dropped or any chunk used the hourly backup.
    summary_msg = (
        'CO-OPS retrieval summary station=%s variable=%s '
        'date_range=%s-%s chunks_attempted=%d chunks_dropped=%d '
        'chunks_hourly_backup=%d'
    )
    summary_args = (
        str(retrieve_input.station), variable,
        retrieve_input.start_date, retrieve_input.end_date,
        chunks_attempted, chunks_dropped, chunks_hourly_backup,
    )
    if chunks_dropped > 0 or chunks_hourly_backup > 0:
        logger.warning(summary_msg, *summary_args)
    else:
        logger.info(summary_msg, *summary_args)

    # Non-currents variables report a single depth per station; currents
    # returns per-bin frames via ``_retrieve_currents_all_bins`` (dispatched
    # above).
    t_c.depth = 0.0

    t_c.total_date = sum(t_c.total_date, [])
    t_c.total_var = sum(t_c.total_var, [])

    if variable == 'wind':
        t_c.total_dir = sum(t_c.total_dir, [])

        obs = pd.DataFrame(
            {
                'DateTime': pd.to_datetime(t_c.total_date),
                'DEP01': pd.to_numeric(t_c.depth),
                'DIR': pd.to_numeric(t_c.total_dir),
                'OBS': pd.to_numeric(t_c.total_var),
            }
        )

    else:
        obs = pd.DataFrame(
            {
                'DateTime': pd.to_datetime(t_c.total_date),
                'DEP01': pd.to_numeric(t_c.depth),
                'OBS': pd.to_numeric(t_c.total_var),
            }
        )

    mask = (obs['DateTime'] >= t_c.start_dt_0) & (
        obs['DateTime'] <= t_c.end_dt_0)
    obs = obs.loc[mask]

    if len(obs.DateTime) > 0:
        obs = obs.sort_values(by='DateTime').drop_duplicates()
        return obs

    return None


def _extract_bin_records(
    bins_payload: Optional[dict],
) -> list[dict]:
    """Return the list of bin records from a CO-OPS bins payload, or []."""
    if not bins_payload or not bins_payload.get('bins'):
        return []
    return list(bins_payload['bins'])


def _bin_depth_from_record(entry: dict) -> Optional[float]:
    """Resolve depth (m) for a single bin record.

    Returns the bin's ``depth`` value when provided by the MDAPI. For
    side-looking (PICS) ADCPs the endpoint returns ``depth: None``; in
    that case this helper returns ``None`` rather than inventing a
    depth from ``distance`` (which is horizontal range along the
    channel, not depth-from-surface — mislabeling it as depth picks a
    wrong model vertical layer and produces a misleading plot title).
    """
    depth = entry.get('depth')
    if depth is not None:
        try:
            return float(depth)
        except (TypeError, ValueError):
            pass
    return None


def _retrieve_currents_all_bins(
    station_id: str,
    start_date: str,
    end_date: str,
    api_url: str,
    mdapi_url: str,
    logger: Logger,
    only_bins: Optional[set[int]] = None,
) -> Optional[dict[int, pd.DataFrame]]:
    """Retrieve CO-OPS currents data for every bin of an ADCP station.

    Flow:
      1. Fetch deployment metadata (orientation, sensor_depth) and bins
         metadata (per-bin depth, real_time_bin) from MDAPI.
      2. Branch on mounting type (issues #140, #141):
         - ``side``: collapse to a single virtual station at
           ``sensor_depth`` using the bins endpoint's ``real_time_bin``.
           Side-looking ADCPs sample one depth horizontally across a
           channel, so the historical fan-out to N bins all at depth=0
           produced N redundant comparisons against the model surface
           layer.
         - ``up``/``down``: keep per-bin fan-out, reading depths from
           the bins endpoint (which always populates them for these
           orientations).
         - ``unknown``: log a WARNING and treat as up/down (per-bin
           fan-out) so the run continues with the per-bin depths the
           bins endpoint reports.
      3. Stamp ``df.attrs['orientation']`` and ``df.attrs['mounting_type']``
         from the deployments endpoint (the per-bin entry never carries
         orientation; the station endpoint does not either). This is
         the source-of-truth fix for issue #141 — previously orientation
         was read from each bin entry and always came back empty,
         causing every downward-looking station to be silently mislabeled
         as upward in plot titles.

    When ``only_bins`` is provided, the bin iteration is restricted to
    that set — short-circuits the CO-OPS HTTP traffic when a user has
    supplied a currents-bins override CSV listing only specific bins.

    Returns ``dict[int, DataFrame]`` keyed by bin number, or ``None`` if
    no bin yielded any in-window data.
    """
    start_dt_0 = datetime.strptime(start_date, '%Y%m%d')
    end_dt_0 = datetime.strptime(end_date, '%Y%m%d')

    bins_payload = get_station_depth(station_id, mdapi_url, logger)
    bin_records = _extract_bin_records(bins_payload)
    deployment_info = get_station_deployment(station_id, mdapi_url, logger)
    station_info = get_station_info(station_id, mdapi_url, logger)
    mounting_type = resolve_mounting_type(deployment_info)
    hfb = _resolve_height_from_bottom(deployment_info, station_info)
    if deployment_info is not None:
        orientation_label = str(
            deployment_info.get('orientation') or '')
    else:
        orientation_label = ''

    # Side-looking detection must not depend on the deployments endpoint
    # being reachable. The bins endpoint signature is unambiguous
    # — every per-bin ``depth`` is null on side-looking ADCPs and
    # populated on up/down (verified across all 88 production stations
    # in the audit snapshot) — so use it as a fallback classifier when
    # the deployment endpoint outage would otherwise re-introduce the
    # 48-bin zero-depth fan-out that issue #140 was filed to fix.
    if (
        mounting_type == 'unknown'
        and bin_records
        and all(b.get('depth') is None for b in bin_records)
    ):
        logger.warning(
            'CO-OPS station %s deployment endpoint unavailable but '
            'bins endpoint reports null per-bin depths — assuming '
            'side-looking based on the bins-endpoint signature.',
            station_id)
        mounting_type = 'side'
        if not orientation_label:
            orientation_label = 'side'

    if mounting_type == 'unknown':
        raw_orient = (
            deployment_info.get('orientation')
            if deployment_info is not None else None
        )
        logger.warning(
            'CO-OPS station %s unrecognized orientation %r — pipeline '
            'will fan out per bin using bins-endpoint depths; verify '
            'classification against MDAPI deployments.',
            station_id, raw_orient)

    if mounting_type == 'side':
        return _retrieve_side_looking_currents(
            station_id=station_id,
            start_dt_0=start_dt_0,
            end_dt_0=end_dt_0,
            api_url=api_url,
            bins_payload=bins_payload,
            deployment_info=deployment_info,
            bin_records=bin_records,
            hfb=hfb,
            orientation_label=orientation_label,
            only_bins=only_bins,
            logger=logger,
        )

    # up / down / unknown: per-bin fan-out path. The bins endpoint
    # returns valid per-bin depths for these orientations (verified
    # across all 48 non-side stations); the bin-depth ordering reflects
    # the orientation (ascending for down, descending for up).
    if not bin_records:
        return _retrieve_currents_legacy_fallback(
            station_id=station_id,
            start_dt_0=start_dt_0,
            end_dt_0=end_dt_0,
            api_url=api_url,
            station_info=station_info,
            bins_payload=bins_payload,
            deployment_info=deployment_info,  # <-- ADDED
            hfb=hfb,
            orientation_label=orientation_label,
            mounting_type=mounting_type,
            logger=logger,
        )

    result: dict[int, pd.DataFrame] = {}
    missing_depth: list[int] = []
    for entry in bin_records:
        try:
            bin_num = int(entry.get('num', entry.get('bin')))  # type: ignore[arg-type]
        except (KeyError, TypeError, ValueError):
            continue

        # Restrict to CSV-requested bins when provided. Skips both the
        # datagetter HTTP call and any downstream processing for bins
        # the user did not pin.
        if only_bins is not None and bin_num not in only_bins:
            continue

        depth = _bin_depth_from_record(entry)
        if depth is None:
            missing_depth.append(bin_num)
            depth = 0.0

        df = _fetch_currents_chunked(
            station_id=station_id,
            bin_num=bin_num,
            start_dt_0=start_dt_0,
            end_dt_0=end_dt_0,
            api_url=api_url,
            logger=logger,
            deployment_info=deployment_info,  # <-- ADDED
        )
        if df is None:
            continue

        df['DEP01'] = pd.to_numeric(depth)
        df.attrs['bin'] = bin_num
        df.attrs['depth'] = depth
        df.attrs['orientation'] = orientation_label
        df.attrs['mounting_type'] = mounting_type
        df.attrs['height_from_bottom'] = hfb
        result[bin_num] = df

    if missing_depth:
        # ERROR rather than WARNING for non-side stations: the bins
        # endpoint should always populate depth for up/down/unknown.
        # A null here is a real anomaly worth investigating, distinct
        # from the side-looking case (which never enters this branch).
        logger.error(
            'CO-OPS station %s (%s) bins endpoint returned no depth for '
            'bins %s; depth recorded as 0.0 m. This is unexpected for '
            'non-side ADCPs — check MDAPI metadata.',
            station_id, mounting_type, missing_depth)

    if not result:
        logger.info(
            'CO-OPS currents retrieval for station %s returned no bins '
            'with data.', station_id)
        return None

    _record_mounting(mounting_type)
    logger.info(
        'CO-OPS currents retrieval for station %s (%s, %d bin(s)): %s',
        station_id, mounting_type, len(result), sorted(result.keys()))
    return result


def _resolve_height_from_bottom(
    deployment_info: Optional[dict],
    station_info: Optional[dict],
) -> Optional[float]:
    """Return ``height_from_bottom`` (m), preferring the deployments source.

    Both endpoints expose ``height_from_bottom`` and the surveyed values
    agree across all 88 currents stations, but the deployments payload
    is the authoritative one (the station endpoint just mirrors it).
    Falls through to the station endpoint if the deployments payload is
    unavailable.
    """
    for source in (deployment_info, station_info):
        if source is None:
            continue
        raw = source.get('height_from_bottom')
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _retrieve_side_looking_currents(
    station_id: str,
    start_dt_0: datetime,
    end_dt_0: datetime,
    api_url: str,
    bins_payload: Optional[dict],
    deployment_info: Optional[dict],
    bin_records: list[dict],
    hfb: Optional[float],
    orientation_label: str,
    only_bins: Optional[set[int]],
    logger: Logger,
) -> Optional[dict[int, pd.DataFrame]]:
    """Side-looking ADCP currents retrieval (issue #140).

    Resolves the dissemination bin from MDAPI (``bins.json`` top-level
    ``real_time_bin`` preferred over ``deployments[0].real_time_bin``)
    and the comparison depth from ``sensor_depth`` (with
    ``measured_depth - height_from_bottom`` as fallback). Returns a
    dict keyed by bin number.

    Behaviour by ``only_bins``:

    * ``None`` (default): one virtual station at the MDAPI
      ``real_time_bin`` — the issue #140 happy path.
    * Single-entry set containing the real-time bin: same as default.
    * Single-entry set with a non-real-time bin: honour the user's
      pinned bin (single virtual station at sensor_depth), log
      WARNING that the choice differs from the dissemination bin.
    * Multi-bin set: honour each bin separately (WARNING that the
      side-looking depth is the same for all of them, since a side
      ADCP samples a single depth — the multi-bin output exists only
      for users explicitly inspecting horizontal-range slices).
    """
    real_time_bin = _resolve_side_real_time_bin(
        bins_payload, deployment_info)
    sensor_depth = _resolve_side_sensor_depth(deployment_info)

    if only_bins:
        chosen_bins = sorted(only_bins)
        if (
            real_time_bin is not None
            and real_time_bin not in only_bins
        ):
            logger.warning(
                'CO-OPS station %s side-looking real_time_bin=%d not in '
                'CSV-requested bins %s; honouring user override.',
                station_id, real_time_bin, chosen_bins)
        if len(chosen_bins) > 1:
            logger.warning(
                'CO-OPS station %s side-looking: CSV requested %d bins '
                '%s, but a side-looking ADCP samples a single depth — '
                'all bins will report the same sensor_depth.',
                station_id, len(chosen_bins), chosen_bins)
    elif real_time_bin is not None:
        chosen_bins = [real_time_bin]
    else:
        logger.warning(
            'CO-OPS station %s side-looking but no real_time_bin '
            'available from MDAPI; defaulting to bin=1 with '
            'depth_unknown=True.', station_id)
        chosen_bins = [1]

    depth_unknown = sensor_depth is None
    depth_value = sensor_depth if sensor_depth is not None else 0.0
    result: dict[int, pd.DataFrame] = {}
    for chosen_bin in chosen_bins:
        df = _fetch_currents_chunked(
            station_id=station_id,
            bin_num=chosen_bin,
            start_dt_0=start_dt_0,
            end_dt_0=end_dt_0,
            api_url=api_url,
            logger=logger,
            deployment_info=deployment_info,  # <-- ADDED
        )
        if df is None:
            logger.info(
                'CO-OPS side-looking station %s (bin=%d) returned no '
                'data.', station_id, chosen_bin)
            continue
        df['DEP01'] = pd.to_numeric(depth_value)
        df.attrs['bin'] = chosen_bin
        df.attrs['depth'] = depth_value
        df.attrs['depth_unknown'] = depth_unknown
        df.attrs['orientation'] = orientation_label or 'side'
        df.attrs['mounting_type'] = 'side'
        df.attrs['height_from_bottom'] = hfb
        result[chosen_bin] = df

    if not result:
        return None

    n_bin_records = len(bin_records)
    depth_label = (
        f'{depth_value:.2f} m' if not depth_unknown else 'UNKNOWN'
    )
    _record_mounting('side')
    logger.info(
        'CO-OPS station %s side-looking: collapsed %d bin record(s) -> '
        '%d virtual station(s) at depth=%s (bins=%s, sensor_depth '
        'source: %s).',
        station_id, n_bin_records, len(result), depth_label,
        sorted(result.keys()),
        'sensor_depth' if sensor_depth is not None else 'unavailable',
    )
    return result


def _retrieve_currents_legacy_fallback(
    station_id: str,
    start_dt_0: datetime,
    end_dt_0: datetime,
    api_url: str,
    station_info: Optional[dict],
    bins_payload: Optional[dict],
    deployment_info: Optional[dict],  # <-- ADDED
    hfb: Optional[float],
    orientation_label: str,
    mounting_type: str,
    logger: Logger,
) -> Optional[dict[int, pd.DataFrame]]:
    """No bin records on MDAPI: fall back to one unfiltered datagetter call.

    Used when the bins endpoint returns nothing (transient MDAPI
    outage, new station not yet populated, etc.). The depth is unknown
    so ``df.attrs['depth_unknown']`` is set; downstream consumers must
    treat the result as suspect.
    """
    fallback_bin = _legacy_fallback_bin(station_info, bins_payload)

    logger.warning(
        'CO-OPS bins metadata unavailable for station %s: DEPTH UNKNOWN '
        '— falling back to unfiltered datagetter for bin=%s. The '
        'measurement will be paired with the model surface layer and '
        "flagged via df.attrs['depth_unknown']=True; downstream "
        'consumers should treat this as suspect.',
        station_id, fallback_bin)

    legacy = _fetch_currents_chunked(
        station_id=station_id,
        bin_num=None,
        start_dt_0=start_dt_0,
        end_dt_0=end_dt_0,
        api_url=api_url,
        logger=logger,
        deployment_info=deployment_info,  # <-- ADDED
    )
    if legacy is None:
        return None
    legacy.attrs['bin'] = fallback_bin
    legacy.attrs['depth'] = 0.0
    legacy.attrs['depth_unknown'] = True
    legacy.attrs['orientation'] = orientation_label
    legacy.attrs['mounting_type'] = mounting_type
    legacy.attrs['height_from_bottom'] = hfb
    _record_mounting(mounting_type)
    logger.warning(
        'CO-OPS currents retrieval for station %s returned 1 bin '
        '(legacy fallback, bin=%s, depth UNKNOWN, mounting=%s).',
        station_id, fallback_bin, mounting_type)
    return {fallback_bin: legacy}

def _legacy_fallback_bin(
    station_info: Optional[dict],
    bins_payload: Optional[dict],
) -> int:
    """Best-effort bin number for the legacy fallback path.

    Tries the active deployment (``retrieved == ''``) of station_info
    first, then top-level ``real_time_bin`` from bins_payload, then any
    historical deployment. Defaults to 1.
    """
    if station_info is not None:
        deployments = station_info.get('deployments') or []
        active = next(
            (
                d for d in deployments
                if isinstance(d, dict) and (d.get('retrieved') or '') == ''
            ),
            None,
        )
        if active is not None:
            rtb = _coerce_int(active.get('real_time_bin'))
            if rtb is not None:
                return rtb
    if isinstance(bins_payload, dict):
        rtb = _coerce_int(bins_payload.get('real_time_bin'))
        if rtb is not None:
            return rtb
    if station_info is not None:
        for entry in station_info.get('deployments') or []:
            if not isinstance(entry, dict):
                continue
            rtb = _coerce_int(entry.get('real_time_bin'))
            if rtb is not None:
                return rtb
    return 1

def _fetch_currents_chunked(
    station_id: str,
    bin_num: Optional[int],
    start_dt_0: datetime,
    end_dt_0: datetime,
    api_url: str,
    logger: Logger,
    deployment_info: Optional[dict] = None,
) -> Optional[pd.DataFrame]:
    """Pull currents data for a station (optionally a specific bin) in
    30-day chunks and return a DataFrame with DateTime/DIR/OBS columns.

    If the full range isn't retrieved, checks if a deployment change
    occurred within the requested window. If so, retries by searching
    backwards using expanding windows.

    Returns ``None`` when no rows fall inside ``[start_dt_0, end_dt_0]``.
    """
    delta = timedelta(days=30)
    cur = start_dt_0
    dates: list[str] = []
    speeds: list[float] = []
    dirs: list[float] = []

    def _process_obs(obs_data: dict) -> None:
        """Helper to extract and append rows from the API response."""
        for row in obs_data.get('data', []) or []:
            if bin_num is not None:
                row_bin_raw = row.get('b')
                if row_bin_raw is not None:
                    try:
                        row_bin = int(row_bin_raw)
                    except (TypeError, ValueError):
                        row_bin = None
                    if row_bin is not None and row_bin != bin_num:
                        logger.warning(
                            'CO-OPS datagetter returned bin=%s for '
                            'request bin=%s, skipping row',
                            row_bin, bin_num)
                        continue
            try:
                speed_m = float(row['s']) / 100  # cm/s → m/s
            except (TypeError, ValueError, KeyError):
                speed_m = float('nan')
            try:
                direction = float(row['d'])
            except (TypeError, ValueError, KeyError):
                direction = float('nan')

            dates.append(row['t'])
            speeds.append(speed_m)
            dirs.append(direction)

    # 1. Forward chunking
    while cur <= end_dt_0:
        date_i = cur.strftime('%Y%m%d')
        date_f = (cur + delta).strftime('%Y%m%d')
        bin_qs = f'&bin={bin_num}' if bin_num is not None else ''
        url = (
            f'{api_url}/datagetter?begin_date={date_i}&end_date={date_f}'
            f'&station={station_id}&product=currents{bin_qs}'
            f'&time_zone=gmt&units=metric&format=json'
        )
        context = f'currents bin={bin_num}' if bin_num is not None else 'currents'
        obs = _get_with_retry(url, station_id, context, logger)

        if obs is not None:
            _process_obs(obs)

        cur += delta

    # 2. Bail if there is no data at all
    if not dates:
        return None

    # 3. Check if the full date range was retrieved
    max_dt = pd.to_datetime(max(dates))

    # 1-hour tolerance for expected sampling intervals
    if max_dt < end_dt_0 - timedelta(hours=1):

        # only backward chunk if a deployment change occurred in the window
        has_deployment_change = False
        if deployment_info and deployment_info.get('deployments'):
            for dep in deployment_info['deployments']:
                for date_key in ('deployed', 'retrieved'):
                    val = dep.get(date_key)
                    if val:
                        try:
                            # NOAA returns dates as YYYY-MM-DD.
                            # Slice first 10 chars to defend against potential time components
                            dt_val = datetime.strptime(val[:10], '%Y-%m-%d')
                            if start_dt_0 <= dt_val <= end_dt_0:
                                has_deployment_change = True
                                break
                        except ValueError:
                            continue
                if has_deployment_change:
                    break

        if has_deployment_change:
            logger.info(
                'Incomplete date range and deployment change detected. '
                'Searching backwards from %s using expanding windows to bridge gap...', end_dt_0
            )
            retry_cur = end_dt_0
            gap_bridged = False

            # Use .date() for the loop condition to avoid the midnight cutoff bug,
            # ensuring we query the API for the day even if max_dt has a PM timestamp.
            while retry_cur.date() >= max_dt.date() and retry_cur.date() >= start_dt_0.date():
                date_str = retry_cur.strftime('%Y%m%d')
                bin_qs = f'&bin={bin_num}' if bin_num is not None else ''

                # Search backward using expanding 3-hour windows up to 24 hours
                for range_hrs in range(3, 27, 3):
                    url = (
                        f'{api_url}/datagetter?end_date={date_str}&range={range_hrs}'
                        f'&station={station_id}&product=currents{bin_qs}'
                        f'&time_zone=gmt&units=metric&format=json'
                    )
                    context = (f'currents bin={bin_num} (retry {range_hrs}h)' if bin_num is not None
                               else f'currents (retry {range_hrs}h)')

                    obs = _get_with_retry(url, station_id, context, logger)

                    if obs is not None and obs.get('data'):
                        _process_obs(obs)

                        # Early exit optimization: stop hammering the API if we just bridged the gap
                        earliest_str = obs['data'][0]['t']
                        earliest_dt = datetime.strptime(earliest_str, '%Y-%m-%d %H:%M')

                        # We still use full datetime here to know exactly when the chronological gap is bridged
                        if earliest_dt <= max_dt or earliest_dt <= start_dt_0:
                            logger.info('Gap bridged at %s. Halting backward search.', earliest_str)
                            gap_bridged = True
                            break

                if gap_bridged:
                    break

                retry_cur -= timedelta(days=1)
        else:
            logger.info(
                'Incomplete date range retrieved, but no deployment change '
                'detected in the %s to %s window. Skipping backward search.',
                start_dt_0.strftime('%Y-%m-%d'), end_dt_0.strftime('%Y-%m-%d')
            )

    # 4. Assemble and filter
    df = pd.DataFrame({
        'DateTime': pd.to_datetime(dates),
        'DEP01': pd.to_numeric(0.0),
        'DIR': pd.to_numeric(dirs),
        'OBS': pd.to_numeric(speeds),
    })

    mask = (df['DateTime'] >= start_dt_0) & (df['DateTime'] <= end_dt_0)
    df = df.loc[mask]

    if df.empty:
        return None

    # Handles overlapping duplicates generated by the expanding 3-hour windows
    df = df.sort_values(by='DateTime').drop_duplicates()

    # 5. Final completion check logging
    final_max_dt = df['DateTime'].max()
    if final_max_dt < end_dt_0 - timedelta(hours=1):
        logger.warning(
            'After all retrievals, data for CO-OPS station %s (bin=%s) is still incomplete. '
            'Latest timestamp is %s (expected %s). Retaining retrieved data.',
            station_id, bin_num, final_max_dt, end_dt_0
        )

    return df


def get_HTTP_error(ex: HTTPError) -> str:
    """
    Parse HTTP error to show CO-OPS API error message.

    Args:
        ex: HTTPError exception from urllib

    Returns:
        Formatted error message string from API response, or default message
    """
    try:
        error_body = ex.read().decode(errors='replace')
        # Attempt to parse JSON if it's JSON formatted
        try:
            error_json = json.loads(error_body)
            error_msg = error_json.get('error', {}).get('message', error_body)
        except json.JSONDecodeError:
            error_msg = error_body
    except Exception:
        error_msg = 'No additional error message available.'

    return error_msg


def retrieve_tidal_predictions(
    retrieve_input: Any,
    logger: Logger,
    config_file=None,
) -> Optional[pd.DataFrame]:
    """
    Retrieve tidal predictions from CO-OPS API.

    Similar to water_level retrieval but uses product=predictions.

    Args:
        retrieve_input: Object with attributes:
            - station: Station ID
            - start_date: Start date in YYYYMMDDHHMMSS format
            - end_date: End date in YYYYMMDDHHMMSS format
            - datum: Vertical datum reference
        logger: Logger instance

    Returns:
        DataFrame with columns:
            - DateTime: Prediction timestamps
            - TIDE: Predicted tidal values
        Returns None if no data available.
        Returns False if station doesn't support predictions.
    """
    t_c = t_and_c_properties.TidesandCurrentsProperties()

    url_params = utils.Utils(config_file).read_config_section('urls', logger)
    t_c.api_url = url_params['co_ops_api_base_url']

    t_c.start_dt_0 = datetime.strptime(
        retrieve_input.start_date, '%Y%m%d%H%M%S'
    )
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, '%Y%m%d%H%M%S')
    t_c.start_dt = datetime.strptime(retrieve_input.start_date, '%Y%m%d%H%M%S')
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, '%Y%m%d%H%M%S')

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var = [], []

    while t_c.start_dt <= t_c.end_dt:
        date_i = t_c.start_dt.strftime('%Y%m%d%%20%H:%M')
        date_f = (t_c.start_dt + t_c.delta).strftime('%Y%m%d%%20%H:%M')

        t_c.station_url = (
            f'{t_c.api_url}/datagetter?begin_date={date_i}&end_date={date_f}'
            f'&station={retrieve_input.station}&product=predictions'
            f'&datum={retrieve_input.datum}&time_zone=gmt&units=metric'
            f'&format=json'
        )

        try:
            response = _rate_limited_get(t_c.station_url, timeout=120)
            response.raise_for_status()
            obs = response.json()
            logger.info(
                'CO-OPS station %s contacted for tidal predictions.',
                retrieve_input.station
            )
        except requests.exceptions.RequestException as ex:
            logger.warning(
                'CO-OPS tidal predictions retrieval failed for %s: %s',
                retrieve_input.station, ex
            )
            t_c.start_dt += t_c.delta
            continue

        # Check for API error message (station doesn't support predictions)
        if 'error' in obs:
            error_msg = obs['error'].get('message', str(obs['error']))
            logger.debug(
                'CO-OPS API error for station %s: %s',
                retrieve_input.station, error_msg
            )
            # Return False to indicate station doesn't support predictions
            return False

        if 'predictions' in obs.keys():
            for i in range(len(obs['predictions'])):
                t_c.total_date.append(obs['predictions'][i]['t'])
                t_c.total_var.append(obs['predictions'][i]['v'])

        t_c.start_dt += t_c.delta

    if not t_c.total_date:
        logger.warning(
            'No tidal prediction data returned from API for station %s',
            retrieve_input.station
        )
        return None

    obs_df = pd.DataFrame({
        'DateTime': pd.to_datetime(t_c.total_date),
        'TIDE': pd.to_numeric(t_c.total_var),
    })

    logger.debug(
        'Tidal data retrieved: %d points from %s to %s',
        len(obs_df), obs_df['DateTime'].min(), obs_df['DateTime'].max()
    )
    logger.debug('Requested range: %s to %s', t_c.start_dt_0, t_c.end_dt_0)

    mask = (obs_df['DateTime'] >= t_c.start_dt_0) & (
        obs_df['DateTime'] <= t_c.end_dt_0
    )
    obs_df = obs_df.loc[mask]

    if len(obs_df.DateTime) > 0:
        obs_df = obs_df.sort_values(by='DateTime').drop_duplicates()
        return obs_df

    logger.warning(
        'Tidal data was retrieved but all %d points were outside requested '
        'time range for station %s',
        len(t_c.total_date), retrieve_input.station
    )

    logger.debug('Exiting tide retrieval.')
    return None


TIMEOUT_SEC = 120


def retrieve_harmonic_constants(
    station: str,
    logger: Logger,
    units: str = 'metric',
    config_file=None,
) -> Optional[dict]:
    """
    Retrieve accepted harmonic constants from the CO-OPS API.

    Calls the ``product=harcon`` endpoint and returns constituent amplitudes,
    phases, and speeds in a format ready for
    :func:`~ofs_skill.tidal_analysis.tidal_prediction.predict_from_constants`
    and
    :func:`~ofs_skill.tidal_analysis.ha_comparison.compare_harmonic_constants`.

    Constituent names are normalized from CO-OPS convention to NOS/UTide
    convention using :data:`~ofs_skill.tidal_analysis.constituents.COOPS_API_NAME_MAP`.

    Args:
        station: CO-OPS station ID (e.g., "8454000").
        logger: Logger instance for diagnostic messages.
        units: Unit system — ``"metric"`` (default) or ``"english"``.

    Returns:
        Dictionary with keys:

        - **amplitudes** — ``{constituent_name: amplitude}`` (metres or feet)
        - **phases** — ``{constituent_name: phase_GMT}`` (degrees Greenwich)
        - **speeds** — ``{constituent_name: speed}`` (degrees/hour)
        - **constituents** — list of constituent names (NOS convention)
        - **number_of_constituents** — int

        Returns ``None`` if the API call fails or the station has no
        harmonic constants.

    Example:
        >>> harcon = retrieve_harmonic_constants("8454000", logger)
        >>> harcon["amplitudes"]["M2"]   # amplitude in metres
        0.543
        >>> harcon["phases"]["M2"]       # phase in degrees
        109.7
    """
    from ofs_skill.tidal_analysis.constituents import normalize_constituent_name

    url_params = utils.Utils(config_file).read_config_section('urls', logger)
    mdapi_url = url_params.get(
        'co_ops_mdapi_base_url',
        'https://api.tidesandcurrents.noaa.gov/mdapi/prod/',
    )

    harcon_url = (
        f'{mdapi_url}webapi/stations/{station}/harcon.json?units={units}'
    )

    try:
        resp = _rate_limited_get(harcon_url, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        response = resp.json()
        logger.info(
            'CO-OPS station %s contacted for harmonic constants retrieval.',
            station,
        )
    except requests.exceptions.RequestException as ex:
        logger.error(
            'CO-OPS harmonic constants retrieval failed for station %s! %s',
            station, ex,
        )
        return None
    except Exception as ex:
        logger.error(
            'Unexpected error retrieving harmonic constants for station %s: %s',
            station, ex,
        )
        return None

    # Check for API-level error (station may not have harmonic constants)
    if 'error' in response:
        error_msg = response['error'].get('message', str(response['error']))
        logger.warning(
            'CO-OPS API error for station %s harmonic constants: %s',
            station, error_msg,
        )
        return None

    if 'HarmonicConstituents' not in response:
        logger.warning(
            'No HarmonicConstituents key in response for station %s.',
            station,
        )
        return None

    raw_constituents = response['HarmonicConstituents']
    if not raw_constituents:
        logger.warning(
            'Empty harmonic constituents list for station %s.', station,
        )
        return None

    amplitudes = {}
    phases = {}
    speeds = {}
    constituent_names = []

    for entry in raw_constituents:
        raw_name = entry.get('name', '')
        nos_name = normalize_constituent_name(raw_name)

        try:
            amp = float(entry['amplitude'])
            phase = float(entry['phase_GMT'])
            speed = float(entry['speed'])
        except (KeyError, ValueError, TypeError) as ex:
            logger.debug(
                'Skipping constituent %s for station %s: %s',
                raw_name, station, ex,
            )
            continue

        amplitudes[nos_name] = amp
        phases[nos_name] = phase
        speeds[nos_name] = speed
        constituent_names.append(nos_name)

    if not constituent_names:
        logger.warning(
            'No valid harmonic constants parsed for station %s.', station,
        )
        return None

    logger.info(
        'Retrieved %d harmonic constants for station %s.',
        len(constituent_names), station,
    )

    return {
        'amplitudes': amplitudes,
        'phases': phases,
        'speeds': speeds,
        'constituents': constituent_names,
        'number_of_constituents': len(constituent_names),
    }


def find_nearest_tidal_stations(
    lat: float,
    lon: float,
    logger: Logger,
    max_stations: int = 10,
    config_file=None,
) -> list[tuple[str, str, float]]:
    """
    Find the nearest CO-OPS stations with tidal predictions.

    Uses the CO-OPS metadata API to get stations with predictions capability
    and calculates distances using Haversine formula.

    Args:
        lat: Latitude of target location
        lon: Longitude of target location
        logger: Logger instance
        max_stations: Maximum number of stations to return (default: 10)

    Returns:
        List of tuples (station_id, station_name, distance_km) sorted by
        distance, or empty list if none found.
    """
    url_params = utils.Utils(config_file).read_config_section('urls', logger)
    mdapi_url = url_params['co_ops_mdapi_base_url']

    # Get list of stations with tidal predictions
    stations_url = f'{mdapi_url}/webapi/stations.json?type=tidepredictions'

    try:
        response = _rate_limited_get(stations_url, timeout=120)
        response.raise_for_status()
        stations_data = response.json()
    except Exception as ex:
        logger.warning('Could not retrieve CO-OPS tidal stations list: %s', ex)
        return []

    if 'stations' not in stations_data or not stations_data['stations']:
        logger.warning('No tidal prediction stations found in CO-OPS API '
                       'response')
        return []

    # Calculate distance using Haversine distance
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great circle distance in km using Haversine formula."""
        R = 6371  # Earth's radius in km
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        return 2 * R * math.asin(math.sqrt(a))

    # Calculate distances for all stations
    station_distances = []
    for station in stations_data['stations']:
        try:
            slat = float(station['lat'])
            slon = float(station['lng'])
            dist = haversine(lat, lon, slat, slon)
            station_distances.append(
                (station['id'], station.get('name', 'Unknown'), dist)
            )
        except (KeyError, ValueError, TypeError):
            continue

    # Sort by distance and return top N
    station_distances.sort(key=lambda x: x[2])
    return station_distances[:max_stations]


def find_nearest_tidal_station(
    lat: float,
    lon: float,
    logger: Logger,
    config_file=None,
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Find the single nearest CO-OPS station with tidal predictions.

    Args:
        lat: Latitude of target location
        lon: Longitude of target location
        logger: Logger instance
        config_file: Optional path to config file

    Returns:
        Tuple of (station_id, station_name, distance_km) or
        (None, None, None) if not found.
    """
    stations = find_nearest_tidal_stations(lat, lon, logger, max_stations=1,
                                           config_file=config_file)
    if stations:
        station_id, station_name, distance = stations[0]
        logger.info(
            'Found nearest tidal station: %s (%s) at %.1f km',
            station_id, station_name, distance
        )
        return station_id, station_name, distance
    return None, None, None
