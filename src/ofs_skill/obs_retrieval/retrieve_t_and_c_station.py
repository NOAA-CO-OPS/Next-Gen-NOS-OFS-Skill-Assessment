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
"""

import json
import math
import random
import threading
import time
from datetime import datetime, timedelta
from logging import Logger
from typing import Optional
from urllib.error import HTTPError

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from ofs_skill.obs_retrieval import t_and_c_properties, utils

# ---------------------------------------------------------------------------
# Module-level HTTP session with connection pooling (Task 2)
# ---------------------------------------------------------------------------
_session = None


def _get_session():
    """Lazily create and return a shared requests.Session with connection pooling."""
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        _session.mount('https://', adapter)
        _session.mount('http://', adapter)
    return _session


# ---------------------------------------------------------------------------
# Global CO-OPS concurrency cap (issue #98)
# ---------------------------------------------------------------------------
# CO-OPS throttles aggressively when many historical datagetter requests
# come from one IP at once. ``write_obs_ctlfile`` parallelizes across
# variables (4) and stations (up to 6), so without a process-wide cap an
# OFS run can fire ~24 concurrent GETs at CO-OPS, trigger IP-level 403s,
# and then synchronize retries into a thundering herd. Every CO-OPS HTTP
# call in this module acquires this semaphore before issuing the GET,
# decoupling HTTP pressure from upstream ThreadPoolExecutor sizing.
_COOPS_CONCURRENCY_LIMIT = 4
_coops_request_semaphore = threading.Semaphore(_COOPS_CONCURRENCY_LIMIT)


def _rate_limited_get(url: str, timeout: int = 120) -> requests.Response:
    """GET ``url`` through the shared session, gated by the CO-OPS semaphore.

    The semaphore caps concurrent in-flight CO-OPS requests across every
    worker thread in the process. Callers still get the shared connection
    pool; only the HTTP round-trip itself counts against the concurrency
    budget. Retry backoff sleeps outside this lock so waiting threads do
    not starve the pool.
    """
    with _coops_request_semaphore:
        return _get_session().get(url, timeout=timeout)


# ---------------------------------------------------------------------------
# Module-level cache for station depth metadata (Task 3)
# ---------------------------------------------------------------------------
_depth_cache = {}  # station_id -> depth_data


def _get_station_depth(station_id, mdapi_url, logger):
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
        depth_data = None

    _depth_cache[station_id] = depth_data
    return depth_data


# CO-OPS occasionally rate-limits or returns transient 5xx when a long
# historical window forces many 30-day chunk requests per station; retry
# a small number of times with full-jitter exponential backoff before
# giving up. Kept in sync with the per-bin retry helper on
# issue-87-currents-bins so both paths share the same backoff profile.
_RETRY_STATUSES = (403, 408, 429, 500, 502, 503, 504)
_RETRY_MAX_ATTEMPTS = 6
# Base delay is the upper bound of the first retry. CO-OPS throttle
# windows for burst IP traffic are typically tens of seconds, so starting
# at 5s lets the first retry clear even short bans instead of re-hitting
# the ceiling. Delay grows as random.uniform(0, _BASE * 2**(attempt-1)).
_RETRY_BASE_DELAY = 5.0


def _backoff_delay(attempt: int) -> float:
    """Return seconds to sleep before the next retry attempt.

    Full-jitter exponential backoff: ``uniform(0, base * 2^(attempt-1))``.
    Randomizing the entire delay (rather than adding a small jitter on top
    of a fixed backoff) decorrelates retry timing across threads that all
    hit the rate limiter in the same instant. Without this, every worker
    that got a 403 together would also retry together, keeping the herd
    effect alive through the full retry budget.
    """
    return random.uniform(0, _RETRY_BASE_DELAY * (2 ** (attempt - 1)))


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
) -> Optional[dict]:
    """Fetch the primary URL with retry; fall back to ``backup_url`` when set.

    The water_temperature / salinity backup URL points at the
    ``NOS.COOPS.TAC.PHYSOCEAN`` endpoint with ``interval=6``, which
    returns HOURLY observations rather than the primary's 6-minute
    cadence. Mixing the two resolutions silently degrades skill metrics
    that assume uniform sampling, so log a WARNING whenever the backup
    produces data so operators notice the resolution change.
    """
    obs = _get_with_retry(primary_url, station_id, variable, logger)
    if obs is not None:
        logger.info(
            'CO-OPS station %s contacted for %s retrieval.',
            station_id, variable)
        return obs

    if backup_url is None:
        return None

    obs = _get_with_retry(backup_url, station_id, variable, logger)
    if obs is None:
        return None
    logger.warning(
        'CO-OPS backup endpoint used for station %s %s - this chunk '
        'returns hourly samples (interval=6) rather than the primary 6-min '
        'cadence; downstream skill metrics may be affected.',
        station_id, variable)
    return obs


def retrieve_t_and_c_station(
    retrieve_input: object,
    logger: Logger
) -> Optional[pd.DataFrame]:
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

    Returns:
        DataFrame with columns:
            - DateTime: Observation timestamps
            - DEP01: Observation depth in meters
            - OBS: Observation values
            - DIR: Direction values (for currents/wind only)
        Returns None if no data retrieved.

    Raises:
        HTTPError: If API request fails after retries
    """
    variable = retrieve_input.variable

    t_c = t_and_c_properties.TidesandCurrentsProperties()

    # Retrieve url from config file
    url_params = utils.Utils().read_config_section('urls', logger)
    t_c.mdapi_url = url_params['co_ops_mdapi_base_url']
    t_c.api_url = url_params['co_ops_api_base_url']

    t_c.start_dt_0 = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    t_c.start_dt = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var, t_c.total_dir = [], [], []

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

        elif variable == 'currents':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product={variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

        backup_url = (t_c.station_url_2
                      if variable in {'water_temperature', 'salinity'}
                      else None)
        obs = _fetch_with_backup(
            t_c.station_url, backup_url, str(retrieve_input.station),
            variable, logger)
        if obs is None:
            t_c.start_dt += t_c.delta
            continue

        t_c.date, t_c.var, t_c.drt = [], [], []
        if 'data' in obs.keys():
            for i in range(len(obs['data'])):
                t_c.date.append(obs['data'][i]['t'])

                if variable in {'water_level',
                                'water_temperature',
                                'air_pressure'}:
                    t_c.var.append(obs['data'][i]['v'])

                elif variable == 'salinity':
                    t_c.var.append(obs['data'][i]['s'])

                elif variable == 'currents':
                    # Convert speed from cm/s to m/s
                    t_c.var.append(float(obs['data'][i]['s']) / 100)
                    t_c.drt.append(obs['data'][i]['d'])

                elif variable == 'wind':
                    t_c.var.append(obs['data'][i]['s'])
                    t_c.drt.append(obs['data'][i]['d'])

            t_c.total_date.append(t_c.date)
            t_c.total_var.append(t_c.var)
            if variable in {'wind', 'currents'}:
                t_c.total_dir.append(t_c.drt)

        t_c.start_dt += t_c.delta

    # Retrieve observation depth from metadata API (cached per station)
    t_c.depth = 0.0
    t_c.depth_url = _get_station_depth(
        str(retrieve_input.station), t_c.mdapi_url, logger)

    if (
        t_c.depth_url is not None
        and t_c.depth_url['bins'] is not None
        and t_c.depth_url['real_time_bin'] is not None
        and t_c.depth_url['bins'][t_c.depth_url['real_time_bin'] - 1][
            'depth'] is not None
    ):
        t_c.depth = float(
            t_c.depth_url['bins'][t_c.depth_url['real_time_bin'] - 1]['depth']
        )

    t_c.total_date = sum(t_c.total_date, [])
    t_c.total_var = sum(t_c.total_var, [])

    if variable in {'wind', 'currents'}:
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
    retrieve_input: object,
    logger: Logger
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

    url_params = utils.Utils().read_config_section('urls', logger)
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

    url_params = utils.Utils().read_config_section('urls', logger)
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
    max_stations: int = 10
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
    url_params = utils.Utils().read_config_section('urls', logger)
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
    logger: Logger
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Find the single nearest CO-OPS station with tidal predictions.

    Args:
        lat: Latitude of target location
        lon: Longitude of target location
        logger: Logger instance

    Returns:
        Tuple of (station_id, station_name, distance_km) or
        (None, None, None) if not found.
    """
    stations = find_nearest_tidal_stations(lat, lon, logger, max_stations=1)
    if stations:
        station_id, station_name, distance = stations[0]
        logger.info(
            'Found nearest tidal station: %s (%s) at %.1f km',
            station_id, station_name, distance
        )
        return station_id, station_name, distance
    return None, None, None
