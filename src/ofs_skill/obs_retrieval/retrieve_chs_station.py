"""
Retrieve CHS (Canadian Hydrographic Service) station observations.

This module uses SEARVEY to retrieve CHS time series data for water level,
water temperature, salinity, and currents.

Supported variables:
    - water_level: Time series code 'wlo'
    - water_temperature: Time series codes 'wt1', 'wt2' (fallback)
    - salinity: Time series codes 'ws1', 'ws2' (fallback)
    - currents: Speed ('wcs1'/'wcs2') + direction ('wcd1'/'wcd2')

@author: PWL
Created on Wed Feb  4 19:51:12 2026
"""

from datetime import datetime, timedelta
from logging import Logger

import pandas as pd
import requests

from ofs_skill.obs_retrieval.chs_utils import (
    CHS_IWLS_BASE_URL,
    chs_get,
    is_chs_uuid,
)

# CHS caps data per request based on resolution:
#   - 1-minute data: limited to 1 week (7 days)
#   - 3-minute data: limited to 3 weeks (21 days)
#   - 5-minute and coarser data: limited to 1 month (31 days)
# See: https://tides.gc.ca/en/web-services-offered-canadian-hydrographic-service
_CHS_RESOLUTION = 'THREE_MINUTES'
_CHS_CHUNK_HOURS = 21 * 24

# CHS time series codes per variable, in priority order (try first, fallback)
_SCALAR_CODE_MAP = {
    'water_level': ['wlo'],
    'water_temperature': ['wt1', 'wt2'],
    'salinity': ['ws1', 'ws2'],
}

_CURRENT_SPEED_CODES = ['wcs1', 'wcs2']
_CURRENT_DIR_CODES = ['wcd1', 'wcd2']

# Matched sensor pairs for currents: speed and direction must come from
# the same sensor number to avoid mixing sensors at different depths.
_CURRENT_SENSOR_PAIRS = [('wcs1', 'wcd1'), ('wcs2', 'wcd2')]

# CHS QC flag codes (from IWLS API):
#   '1' = Not quality controlled
#   '2' = Correct value
#   '3' = Suspect/doubtful
#   '4' = Erroneous/rejected
# Accept codes 1 and 2; reject 3 (suspect) and 4 (erroneous).
_ACCEPTED_QC_CODES = {'1', '2'}


def _get_chs_uuid(identifier: str, logger: Logger) -> str | None:
    """Helper to resolve a CHS station code to its UUID."""
    if is_chs_uuid(identifier):
        return identifier

    url = f'{CHS_IWLS_BASE_URL}/stations?code={identifier}'
    try:
        response = chs_get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data and isinstance(data, list):
            return data[0].get('id')
    except Exception as e:
        logger.error('Failed to map CHS code %s to UUID: %s', identifier, e)
    return None


def _make_date_chunks(start_date, end_date, interval_hours):
    """
    Generates a list of datetimes every `interval_hours` between start and
    end dates (inclusive of start & end). Returns list of datetime objects.
    """
    date_list = []
    delta = timedelta(hours=interval_hours)

    while start_date <= end_date:
        date_list.append(start_date)
        start_date += delta
        if start_date > end_date:
            date_list.append(end_date)

    return date_list


def _fetch_chs_window(id_number, time_series_code, start, end, logger):
    """
    Fetch one CHS data window at ``_CHS_RESOLUTION``.

    searvey's ``fetch_chs_station`` cannot be used here because it does not
    expose the ``resolution`` parameter and rejects any interval longer than
    7 days. Rate limiting and transient-failure retry are applied by
    ``chs_get``.

    Returns an empty frame when the window genuinely holds no data, and
    ``None`` when it could not be retrieved, so a failure is not mistaken
    for an absence -- a distinction searvey could not express, since it
    returned a frame either way. Never raises: one bad window must not
    abort the station.
    """
    url = (
        f'{CHS_IWLS_BASE_URL}/stations/{id_number}/data'
        f'?time-series-code={time_series_code}'
        f'&resolution={_CHS_RESOLUTION}'
        f'&from={datetime.strftime(start, "%Y-%m-%dT%H:%M:%SZ")}'
        f'&to={datetime.strftime(end, "%Y-%m-%dT%H:%M:%SZ")}'
    )
    try:
        response = chs_get(url, timeout=30)
        response.raise_for_status()
        # Inside the try: a 200 carrying a non-JSON body (an interstitial
        # or truncated response) raises here, and must degrade like any
        # other failed window rather than propagating out of the station.
        payload = response.json()
    except requests.HTTPError as exc:
        status = (
            exc.response.status_code if exc.response is not None else None
        )
        if status == 404:
            # The station does not publish this time-series code at all.
            # That is a genuine absence, not a failure, and is expected
            # while probing fallback codes (wt1 -> wt2).
            logger.debug(
                'CHS station %s does not publish time series %s.',
                str(id_number), time_series_code,
            )
            return pd.DataFrame()
        logger.warning(
            'CHS request failed with HTTP %s for station %s code %s '
            '(%s to %s) after retries.',
            status, str(id_number), time_series_code,
            start.date(), end.date(),
        )
        return None
    except requests.RequestException as exc:
        logger.warning(
            'CHS request errored for station %s code %s (%s to %s) after '
            'retries: %s',
            str(id_number), time_series_code, start.date(), end.date(), exc,
        )
        return None
    except ValueError as exc:
        # requests raises JSONDecodeError (a ValueError) for a body that
        # parsed as neither JSON nor an error status.
        logger.warning(
            'CHS returned an unreadable body for station %s code %s '
            '(%s to %s): %s',
            str(id_number), time_series_code, start.date(), end.date(), exc,
        )
        return None

    # A successful query returns a JSON list of observations; anything else
    # is an error object, which must not be treated as data.
    if not isinstance(payload, list):
        return None
    return pd.DataFrame(payload)


def _fetch_chs_chunked(date_list, id_number, time_series_code, logger):
    """
    Fetch CHS data in ``_CHS_CHUNK_HOURS`` chunks for a single time series code.

    Includes global module rate limiting: Max 3 req/sec AND Max 30 req/min.

    A window that could not be retrieved leaves a gap of up to
    ``_CHS_CHUNK_HOURS`` in the returned series. Concatenating the survivors
    and returning them would report success over a record with an
    undisclosed hole, so the number of failed windows is logged as a
    warning naming the station and how much of the window is missing.
    """
    data_all_append = []
    failed_windows = []
    for start, end in zip(date_list, date_list[1:]):
        # _make_date_chunks can emit a zero-length final chunk when the
        # window ends exactly on a boundary; requesting it wastes a call
        # against the rate limit and returns nothing.
        if start >= end:
            continue
        data_station = _fetch_chs_window(
            id_number, time_series_code, start, end, logger
        )
        if data_station is None:
            failed_windows.append((start, end))
            continue
        if 'errors' in data_station.columns or data_station.empty:
            continue
        data_all_append.append(data_station)

    if failed_windows:
        missing_days = sum(
            (end - start).days for start, end in failed_windows
        )
        logger.warning(
            'CHS station %s code %s: %d of %d window(s) could not be '
            'retrieved, so roughly %d day(s) are missing from this series. '
            'Skill statistics computed from it cover less than the '
            'requested period. First gap: %s to %s.',
            str(id_number), time_series_code, len(failed_windows),
            len(failed_windows) + len(data_all_append), missing_days,
            failed_windows[0][0].date(), failed_windows[0][1].date(),
        )

    if data_all_append:
        return pd.concat(data_all_append, ignore_index=True)
    return None


def _filter_qc(data_all):
    """Filter out records with rejected/suspect QC flags."""
    if 'qcFlagCode' in data_all.columns:
        data_all = data_all[data_all['qcFlagCode'].isin(_ACCEPTED_QC_CODES)]
    return data_all


def _format_raw_data(data_all):
    """Filter QC flags and format raw CHS API response into standard columns."""
    data_all = _filter_qc(data_all)
    data_all['DateTime'] = pd.to_datetime(
        data_all['eventDate'], format='%Y-%m-%dT%H:%M:%SZ'
    )
    drop_cols = [
        c
        for c in ['eventDate', 'qcFlagCode', 'timeSeriesId', 'reviewed']
        if c in data_all.columns
    ]
    data_all = data_all.drop(columns=drop_cols)
    data_all.rename(columns={'value': 'OBS'}, inplace=True)
    data_all.drop_duplicates(subset=['DateTime'], keep='first', inplace=True)
    return data_all


def _retrieve_chs_scalar(date_list, id_number, variable, logger):
    """
    Retrieve a scalar CHS variable (water_level, temperature, salinity).

    Tries codes in priority order; returns data from the first code
    that yields results.
    """
    codes = _SCALAR_CODE_MAP.get(variable)
    if codes is None:
        return None

    data_all = None
    for code in codes:
        data_all = _fetch_chs_chunked(date_list, id_number, code, logger)
        if data_all is not None:
            logger.info(
                'CHS %s data found using code %s for station %s',
                variable,
                code,
                str(id_number),
            )
            break

    if data_all is None:
        return None

    data_all = _format_raw_data(data_all)
    data_all['DEP01'] = 0.0
    if variable == 'water_level':
        data_all['Datum'] = 'IGLD'

    return data_all


def _retrieve_chs_currents(date_list, id_number, logger):
    """Retrieve CHS current speed and direction, merge into single DataFrame."""
    for speed_code, dir_code in _CURRENT_SENSOR_PAIRS:
        speed_data = _fetch_chs_chunked(
            date_list, id_number, speed_code, logger
        )
        if speed_data is None:
            continue
        dir_data = _fetch_chs_chunked(
            date_list, id_number, dir_code, logger
        )
        if speed_data is not None and dir_data is not None:
            logger.info(
                'CHS currents found using matched pair %s/%s for station %s',
                speed_code,
                dir_code,
                str(id_number),
            )
            break
    else:
        logger.warning(
            'CHS currents: no matched speed/direction sensor pair found for station %s',
            str(id_number),
        )
        return None

    speed_data = _filter_qc(speed_data)
    dir_data = _filter_qc(dir_data)

    if speed_data.empty or dir_data.empty:
        return None

    for df in [speed_data, dir_data]:
        df['DateTime'] = pd.to_datetime(
            df['eventDate'], format='%Y-%m-%dT%H:%M:%SZ'
        )

    speed_data = speed_data[['DateTime', 'value']].drop_duplicates(
        subset=['DateTime'], keep='first'
    )
    dir_data = dir_data[['DateTime', 'value']].drop_duplicates(
        subset=['DateTime'], keep='first'
    )

    merged = speed_data.merge(
        dir_data, on='DateTime', suffixes=('_speed', '_dir')
    )
    merged.rename(
        columns={'value_speed': 'OBS', 'value_dir': 'DIR'}, inplace=True
    )
    merged['DEP01'] = 0.0

    return merged


def retrieve_chs_station(
    start_date: str,
    end_date: str,
    id_number: str,
    variable: str,
    logger: Logger,
) -> pd.DataFrame | None:
    """Retrieve CHS station data using SEARVEY library."""
    chs_uuid = _get_chs_uuid(id_number, logger)
    if not chs_uuid:
        logger.error('Could not resolve CHS UUID for station %s', str(id_number))
        return None

    start_date_str = (
        start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
    )
    end_date_str = end_date[:4] + '-' + end_date[4:6] + '-' + end_date[6:]
    start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    if (end_date_dt - start_date_dt).days > _CHS_CHUNK_HOURS / 24:
        date_list = _make_date_chunks(
            start_date_dt, end_date_dt, _CHS_CHUNK_HOURS
        )
    else:
        date_list = [start_date_dt, end_date_dt]

    if variable == 'currents':
        data_all = _retrieve_chs_currents(date_list, chs_uuid, logger)
    else:
        data_all = _retrieve_chs_scalar(date_list, chs_uuid, variable, logger)

    if data_all is None:
        logger.error(
            'Retrieve CHS station %s failed for %s -- station contacted, '
            'but no data available.',
            str(id_number),
            variable,
        )
    return data_all
