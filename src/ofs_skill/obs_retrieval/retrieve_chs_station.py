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
from typing import Optional

import pandas as pd
from searvey._chs_api import fetch_chs_station

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


def _fetch_chs_chunked(date_list, id_number, time_series_code):
    """
    Fetch CHS data in 7-day chunks for a single time series code.

    Uses searvey's built-in rate limiting (default 5 req/sec).

    Args:
        date_list: List of datetime chunk boundaries
        id_number: CHS station ID
        time_series_code: CHS time series code (e.g., 'wlo', 'wt1')

    Returns:
        Concatenated DataFrame of all chunks, or None if no data.
    """
    data_all_append = []
    for start, end in zip(date_list, date_list[1:]):
        data_station = fetch_chs_station(
            station_id=str(id_number),
            time_series_code=time_series_code,
            start_date=datetime.strftime(start, '%Y-%m-%d'),
            end_date=datetime.strftime(end, '%Y-%m-%d'),
        )
        if 'errors' in data_station.columns or data_station.empty:
            continue
        data_all_append.append(data_station)

    if data_all_append:
        return pd.concat(data_all_append, ignore_index=True)
    return None


def _filter_qc(data_all):
    """Filter out records with rejected/suspect QC flags."""
    if 'qcFlagCode' in data_all.columns:
        data_all = data_all[
            data_all['qcFlagCode'].isin(_ACCEPTED_QC_CODES)]
    return data_all


def _format_raw_data(data_all):
    """Filter QC flags and format raw CHS API response into standard columns."""
    data_all = _filter_qc(data_all)
    data_all['DateTime'] = pd.to_datetime(
        data_all['eventDate'], format='%Y-%m-%dT%H:%M:%SZ')
    drop_cols = [c for c in ['eventDate', 'qcFlagCode',
                             'timeSeriesId', 'reviewed']
                 if c in data_all.columns]
    data_all = data_all.drop(columns=drop_cols)
    data_all.rename(columns={'value': 'OBS'}, inplace=True)
    data_all.drop_duplicates(subset=['DateTime'], keep='first',
                             inplace=True)
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
        data_all = _fetch_chs_chunked(date_list, id_number, code)
        if data_all is not None:
            logger.info('CHS %s data found using code %s for station %s',
                        variable, code, str(id_number))
            break

    if data_all is None:
        return None

    data_all = _format_raw_data(data_all)
    data_all['DEP01'] = 0.0
    if variable == 'water_level':
        data_all['Datum'] = 'IGLD'

    return data_all


def _retrieve_chs_currents(date_list, id_number, logger):
    """
    Retrieve CHS current speed and direction, merge into single DataFrame.

    Tries matched sensor pairs (wcs1+wcd1, then wcs2+wcd2) to avoid
    mixing sensors at different depths. Falls back across pairs only if
    a matched pair is not available.
    """
    # Try matched sensor pairs first (same sensor number for speed+dir)
    for speed_code, dir_code in _CURRENT_SENSOR_PAIRS:
        speed_data = _fetch_chs_chunked(date_list, id_number, speed_code)
        dir_data = _fetch_chs_chunked(date_list, id_number, dir_code)
        if speed_data is not None and dir_data is not None:
            logger.info('CHS currents found using matched pair %s/%s '
                        'for station %s',
                        speed_code, dir_code, str(id_number))
            break
    else:
        # No matched pair found
        logger.warning('CHS currents: no matched speed/direction sensor '
                       'pair found for station %s', str(id_number))
        return None

    # Apply QC filtering before merge
    speed_data = _filter_qc(speed_data)
    dir_data = _filter_qc(dir_data)

    if speed_data.empty or dir_data.empty:
        return None

    # Parse DateTimes for both
    for df in [speed_data, dir_data]:
        df['DateTime'] = pd.to_datetime(
            df['eventDate'], format='%Y-%m-%dT%H:%M:%SZ')

    speed_data = speed_data[['DateTime', 'value']].drop_duplicates(
        subset=['DateTime'], keep='first')
    dir_data = dir_data[['DateTime', 'value']].drop_duplicates(
        subset=['DateTime'], keep='first')

    merged = speed_data.merge(dir_data, on='DateTime',
                              suffixes=('_speed', '_dir'))
    merged.rename(columns={'value_speed': 'OBS', 'value_dir': 'DIR'},
                  inplace=True)
    merged['DEP01'] = 0.0

    return merged


def retrieve_chs_station(
    start_date: str,
    end_date: str,
    id_number: str,
    variable: str,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve CHS station data using SEARVEY library.

    This function fetches CHS data for a given station and time period.

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        id_number: CHS station ID
        variable: Variable to retrieve ('water_level',
                  'water_temperature', 'salinity', 'currents')
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - DateTime: Observation timestamps
            - DEP01: Depth (0.0 for all CHS observations)
            - OBS: Observation values
            - DIR: Direction (for currents only)
            - Datum: Vertical datum (for water_level only, 'IGLD')
        Returns None if no data available.
    """
    start_date_str = (start_date[:4] + '-' + start_date[4:6]
                      + '-' + start_date[6:])
    end_date_str = (end_date[:4] + '-' + end_date[4:6]
                    + '-' + end_date[6:])
    start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d')

    if (end_date_dt - start_date_dt).days > 7:
        date_list = _make_date_chunks(start_date_dt, end_date_dt, 7 * 24)
    else:
        date_list = [start_date_dt, end_date_dt]

    if variable == 'currents':
        data_all = _retrieve_chs_currents(date_list, id_number, logger)
    else:
        data_all = _retrieve_chs_scalar(
            date_list, id_number, variable, logger)

    if data_all is None:
        logger.error(
            'Retrieve CHS station %s failed for %s -- station contacted, '
            'but no data available.', str(id_number), variable)
    return data_all
