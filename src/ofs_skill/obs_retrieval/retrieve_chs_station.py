"""
Retrieve CHS (Canadian Hydrographic Service) station observations.

This module uses SEARVEY to retrieve all CHS water level time series.

@author: PWL
Created on Wed Feb  4 19:51:12 2026
"""

import time
from datetime import datetime, timedelta
from logging import Logger
from typing import Optional

import pandas as pd
from searvey._chs_api import fetch_chs_station


def make_datetime_list(start_date, end_date, interval_hours):
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


def retrieve_chs_station(
    start_date: str,
    end_date: str,
    id_number: str,
    variable: str,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve CHS station data using SEARVEY library.

    This function fetches all CHS data for a given station and time period.

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        id_number: CHS station ID
        variable: Variable to retrieve -- water level only!
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - DateTime: Observation timestamps
            - DEP01: Depth (for currents/temperature/salinity)
            - OBS: Observation values
            - DIR: Direction (for currents only)
            - Datum: Vertical datum (for water_level only)
        Returns None if no data available.

    Note:
        - Water level returned in LWD datum
    """
    data_station = []
    start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
    end_date = end_date[:4] + '-' + end_date[4:6] + '-' + end_date[6:]
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    if (end_date_dt - start_date_dt).days > 7:
        # Chunk the time
        date_list = make_datetime_list(start_date_dt, end_date_dt, 7*24)
    else:
        date_list = [start_date_dt, end_date_dt]
    data_all_append = []
    for i in range(len(date_list)-1):
        data_station = fetch_chs_station(
            station_id=str(id_number),
            time_series_code='wlo',
            start_date=datetime.strftime(date_list[i],'%Y-%m-%d'),
            end_date=datetime.strftime(date_list[i+1],'%Y-%m-%d'),
            )
        if 'errors' in data_station.columns or data_station.empty is True:
            continue
        data_all_append.append(data_station)
        time.sleep(0.33)

    if len(data_all_append) > 0:
        data_all = pd.concat(data_all_append, ignore_index=True)
        # Do a bunch of formatting
        data_all['DateTime'] = pd.to_datetime(data_all['eventDate'],
                                              format='%Y-%m-%dT%H:%M:%SZ')
        data_all = data_all.drop(columns=['eventDate',
                                          'qcFlagCode',
                                          'timeSeriesId',
                                          'reviewed'])
        data_all.rename(columns={'value': 'OBS'},
                        inplace=True)
        data_all['DEP01'] = 0.0
        data_all['Datum'] = 'IGLD'
        data_all.drop_duplicates(subset=['DateTime'], keep='first',
                                 inplace=True)
    else:
        data_all = None
    if data_all is None:
        logger.error(
            'Retrieve CHS station %s failed for %s -- station contacted, '
            'but no data available.', str(id_number), variable)
    return data_all
