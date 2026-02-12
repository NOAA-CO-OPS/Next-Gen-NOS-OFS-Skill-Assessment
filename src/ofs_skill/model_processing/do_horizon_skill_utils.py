# -*- coding: utf-8 -*-
"""

Utility functions to support the forecast horizon skill option. Called by
do_horizon_skill and/or get_node_ofs, the functions are
described below and include:
    -pandas_merge
    -pandas_processing
    -get_forecast_hours
    -get_horizon_filenames

Created on Wed Jan 14 08:24:39 2026

@author: PWL
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def pandas_merge(filepath, df, datecycle, prop):
    '''
    Merges/appends a single model cycle time series dataframe to an existing
    dataframe containing all model cycle time series.
    Called by get_node_ofs.py.

    Parameters
    ----------
    filepath: path to existing dataframe with previously merged model cycles.
    df: dataframe of new model cycle series to be merged onto existing
    dataframe.
    datecycle: column name string with date and model cycle of series
    to be merged.
    logger : logging interface.

    Returns
    -------
    df: merged dataframe with existing & new model cycle series.

    '''
    # Existing dataframe with previously merged model cycle series
    prd = pd.read_csv(filepath)
    # Clean up existing dataframe if there are columns from a previous run
    desired_cols = prop.datecycles
    diff_cols = list(prd.columns.difference(desired_cols))
    cols_to_drop = [item for item in diff_cols if 'hr' in item]
    if cols_to_drop:
        prd.drop(columns=cols_to_drop, inplace=True)
    # Set datatypes of new model cycle series before merging
    df = df.astype({
        'julian': 'float',
        'year': 'int64',
        'month': 'int64',
        'day': 'int64',
        'hour': 'int64',
        'minute': 'int64',
        datecycle: 'float',
    })
    # Merge away, but avoid duplicates if files exist from a previous run!
    # This is especially relevant to server/cron runs!
    if datecycle in prd.columns:
        prd.drop(columns=datecycle, inplace=True)
    df = pd.merge(
        prd, df,
        on=[
            'julian',
            'year',
            'month',
            'day',
            'hour',
            'minute',
        ],
        how='outer',
    )

    return df


def pandas_processing(name_conventions, datecycle, formatted_series):
    '''
    Processes & parses model time series into pandas dataframes.
    Called by get_node_ofs.py.

    Parameters
    ----------
    name_conventions: variable name, e.g., wl, cu, salt, temp
    datecycle: column name string with date and model cycle of series
    to be merged.
    formatted_series: time series (list) that needs to be
    processed to pandas dataframe.
    logger : logging interface.

    Returns
    -------
    df: dataframe with model cycle time series -- the string assigned to
    'datecycle' is the series/column name.

    '''

    # Get date and forecast cycle
    for k in range(len(formatted_series)):
        formatted_series[k] = \
            formatted_series[k].replace('   ', ' ')
        formatted_series[k] = \
            formatted_series[k].replace('  ', ' ')
    df = pd.DataFrame(formatted_series)
    df.columns = ['temp']
    if name_conventions != 'cu':
        df[[
            'julian',
            'year',
            'month',
            'day',
            'hour',
            'minute',
            datecycle,
        ]] = df['temp'].str.split(
            ' ',
            n=6, expand=True,
        )
        columns_to_drop = ['temp']
        # df = df.replace(r'^\s*$', np.nan, regex=True)
    else:
        df[[
            'julian',
            'year',
            'month',
            'day',
            'hour',
            'minute',
            datecycle,
            'temp2',
            'temp3',
            'temp4',
        ]] = df['temp'].str.split(
            ' ',
            n=9, expand=True,
        )
        columns_to_drop = ['temp', 'temp2', 'temp3', 'temp4']
        # df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.drop(columns_to_drop, axis=1)
    return df


def get_forecast_hours(ofs):
    '''
    Just what the name says -- gets model forecast cycle hours and forecast
    length (max horizon) in hours.
    Called by do_horizon_skill_utils.get_horizon_filenames

    Parameters
    ----------
    ofs: string, model OFS
    logger: logging interface

    Returns
    -------
    fcstlength: max length of forecast in hours for OFS
    fcstcycles: list of forecast cycle hours for OFS

    '''

    # Need to know forecast cycle hours (e.g. 00Z) and forecast length (hours)
    if ofs in (
        'cbofs', 'dbofs', 'gomofs', 'ciofs', 'leofs', 'lmhofs', 'loofs',
        'lsofs', 'tbofs',
    ):
        fcstcycles = np.array([0, 6, 12, 18])
    elif ofs in ('creofs', 'ngofs2', 'sfbofs', 'sscofs'):
        fcstcycles = np.array([3, 9, 15, 21])
    elif ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        fcstcycles = 12
    else:
        fcstcycles = 3
    # Now need to know forecast length in hours
    if ofs in (
        'cbofs', 'ciofs', 'creofs', 'dbofs', 'ngofs2', 'sfbofs',
        'tbofs',
    ):
        fcstlength = 48
    elif ofs in ('gomofs', 'wcofs', 'sscofs'):
        fcstlength = 72
    elif ofs in ('stofs_3d_atl'):
        fcstcycles = 96
    elif ofs in ('stofs_3d_pac'):
        fcstcycles = 48
    else:
        fcstlength = 120

    return fcstlength, fcstcycles


def get_horizon_filenames(ofs, start_date, end_date, logger):
    '''
    This function is called by make_horizon_series. It figures out the file
    names that correspond to each model cycle received from do_horizon_skill.
    The file names are then each sent to get_node_ofs.py where they are lazily
    loaded and processed to model time series.

    Parameters
    -------
    ofs: model OFS
    start_date: datetime object of string prop.start_date_full
    end_date: datetime object of string prop.end_date_full
    logger: logging interface

    Returns
    -------
    unique_filenames: a list of unique filenames for each model cycle within
    the time range between start_date and end_date.
    '''

    # Now zoom backwards through time to find first available forecast cycle
    # for the input date
    if isinstance(start_date, datetime) and isinstance(end_date, datetime):
        startdatedt = start_date
        enddatedt = end_date
    else:
        logger.error('Incorrect date format in get_horizon_filenames!')
        sys.exit(-1)

    # Get OFS forecast length & cycle info
    fcstlength, fcstcycles = get_forecast_hours(ofs)

    dates_all = []
    fcst_horizons_all = []
    filenames_all = []
    cycles_all = []
    date_iterate = startdatedt
    while date_iterate <= enddatedt:
        datedt = date_iterate
        # Round down to nearest hour to find cycle where data point would
        # appear
        datedt = datedt.replace(minute=0, second=0, microsecond=0)
        d_0 = datedt - timedelta(hours=fcstlength)
        d_0hr = d_0.hour
        if not isinstance(fcstcycles, int):
            dist = np.concatenate(
                (fcstcycles, fcstcycles+24), axis=0)-int(d_0hr)
        else:
            dist = np.array([fcstcycles, fcstcycles+24]) - int(d_0hr)
        index = np.where(dist >= 0)
        base_forecast_date = d_0 + timedelta(hours=int(dist[index][0]))
        n_extra = 0
        if dist[index][0] == 0:
            n_extra = 1
        # Now find every cycle date between base date and input date
        ndates = int(len(np.atleast_1d(fcstcycles))*(fcstlength/24)) + n_extra
        d_t = int(24/len(np.atleast_1d(fcstcycles)))
        dates = []
        fcst_horizons = []
        filenames = []
        cycles = []
        for i in range(0, ndates):
            dt_i = d_t*i
            dates.append(base_forecast_date + timedelta(hours=dt_i))
            fcst_horizons.append(int((datedt-dates[i]).total_seconds()/3600))
            datestrlong = datetime.strftime(dates[i], '%Y-%m-%dT%H:%M:%SZ')
            datestr = datestrlong.split('T')[0].replace('-', '')
            cycle = datestrlong.split('T')[1][0:2]
            cycles.append(str(cycle))
            cast = 'forecast'
            if fcst_horizons[i] <= 0:
                cast = 'nowcast'
            filenames.append(
                ofs + '.t' + cycle.zfill(2) + 'z.' + datestr +
                '.stations.' + cast + '.nc',
            )
        date_iterate += timedelta(hours=1)
        dates_all.append(dates)
        fcst_horizons_all.append(fcst_horizons)
        filenames_all.append(filenames)
        cycles_all.append(cycles)
    # Get unique filenames & cycles
    flat_list = [item for sublist in filenames_all for item in sublist]
    unique_filenames = list(set(flat_list))
    return unique_filenames
