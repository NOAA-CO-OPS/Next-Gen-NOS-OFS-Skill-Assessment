"""

Utility functions to support the forecast horizon skill option. Called by
do_horizon_skill and/or get_node_ofs, the functions are
described below and include:
    -pandas_merge
    -pandas_processing
    -get_horizon_filenames

Created on Wed Jan 14 08:24:39 2026

@author: PWL
"""

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from ofs_skill.model_processing import (
    get_fcst_cycle,
)


def _coerce_cycle_dtypes(df, datecycle):
    """Set the standard dtypes on a single-cycle model series dataframe."""
    return df.astype(
        {
            'julian': 'float',
            'year': 'int64',
            'month': 'int64',
            'day': 'int64',
            'hour': 'int64',
            'minute': 'int64',
            datecycle: 'float',
        }
    )


# Time-axis columns shared by every model-cycle series. Used as the merge key
# when combining accumulated cycles into a single wide dataframe.
_TIME_KEYS = ['year', 'month', 'day', 'hour', 'minute']

# Guards the in-memory horizon accumulator, since station/variable extraction
# in get_node_ofs may run in parallel threads that all append into the same
# shared dict.
_ACCUMULATOR_LOCK = threading.Lock()


def pandas_merge(filepath, df, datecycle, prop):
    """
    Merges/appends a single model cycle time series dataframe to an existing
    dataframe containing all model cycle time series.
    Called by get_node_ofs.py.

    Performance note
    ----------------
    Historically this re-read the growing on-disk CSV and did a full outer
    merge on every single model cycle, which made the horizon workflow scale
    as O(cycles**2) on CSV I/O -- painful for week+ windows. When an in-memory
    accumulator is available on ``prop`` (see ``accumulate_cycle`` /
    ``flush_horizon_series``), get_node_ofs uses that instead and this
    disk-merge path is only a fallback for callers that still pass a filepath.

    Parameters
    ----------
    filepath: path to existing dataframe with previously merged model cycles.
    df: dataframe of new model cycle series to be merged onto existing
    dataframe.
    datecycle: column name string with date and model cycle of series
    to be merged.
    prop: ModelProperties object (``prop.datecycles`` lists the model
    cycle columns to keep from the existing dataframe).

    Returns
    -------
    df: merged dataframe with existing & new model cycle series, merged
    on the integer date-component columns; the julian column is taken
    from the new cycle series (rows only present in the existing
    dataframe carry NaN julian).

    """
    # Existing dataframe with previously merged model cycle series
    prd = pd.read_csv(filepath)
    # Clean up existing dataframe if there are columns from a previous run
    desired_cols = prop.datecycles
    diff_cols = list(prd.columns.difference(desired_cols))
    # Target 'forecast' columns instead of looking for 'hr'
    cols_to_drop = [item for item in diff_cols if 'forecast' in item]
    if cols_to_drop:
        prd.drop(columns=cols_to_drop, inplace=True)
    # Set datatypes of new model cycle series before merging
    df = _coerce_cycle_dtypes(df, datecycle)
    # Merge away, but avoid duplicates if files exist from a previous run!
    # This is especially relevant to server/cron runs!
    if datecycle in prd.columns:
        prd.drop(columns=datecycle, inplace=True)

    # Drop cached julian to prevent float-precision merge mismatches
    if 'julian' in prd.columns:
        prd.drop(columns='julian', inplace=True)

    df = pd.merge(
        prd,
        df,
        on=_TIME_KEYS,
        how='outer',
    )

    return df


def accumulate_cycle(prop, station_key, df, datecycle):
    """
    Stash a single model-cycle series in an in-memory accumulator on ``prop``
    instead of re-reading and re-merging the growing per-station CSV on every
    cycle. This keeps the horizon workflow scaling linearly with the number of
    model cycles rather than quadratically on disk I/O.

    The accumulator lives at ``prop._horizon_accumulator`` as a dict keyed by
    ``station_key`` -> list of (datecycle, single-cycle dataframe). It is
    flushed to CSV by ``flush_horizon_series`` once all cycles are loaded.

    Parameters
    ----------
    prop : model properties object (carried across all cycle iterations).
    station_key : unique key for the station/variable CSV (its filename).
    df : single-cycle model series dataframe (time keys + the datecycle col).
    datecycle : column name for this cycle's series.
    """
    df = _coerce_cycle_dtypes(df, datecycle)
    with _ACCUMULATOR_LOCK:
        store = getattr(prop, '_horizon_accumulator', None)
        if store is None:
            store = {}
            prop._horizon_accumulator = store
        # Guard against a duplicate cycle appearing twice (e.g. resumed runs).
        cycles = store.setdefault(station_key, [])
        cycles[:] = [(dc, cdf) for (dc, cdf) in cycles if dc != datecycle]
        cycles.append((datecycle, df))


def flush_horizon_series(prop, logger=None):
    """
    Merge all accumulated per-station model-cycle series into a single wide
    dataframe each and write them to CSV. This performs one merge/write pass
    per station at the end of the cycle loop, replacing the previous
    per-cycle read-merge-write cycle.

    If a CSV already exists on disk (e.g. a resumed/cron run), its cycle
    columns are folded in so nothing already computed is lost.

    Parameters
    ----------
    prop : model properties object holding ``_horizon_accumulator`` and
        ``data_horizon_1d_node_path``.
    logger : optional logging interface.

    Returns
    -------
    int : number of station CSVs written.
    """
    store = getattr(prop, '_horizon_accumulator', None)
    if not store:
        return 0

    written = 0
    for filename, cycles in store.items():
        if not cycles:
            continue
        # Build the wide dataframe by merging each cycle on the shared time
        # keys. Start from the first cycle and outer-merge the rest.
        base_dc, wide = cycles[0]
        for datecycle, cdf in cycles[1:]:
            # Drop julian from subsequent cycles to prevent _x / _y suffixes
            if 'julian' in cdf.columns:
                cdf = cdf.drop(columns='julian')
            wide = pd.merge(wide, cdf, on=_TIME_KEYS, how='outer')

        filepath = os.path.join(prop.data_horizon_1d_node_path, filename)
        # Fold in any pre-existing on-disk cycles from a prior run so we do
        # not clobber previously computed horizons.
        if os.path.isfile(filepath):
            try:
                prd = pd.read_csv(filepath)
                desired_cols = getattr(prop, 'datecycles', [])

                # Change 'hr' to 'forecast' here:
                stale = [c for c in prd.columns.difference(desired_cols) if 'forecast' in c]

                if stale:
                    prd = prd.drop(columns=stale)
                    # Drop any cycle columns we just recomputed to avoid dupes.
                new_cols = [dc for (dc, _) in cycles]
                overlap = [c for c in new_cols if c in prd.columns]
                if overlap:
                    prd = prd.drop(columns=overlap)

                # Drop cached julian to prevent float-precision merge mismatches
                if 'julian' in prd.columns:
                    prd = prd.drop(columns='julian')

                # Only merge if the old file still has meaningful cycle cols.
                extra_cols = [c for c in prd.columns if c not in _TIME_KEYS and 'forecast' in c]
                if extra_cols:
                    wide = pd.merge(wide, prd[_TIME_KEYS + extra_cols], on=_TIME_KEYS, how='outer')
            except Exception as e_x:  # pylint: disable=broad-except
                if logger is not None:
                    logger.warning(
                        'Could not fold existing horizon CSV %s into '
                        'accumulator; overwriting. Error: %s',
                        filepath,
                        e_x,
                    )

        try:
            wide.to_csv(filepath, index=False)
            written += 1
        except Exception as e_x:  # pylint: disable=broad-except
            if logger is not None:
                logger.error(
                    "Couldn't save accumulated forecast horizons to %s! " 'Error: %s', filepath, e_x
                )

    # Clear the accumulator so a subsequent run in the same process starts
    # fresh.
    prop._horizon_accumulator = {}
    return written


def pandas_processing(name_conventions, datecycle, formatted_series):
    """
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

    """

    # Get date and forecast cycle
    for k in range(len(formatted_series)):
        formatted_series[k] = formatted_series[k].replace('   ', ' ')
        formatted_series[k] = formatted_series[k].replace('  ', ' ')
    df = pd.DataFrame(formatted_series)
    df.columns = ['temp']
    if name_conventions != 'cu':
        df[
            [
                'julian',
                'year',
                'month',
                'day',
                'hour',
                'minute',
                datecycle,
            ]
        ] = df['temp'].str.split(
            ' ',
            n=6,
            expand=True,
        )
        columns_to_drop = ['temp']
        # df = df.replace(r'^\s*$', np.nan, regex=True)
    else:
        df[
            [
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
            ]
        ] = df['temp'].str.split(
            ' ',
            n=9,
            expand=True,
        )
        columns_to_drop = ['temp', 'temp2', 'temp3', 'temp4']
        # df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.drop(columns_to_drop, axis=1)
    return df


def get_horizon_filenames(ofs, start_date, end_date, logger):
    """
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
    """

    # Now zoom backwards through time to find first available forecast cycle
    # for the input date
    if isinstance(start_date, datetime) and isinstance(end_date, datetime):
        startdatedt = start_date
        enddatedt = end_date
    else:
        logger.error('Incorrect date format in get_horizon_filenames!')
        sys.exit(-1)

    # Get OFS forecast length & cycle info
    fcstlength, fcstcycles = get_fcst_cycle.get_fcst_hours(ofs)

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
            dist = np.concatenate((fcstcycles, fcstcycles + 24), axis=0) - int(d_0hr)
        else:
            dist = np.array([fcstcycles, fcstcycles + 24]) - int(d_0hr)
        index = np.where(dist >= 0)
        base_forecast_date = d_0 + timedelta(hours=int(dist[index][0]))
        n_extra = 0
        if dist[index][0] == 0:
            n_extra = 1
        # Now find every cycle date between base date and input date
        ndates = int(len(np.atleast_1d(fcstcycles)) * (fcstlength / 24)) + n_extra
        d_t = int(24 / len(np.atleast_1d(fcstcycles)))
        dates = []
        fcst_horizons = []
        filenames = []
        cycles = []
        for i in range(0, ndates):
            dt_i = d_t * i
            dates.append(base_forecast_date + timedelta(hours=dt_i))
            fcst_horizons.append(int((datedt - dates[i]).total_seconds() / 3600))
            datestrlong = datetime.strftime(dates[i], '%Y-%m-%dT%H:%M:%SZ')
            datestr = datestrlong.split('T')[0].replace('-', '')
            cycle = datestrlong.split('T')[1][0:2]
            cycles.append(str(cycle))
            cast = 'forecast'
            if fcst_horizons[i] <= 0:
                cast = 'nowcast'
            filenames.append(
                ofs + '.t' + cycle.zfill(2) + 'z.' + datestr + '.stations.' + cast + '.nc',
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
