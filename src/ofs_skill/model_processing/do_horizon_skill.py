"""
Created on Thu Apr 3 10:11:14 2025

Main module for fetching and aggregating all model cycle time series that
overlap with the user-defined date range, and then merging cycle series with
observation time series. Please see each routine for detailed info.

@author: PWL
Last updated: 8/12/25
"""
from __future__ import annotations

import copy
import os
import sys
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from ofs_skill.model_processing.get_node_ofs import get_node_ofs
from ofs_skill.model_processing import do_horizon_skill_utils
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.skill_assessment.get_skill import ofs_ctlfile_extract, name_convent
from ofs_skill.visualization import plot_forecast_hours

def make_horizon_series(prop, logger):
    '''
    This function repeatedly calls get_node_ofs.py to load and write each model
    forecast cycle within the date range. It calls the get_horizon_filenames
    function to return a list of filenames/model cycles to load. This function
    returns to get_skill.py when it is complete.

    Parameters
    ----------
    prop: model properties object containing date range, etc.
    logger: logging interface

    Returns
    -------
    NOTHING. Calls get_node_ofs.py which writes CSVs to file.

    '''
    # First make dummy copy of prop to manipulate in here
    prop11 = copy.deepcopy(prop)

    # First get all hours & dates between start and end dates
    try:
        start_datedt = datetime.strptime(
            prop.start_date_full, '%Y%m%d-%H:%M:%S',
        )
        end_datedt = datetime.strptime(
            prop.end_date_full, '%Y%m%d-%H:%M:%S',
        )
    except ValueError:
        try:
            start_datedt = datetime.strptime(
                prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ',
            )
            end_datedt = datetime.strptime(
                prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ',
            )
        except ValueError:
            logger.error('Incorrect date format!')
            sys.exit(-1)
    filenames = do_horizon_skill_utils.get_horizon_filenames(
        prop.ofs, start_datedt, end_datedt,
        logger,
    )

    # Make list of datecycle column headers
    datecycles = []
    for i, filename in enumerate(filenames):
        datecycles.append(
            filename.split('.')[2] + '-' +
            filename.split('.')[1][1:3] + 'hr-forecast',
        )

    # Assign relevant things to prop11
    prop11.datecycles = datecycles
    prop11.whichcast = 'forecast_a'
    fcstlength, _ = do_horizon_skill_utils.get_forecast_hours(prop.ofs)
    for i, filename in enumerate(filenames):
        if 'nowcast' in str(filename.split('.')):
            continue  # Skip any nowcasts
        start_date = filename.split('.')[2]
        prop11.start_date_full = \
            start_date[0:4] + '-' + start_date[4:6] + '-' +\
            start_date[6:8] + 'T00:00:00Z'
        prop11.end_date_full = datetime.strftime(
            (
                datetime.strptime(prop11.start_date_full,
                                  '%Y-%m-%dT%H:%M:%SZ') +
                timedelta(hours=fcstlength)
            ), '%Y-%m-%dT%H:%M:%SZ',
        )
        prop11.forecast_hr = filename.split('.')[1][1:3]+'hr'
        try:
            logger.info(
                'Running get_node for %s...',
                str(filename),
            )
            # Call get node
            get_node_ofs(prop11, logger)
            logger.info(
                'Forecast cycles are %s percent '
                'complete!\n',
                str(np.round(((i+1)/len(filenames))*100, decimals=2)),
            )
        except Exception as e_x:
            logger.error(
                'Error making horizon series! '
                'Passing to next horizon. '
                'Error: %s', e_x,
            )

    logger.info(
        'Done loading and saving model forecast horizon series! '
        'Starting observation pairing to model horizons...',
    )


def merge_obs_series_scalar(prop, logger):
    '''
    Hefty function here that pairs (merges) observation data for each station
    to the existing dataframe of all model cycle time series for the same
    station. Most syntax was adapted from
    /bin/skill_assessment/format_paired_one_d.py, and in the future we could
    probably just call format_paired_one_d.py and remove this function.
    This function calls horizon_skill & make_flag_images.

    Parameters
    ----------
    prop: model properties object containing date range, etc.
    logger: logging interface

    Returns
    -------
    NOTHING.
    Writes CSV to file after observations are paired with model cycle time
    series.
    Calls horizon_skill & make_flag_images which write plots and/or tables.

    '''
    doextraplots = False  # First decide if you want to make plots of all model
    # cycle time series, and scorecard plots

    # Try converting start/end dates to datetime
    try:
        datetime_start = datetime.strptime(
            prop.start_date_full,
            '%Y-%m-%dT%H:%M:%SZ',
        )
        datetime_end = datetime.strptime(
            prop.end_date_full,
            '%Y-%m-%dT%H:%M:%SZ',
        )
    except ValueError:
        logger.error(
            'Wrong date format in merge_obs for %s! Trying '
            'different format...', prop.start_date_full,
        )
        try:
            datetime_start = datetime.strptime(
                prop.start_date_full,
                '%Y%m%d-%H:%M:%S',
            )
            datetime_end = datetime.strptime(
                prop.end_date_full,
                '%Y%m%d-%H:%M:%S',
            )
            logger.info(
                '...correct date format in merge_obs for %s!',
                prop.start_date_full,
            )
        except ValueError:
            logger.error(
                'Wrong date format in get_skill for %s! Bye.',
                prop.start_date_full,
            )
            sys.exit(-1)
    # Loop through variables
    for variable in prop.var_list:
        # Read ctl files
        name_var = name_convent(variable)
        ctl_path = os.path.join(
            prop.control_files_path, str(
                prop.ofs+'_' +
                name_var+'_station.ctl',
            ),
        )
        read_station_ctl_file = \
            station_ctl_file_extract(ctl_path)
        ctl_path = os.path.join(prop.control_files_path,
                            str(prop.ofs+'_'+name_var+'_model_station.ctl'))
        if prop.ofsfiletype == 'fields':
            ctl_path = os.path.join(prop.control_files_path,
                                str(prop.ofs+'_'+name_var+'_model.ctl'))
        if read_station_ctl_file is None or os.path.isfile(ctl_path) is False:
            continue

        read_ofs_ctl_file = ofs_ctlfile_extract(
            prop, name_var, logger,)
        if read_ofs_ctl_file is not None:
            # Loop through stations
            for i in range(0, len(read_ofs_ctl_file[4])):
                # Now pair/merge the obs time series with the horizons
                obs_path = os.path.join(
                    prop.data_observations_1d_station_path,
                    str(
                        read_ofs_ctl_file[4][i]+'_'+prop.ofs+'_'+name_var +
                        '_station.obs',
                    ),
                )
                # Open .obs file
                if os.path.isfile(obs_path):
                    if os.path.getsize(obs_path) > 0:
                        obs_df = pd.read_csv(
                            obs_path,
                            delim_whitespace=True,
                            header=None,
                        )
                    else:
                        logger.error(
                            '%s/%s_%s_%s_station.obs is empty',
                            prop.data_observations_1d_station_path,
                            read_ofs_ctl_file[4][i],
                            prop.ofs,
                            name_var,
                        )
                # Open CSV file with model time series
                filename = (
                    f'{prop.ofs}_{read_ofs_ctl_file[4][i]}_'
                    f'{name_var}_fcst_horizons.csv'
                )
                filepath = os.path.join(
                    prop.data_horizon_1d_node_path,
                    filename,
                )
                if os.path.isfile(filepath):
                    try:
                        ofs_df = pd.read_csv(filepath)
                        ofs_df['DateTime'] = pd.to_datetime(
                            ofs_df[['year', 'month', 'day', 'hour', 'minute']],
                        )
                        paired_0 = pd.DataFrame()
                        paired_0['DateTime'] = ofs_df['DateTime']
                        if 'OBS' in ofs_df.columns:
                            ofs_df = ofs_df.drop('OBS', axis=1)
                        # Prep obs series first
                        # Reading the input dataframes
                        obs_df['DateTime'] = pd.to_datetime(
                            dict(
                                year=obs_df[1],
                                month=obs_df[2],
                                day=obs_df[3],
                                hour=obs_df[4],
                                minute=obs_df[5],
                            ),
                        )
                        obs_df = obs_df.rename(columns={6: 'OBS'})
                        obs_df = obs_df.sort_values(by='DateTime')
                        obs_df = pd.concat([paired_0, obs_df]).sort_values(
                            by='DateTime',
                        )
                        obs_df = obs_df[
                            ~obs_df['DateTime'].duplicated(keep=False)
                            | obs_df[['OBS']].notnull().any(axis=1)
                        ]
                        obs_df = (
                            obs_df.sort_values(by='DateTime')
                            .set_index('DateTime')
                            .astype(float)
                            .interpolate(method='linear', limit=3)
                            .ffill(limit=1)
                            .bfill(limit=1)
                            .reset_index()
                        )
                    except Exception as e_x:
                        logger.error(
                            'Error prepping obs series for merging to '
                            'forecast horizon CSV! Error: %s', e_x,
                        )
                    try:
                        ofs_df = ofs_df.sort_values(by='DateTime')
                        ofs_df = (
                            ofs_df.sort_values(by='DateTime')
                            .set_index('DateTime')
                            # .interpolate(method="linear",limit=3)
                            # .ffill(limit=3)
                            # .bfill(limit=3)
                            .astype(float)
                            .reset_index()
                        )
                    except Exception as e_x:
                        logger.error(
                            'Error prepping model series for merging to '
                            'forecast horizon CSV! Error: %s', e_x,
                        )
                    try:
                        paired = pd.merge(
                            ofs_df,
                            obs_df[['DateTime', 'OBS']],
                            on=['DateTime'],
                            how='left',
                        )
                        paired = paired.loc[(
                            paired['DateTime'] >=
                            datetime_start
                        )
                            & (paired['DateTime'] <= datetime_end)]
                        paired['OBS'] = paired['OBS'].fillna(np.nan)
                        paired.to_csv(filepath, index=False)
                    except Exception as e_x:
                        logger.error(
                            'Error during obs-model merge in horizon '
                            'series! Error: %s', e_x,
                        )
                    # Do stats
                    try:
                        obs_row = [y[0] for y in read_station_ctl_file[0]].\
                            index(read_ofs_ctl_file[4][i])
                        if read_station_ctl_file[0][obs_row][0] != \
                                read_ofs_ctl_file[4][i]:
                            raise Exception
                    except Exception:
                        logger.error(
                            'Could not match station ID %s between '
                            'control file in get_node_ofs!',
                            read_ofs_ctl_file[-1][i],
                        )
                        sys.exit(-1)
                    info = [
                        name_var,  # variable
                        read_ofs_ctl_file[1][i],  # node
                        read_ofs_ctl_file[-1][i],  # station id
                        read_station_ctl_file[0][obs_row][2],  # station name
                        read_station_ctl_file[0][obs_row][1].
                        split('_')[-1],  # station owner
                        read_ofs_ctl_file[-1],
                        doextraplots,
                        variable,
                    ]  # full variable name
                    horizon_skill(prop, paired, info, logger)
                else:
                    logger.info(
                        'There is no forecast horizon CSV file for obs '
                        'station %s.', read_station_ctl_file[0][i][0],
                    )
                    continue
        logger.info(
            'Forecast horizon skill: completed merging obs & model '
            'cycles and all plotting for %s.', variable,
        )
    # Finally make the flag images/matrices
    if doextraplots:
        plot_forecast_hours.make_flag_images(prop, logger)


def horizon_skill(prop, df, info, logger):
    '''
    Here we reshape the massive dataframe that has paired observations and
    model cycle time series for each station, and add 6-hour forecast horizon
    bins. The forecast cycles are extracted from the model cycle column
    names, and sorted as input to the plotting functions. The dataframe is
    reshaped to facilitate plotting and statistics calculations. The reshaped
    dataframe can be saved by uncommenting the section called
    'Save df_all to CSV'.
    This function is called by merge_obs_series_scalar, and calls:
        plot_forecast_hours.make_horizonbin_plots
        plot_forecast_hours.make_horizonbin_freq_plots
        plot_forecast_hours.make_timeseries_plots

    PARAMETERS
    ----------
    prop: model properties object containing date range, etc.
    df: pandas dataframe for an individual station that has each model cycle
    time series paired with observation time series
    info: list of station info -->
        info[0] = variable
        info[1] = model
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
        info[5] = list of all station IDs
        info[6] = 'doextraplots' boolean True/False
        info[7] = full variable name
    logger: logging interface

    Returns
    -------
    NOTHING.
    Calls plotting functions that write plots to file.
    '''

    # Get number of columns that have horizon series
    fcstlength, _ = do_horizon_skill_utils.get_forecast_hours(prop.ofs)
    forecast_cols = [col for col in df.columns if 'forecast' in col]
    bins = np.arange(0, fcstlength+6, 6)
    for fcst_col in forecast_cols:
        col_time = fcst_col.split('-')[0] + '-' + \
            fcst_col.split('-')[1][0:2] + ':00:00'
        time_diff = (
            df['DateTime'] - datetime.strptime(
                col_time, '%Y%m%d-%H:%M:%S',
            )
        )
        df['horizon_category'] = np.floor(
            time_diff.dt.total_seconds()/3600,
        ).astype('int')
        df['hour_bins'] = df['horizon_category']
        for j in range(len(bins)-1):
            df.loc[
                (df['horizon_category'] >= bins[j]) &
                (df['horizon_category'] < bins[j+1]), 'hour_bins',
            ] = \
                bins[j+1]
        df['model_cycle'] = fcst_col
        # Make new dataframe with model, obs, and horizon category
        df_temp = df[[
            'DateTime', 'model_cycle', 'horizon_category', 'hour_bins',
            'OBS', fcst_col,
        ]]
        df_temp = df_temp.rename(columns={fcst_col: 'OFS'})
        if fcst_col == forecast_cols[0]:
            df_all = df_temp
        else:
            df_all = pd.concat([df_all, df_temp])

    # Now bin errors by hour after loop is finished
    df_all = df_all[(df_all['horizon_category'] <= fcstlength) &
                    (df_all['horizon_category'] >= 0)]
    df_all['error'] = df_all['OFS'] - df_all['OBS']
    df_all['square_error'] = (df_all['OFS'] - df_all['OBS'])**2

    # Save df_all to CSV
    # filename=f"{prop.ofs}_{info[0]}_{info[2]}_fcsthorizons_pair.csv"
    # df_all.to_csv(os.path.join(prop.data_horizon_1d_pair_path,filename),
    #               index=False)

    # Sort the model cycles by date
    forecast_cols_dt = []
    # First get all column names & format them
    for fcst_col in forecast_cols:
        forecast_cols_dt.append(
            datetime.strptime(
                fcst_col.split('-')[0] +
                fcst_col.split('-')[1][0:2],
                '%Y%m%d%H',
            ),
        )
    # Now combine lists with original indices and sort
    combined_list = list(enumerate(zip(forecast_cols_dt, forecast_cols)))
    sorted_combined_list = sorted(combined_list, key=lambda item: item[1][0])
    forecast_cols_sort = [item[1][1] for item in sorted_combined_list]

    # Let's plot
    plot_forecast_hours.make_horizonbin_plots(df_all, info, prop, logger)
    plot_forecast_hours.make_horizonbin_freq_plots(df_all, info, prop, logger)
    if info[6]:
        plot_forecast_hours.make_timeseries_plots(
            df_all, forecast_cols_sort,
            info, prop, logger,
        )
