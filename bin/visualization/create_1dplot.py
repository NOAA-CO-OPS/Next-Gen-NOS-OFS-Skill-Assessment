"""
-*- coding: utf-8 -*-

Documentation for Scripts create_1dplot.py

Script Name: create_1dplot.py

Technical Contact(s): Name:  FC

Abstract:

   This module is used to create all 1D plots.
   The main function (create_1dplot) controls the iterations over all variables
   and paired datasets (station+node).
   This script uses the ofs control file to list all the paired datasets
   if the paired dataset (or ofs control file) is not found, create_1dplot calls
   the respective modules (ofs and/or skill assessment modules) to create the
   missing file.

Language:  Python 3.8

Estimated Execution Time: < 10sec

Scripts/Programs Called:
 get_skill(start_date_full, end_date_full, datum, path, ofs, whichcast)
 --- This is called in case the paired dataset is not found

 get_node_ofs(start_date_full,end_date_full,path,ofs,whichcast,*args)
 --- This is called in case the ofs control file is not found

Usage: python create_1dplot.py

Arguments:
 -h, --help            show this help message and exit
 -o ofs, --ofs OFS     Choose from the list on the ofs_extents/ folder, you
                       can also create your own shapefile, add it top the
                       ofs_extents/ folder and call it here
 -p Path, --path PATH  Inventary File Path
 -s StartDate_full, --StartDate STARTDATE
                       Start Date
 -e EndDate_full, --EndDate ENDDATE
                       End Date
 -d StartDate_full, --datum",
                      'MHHW', 'MHW', 'MLW', 'MLLW','NAVD88','IGLD85','LWD'",
 -ws whichcast, --Whichcast"
                       'Nowcast', 'Forecast_A', 'Forecast_B'
 -so stationowner, --Station_Owner" [optional]
                       'NDBC', 'CO-OPS', 'USGS', 'CHS'
Output:
Output:
Name                 Description
scalar_plot          Standard html scalar timeseries plot of obs and ofs
vector_plot          Standard html vector timeseries plot of obs and ofs
wind_rose            Standard html polar wind rose plot of obs and ofs

Author Name:  FC       Creation Date:  09/20/2023

Revisions:
Date          Author     Description
05/9/2025    AJK        Moving plotting routines to their own files.

Remarks:
"""

import argparse
import copy
import gc
import logging
import logging.config
import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from ofs_skill.model_processing import (
    check_model_files,
    get_fcst_dates,
    get_fcst_hours,
    model_properties,
    parse_ofs_ctlfile,
    read_vdatum_from_bucket,
)
from ofs_skill.obs_retrieval import parse_arguments_to_list, utils
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.obs_retrieval.utils import get_parallel_config
from ofs_skill.skill_assessment.get_skill import get_skill
from ofs_skill.visualization import create_gui, plotting_scalar, plotting_vector

warnings.filterwarnings('ignore')

def parameter_validation(prop, logger):
    """ Parameter validation """

def ofs_ctlfile_read(prop, name_var, logger):
    '''
    This reads the OFS control file for a given ofs and variable.
    If not found, it calls the OFS module to create the control file.
    '''
    logger.info(
        f'Trying to extract {prop.ofs} control file for {name_var} from {prop.control_files_path}'
    )

    filename = None
    if prop.ofsfiletype == 'fields':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl'
    elif prop.ofsfiletype == 'stations':
        filename = f'{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl'
    else:
        logger.error('Invalid OFS file type.')
        return None

    if not os.path.isfile(filename):
        for i in prop.whichcasts:
            prop.whichcast = i.lower()
            logger.info(f'Running scripts for whichcast = {i}')

            if prop.start_date_full.find('T') == -1:
                prop.start_date_full = prop.start_date_full_before
                prop.end_date_full = prop.end_date_full_before

            get_skill(prop, logger)

    # If file exists, use method A to parse it
    if os.path.isfile(filename):
        if os.path.getsize(filename):
            return parse_ofs_ctlfile(filename)
        else:
            logger.info('%s model ctl file is blank!', name_var)
            logger.info('For GLOFS, salt and cu ctl files may be blank. '
                        'If running with a single station provider/owner, '
                        'ctl files may also be blank.')
    logger.info(f'Not able to extract/create {prop.ofs} control file for \
    {name_var} from {prop.control_files_path}')
    return None

def _process_station_plot(
        i, read_ofs_ctl_file, read_station_ctl_file, prop, var_info, logger):
    """
    Process a single station's plots. Designed to run inside a
    ThreadPoolExecutor.  Returns the station ID on success, None on failure.

    A shallow copy of ``prop`` is used so that ``prop.whichcast`` can be
    set per-cast without racing against other threads.
    """
    station_prop = copy.deepcopy(prop)
    station_id_val = read_ofs_ctl_file[-1][i]

    try:
        obs_row = [y[0] for y in read_station_ctl_file[0]].index(
            station_id_val)
        if read_station_ctl_file[0][obs_row][0] != station_id_val:
            raise ValueError('Station ID mismatch')
    except (ValueError, IndexError):
        logger.error('Could not match station ID %s between control '
                     'file in get_node_ofs!', station_id_val)
        return None

    now_fores_paired = []
    deltat = 0
    for cast in station_prop.whichcasts:
        paired_data = None
        current_cast = cast.lower()
        station_prop.whichcast = current_cast

        pair_file = (
            f'{station_prop.data_skill_1d_pair_path}/'
            f'{station_prop.ofs}_{var_info[1]}_{station_id_val}_'
            f'{read_ofs_ctl_file[1][i]}_{current_cast}_'
            f'{station_prop.ofsfiletype}_pair.int'
        )

        if not os.path.isfile(pair_file):
            logger.error(
                'Paired dataset (%s_%s_%s_%s_%s_%s_pair.int) not found '
                'in %s. ',
                station_prop.ofs, var_info[1], station_id_val,
                read_ofs_ctl_file[1][i], current_cast,
                station_prop.ofsfiletype,
                station_prop.visuals_1d_station_path)
        else:
            paired_data = pd.read_csv(
                pair_file,
                sep=r'\s+', names=var_info[2],
                header=0)
            # Format paired data dates
            paired_data['DateTime'] = pd.to_datetime(
                paired_data[['year', 'month', 'day', 'hour', 'minute']])
            # Read time series key
            filename = (
                f'{station_prop.ofs}_{current_cast}_filename_key.csv')
            filepath = (
                Path(station_prop.data_model_1d_node_path) / filename
            ).as_posix()
            try:
                serieskey = pd.read_csv(filepath)
                serieskey['DateTime'] = pd.to_datetime(
                    serieskey['DateTime'])
                paired_data = pd.merge(
                    paired_data, serieskey, on='DateTime', how='inner')
            except FileNotFoundError:
                logger.error(
                    'No model series filename key found! Skipping')
            except Exception as ex:
                logger.error(
                    'Exception caught when loading and merging '
                    'model filename key! Error: %s', ex)
            logger.info(
                'Paired dataset (%s_%s_%s_%s_%s_%s_pair.int) found '
                'in %s',
                station_prop.ofs, var_info[1], station_id_val,
                read_ofs_ctl_file[1][i], current_cast,
                station_prop.ofsfiletype,
                station_prop.visuals_1d_station_path)
        if paired_data is not None:
            # Subsample time series if using 6-minute resolution
            deltat = (paired_data['DateTime'].iloc[-1]
                      - paired_data['DateTime'].iloc[0]).days
            if (station_prop.ofsfiletype == 'stations'
                    and deltat > 185):
                paired_data = paired_data.loc[
                    paired_data.groupby(
                        ['year', 'month', 'day', 'hour'],
                        observed=True)['minute'].idxmin()]
            now_fores_paired.append(paired_data)

    if len(now_fores_paired) > 0:
        try:
            if var_info[1] in ('wl', 'temp', 'salt'):
                logger.info(
                    'Trying to build timeseries %s plot for paired '
                    'dataset: %s_%s_%s_%s_%s_%s_pair.int',
                    var_info[0], station_prop.ofs, var_info[1],
                    station_id_val, read_ofs_ctl_file[1][i],
                    station_prop.whichcast, station_prop.ofsfiletype)
                plotting_scalar.oned_scalar_plot(
                    now_fores_paired, var_info[1],
                    [station_id_val,
                     read_station_ctl_file[0][obs_row][2],
                     read_station_ctl_file[0][obs_row][1].split('_')[-1],
                     read_station_ctl_file[1][obs_row][2]],
                    read_ofs_ctl_file[1][i],
                    station_prop, logger)
            elif var_info[1] == 'cu':
                logger.info(
                    'Trying to build timeseries %s plot for paired '
                    'dataset: %s_%s_%s_%s_%s_%s_pair.int',
                    var_info[0], station_prop.ofs, var_info[1],
                    station_id_val, read_ofs_ctl_file[1][i],
                    station_prop.whichcast, station_prop.ofsfiletype)
                plotting_vector.oned_vector_plot1(
                    now_fores_paired, var_info[1],
                    [station_id_val,
                     read_station_ctl_file[0][obs_row][2],
                     read_station_ctl_file[0][obs_row][1].split('_')[-1],
                     read_station_ctl_file[1][obs_row][2]],
                    read_ofs_ctl_file[1][i],
                    station_prop, logger)

                logger.info(
                    'Trying to build wind rose %s plot for paired '
                    'dataset: %s_%s_%s_%s_%s_%s_pair.int',
                    var_info[0], station_prop.ofs, var_info[1],
                    station_id_val, read_ofs_ctl_file[1][i],
                    station_prop.whichcast, station_prop.ofsfiletype)
                plotting_vector.oned_vector_plot2b(
                    plotting_vector.oned_vector_plot2a(
                        now_fores_paired, logger),
                    var_info[1],
                    [station_id_val,
                     read_station_ctl_file[0][obs_row][2],
                     read_station_ctl_file[0][obs_row][1].split('_')[-1],
                     read_station_ctl_file[1][obs_row][2]],
                    read_ofs_ctl_file[1][i],
                    station_prop, logger)
                if deltat <= -1:
                    logger.info(
                        'Trying to build stick %s plot for paired '
                        'dataset: %s_%s_%s_%s_%s_pair.int',
                        var_info[0], station_prop.ofs, var_info[1],
                        station_id_val, read_ofs_ctl_file[1][i],
                        station_prop.whichcast)
                    plotting_vector.oned_vector_plot3(
                        now_fores_paired, var_info[1],
                        [station_id_val,
                         read_station_ctl_file[0][obs_row][2],
                         read_station_ctl_file[0][obs_row][1].split(
                             '_')[-1],
                         read_station_ctl_file[1][obs_row][2]],
                        read_ofs_ctl_file[1][i],
                        station_prop, logger)
                    logger.info(
                        'Trying to build stick %s plot for vector '
                        'difference: %s_%s_%s_%s_%s_%s_pair.int',
                        var_info[0], station_prop.ofs, var_info[1],
                        station_id_val, read_ofs_ctl_file[1][i],
                        station_prop.whichcast,
                        station_prop.ofsfiletype)
                    plotting_vector.oned_vector_diff_plot3(
                        now_fores_paired, var_info[1],
                        [station_id_val,
                         read_station_ctl_file[0][obs_row][2],
                         read_station_ctl_file[0][obs_row][1].split(
                             '_')[-1],
                         read_station_ctl_file[1][obs_row][2]],
                        read_ofs_ctl_file[1][i],
                        station_prop, logger)
        except Exception as ex:
            logger.info(
                'Fail to create the plot  '
                '---  %s ...Continuing to next plot', ex)
            return None

    return station_id_val


def _ensure_paired_data_exists(read_ofs_ctl_file, prop, var_info, logger):
    """
    Pre-check for missing paired data files and call get_skill()
    sequentially for any casts that need it.  This must happen BEFORE
    parallel dispatch because get_skill() mutates shared state
    (prop.whichcast) and creates control files.
    """
    casts_needing_skill = set()
    for i in range(len(read_ofs_ctl_file[1])):
        for cast in prop.whichcasts:
            current_cast = cast.lower()
            pair_file = (
                f'{prop.data_skill_1d_pair_path}/'
                f'{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_'
                f'{read_ofs_ctl_file[1][i]}_{current_cast}_'
                f'{prop.ofsfiletype}_pair.int'
            )
            if not os.path.isfile(pair_file):
                if (prop.ofsfiletype == 'fields'
                        or read_ofs_ctl_file[1][i] >= 0):
                    casts_needing_skill.add(current_cast)

    for current_cast in sorted(casts_needing_skill):
        logger.info(
            'Pre-generating paired data via get_skill for cast %s',
            current_cast)
        prop.whichcast = current_cast
        if prop.start_date_full.find('T') == -1:
            prop.start_date_full = prop.start_date_full_before
            prop.end_date_full = prop.end_date_full_before
        get_skill(prop, logger)


def create_1dplot_2nd_part(
        read_ofs_ctl_file, prop, var_info, logger):
    '''
    This is the function that actually creates the plots
    it had to be split from the original function due to size (PEP8)
    '''
    logger.info(
        f'Searching for paired dataset for {prop.ofs}, variable '
        f'{var_info[0]}')

    # Read obs station ctl files
    try:
        read_station_ctl_file = station_ctl_file_extract(
            r'' + prop.control_files_path + '/' + prop.ofs + '_'
            + var_info[1] + '_station.ctl'
        )
        logger.info(
            'Station ctl file (%s_%s_station.ctl) found in get_title. ',
            prop.ofs,
            var_info[1]
        )
    except FileNotFoundError:
        logger.error('Station ctl file not found.')
        sys.exit(-1)

    # Ensure all paired data files exist before parallel dispatch.
    # get_skill() mutates prop and creates shared control files, so it
    # must run sequentially.
    _ensure_paired_data_exists(read_ofs_ctl_file, prop, var_info, logger)

    parallel_config = get_parallel_config(logger)
    num_stations = len(read_ofs_ctl_file[1])
    use_parallel = (parallel_config.get('parallel_plotting', True)
                    and num_stations > 1)

    if use_parallel:
        max_workers = min(num_stations,
                          parallel_config.get('plot_workers', 6))
        logger.info('Plotting %d stations in parallel with %d workers',
                    num_stations, max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i in range(num_stations):
                prop_copy = copy.copy(prop)
                futures[executor.submit(
                    _process_station_plot, i, read_ofs_ctl_file,
                    read_station_ctl_file, prop_copy, var_info, logger
                )] = i
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        logger.info('Completed plot for station %s',
                                    result)
                except Exception as ex:
                    logger.error(
                        'Unhandled exception for station index %d: %s',
                        idx, ex)
    else:
        logger.info('Plotting %d stations sequentially', num_stations)
        for i in range(num_stations):
            try:
                result = _process_station_plot(
                    i, read_ofs_ctl_file, read_station_ctl_file,
                    prop, var_info, logger)
                if result is not None:
                    logger.info('Completed plot for station %s', result)
            except Exception as ex:
                logger.error(
                    'Plot failed for station index %d: %s', i, ex)


def _process_forecast_cycle(cycle_hr, prop_template, logger):
    """
    Run the full 1D plotting pipeline for a single forecast_a cycle.

    This is designed to be dispatched in parallel via ThreadPoolExecutor.
    Each call receives a deep copy of ``prop_template`` so that date and
    forecast_hr mutations are isolated from other cycles.

    Parameters
    ----------
    cycle_hr : int
        Forecast cycle hour (e.g. 0, 6, 12, 18).
    prop_template : ModelProperties
        A deep copy of the fully-validated prop object.  This function
        will mutate ``start_date_full``, ``end_date_full``, and
        ``forecast_hr`` on the copy.
    logger : logging.Logger
        Logger instance.
    """
    prop_copy = copy.deepcopy(prop_template)
    forecast_hr_str = f'{cycle_hr:02d}hr'
    prop_copy.forecast_hr = forecast_hr_str

    # Recompute start/end dates for this specific cycle using the
    # original user-supplied start date (before any single-cycle
    # adjustment that happened during validation).
    prop_copy.start_date_full, prop_copy.end_date_full = get_fcst_dates(
        prop_copy.ofs, prop_copy.start_date_full_original, forecast_hr_str,
        logger)
    prop_copy.forecast_hr = (
        prop_copy.start_date_full.split('T')[1][0:2] + 'hr')

    # Update the _before snapshots so that ofs_ctlfile_read and
    # _ensure_paired_data_exists can fall back to them when needed.
    prop_copy.start_date_full_before = prop_copy.start_date_full
    prop_copy.end_date_full_before = prop_copy.end_date_full

    logger.info('Forecast cycle %02dZ: period %s to %s',
                cycle_hr, prop_copy.start_date_full,
                prop_copy.end_date_full)

    # Run variable plotting for this cycle
    for variable in prop_copy.var_list:
        _plot_variable_for_cycle(variable, prop_copy, logger)

    logger.info('Completed forecast cycle %02dZ', cycle_hr)
    return cycle_hr


def _plot_variable_for_cycle(variable, prop, logger):
    """
    Plot a single variable for a given prop configuration.

    Mirrors the _plot_variable inner function in create_1dplot but is
    a module-level function so it can be called from
    _process_forecast_cycle.
    """
    if variable == 'water_level':
        name_var = 'wl'
        list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                            'minute', 'OBS', 'OFS', 'BIAS']
        logger.info('Creating Water Level plots.')
    elif variable == 'water_temperature':
        name_var = 'temp'
        list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                            'minute', 'OBS', 'OFS', 'BIAS']
        logger.info('Creating Water Temperature plots.')
    elif variable == 'salinity':
        name_var = 'salt'
        list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                            'minute', 'OBS', 'OFS', 'BIAS']
        logger.info('Creating Salinity plots.')
    elif variable == 'currents':
        name_var = 'cu'
        list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                            'minute', 'OBS_SPD', 'OFS_SPD', 'BIAS_SPD',
                            'OBS_DIR', 'OFS_DIR', 'BIAS_DIR']
        logger.info('Creating Currents plots.')
    else:
        return

    var_info = [variable, name_var, list_of_headings]

    read_ofs_ctl_file = ofs_ctlfile_read(prop, name_var, logger)
    if read_ofs_ctl_file is not None:
        create_1dplot_2nd_part(
            read_ofs_ctl_file, prop, var_info, logger)


def create_1dplot(prop, logger):
    '''
    This is the main function for plotting 1d paired datasets
    Specify defaults (can be overridden with command line options)
    '''
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(Path(prop.path), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting Visualization Process ---')

    dir_params = utils.Utils().read_config_section('directories', logger)
    # Retrieve datum list from config file
    prop.datum_list = (utils.Utils().read_config_section('datums', logger)\
                       ['datum_list']).split(' ')
    conf_settings = utils.Utils().read_config_section('settings', logger)
    prop.static_plots = conf_settings['static_plots']


    # Parse incoming arguments stored in prop from string to a list
    prop.whichcasts = parse_arguments_to_list(prop.whichcasts, logger)
    prop.stationowner = parse_arguments_to_list(prop.stationowner, logger)
    prop.var_list = parse_arguments_to_list(prop.var_list, logger)
    # Format the other incoming arguments
    prop.ofs = prop.ofs.lower()
    prop.datum = prop.datum.upper()
    prop.ofsfiletype = prop.ofsfiletype.lower()

    logger.info('Starting parameter validation...')

    # Save original (user-supplied) start date before any forecast_a
    # adjustment.  _process_forecast_cycle uses this to independently
    # recompute dates for each cycle.
    prop.start_date_full_original = prop.start_date_full

    # Do forecast_a start and end date reshuffle

    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is not None:
            prop.start_date_full, prop.end_date_full =\
            get_fcst_dates(prop, logger)
            prop.forecast_hr = prop.start_date_full.split('T')[1][0:2] + 'z'
            logger.info(f'Forecast_a: start date reassigned to '
                             f'{prop.start_date_full}')
            logger.info(f'Forecast_a: end date reassigned to '
                             f'{prop.end_date_full}')
        else:
            raise SystemExit(1)
    # Start Date and End Date validation
    # Enforce end date for whichcasts other than forecast_a
    if prop.end_date_full is None or prop.start_date_full is None:
        logger.error('If not using forecast_a, you must set start and end dates! '
                     'Abort.')
        raise SystemExit(1)
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (f'Please check Start Date - '
                         f'{prop.start_date_full}, End Date - '
                         f'{prop.end_date_full}. Abort!')
        logger.error(error_message)
        raise SystemExit(1)
    if datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ') > datetime.strptime(
        prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'):
        error_message = (f'End Date {prop.end_date_full} '
                         f'is before Start Date {prop.end_date_full}. Abort!')
        logger.error(error_message)
        raise SystemExit(1)
    if datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC) > datetime.now(UTC):
        logger.error('Start date is in the future! Unless you have a time machine, '
                     'please set a start date that is before the current date.'
                     )
        raise SystemExit(1)

    if prop.path is None:
        prop.path = dir_params['home']

    # prop.path validation
    ofs_extents_path = os.path.join(
        prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = (f'ofs_extents/ folder is not found. '
                         f'Please check prop.path - {prop.path}. Abort!')
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        error_message = (f'Shapefile {prop.ofs} is not found at '
                         f'the folder {ofs_extents_path}. Abort!')
        logger.error(error_message)
        sys.exit(-1)
    if prop.ofs == 'stofs_2d_glo':
        logger.warning('IMPORTANT NOTE: STOFS-2D-Global currently uses a '
                       'copy of the GOMOFS extent file for testing purposes. '
                       'This may cause issues with some workflows!')

    # Datum validations!
    if prop.datum not in prop.datum_list:
        logger.error('Entered datum is not valid!')
        if 'l' not in prop.ofs[0]:
            prop.datum = 'MLLW'
        else:
            prop.datum = 'LWD'
        logger.warning('Switching to %s', prop.datum)
    # Check vdatum file to see if the requested datum is available for this OFS
    vdatums = read_vdatum_from_bucket(prop,logger)
    try:
        if 'l' not in prop.ofs[0]:
            vdatums[f'{prop.datum.lower()}tomsl']
        else:
            if prop.datum.lower() != 'lwd':
                vdatums[f'{prop.datum.lower()}tolwd']
        logger.info('Specified datum %s available for model conversion!',
                prop.datum)
    except KeyError:
        if (prop.ofs.lower() not in ['loofs','lmhofs','leofs','lsofs'] and
            'stofs' not in prop.ofs.lower()):
            logger.warning('Datum %s is NOT available for %s! '
                         'Switching to MLLW...', prop.datum, prop.ofs)
            prop.datum = 'MLLW'
        else:
            logger.warning('Datum %s is NOT available for %s! '
                         'Switching to IGLD...', prop.datum, prop.ofs)
            prop.datum = 'IGLD85'
    except TypeError:
        if (vdatums == -9995) and prop.ofs.lower() in ('stofs_2d_glo'):
            logger.info('No vdatum file for STOFS-2D-Global, as expected.')
        else:
            logger.error('Failure checking for datum netcdf file on the NODD S3 '
                        'bucket! Datum conversions may fail. Continuing...')

    # Date-gate for forecast horizon functionality
    if ((datetime.strptime(prop.end_date_full,'%Y-%m-%dT%H:%M:%SZ')-
         datetime.strptime(prop.start_date_full,'%Y-%m-%dT%H:%M:%SZ')).days > 2
        and prop.horizonskill):
        logger.error('Time range of %s days is too long for forecast '
                    'horizon skill! Resetting forecast horizon skill argument '
                    'to False.',str(
                        (datetime.strptime(prop.end_date_full,\
                                           '%Y-%m-%dT%H:%M:%SZ')-
                         datetime.strptime(prop.start_date_full,\
                                           '%Y-%m-%dT%H:%M:%SZ')).days))
        prop.horizonskill = False
    # Cast-gate for nowcast horizon functionality
    if ('forecast_b' not in prop.whichcasts) and prop.horizonskill:
        logger.error('Forecast horizon skill only works for forecast_b mode. '
                    'Resetting forecast horizon skill argument to False.')
        prop.horizonskill = False
    # file-gate for nowcast horizon functionality
    if (prop.ofsfiletype == 'fields') and prop.horizonskill:
        logger.error('Forecast horizon skill only works for station files. '
                    'Resetting forecast horizon skill argument to False.')
        prop.horizonskill = False
    # Static plot boolean conversion and validation
    truthy_strings = {'true': True, 'yes': True, '1': True, 'True': True}
    falsy_strings = {'false': False,
                     'no': False, '0': False, 'False': False}
    if prop.static_plots in truthy_strings:
        prop.static_plots = truthy_strings[prop.static_plots]
    elif prop.static_plots in falsy_strings:
        prop.static_plots = falsy_strings[prop.static_plots]
    else:
        prop.static_plots = False

    # Hindcast validation -- LOOFS2 only! Also, LOOFS2 cannot use nowcast or
    # forecast yet.
    if prop.ofs == 'loofs2':
        prop.whichcasts = ['hindcast']
    if 'hindcast' in prop.whichcasts and prop.ofs != 'loofs2':
        logger.warning('Hindcast can only be used with loofs2! Switching to '
                       'nowcast + forecast_b...')
        prop.whichcasts = ['nowcast', 'forecast_b']

    # Handle variable input argument
    correct_var_list = ['water_level','water_temperature',
                        'salinity','currents']
    list_diff = list(set(prop.var_list) - set(correct_var_list))
    if len(list_diff) != 0:
        logger.error('Incorrect inputs to variable selection argument: %s. '
                     'Please use %s. Exiting...', list_diff,
                     correct_var_list)
        sys.exit()
    # If using 'list' for station providers, add all providers
    if 'list' in prop.stationowner:
        prop.stationowner = 'co-ops,ndbc,usgs,chs,list'

    logger.info('Parameter validation complete!')
    logger.info('Making directory tree...')
    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir'])
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['observations_dir'],
        dir_params['1d_station_dir'], )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'], )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir'], )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'], )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'], )
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    prop.visuals_horizon_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['visual_horizon_dir'])
    os.makedirs(prop.visuals_horizon_path, exist_ok=True)

    prop.data_horizon_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'], dir_params['horizon_model_dir'])
    os.makedirs(prop.data_horizon_1d_node_path, exist_ok=True)

    prop.data_horizon_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir'], dir_params['1d_horizon_pair_dir'])
    os.makedirs(prop.data_horizon_1d_pair_path, exist_ok=True)

    logger.info('Directory tree built!')

    # Path to save O&M files
    prop.om_files = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['om_dir'])
    os.makedirs(prop.om_files, exist_ok=True)

    # Before starting, let's check if all necessary model files are
    # available. If not, program will exit. Or, if exception, program will
    # continue onwards but not before shouting a warning at you :)
    try:
        check_model_files(prop,logger)
        # if fails call nodd_otf
    except Exception as e_x:
        logger.error('Error caught in check_model_files! %s', e_x)
        logger.warning('Could not verify if all necessary model files '
                    'are present! Check final time series for accuracy.')

    def _plot_variable(variable, p):
        """Plot a single variable."""
        if variable == 'water_level':
            name_var = 'wl'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Water Level plots.')
        elif variable == 'water_temperature':
            name_var = 'temp'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Water Temperature plots.')
        elif variable == 'salinity':
            name_var = 'salt'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Salinity plots.')
        elif variable == 'currents':
            name_var = 'cu'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS_SPD', 'OFS_SPD', 'BIAS_SPD',
                                'OBS_DIR', 'OFS_DIR', 'BIAS_DIR']
            logger.info('Creating Currents plots.')

        var_info = [variable, name_var, list_of_headings]

        # Read OFS model ctl files
        read_ofs_ctl_file = ofs_ctlfile_read(
            p, name_var, logger)

        if read_ofs_ctl_file is not None:
            create_1dplot_2nd_part(
                read_ofs_ctl_file, p, var_info,
                logger)

    # --- Forecast cycle parallelism for forecast_a mode ---
    parallel_config = get_parallel_config(logger)
    if 'forecast_a' in prop.whichcasts:
        _, forecast_cycles = get_fcst_hours(prop.ofs)
        use_parallel_cycles = (
            parallel_config.get('parallel_forecast_cycles', True)
            and len(forecast_cycles) > 1)

        if use_parallel_cycles:
            max_cycle_workers = min(len(forecast_cycles), 4)
            logger.info(
                'Processing %d forecast cycles in parallel with %d '
                'workers', len(forecast_cycles), max_cycle_workers)
            with ThreadPoolExecutor(
                    max_workers=max_cycle_workers) as executor:
                futures = {}
                for cycle_hr in forecast_cycles:
                    futures[executor.submit(
                        _process_forecast_cycle, int(cycle_hr),
                        prop, logger)] = int(cycle_hr)
                for future in as_completed(futures):
                    cycle = futures[future]
                    try:
                        future.result()
                        logger.info(
                            'Completed forecast cycle %02dZ', cycle)
                    except Exception as ex:
                        logger.error(
                            'Forecast cycle %02dZ failed: %s',
                            cycle, ex)
        else:
            logger.info('Processing %d forecast cycles sequentially',
                        len(forecast_cycles))
            for cycle_hr in forecast_cycles:
                try:
                    _process_forecast_cycle(
                        int(cycle_hr), prop, logger)
                except Exception as ex:
                    logger.error(
                        'Forecast cycle %02dZ failed: %s',
                        int(cycle_hr), ex)
    else:
        # Non-forecast_a modes: variable plotting runs sequentially here
        # because each variable's ofs_ctlfile_read() may trigger
        # get_skill() -> get_node_ofs() which loads the model. Variable
        # parallelism is handled inside get_node_ofs and get_skill where
        # the model is loaded once and shared.
        for variable in prop.var_list:
            _plot_variable(variable, prop)

    return logger


# Execution:
if __name__ == '__main__':
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='create_1dplot.py', usage='%(prog)s',
        description='Run skill assessment program', )
    parser.add_argument(
        '-o', '--OFS',
        required=False,
        help="""Choose from the list on the ofs_extents/folder,
        you can also create your own shapefile, add it at the
        ofs_extents/folder and call it here""", )
    parser.add_argument(
        '-p', '--Path',
        required=False,
        help='Inventory File path where ofs_extents/folder is located', )
    parser.add_argument(
        '-s', '--StartDate_full',
        required=False,
        help='Assessment start date: YYYY-MM-DDThh:mm:ssZ '
        "e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        '-e', '--EndDate_full',
        required=False,
        help='Assessment end date: YYYY-MM-DDThh:mm:ssZ '
        "e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        '-d', '--Datum',
        required=False,
        default='MLLW',
        help="datum options: 'MHW', 'MHHW' \
        'MLW', 'MLLW', 'NAVD88', 'XGEOID20B', 'IGLD85', 'LWD'")
    parser.add_argument(
        '-ws', '--Whichcasts',
        required=False,
        default='nowcast,forecast_b',
        help="whichcasts: 'nowcast', 'forecast_a', 'forecast_b'", )
    parser.add_argument(
        '-t', '--FileType',
        required=False,
        default='stations',
        help="OFS model output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        default='now',
        help='Specify model cycle to assess. Used with forecast_a mode only: '
        "'02z', '06Z', '12z'; use 'now' to assess the most recent available "
        'model forecast cycle.', )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        default='co-ops,ndbc,usgs,chs',
        help='Input station provider to use in skill assessment: '
        "'CO-OPS', 'NDBC', 'USGS', 'CHS', 'list'", )
    parser.add_argument(
        '-hs',
        '--Horizon_Skill',
        action='store_true',
        help='Use all available forecast horizons between the '
        'start and end dates? True or False (boolean)')
    parser.add_argument(
        '-vs',
        '--Var_Selection',
        required=False,
        default='water_level,water_temperature,salinity,currents',
        help='Which variables do you want to skill assess? Options are: '
            'water_level, water_temperature, salinity, and currents. Choose '
            'any combination. Default (no argument) is all variables.')

    args = parser.parse_args()

    # Launch GUI to accept argument input if no OFS args are present
    if args.OFS is None:
        args = create_gui.create_gui(parser)
        gc.collect() # garbage collect from GUI window not in main thread

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts
    prop1.datum = args.Datum
    prop1.ofsfiletype = args.FileType
    prop1.stationowner = args.Station_Owner
    prop1.horizonskill = args.Horizon_Skill
    prop1.forecast_hr = args.Forecast_Hr
    prop1.var_list = args.Var_Selection
    # This can only be changed if directly running get_node_ofs.py!
    prop1.user_input_location = False

    logger = create_1dplot(prop1, None)

    logger.info('Finished create_1dplot!')
