"""
Called by do_iceskill.py to list model files and use intake lazy load them.
Returns 2D model output, lats, lons, and time as numpy arrays.
"""
from __future__ import annotations
import argparse
import logging.config
import os
import sys
from datetime import datetime
from datetime import timedelta
from pathlib import Path

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import intake_scisa
from model_processing import list_of_files
from obs_retrieval import utils
from model_processing import model_source
from model_processing import model_properties

import numpy as np
import pandas as pd


def get_days_between_dates(start_date, end_date):
    """
    Generates a list of all dates between a start_date and an end_date
    (inclusive).

    Args:
        start_date (datetime.date): The starting date.
        end_date (datetime.date): The ending date.

    Returns:
        list: A list of datetime.date objects representing all days
              between the start and end dates.
    """
    if start_date > end_date:
        raise ValueError('start_date cannot be after end_date!')

    list_days = []
    current_date = start_date
    while current_date <= end_date:
        list_days.append(current_date)
        current_date += timedelta(days=1)
    return list_days


def file_name_to_datetime(list_files):
    '''
    Takes list of OFS model file names and converts to a separate list of
    datetime objects that represent the actual year/month/day/hour of the model
    output. The datetime list is returned. Handles both old & new OFS file
    naming conventions.

    New file convention:
    cbofs.t00z.20240901.fields.n001.nc
    Old file convention:
    nos.cbofs.fields.n001.20240901.t00z.nc
    '''
    list_files_dates = []
    for file in list_files:
        if 'nos.' in file:
            # First find hour of forecast/nowcast
            file_split = file.split('/')[-1].split('.')
            cycle_date = datetime.strptime(
                file_split[4] + str(file_split[5][1:3]),
                '%Y%m%d%H',
            )
            if 'n' in file_split[3]:  # nowcast
                list_files_dates.append(
                    cycle_date-timedelta(
                        hours=6-int(file_split[3][1:]),
                    ),
                )
            elif 'f' in file_split[3]:  # forecast
                list_files_dates.append(
                    cycle_date+timedelta(
                        hours=int(file_split[3][1:]),
                    ),
                )
        else:
            # First find hour of forecast/nowcast
            file_split = file.split('/')[-1].split('.')
            cycle_date = datetime.strptime(
                file_split[2] + str(file_split[1][1:3]),
                '%Y%m%d%H',
            )
            if 'n' in file_split[4]:  # nowcast
                list_files_dates.append(
                    cycle_date-timedelta(
                        hours=6-int(file_split[4][1:]),
                    ),
                )
            elif 'f' in file_split[4]:  # forecast
                list_files_dates.append(
                    cycle_date+timedelta(
                        hours=int(file_split[4][1:]),
                    ),
                )
    return list_files_dates


def get_indices_for_day(datetime_list, target_date):
    """
    Returns a list of indices where datetime objects in datetime_list
    match the given target_date (year, month, day).

    Args:
        datetime_list (list): A list of datetime.datetime objects.
        target_date (datetime.date): The specific date to match.

    Returns:
        list: A list of integers representing the indices.
    """
    indices = []
    for i, dt_obj in enumerate(datetime_list):
        if dt_obj.date() == target_date.date():
            indices.append(i)
    return indices


def get_icecover_fvcom(prop, logger):
    """
    write_ofs_ctlfile
    """
    prop.model_source = model_source.model_source(prop.ofs)
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent / log_config_file).resolve()

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

    logger.info('--- Starting OFS FVCOM ice cover process ---')

    start_date_full = prop.start_date_full.replace('-', '')
    end_date_full = prop.end_date_full.replace('-', '')
    start_date_full = start_date_full.replace('Z', '')
    end_date_full = end_date_full.replace('Z', '')
    start_date_full = start_date_full.replace('T', '-')
    end_date_full = end_date_full.replace('T', '-')

    prop.startdate = (
        datetime.strptime(
            start_date_full.split('-')[0], '%Y%m%d',
        )
    ).strftime(
        '%Y%m%d',
    ) + '00'
    prop.enddate = (
        datetime.strptime(
            end_date_full.split('-')[0], '%Y%m%d',
        )
    ).strftime(
        '%Y%m%d',
    ) + '23'

    dir_list = list_of_files.list_of_dir(prop, logger)
    list_files = list_of_files.list_of_files(prop, dir_list, logger)

    # If daily resolution, pare down list of files to one per day
    logger.info(
        'Trimming list of model output files from hourly to daily...',
    )
    list_files_daily = []
    if prop.ice_dt == 'daily' and prop.dailyavg is False:
        # What cycle & hour do we search for?
        if prop.whichcast == 'nowcast':
            cycle = 't12z'
            hour = 'n006'
        elif prop.whichcast == 'forecast_b':
            cycle = 't06z'
            hour = 'f006'
        for i in range(0, len(list_files)):
            if (cycle in list_files[i] and hour in list_files[i]):
                list_files_daily.append(list_files[i])
    # Do composite daily model ice for each day
    # First list all days between start and end date, then convert list_files
    # to datetime objects
    if prop.dailyavg:
        daily_composite_all = []
        # List all days
        list_days = get_days_between_dates(
            datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ'),
            datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'),
        )
        # Convert list_files to datetime objects
        list_files_datetime = file_name_to_datetime(list_files)
        # Now loop through days and collect files that have a date in that day
        for day in list_days:
            logger.info('Making model daily average for %s', day)
            daystring = datetime.strftime(day, '%Y%m%d')
            filename = f'{prop.ofs}_{prop.whichcast}_' + \
                f'{daystring}_composite_iceconc.csv'
            filepath = os.path.join(prop.data_model_ice_path, filename)
            if os.path.exists(filepath) and os.path.isfile(filepath):
                try:
                    df_load = pd.read_csv(filepath)
                    daily_composite = df_load['daily_composite']
                    lon_m = df_load['lon']
                    lat_m = df_load['lat']
                except Exception as e_x:
                    logger.error(
                        'Error when loading model daily average '
                        'from CSV: %s', e_x,
                    )
                    date_indices = get_indices_for_day(
                        list_files_datetime, day)
                    # Now find files from file_list in each day
                    file_list_composite = [list_files[i] for i in date_indices]
                    # Now load files using intake
                    concated_model = intake_scisa.intake_model(
                        file_list_composite,
                        prop, logger,
                    )
                    daily_composite = np.nanmean(
                        np.asarray(
                            concated_model.
                            variables['aice'][:],
                        ),
                        axis=0,
                    )
                    # Save to CSV
                    lon_m = np.asarray(concated_model.variables['lon'][:])
                    lat_m = np.asarray(concated_model.variables['lat'][:])
                    data = {
                        'lon': lon_m,
                        'lat': lat_m,
                        'daily_composite': daily_composite,
                    }
                    df_save = pd.DataFrame(data)
                    df_save.to_csv(filepath, index=False)
            else:
                date_indices = get_indices_for_day(list_files_datetime, day)
                # Now find files from file_list in each day
                file_list_composite = [list_files[i] for i in date_indices]
                # Now load files using intake
                concated_model = intake_scisa.intake_model(
                    file_list_composite,
                    prop, logger,
                )
                daily_composite = np.nanmean(
                    np.asarray(
                        concated_model.
                        variables['aice'][:],
                    ),
                    axis=0,
                )
                # Save to CSV
                lon_m = np.asarray(concated_model.variables['lon'][:])
                lat_m = np.asarray(concated_model.variables['lat'][:])
                data = {
                    'lon': lon_m,
                    'lat': lat_m,
                    'daily_composite': daily_composite,
                }
                df_save = pd.DataFrame(data)
                df_save.to_csv(filepath, index=False)
            # Make composite for each day
            daily_composite_all.append(daily_composite)

        # Stack list to numpy array
        icecover_m = np.stack(daily_composite_all)
        time_m = list_days
        logger.info('Finished model daily averages!')
        return icecover_m, lon_m, lat_m, time_m

    if prop.ice_dt == 'daily':
        list_files = list_files_daily
    if len(list_files) > 0:
        logger.info(
            'Model %s **icecover** netcdf files for %s found for the period '
            'from %s to %s',
            prop.whichcast,
            prop.ofs,
            prop.startdate,
            prop.enddate,
        )
        logger.info('Calling intake_scisa from get_icecover_fvcom.py...')
        concated_model = intake_scisa.intake_model(list_files, prop, logger)
        logger.info(
            'Returned from call to intake_scisa from get_icecover_fvcom.py!')
    else:
        logger.error('No model files to load!')
        sys.exit(-1)

    # -- Get lat, lon and ice cover from FVCOM output (model)
    lon_m = np.asarray(concated_model.variables['lon'][:])
    lat_m = np.asarray(concated_model.variables['lat'][:])
    try:
        icecover_m = np.asarray(concated_model.variables['aice'][:])
    except:
        logger.error('No modeled ice concentration available! Abort')
        sys.exit(-1)
    time_m = np.asarray(concated_model.variables['time'][:])
    # depth = np.array(concated_model.variables['h'][:])
    return icecover_m, lon_m, lat_m, time_m


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python get_icecover_fvcom.py',
        usage='%(prog)s',
        description='Retrieve and load GLOFS ice concentration from FVCOM',
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the OFS_Extents folder, you can also '
             'create your own shapefile, add it top the OFS_Extents folder '
             'and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=False,
        help='Use /home as the default. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate',
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e',
        '--EndDate',
        required=True,
        help="End Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-w',
        '--Whichcasts',
        required=False,
        help='nowcast, forecast_a, '
             'forecast_b(it is the forecast between cycles)',
    )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ",
    )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.ofs_extents_path = r'' + prop1.path + 'ofs_extents' + '/'
    prop1.start_date_full = args.StartDate
    prop1.end_date_full = args.EndDate
    prop1.whichcast = args.Whichcast

    prop1.model_source = model_source.model_source(args.OFS)

    if prop1.whichcast != 'forecast_a':
        prop1.forecast_hr = None
    else:
        prop1.forecast_hr = args.Forecast_Hr

    get_icecover_fvcom(prop1, None)
