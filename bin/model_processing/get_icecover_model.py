"""
Called by do_iceskill.py to list model files and use intake lazy load them.
Returns 2D model output, lats, lons, and time as numpy arrays.
"""
from __future__ import annotations

import argparse
import logging.config
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer

from ofs_skill.model_processing import intake_scisa, model_properties, model_source
from ofs_skill.model_processing.list_of_files import list_of_dir, list_of_files
from ofs_skill.obs_retrieval import utils


def get_days_between_dates(start_date, end_date):
    """Generates a list of all dates between start_date and end_date (inclusive)."""
    if start_date > end_date:
        raise ValueError('start_date cannot be after end_date!')
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def file_name_to_datetime(list_files):
    '''Converts OFS model file names to datetime objects. Handles old & new naming conventions.'''
    list_files_dates = []
    for file in list_files:
        file_split = file.split('/')[-1].split('.')
        is_old = 'nos.' in file

        if is_old:
            cycle_date = datetime.strptime(file_split[4] + file_split[5][1:3], '%Y%m%d%H')
            hour_info = file_split[3]
        else:
            cycle_date = datetime.strptime(file_split[2] + file_split[1][1:3], '%Y%m%d%H')
            hour_info = file_split[4]

        if hour_info == 'hindcast':
            list_files_dates.append(cycle_date)
        elif 'n' in hour_info:  # nowcast
            list_files_dates.append(cycle_date - timedelta(hours=6 - int(hour_info[1:])))
        elif 'f' in hour_info:  # forecast
            list_files_dates.append(cycle_date + timedelta(hours=int(hour_info[1:])))

    return list_files_dates


def get_indices_for_day(datetime_list, target_date):
    """Returns indices where datetime objects match the target_date."""
    return [i for i, dt_obj in enumerate(datetime_list) if dt_obj.date() == target_date.date()]


def _setup_logger(logger, config_file=None):
    """Initialize logger if not provided."""
    if logger is not None:
        return logger

    config_file = utils.Utils(config_file).get_config_file()
    log_config_file = (Path(__file__).parent.parent.parent / 'conf/logging.conf').resolve()

    for file in [log_config_file, config_file]:
        if not os.path.isfile(file):
            sys.exit(-1)

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using config %s', config_file)
    logger.info('Using log config %s', log_config_file)
    return logger


def _get_variable_names(model_src):
    """Get variable names based on model source (FVCOM vs SCHISM)."""
    if model_src == 'schism':
        return 'iceTracer_2', 'SCHISM_hgrid_node_x', 'SCHISM_hgrid_node_y'
    return 'aice', 'lon', 'lat'


def _process_daily_composite(prop, logger, list_files, list_days, ice_name, x_name, y_name):
    """Process daily composite ice cover."""
    daily_composite_all = []
    list_files_datetime = file_name_to_datetime(list_files)
    for day in list_days:
        logger.info('Making model daily average for %s', day)
        daystring = datetime.strftime(day, '%Y%m%d')
        filename = f'{prop.ofs}_{prop.whichcast}_{daystring}_composite_iceconc.csv'
        filepath = os.path.join(prop.data_model_ice_path, filename)

        try:
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                daily_composite, lon_m, lat_m = df['daily_composite'], df['lon'], df['lat']
            else:
                raise FileNotFoundError()
        except (FileNotFoundError, pd.errors.EmptyDataError, KeyError):

            date_indices = get_indices_for_day(list_files_datetime, day)
            if prop.model_source == 'schism':
                date_indices.append(date_indices[-1]+1)
            try:
                file_list_composite = [list_files[i] for i in date_indices]
            except IndexError:
                file_list_composite = [list_files[i] for i in date_indices[:-1]]
            concated_model = intake_scisa.intake_model(file_list_composite, prop, logger)
            if prop.model_source == 'schism':
                concated_model = concated_model.sel(time=datetime.strftime(day,'%Y-%m-%d'))
            daily_composite = np.nanmean(np.asarray(concated_model.variables[ice_name][:]), axis=0)
            lon_m, lat_m = np.asarray(concated_model.variables[x_name][:]), np.asarray(concated_model.variables[y_name][:])

            df_save = pd.DataFrame({'lon': lon_m, 'lat': lat_m, 'daily_composite': daily_composite})
            df_save.to_csv(filepath, index=False)

        daily_composite_all.append(daily_composite)
    if prop.model_source == 'schism':
        transformer = Transformer.from_crs('EPSG:3174', 'EPSG:4326', always_xy=True)
        lon_m, lat_m = transformer.transform(lon_m, lat_m)
    else:
        lon_m = lon_m - 360
    return np.stack(daily_composite_all), lon_m, lat_m, list_days


def get_icecover_model(prop, logger):
    """Main function to retrieve OFS ice cover model data."""
    prop.model_source = model_source.model_source(prop.ofs)
    logger = _setup_logger(logger, getattr(prop, 'config_file', None))
    logger.info('--- Starting OFS ice cover process ---')

    # Reformat dates
    start_dt = datetime.strptime(prop.start_date_full.replace('-', '').replace('Z', '').replace('T', '-').split('-')[0], '%Y%m%d')
    end_dt = datetime.strptime(prop.end_date_full.replace('-', '').replace('Z', '').replace('T', '-').split('-')[0], '%Y%m%d')
    prop.startdate, prop.enddate = start_dt.strftime('%Y%m%d') + '00', end_dt.strftime('%Y%m%d') + '23'

    # Get list of model files
    dir_list = list_of_dir(prop, logger)
    list_files = list_of_files(prop, dir_list, logger)

    ice_name, x_name, y_name = _get_variable_names(prop.model_source)

    # Handle daily resolution
    if prop.ice_dt == 'daily' and not prop.dailyavg:
        cycle, hour = ('t12z', 'n006') if prop.whichcast == 'nowcast' else ('t06z', 'f006')
        if prop.model_source == 'schism':
            cycle = 't12z'
            hour = 'out2d'
        list_files = [f for f in list_files if cycle in f and hour in f]

    # Process daily averages if requested
    if prop.dailyavg and prop.ice_dt == 'daily':
        list_days = get_days_between_dates(
            datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ'),
            datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'),
        )
        icecover_m, lon_m, lat_m, time_m = _process_daily_composite(prop, logger, list_files, list_days, ice_name, x_name, y_name)
        logger.info('Finished model daily averages!')
        return icecover_m, lon_m, lat_m, time_m

    # Load model files
    if len(list_files) == 0:
        logger.error('No model files to load!')
        sys.exit()

    logger.info('Model %s **icecover** netcdf files for %s found for period %s to %s', prop.whichcast, prop.ofs, prop.startdate, prop.enddate)
    concated_model = intake_scisa.intake_model(list_files, prop, logger)

    # Parse model output

    lon_m = np.asarray(concated_model.variables[x_name][:])
    lat_m = np.asarray(concated_model.variables[y_name][:])

    if prop.model_source == 'schism':
        transformer = Transformer.from_crs('EPSG:3174', 'EPSG:4326', always_xy=True)
        lon_m, lat_m = transformer.transform(lon_m, lat_m)
    else:
        lon_m = lon_m - 360

    try:
        icecover_m = np.asarray(concated_model.variables[ice_name][:])
    except KeyError:
        logger.error('No modeled ice concentration available! Abort')
        sys.exit(-1)

    time_m = np.asarray(concated_model.variables['time'][:])

    # Handle SCHISM daily extraction
    if prop.model_source == 'schism' and prop.ice_dt == 'daily':
        hourly_indices = np.where((time_m.astype('datetime64[h]').view('i8') % 24) == 12)
        time_m = time_m[hourly_indices]
        icecover_m = icecover_m[hourly_indices]

    return icecover_m, lon_m, lat_m, time_m


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python get_icecover_fvcom.py',
        description='Retrieve and load GLOFS ice concentration from FVCOM',
    )
    parser.add_argument('-o', '--OFS', required=True, help='OFS model name')
    parser.add_argument('-p', '--Path', required=False, help='Path to data')
    parser.add_argument('-s', '--StartDate', required=True, help='Start Date YYYY-MM-DDThh:mm:ssZ')
    parser.add_argument('-e', '--EndDate', required=True, help='End Date YYYY-MM-DDThh:mm:ssZ')
    parser.add_argument('-w', '--Whichcasts', required=False, help='nowcast, forecast_a, forecast_b')
    parser.add_argument('-f', '--Forecast_Hr', required=False, help="'02hr', '06hr', '12hr', '24hr'")

    parser.add_argument(
        '-c', '--config',
        required=False,
        help='Path to configuration file (default: conf/ofs_dps.conf)')
    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.config_file = args.config
    prop1.ofs_extents_path = args.Path + 'ofs_extents/'
    prop1.start_date_full = args.StartDate
    prop1.end_date_full = args.EndDate
    prop1.whichcast = args.Whichcasts
    prop1.model_source = model_source.model_source(args.OFS)
    prop1.forecast_hr = args.Forecast_Hr if prop1.whichcast == 'forecast_a' else None

    get_icecover_model(prop1, None)
