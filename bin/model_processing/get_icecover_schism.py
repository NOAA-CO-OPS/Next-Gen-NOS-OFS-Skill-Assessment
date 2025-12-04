"""
Called by do_iceskill.py to list model files and then use intake lazy load them.
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
from obs_retrieval import utils
from model_processing import model_source
from model_processing import model_properties

import numpy as np


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


def list_of_loofs_files(prop, logger):
    '''
    Returns a list of SCHISM model ice files for LOOFS
    '''
    list_days = get_days_between_dates(
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ'),
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'),
    )
    # Build directory paths
    list_files = []
    hours_list = ['00', '06', '12', '18']
    for i, date in enumerate(list_days):
        year = date.year
        month = date.month
        day = date.day
        for i in range(len(hours_list)):
            model_file = f'{prop.model_path_schism}/{year}/{month:02}{day:02}{hours_list:02}/out/out2d_1.nc'
            # Check if file exists
            if os.path.exists(model_file) and os.path.isfile(model_file):
                list_files.append(model_file)
    return list_files


def get_icecover_schism(prop, logger):
    """
    Load and parse model ice conc data from netcdf files. Return model ice
    conc, lat, lon, and time.
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

    logger.info('--- Starting OFS SCHISM ice cover process ---')

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
    # Needs to be custom for loofs ice SCHISM
    #################################
    list_files = list_of_loofs_files(prop, logger)
    #################################

    if len(list_files) > 0:
        logger.info(
            'Model %s **icecover** netcdf files for the OFS %s found for the '
            'period from %s to %s',
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

    # -- Get lat, lon and ice cover from SCHISM output (model)
    lon_m = np.asarray(concated_model.variables['SCHISM_hgrid_node_x'][:])
    lat_m = np.asarray(concated_model.variables['SCHISM_hgrid_node_y'][:])
    try:
        icecover_m = np.asarray(concated_model.variables['iceTracer_2'][:])
    except:
        logger.error('No modeled ice concentration available! Abort')
        sys.exit(-1)
    time_m = np.asarray(concated_model.variables['time'][:])
    # depth = np.array(concated_model.variables['h'][:])
    return icecover_m, lon_m, lat_m, time_m


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python get_icecover_schism.py',
        usage='%(prog)s',
        description='Retrieve and load GLOFS ice concentration from SCHISM',
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

    get_icecover_schism(prop1, None)
