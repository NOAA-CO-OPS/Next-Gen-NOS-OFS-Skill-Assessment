"""
-*- coding: utf-8 -*-

Documentation for Scripts create_2dplot.py

Directory Location:   /path/to/ofs_dps/server/bin/visualization

Technical Contact(s): Name:  AJK & PWL

This is the main script of the 2d visualizations module.

Language:  Python 3.11

Estimated Execution Time: <5 min

usage: python bin/visualization/create_2dplot.py -s 2023-11-29T00:00:00Z -e
2023-11-30T00:00:00Z -p ./ -o cbofs -cs -ws Forecast_b

Arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Choose from the list on the ofs_Extents folder, you
                        can also create your own shapefile, add it top the
                        ofs_Extents folder and call it here
  -p PATH, --path PATH  Path to home
  -s STARTDATE_FULL, --StartDate_full STARTDATE_FULL
                        Start Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20220115-05:05:05'
  -e ENDDATE_FULL, --EndDate_full ENDDATE_FULL
                        End Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20230808-05:05:05'
  -ws whichcast, --Whichcast
                       'Nowcast', 'Forecast_A', 'Forecast_B'

Author Name:  FC       Creation Date:  03/19/2024

Revisions:
    Date          Author             Description
    09/2024       AJK                Added leaflet contour plot output
    11/2024       PWL                Added 2D stats module(s), new plotting,
                                     new stats table
    03/2025       AJK                Updates for intake
    08/2025       AJK                Updates for SPoRT

"""
import argparse
import json
import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ofs_skill.model_processing import (
    check_model_files,
    get_model_source,
    intake_model,
    list_of_dir,
    list_of_files,
    model_properties,
)
from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization import plotting_2d


def validate_and_initialize_parameters(prop):
    """
    Validates input parameters, sets up logger, config paths, and initializes
    directory paths for output. Also normalizes date strings for downstream use.

    Returns:
        prop (object): Updated prop with validated and derived paths and dates.
        logger (logging.Logger): Initialized logger.
    """
    # Setup logger
    config_file = utils.Utils().get_config_file()
    log_config_file = (Path(__file__).parent.parent.parent / 'conf' / 'logging.conf').resolve()

    if not os.path.isfile(log_config_file):
        sys.exit('Logging config file not found. Abort!')
    if not os.path.isfile(config_file):
        sys.exit('Main config file not found. Abort!')

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using config %s', config_file)
    logger.info('Using log config %s', log_config_file)
    logger.info('--- Starting Visualization Process ---')

    # Load directory parameters
    dir_params = utils.Utils().read_config_section('directories', logger)

    # Date validation
    try:
        start_dt = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        logger.error('Invalid date format. Start: %s,\
            End: %s. Abort!',prop.start_date_full, prop.end_date_full)
        sys.exit(-1)

    if start_dt > end_dt:
        logger.error('End Date %s is before Start Date\
            %s. Abort!', prop.end_date_full, prop.start_date_full)
        sys.exit(-1)

    # Path and file validation
    if prop.path is None:
        prop.path = dir_params['home']

    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        logger.error('ofs_extents/ folder is not found at %s. Abort!',prop.path)
        sys.exit(-1)

    shapefile = os.path.join(ofs_extents_path, f'{prop.ofs}.shp')
    if not os.path.isfile(shapefile):
        logger.error("Shapefile '%s' is not found at %s. Abort!", prop.ofs, ofs_extents_path)
        sys.exit(-1)

    # Whichcast validation
    prop.whichcasts = prop.whichcasts.replace('[', '')
    prop.whichcasts = prop.whichcasts.replace(']', '')
    prop.whichcasts = prop.whichcasts.split(',')
    for whichcast in prop.whichcasts:
        if whichcast not in {'nowcast', 'forecast_a', 'forecast_b'}:
            logger.error("Invalid whichcast value: '%s'. Abort!",prop.whichcasts)
            sys.exit(-1)

    if prop.whichcasts == 'forecast_a' and prop.forecast_hr is None:
        logger.error('Forecast_Hr is required if Whichcast is forecast_a. Abort!')
        sys.exit(-1)

    # Create necessary directories
    prop.data_observations_2d_satellite_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['observations_dir'],\
        dir_params['2d_satellite_dir']
    )
    prop.visuals_2d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir']
    )
    prop.data_skill_2d_json_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'], '2d'
    )
    prop.data_observations_2d_json_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['observations_dir'], '2d'
    )
    prop.data_model_2d_json_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'], '2d'
    )
    prop.data_skill_2d_table_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'], dir_params['stats_dir']
    )
    prop.data_skill_2d_px_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        '2d', 'plotly_maps'
    )

    for directory in [
        prop.data_observations_2d_satellite_path,
        prop.visuals_2d_station_path,
        prop.data_skill_2d_json_path,
        prop.data_observations_2d_json_path,
        prop.data_model_2d_json_path,
        prop.data_skill_2d_table_path,
        prop.data_skill_2d_px_path
    ]:
        os.makedirs(directory, exist_ok=True)

    # Additional formatting for model path and date fields
    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    # Normalize date strings: "YYYYMMDD-HH:MM:SS"
    for attr in ['start_date_full', 'end_date_full']:
        value = getattr(prop, attr)
        value = value.replace('-', '').replace('Z', '').replace('T', '-')
        setattr(prop, attr, value)

    # Derive startdate and enddate in format: "YYYYMMDDHH"
    prop.startdate = (
        datetime.strptime(prop.start_date_full.split('-')[0], '%Y%m%d')
        .strftime('%Y%m%d') + '00'
    )
    prop.enddate = (
        datetime.strptime(prop.end_date_full.split('-')[0], '%Y%m%d')
        .strftime('%Y%m%d') + '23'
    )

    # Check if all model files exist!
    try:
        check_model_files(prop,
                         logger)
    except Exception as e_x:
        logger.error('Error caught in '
                     'check_model_files: %s', e_x)
        logger.info('Warning: could not verify if '
                    'all necessary model files '
                    'are present! Check final '
                    'time series for accuracy.')
    # Now check if satellite files exist
    datapath = Path(os.path.join(prop.path,'data',
                           'observations',
                           '2d_satellite',))
    l3cfile = Path(os.path.join(datapath,
                           str(prop.ofs+'.nc')))
    sportfile = Path(os.path.join(datapath,
                             str(prop.ofs + \
                                 '_sport.nc')))
    if os.path.exists(datapath):
        if os.path.isfile(l3cfile):
            logger.info('Found L3C data!')
            prop.l3c = True
        else:
            logger.error('No L3C data.')
            prop.l3c = False
        if os.path.isfile(sportfile):
            logger.info('Found SPoRT data!')
            prop.sport = True
        else:
            logger.error('No SPoRT data.')
            prop.sport = False
    if prop.l3c is False and prop.sport is False:
        logger.error('Both L3C and SPoRT satellite '
                     'data are missing! Exiting.')
        sys.exit(-1)

    return prop, logger


def write_2dskill_csv(prop1,stats,time_all,logger):
    '''Put stats into pandas dataframe, and write it to csv!
    [obs_mean, obs_std, mod_mean, mod_std, modobs_bias, modobs_bias_std,
                r_value, rmse, cf, pof, nof]

    '''
    # Pandas, go!
    ## Might need to reformat dates first
    # Make time array
    date_all = []
    for i in range(0,len(time_all)):
        date_all.append(datetime.strptime(time_all[i],'%Y%m%d-%Hz'))

    variable='temperature'
    stats = np.round(stats,decimals=2)
    pd.DataFrame(
        {
            'Date': date_all,
            'Obs mean': list(zip(*stats))[0],
            'Obs stdev': list(zip(*stats))[1],
            'Model mean': list(zip(*stats))[2],
            'Model stdev': list(zip(*stats))[3],
            'Bias': list(zip(*stats))[4],
            'Bias stdev': list(zip(*stats))[5],
            'R': list(zip(*stats))[6],
            'RMSE': list(zip(*stats))[7],
            'Central frequency (%)': list(zip(*stats))[8],
            'Negative outlier freq (%)': list(zip(*stats))[9],
            'Positive outlier freq (%)': list(zip(*stats))[10],
        }
    ).to_csv(
        r'' + f'{prop1.data_skill_2d_table_path}/'
              f'skill_2d_{prop1.ofs}_'
        f'{variable}_{prop1.whichcast}.csv'
    )

    logger.info(
        '2D summary skill table for %s and variable %s '
        'is created successfully',
        prop1.ofs,
        variable,
    )
    logger.info('Program complete!')

def get_intersection(list1,list2):
    '''this little guy gets the intersecting values & indices from list1
        compared to list2, and sorts them by date. This is used to make sure
        the obs and model data are paired correctly.
    '''
    # Get intersection and indices of intersecting values
    ind_dict = {k:i for i,k in enumerate(list1)}
    inter_values = set(ind_dict).intersection(list2)
    indices = [ind_dict[x] for x in inter_values]
    # Zip values and indices together for sorting
    tupfiles = tuple(zip(indices,inter_values))
    # Sort by date
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0])))
    # Unzip, get sorted values & index lists back
    inter_values_sort = list(zip(*tupfiles))[1]
    inter_values_sort = list(inter_values_sort)
    indices_sort = list(zip(*tupfiles))[0]
    indices_sort = list(indices_sort)
    # Give 'em back
    return indices_sort, inter_values_sort

def list_of_json_files(filepath, prop1, logger):
    '''Peek in JSON dirs and return sorted list of files'''
    all_files = os.listdir(filepath)
    if len(all_files) == 0:
        logger.error('The JSON directory (%s) is totally empty!', filepath)
        sys.exit(-1)
    spltstr = []
    files = []
    for af_name in all_files:
        if 'model' in af_name and 'daily' not in af_name and 'SPoRT' not in af_name and 'ssh' not in af_name and 'sss' not in af_name and 'ssu' not in af_name and 'ssv' not in af_name: # ignore daily avg and ssh, sss, ssu, and ssv
            if ((datetime.strptime(af_name.split('_')[1],'%Y%m%d-%Hz') >=
                  datetime.strptime(prop1.start_date_full, '%Y%m%d-%H:%M:%S'))
                and (datetime.strptime(af_name.split('_')[1],'%Y%m%d-%Hz') <=
                      datetime.strptime(prop1.end_date_full, '%Y%m%d-%H:%M:%S'))
                and af_name.split('_')[0] == prop1.ofs
                and prop1.whichcast in af_name.split('.')[-2]):
                spltstr.append(af_name.split('_')[1]) # Date info for sorting
                files.append(filepath + '/' + af_name) # Full file path
        #elif 'daily' not in af_name and 'SPoRT' not in af_name: #ignore daily avg
        elif 'model' not in af_name and 'daily' not in af_name and 'lnc' not in af_name: #ignore daily avg
            if ((datetime.strptime(af_name.split('_')[1],'%Y%m%d-%Hz') >=
                  datetime.strptime(prop1.start_date_full, '%Y%m%d-%H:%M:%S'))
                and (datetime.strptime(af_name.split('_')[1],'%Y%m%d-%Hz') <=
                      datetime.strptime(prop1.end_date_full, '%Y%m%d-%H:%M:%S'))
                and af_name.split('_')[0] == prop1.ofs):
                spltstr.append(af_name.split('_')[1]) # Date info for sorting
                files.append(filepath + '/' + af_name) # Full file path
    try:
        files[0]
    except IndexError:
        logger.error('No JSON files found in directory %s! Exiting.',
                     filepath)
        sys.exit(-1)

    # Sort file list
    tupfiles = tuple(zip(spltstr,files))
    # Sort by year, month, day, then hour
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-3:-1])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][6:8])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][4:6])))
    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:4])))

    # Unzip, get sorted file list back
    spltstr = list(zip(*tupfiles))[0]
    spltstr = list(spltstr)
    files = list(zip(*tupfiles))[1]
    files = list(files)

    return files, spltstr

def json_to_numpy(files,logger):
    '''Takes sorted file list of JSON files and converts to numpy.
    Needs to load files in correct (sorted) chronological order!!! Which is
    handled by function list_of_json_files'''
    z_all = []
    x = None
    y = None
    for index,value in enumerate(files):
        with open(value) as file:
            jsondata = json.load(file)
        if index == 0:
            x = np.array(jsondata['lons'],dtype=float)
            y = np.array(jsondata['lats'],dtype=float)
        z = np.array(jsondata['sst'],dtype=float)
        z_all.append(z)
    try:
        z_all = np.stack(z_all)
    except ValueError:
        logger.error("Can't stack arrays with different shapes!")
        sys.exit(-1)

    return x,y,z_all


if __name__ == '__main__':

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python write_obs_ctlfile.py',
        usage='%(prog)s',
        description='ofs write Station Control File',
    )
    parser.add_argument(
        '-o',
        '--ofs',
        required=True,
        help='Choose from the list on the ofs_Extents folder, you can also '
             'create your own shapefile, add it top the ofs_Extents folder and '
             'call it here',
    )
    parser.add_argument('-p', '--path', required=True,
                        help='Path to /opt/ofs_dps')
    parser.add_argument('-s', '--StartDate_full', required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument('-e', '--EndDate_full', required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument('-ws', '--whichcasts', required=True,
        help="whichcast: 'Nowcast', 'Forecast_A', 'Forecast_B'", )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    prop1.ofs_extents_path = r'' + prop1.path + 'ofs_extents' + '/'
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.whichcasts.lower()
    prop1.model_source = get_model_source(args.ofs)
    prop1.ofsfiletype='fields' #hardcoding - 2d always uses fields

    ''' Set up paths & assign to prop1, do date validation '''
    prop1, logger = validate_and_initialize_parameters(prop1)

    try:
        for i in prop1.whichcasts:
            prop1.whichcast = i.lower()
            logger.info('Running scripts for whichcast = %s',i)

            dir_list = list_of_dir(prop1,
                                  logger)
            list_files = list_of_files(prop1,
                                      dir_list,
                                      logger)
            logger.info('Calling intake_scisa from '
                        'create_2dplot.')
            model = intake_model(list_files,
                                prop1,
                                logger)
            logger.info('Returned from call to '
                        'intake_scisa inside of '
                        'create_2dplot.')
            satdatapath = Path(os.path.join(prop1.path,'data',
                                   'observations',
                                   '2d_satellite',))
            # Lazy import to avoid pyinterp dependency issues on Windows
            from ofs_skill.visualization import processing_2d

            if prop1.l3c:
                l3cfile = Path(os.path.join(satdatapath,
                                   str(prop1.ofs+'.nc')))
                processing_2d.parse_leaflet_json(model,
                                                 l3cfile,
                                                 prop1)
            if prop1.sport:
                sportfile = Path(os.path.join(satdatapath,
                                         str(prop1.ofs + \
                                             '_sport.nc')))
                processing_2d.parse_leaflet_json(model,
                                                 sportfile,
                                                 prop1)
            try:
                plotting_2d.plot_2d(prop1,logger)
            except Exception as e:
                logger.error('Problem calling plotting_2d.plot_2d - ABORT')
                logger.error('Exception: %s', e)

            logger.info('Finished 2D processing for %s', prop1.whichcast)

    except Exception as e:
        logger.error('Problem processing 2d files - ABORT')
        logger.error('Exception: %s', e)

    logger.info('Finished create_2d_plot.py!')
