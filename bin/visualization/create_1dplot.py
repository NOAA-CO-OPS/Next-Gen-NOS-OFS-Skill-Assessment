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
                       'NDBC', 'CO-OPS', 'USGS'
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
import gc
import logging
import logging.config
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

from ofs_skill.model_processing import (
    check_model_files,
    get_fcst_cycle,
    model_properties,
    parse_ofs_ctlfile,
    read_vdatum_from_bucket,
)
from ofs_skill.obs_retrieval import parse_arguments_to_list, utils
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
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

def create_1dplot_2nd_part(
        read_ofs_ctl_file, prop, var_info, logger):
    '''
    This is the function that actually creates the plots
    it had to be split from the original function due to size (PEP8)
    '''
    logger.info(
        f'Searching for paired dataset for {prop.ofs}, variable {var_info[0]}')

    # Read obs station ctl files
    try:
        read_station_ctl_file = station_ctl_file_extract(
            r'' + prop.control_files_path + '/' + prop.ofs + '_' + \
                var_info[1] + '_station.ctl'
        )
        logger.info(
            'Station ctl file (%s_%s_station.ctl) found in get_title. ',
            prop.ofs,
            var_info[1]
        )
    except FileNotFoundError:
        logger.error('Station ctl file not found.')
        sys.exit(-1)

    for i in range(len(read_ofs_ctl_file[1])):
        try:
            obs_row = [y[0] for y in read_station_ctl_file[0]].\
                index(read_ofs_ctl_file[-1][i])
            if read_station_ctl_file[0][obs_row][0] != \
                read_ofs_ctl_file[-1][i]:
                raise Exception
        except Exception:
            logger.error('Could not match station ID %s between control '
                         'file in get_node_ofs!', read_ofs_ctl_file[-1][i])
            continue
        now_fores_paired = []
        for cast in prop.whichcasts:
            paired_data = None
            # Here we try to open the paired data set, if not found, create.
            prop.whichcast = cast.lower()
            if (os.path.isfile(
                    f'{prop.data_skill_1d_pair_path}/'
                    f'{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_'
                    f'{read_ofs_ctl_file[1][i]}_{prop.whichcast}_'
                    f'{prop.ofsfiletype}_pair.int'
                ) is False):
                logger.error(
                    'Paired dataset (%s_%s_%s_%s_%s_%s_pair.int) not found in %s. ',
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast, prop.ofsfiletype,
                    prop.visuals_1d_station_path)
                logger.info(
                    'Calling Skill Assessment module for whichcast %s. ',
                    prop.whichcast
                )

                if prop.ofsfiletype == 'fields' or read_ofs_ctl_file[1][i] >= 0:
                    get_skill(prop, logger)


            if (os.path.isfile(
                    f'{prop.data_skill_1d_pair_path}/'
                    f'{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_'
                    f'{read_ofs_ctl_file[1][i]}_{prop.whichcast}_'
                    f'{prop.ofsfiletype}_pair.int'
                ) is False):
                logger.error(
                    'Paired dataset (%s_%s_%s_%s_%s_%s_pair.int) not found in %s. ',
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast, prop.ofsfiletype,
                    prop.visuals_1d_station_path)

            else:
                paired_data = pd.read_csv(
                    r'' + f'{prop.data_skill_1d_pair_path}/'
                    f'{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_'
                    f'{read_ofs_ctl_file[1][i]}_{prop.whichcast}_'
                    f'{prop.ofsfiletype}_pair.int',
                    sep=r'\s+', names=var_info[2],
                    header=0) #change to skip header for human readability
                #print(read_ofs_ctl_file[-1][i])
                paired_data['DateTime'] = pd.to_datetime(
                    paired_data[['year', 'month', 'day', 'hour', 'minute']])
                logger.info(
                    'Paired dataset (%s_%s_%s_%s_%s_%s_pair.int) found in %s',
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast, prop.ofsfiletype,
                    prop.visuals_1d_station_path)
            if paired_data is not None:
                # NEW! subsample time series if using 6-minute resolution from stations files
                deltat = (paired_data['DateTime'].iloc[-1] - paired_data['DateTime'].iloc[0]).days
                if (prop.ofsfiletype == 'stations'
                    and (deltat > 185 #or var_info[1] == 'cu'
                         )):
                    paired_data = paired_data.loc[paired_data.groupby(['year','month','day','hour'],
                                                                      observed=True)
                                                                    ['minute'].idxmin()]
                now_fores_paired.append(paired_data)

        if len(now_fores_paired) > 0:
            try:
                if var_info[1] == 'wl' or var_info[1] == 'temp' or var_info[
                    1] == 'salt':
                    logger.info(
                        'Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_%s_pair.int', var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop.whichcast, prop.ofsfiletype)
                    plotting_scalar.oned_scalar_plot(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][obs_row][2],
                         read_station_ctl_file[0][obs_row][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                elif var_info[1] == 'cu':
                    logger.info(
                        'Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_%s_pair.int', var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i],
                        read_ofs_ctl_file[1][i],
                        prop.whichcast,prop.ofsfiletype)
                    plotting_vector.oned_vector_plot1(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][obs_row][2],
                         read_station_ctl_file[0][obs_row][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)

                    logger.info(
                        'Trying to build wind rose %s plot for paired dataset: \
    %s_%s_%s_%s_%s_%s_pair.int', var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop.whichcast,prop.ofsfiletype)
                    plotting_vector.oned_vector_plot2b(
                        plotting_vector.oned_vector_plot2a(now_fores_paired, logger),
                        var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][obs_row][2],
                         read_station_ctl_file[0][obs_row][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop, logger)
                    if deltat <= -1:
                        logger.info(
                            'Trying to build stick %s plot for paired dataset: \
        %s_%s_%s_%s_%s_pair.int', var_info[0], prop.ofs, var_info[1],
                            read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                        plotting_vector.oned_vector_plot3(
                            now_fores_paired, var_info[1],
                            [read_ofs_ctl_file[-1][i],
                              read_station_ctl_file[0][obs_row][2],
                              read_station_ctl_file[0][obs_row][1].split('_')[-1]],
                            read_ofs_ctl_file[1][i],
                            prop,logger)
                        logger.info(
                            'Trying to build stick %s plot for vector difference: \
        %s_%s_%s_%s_%s_%s_pair.int', var_info[0], prop.ofs, var_info[1],
                            read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                            prop.whichcast, prop.ofsfiletype)
                        plotting_vector.oned_vector_diff_plot3(
                            now_fores_paired, var_info[1],
                            [read_ofs_ctl_file[-1][i],
                              read_station_ctl_file[0][obs_row][2],
                              read_station_ctl_file[0][obs_row][1].split('_')[-1]],
                            read_ofs_ctl_file[1][i],
                            prop,logger)
            except Exception as ex:
                logger.info(
                    'Fail to create the plot  \
    ---  %s ...Continuing to next plot', ex)

    # # Add output skill tables to sqlite historical database, if option is True
    # if prop.sqlite:
    #     # Convert start/end dates to correct sqlite format:
    #     # "YYYY-MM-DD HH:MM:SS.SSS"
    #     if 'T' in prop.start_date_full:
    #         start_date = prop.start_date_full.split('T')[0] + ' ' + \
    #                 prop.start_date_full.split('T')[1].rstrip('Z')
    #         end_date = prop.end_date_full.split('T')[0] + ' ' + \
    #                 prop.end_date_full.split('T')[1].rstrip('Z')
    #     else:
    #         start_date = prop.start_date_full.split('-')[0][0:4] + '-' +\
    #             prop.start_date_full.split('-')[0][4:6] + '-' +\
    #                 prop.start_date_full.split('-')[0][6:] + ' ' +\
    #                     prop.start_date_full.split('-')[1]
    #         end_date = prop.end_date_full.split('-')[0][0:4] + '-' +\
    #             prop.end_date_full.split('-')[0][4:6] + '-' +\
    #                 prop.end_date_full.split('-')[0][6:] + ' ' +\
    #                     prop.end_date_full.split('-')[1]
    #     # Make db path
    #     db_path = os.path.join(prop.path,'db',(prop.ofs + '.db'))
    #     # Make skill table file path
    #     insert_skill_stats.main([start_date, end_date],
    #                             prop.data_skill_stats_path,
    #                             db_path,
    #                             prop.sqlite)


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
    prop.ofsfiletype = prop1.ofsfiletype.lower()

    logger.info('Starting parameter validation...')

    # Do forecast_a start and end date reshuffle
    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is None:
            error_message = (
                'prop.forecast_hr is required if prop.whichcast is '
                'forecast_a. Abort!')
            logger.error(error_message)
            sys.exit(-1)
        elif prop.forecast_hr is not None:
            try:
                int(prop.forecast_hr[:-2])
            except ValueError:
                error_message = (f'Please check Forecast Hr format - '
                                 f'{prop.forecast_hr}. Abort!')
                logger.error(error_message)
                sys.exit(-1)
            if prop.forecast_hr[-2:] == 'hr':
                prop.start_date_full, prop.end_date_full =\
                get_fcst_cycle(prop.ofs,prop.start_date_full,prop.forecast_hr,logger)
                logger.info(f'Forecast_a: end date reassigned to '
                                 f'{prop.end_date_full}')
            else:
                error_message = (f'Please check Forecast Hr (hr) format - '
                                 f'{prop.forecast_hr}. Abort!')
                logger.error(error_message)
                sys.exit(-1)

    # Start Date and End Date validation
    # Enforce end date for whichcasts other than forecast_a
    if prop.end_date_full is None:
        print('If not using forecast_a, you must set an end date! Abort.')
        sys.exit(-1)
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
        sys.exit(-1)

    if datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ') > datetime.strptime(
        prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'):
        error_message = (f'End Date {prop.end_date_full} '
                         f'is before Start Date {prop.end_date_full}. Abort!')
        logger.error(error_message)
        sys.exit(-1)

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

    # Datum validations!
    if prop.datum not in prop.datum_list:
        logger.error('Datum %s is not valid! Switching to MLLW...', prop.datum)
        prop.datum = 'MLLW'
    # Check vdatum file to see if the requested datum is available for this OFS
    vdatums = read_vdatum_from_bucket(prop,logger)
    try:
        vdatums[f'{prop.datum.lower()}tomsl']
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
        logger.error('Failure checking for datum netcdf file on the NODD S3 '
                     'bucket! Datum conversions may fail. Continuing...')

    # Date-gate for forecast horizon functionality
    if ((datetime.strptime(prop.end_date_full,'%Y-%m-%dT%H:%M:%SZ')-
         datetime.strptime(prop.end_date_full,'%Y-%m-%dT%H:%M:%SZ')).days > 2
        and (prop.horizonskill == True)):
        logger.error('Time range of %s days is too long for forecast '
                    'horizon skill! Resetting forecast horizon skill argument '
                    'to False.',str(
                        (datetime.strptime(prop.end_date_full,\
                                           '%Y-%m-%dT%H:%M:%SZ')-
                         datetime.strptime(prop.end_date_full,\
                                           '%Y-%m-%dT%H:%M:%SZ')).days))
        prop.horizonskill = False
    # Cast-gate for nowcast horizon functionality
    if ('forecast_b' not in prop.whichcasts) and (prop.horizonskill == True):
        logger.error('Forecast horizon skill only works for forecast_b mode. '
                    'Resetting forecast horizon skill argument to False.')
        prop.horizonskill = False
    # file-gate for nowcast horizon functionality
    if (prop.ofsfiletype == 'fields') and (prop.horizonskill == True):
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
        prop.stationowner = 'co-ops,ndbc,usgs,list'

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
        logger.error('Error caught in check_model_files: %s', e_x)
        logger.info('Warning: could not verify if all necessary model files '
                    'are present! Check final time series for accuracy.')

    for variable in prop.var_list:
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
            prop, name_var, logger)

        if read_ofs_ctl_file is not None:
            create_1dplot_2nd_part(
                read_ofs_ctl_file, prop, var_info,
                logger)
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
        default='00hr',
        help='Specify model cycle to assess. Used with forecast_a mode only: '
        "'02hr', '06hr', '12hr', ... ", )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        default='co-ops,ndbc,usgs',
        help='Input station provider to use in skill assessment: '
        "'CO-OPS', 'NDBC', 'USGS',", )
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

    # Launch GUI to accept argument input if no required args are present
    if (args.OFS is None or
        args.StartDate_full is None
        or args.EndDate_full is None):
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
