"""
 This is the final skill assessment script.
 This function reads the obs and ofs control files, search for the
 respective data and creates the paired (.int) datasets and skill table.
"""

import argparse
import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ofs_skill.model_processing import get_forecast_hours, model_properties
from ofs_skill.model_processing.get_node_ofs import get_node_ofs
from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.get_station_observations import get_station_observations
from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.skill_assessment import format_paired_one_d, make_skill_maps, metrics_paired_one_d


def ofs_ctlfile_extract(prop, name_var, logger):
    """
    Extract info from model control files. If control file does not exist,
    create it.
    """

    if prop.ofsfiletype == 'fields':
        ctl_path = os.path.join(prop.control_files_path,
                                str(prop.ofs+'_'+name_var+'_model.ctl'))
        if (
            os.path.isfile(ctl_path
            )
            is False
        ):
            get_node_ofs(prop, logger)
    elif prop.ofsfiletype == 'stations':
        ctl_path = os.path.join(prop.control_files_path,
                            str(prop.ofs+'_'+name_var+'_model_station.ctl'))
        if (
            os.path.isfile(ctl_path
            )
            is False
        ):
            get_node_ofs(prop, logger)

    try:
        if (os.path.getsize(ctl_path) > 0):
            with open(#ctl_path
                file=ctl_path,
                encoding='utf-8',
            ) as file:
                read_ofs_ctl_file = file.read()

                lines = read_ofs_ctl_file.split('\n')
                lines = [i.split(' ') for i in lines]
                lines = [list(filter(None, i)) for i in lines]

                nodes = np.array(lines[:-1])[:, 0]
                nodes = [int(i) for i in nodes]

                depths = np.array(lines[:-1])[:,1]
                # this is the index of the nearest siglay to the
                # observations station
                depths = [int(i) for i in depths]

                shifts = np.array(lines[:-1])[:, -1]
                # this is the shift that can be applied to the ofs timeseries,
                # for instance if there is a known bias in the model
                shifts = [float(i) for i in shifts]

                ids = np.array(lines[:-1])[:, -2]
                ids = [str(i) for i in ids]

                return lines, nodes, depths, shifts, ids
    except FileNotFoundError:
        logger.warning(
            '%s model control file is missing.', name_var)
    return None


def prepare_series(read_station_ctl_file, read_ofs_ctl_file, prop,
                   name_var, i, obs_row, logger):
    """
    This is creating the paired (model and obs) timeseries used in the skill
    assessment using the .prd and .obs text files.
    """
    formatted_series = 'NoDataFound'
    obs_df = None
    ofs_df = None

    if read_station_ctl_file[0][obs_row][0] == read_ofs_ctl_file[-1][i]:
        obs_path = os.path.join(prop.data_observations_1d_station_path,
                str(read_station_ctl_file[0][obs_row][0]+'_'+prop.ofs+'_'+name_var+\
                    '_station.obs'))

        if os.path.isfile(obs_path):
            if os.path.getsize(obs_path) > 0:
                obs_df = pd.read_csv(obs_path,
                    sep=r'\s+',
                    header=None,
                )
            else:
                logger.error(
                    '%s/%s_%s_%s_station.obs is empty',
                    prop.data_observations_1d_station_path,
                    read_station_ctl_file[0][obs_row][0],
                    prop.ofs,
                    name_var,
                )
                return formatted_series
        else:
            logger.error(
                '%s/%s_%s_%s_station.obs is missing',
                prop.data_observations_1d_station_path,
                read_station_ctl_file[0][obs_row][0],
                prop.ofs,
                name_var,
            )
        if prop.whichcast == 'forecast_a':
            prdfile = str(read_ofs_ctl_file[-1][i]) +\
                '_'+prop.ofs+'_'+name_var+'_'+str(read_ofs_ctl_file[1][i]) +\
                '_'+prop.whichcast+'_'+str(prop.forecast_hr)+\
                '_'+str(prop.ofsfiletype)+'_model.prd'

            prd_path = os.path.join(prop.data_model_1d_node_path,prdfile)
            if os.path.isfile(prd_path):
                ofs_df = pd.read_csv(prd_path,
                    sep=r'\s+',
                    header=None,
                )
            else:
                logger.error(
                    '%s/%s_%s_%s_%s_%s_%s_%s_model.prd is missing',
                    prop.data_model_1d_node_path,
                    read_ofs_ctl_file[-1][i],
                    prop.ofs,
                    name_var,
                    read_ofs_ctl_file[1][i],
                    prop.whichcast,
                    prop.forecast_hr,
                    prop.ofsfiletype
                )
        else:
            prd_path = os.path.join(prop.data_model_1d_node_path,
                    str(read_ofs_ctl_file[-1][i]+'_'+prop.ofs+'_'+name_var
                    +'_'+str(read_ofs_ctl_file[1][i])+'_'+prop.whichcast+\
                    '_'+prop.ofsfiletype+'_model.prd'))
            if os.path.isfile(prd_path) is False :
                logger.info(
                    '%s/%s_%s_%s_%s_%s_%s_model.prd is missing',
                    prop.data_model_1d_node_path,
                    read_ofs_ctl_file[-1][i],
                    prop.ofs,
                    name_var,
                    read_ofs_ctl_file[1][i],
                    prop.whichcast,
                    prop.ofsfiletype
                )
                logger.info(
                    'Calling OFS module for %s',
                    prop.whichcast,
                )
                get_node_ofs(prop, logger)

            ofs_df = pd.read_csv(prd_path,
                sep=r'\s+',
                header=None,
                )

        if (
            ofs_df is not None
            and obs_df is not None
            and len(obs_df) > 0
            and len(ofs_df) > 0
        ):
            if name_var == 'cu':
                formatted_series = format_paired_one_d.paired_vector(
                    obs_df, ofs_df, prop.start_date_full, prop.end_date_full,
                    logger
                )
            else:
                formatted_series = format_paired_one_d.paired_scalar(
                    obs_df, ofs_df, prop.start_date_full, prop.end_date_full,
                    logger
                )

    return formatted_series


def skill(read_station_ctl_file, read_ofs_ctl_file, prop, name_var, logger):
    """
    this function 1) writes the paired observation and model time series to
    file (.int), and 2) sends the paired time series to
    metrics_paired_one_d to calculate skill stats, which returned from the
    function.
    """

    output = {
        'station_id': [],
        'X': [],
        'Y': [],
        'obs_depth': [],
        'mod_depth': [],
        'node': [],
        'skill': []
              }

    data_length = len(read_station_ctl_file[0])
    if len(read_ofs_ctl_file[-1]) < data_length:
        data_length = len(read_ofs_ctl_file[-1])

    for i in range(0, data_length):
        # First, match rows using station ID between model and obs control
        # files
        try:
            obs_row = [y[0] for y in read_station_ctl_file[0]].\
                index(read_ofs_ctl_file[-1][i])
            if read_station_ctl_file[0][obs_row][0] != \
                read_ofs_ctl_file[-1][i]:
                raise Exception
        except Exception:
            logger.error('Could not match station ID %s between control '
                         'file in get_node_ofs!', read_ofs_ctl_file[-1][i])
            sys.exit(-1)

        # Now continue formatting paired series
        formatted_series = prepare_series(
            read_station_ctl_file, read_ofs_ctl_file, prop,
            name_var, i, obs_row, logger
        )
        if (
            formatted_series is not None
            and formatted_series != 'NoDataFound'
            and len(formatted_series[0]) > 1
        ):
            if name_var == 'cu':
                # stats = metrics_paired_one_d.skill_vector
                # (formatted_series[-1])
                logger.info('Start cu metrics for %s',
                            read_station_ctl_file[0][obs_row][0])
                output['station_id'].append(
                    read_station_ctl_file[0][obs_row][0])
                output['node'].append(
                    read_ofs_ctl_file[1][i])
                output['obs_depth'].append(
                    read_station_ctl_file[1][obs_row][-2])
                output['mod_depth'].append(
                    read_ofs_ctl_file[-2][i])
                output['X'].append(
                    read_station_ctl_file[1][obs_row][1])
                output['Y'].append(
                    read_station_ctl_file[1][obs_row][0])
                output['skill'].append(
                    metrics_paired_one_d.skill_vector(
                        formatted_series[-1], name_var,
                        prop, logger
                    )
                )

            else:
                # stats = metrics_paired_one_d.skill_scalar
                # (formatted_series[-1])
                logger.info('Start %s metrics for %s',
                            name_var,
                            read_station_ctl_file[0][obs_row][0])
                output['station_id'].append(
                    read_station_ctl_file[0][obs_row][0])
                output['node'].append(
                    read_ofs_ctl_file[1][i])
                output['obs_depth'].append(
                    read_station_ctl_file[1][obs_row][-2])
                output['mod_depth'].append(
                    read_ofs_ctl_file[-2][i])
                #temp_x = str(float(
                    #read_station_ctl_file[1][i][1])+360)
                temp_x = str(float(
                    read_station_ctl_file[1][obs_row][1]))
                output['X'].append(temp_x)
                output['Y'].append(
                    read_station_ctl_file[1][obs_row][0])
                output['skill'].append(
                    metrics_paired_one_d.skill_scalar(
                        formatted_series[-1], name_var,
                        prop, logger
                    )
                )

            # This section writes the paired time series file with all the
            # data found and formatted in the ctl_file list
            # edited to add header for human readability - AJK
            int_path = os.path.join(prop.data_skill_1d_pair_path,
                        str(prop.ofs+'_'+name_var+'_'+read_station_ctl_file[0][obs_row][0]
                        +'_'+str(read_ofs_ctl_file[1][i])+'_'+prop.whichcast+
                        '_'+prop.ofsfiletype+'_pair.int'))
            with open(int_path, 'w', encoding='utf-8') as output_2:
                if  name_var == 'cu': #if cu file
                    output_2.write('DNUM_JAN1 '+'YEAR '+'MONTH '+'DAY '
                        +'HOUR '+'MINUTE '+'SPEED_OB '+'SPEED_MODEL '+'BIAS_SPEED '
                        +'DIR_OB '+'DIR_MODEL '+'BIAS_DIR '+'\n')
                else: #if not cu file
                    output_2.write('DNUM_JAN1 '+'YEAR '+'MONTH '+'DAY '
                        +'HOUR '+'MINUTE '+'VAL_OB '+'VAL_MODEL '+'BIAS '+'\n')
                for p_value in formatted_series[0]:
                    p_value = str(p_value)
                    p_value = p_value.replace(',', ' ')
                    p_value = p_value.replace('[', '')
                    p_value = p_value.replace(']', '')
                    output_2.write(p_value + '\n')
            logger.info(
                '%s_%s_%s_%s_%s_%s_pair.int is created successfully',
                prop.ofs, name_var, read_station_ctl_file[0][obs_row][0],
                read_ofs_ctl_file[1][i], prop.whichcast, prop.ofsfiletype)
        else:
            logger.error(
                '%s_%s_%s_%s_%s_%s_pair.int is not created successfully',
                prop.ofs,
                name_var,
                read_station_ctl_file[0][obs_row][0],
                read_ofs_ctl_file[1][i],
                prop.whichcast,
                prop.ofsfiletype
            )

    return output


def name_convent(variable):
    """
    Set variable names so they correspond to names used in model output data
    """
    name_var = []
    if variable == 'water_level':
        name_var = 'wl'

    elif variable == 'water_temperature':
        name_var = 'temp'

    elif variable == 'salinity':
        name_var = 'salt'

    elif variable == 'currents':
        name_var = 'cu'

    return name_var


def get_skill(prop, logger):
    """
 This is the final skill assessment script.
 This function reads the obs and ofs control files, search for the
 respective data and creates the paired (.int) datasets and skill table.
    """

    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting skill assessment process ---')

    dir_params = utils.Utils().read_config_section('directories', logger)
    prop.datum_list = (utils.Utils().read_config_section('datums', logger)\
                       ['datum_list']).split(' ')

    try:
        start_date = datetime.strptime(prop.start_date_full,'%Y%m%d-%H:%M:%S')
        end_date = datetime.strptime(prop.end_date_full,'%Y%m%d-%H:%M:%S')
        prop.start_date_full = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        prop.end_date_full = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    except ValueError:
        pass

    # Start Date and End Date validation
    try:
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (
            f'Please check Start Date - '
            f'{prop.start_date_full}, End Date - '
            f'{prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
        prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ'
    ) > datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'):
        error_message = (
            f'End Date {prop.end_date_full} '
            f'is before Start Date {prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = Path(dir_params['home'])

    # prop.path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. '
            f'Please check prop.path - {prop.path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        error_message = (
            f'Shapefile {prop.ofs} is not found at '
            f'the folder {ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.whichcast validation
    if (prop.whichcast is not None) and (
        prop.whichcast not in ['nowcast', 'forecast_a', 'forecast_b']
    ):
        error_message = f'Please check prop.whichcast - ' \
                        f'{prop.whichcast}. Abort!'

        logger.error(error_message)
        sys.exit(-1)

    if prop.whichcast == 'forecast_a' and prop.forecast_hr is None:
        error_message = (
            'prop.forecast_hr is required if prop.whichcast is '
            'forecast_a. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir']
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['1d_station_dir'],
    )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['model_dir'],
        dir_params['1d_node_dir'],
    )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['skill_dir'],
        dir_params['1d_pair_dir'],
    )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_1d_table_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['skill_dir'],
        dir_params['stats_dir'],
    )
    os.makedirs(prop.data_skill_1d_table_path, exist_ok=True)

    # This outer loop is used to download all data for all variables
    # Inside this loop there is another loop that will go over each line
    # in the station ctl file and will try to download the data from TandC,
    # USGS, and NDBC based on the station data source

    for variable in prop.var_list:

        name_var = name_convent(variable)

        # =================================================================
        # This will try to read the station ctl file for the given ofs and
        # for all
        # variables. If not found then it will create it using
        # get_station_observations.py
        # =================================================================
        logger.info('Searching for the %s %s station ctl files',
                    prop.ofs, variable)
        ctl_path = os.path.join(prop.control_files_path,str(prop.ofs+'_'+\
                                name_var+'_station.ctl'))
        if os.path.isfile(ctl_path) is False:
            logger.info(
                'Station ctl file not found. Creating station '
                'ctl file!. This might take a couple of minutes'
            )
            get_station_observations(prop, logger)
        read_station_ctl_file = \
            station_ctl_file_extract(ctl_path)
        if read_station_ctl_file is not None:
            logger.info(
                'Station ctl file (%s_%s_station.ctl) found in "%s/". '
                'If you instead want to create a new Inventory file, '
                'please change the name/delete the current %s_%s_station.ctl',
                prop.ofs,
                name_var,
                prop.control_files_path,
                prop.ofs,
                name_var,
            )
    ######## Checking for the .obs files:
            for i in range(0, len(read_station_ctl_file[0])):
                obs_path = os.path.join(prop.data_observations_1d_station_path,
                        str(read_station_ctl_file[0][i][0]+'_'+prop.ofs+'_'+\
                            name_var+'_station.obs'))
                if os.path.isfile(obs_path):
                    if os.path.getsize(obs_path)> 0:
                        logger.info(
                            '%s/%s_%s_%s_station.obs found',
                            prop.data_observations_1d_station_path,
                            read_station_ctl_file[0][i][0],
                            prop.ofs,
                            name_var,
                        )
                    else:
                        logger.error(
                            '%s/%s_%s_%s_station.obs is empty',
                            prop.data_observations_1d_station_path,
                            read_station_ctl_file[0][i][0],
                            prop.ofs,
                            name_var,
                        )

                else:
                    logger.error(
                        '%s/%s_%s_%s_station.obs is missing, calling Obs Module',
                        prop.data_observations_1d_station_path,
                        read_station_ctl_file[0][i][0],
                        prop.ofs,
                        name_var,
                    )

                    get_station_observations(
                        prop, logger)
                    break

        else:
            logger.info('Observation ctl file for %s and %s is empty.',
            prop.ofs,
            name_var)
            continue


        logger.info('Searching for the %s %s model control files',
                    prop.ofs, variable)
        read_ofs_ctl_file = ofs_ctlfile_extract(
            prop, name_var, logger
        )  # lines, nodes, depths, shifts, ids
        if read_ofs_ctl_file is not None:
            ######## Checking for the .prd files:
            for i in range(0, len(read_ofs_ctl_file[-1])):
                if prop.whichcast == 'forecast_a':
                    if os.path.isfile(
                        f'{prop.data_model_1d_node_path}/'
                        f'{read_ofs_ctl_file[-1][i]}_{prop.ofs}_{name_var}_'
                        f'{read_ofs_ctl_file[1][i]}_{prop.whichcast}_'
                        f'{prop.forecast_hr}_{prop.ofsfiletype}_model.prd'
                    ) is False:
                        logger.error(
                            '%s/%s_%s_%s_%s_%s_%s_%s_model.prd is missing',
                            prop.data_model_1d_node_path,
                            read_ofs_ctl_file[-1][i],
                            prop.ofs,
                            name_var,
                            read_ofs_ctl_file[1][i],
                            prop.whichcast,
                            prop.forecast_hr,
                            prop.ofsfiletype
                        )
                        logger.info(
                            'Calling OFS module for %s',
                            prop.whichcast,
                        )
                        get_node_ofs(prop, logger)
                        break
                else:
                    if os.path.isfile(
                        f'{prop.data_model_1d_node_path}/'
                        f'{read_ofs_ctl_file[-1][i]}_{prop.ofs}_{name_var}_'
                        f'{read_ofs_ctl_file[1][i]}_{prop.whichcast}_'
                        f'{prop.ofsfiletype}_model.prd'
                    ) is False:
                        logger.info(
                            '%s/%s_%s_%s_%s_%s_%s_model.prd is missing',
                            prop.data_model_1d_node_path,
                            read_ofs_ctl_file[-1][i],
                            prop.ofs,
                            name_var,
                            read_ofs_ctl_file[1][i],
                            prop.whichcast,
                            prop.ofsfiletype
                        )
                        logger.info(
                            'Calling OFS module for %s',
                            prop.whichcast,
                        )
                        get_node_ofs(prop, logger)
                        break
        else:
            logger.info('Model ctl file for %s and %s is empty.',
            prop.ofs,
            name_var)

        if read_ofs_ctl_file is not None:
            skill_results = skill(
                read_station_ctl_file, read_ofs_ctl_file, prop,
                name_var, logger
            )

            if (
                len(skill_results.get('station_id')) != 0
                and len(skill_results.get('node')) != 0
                and len(skill_results.get('X')) != 0
                and len(skill_results.get('Y')) != 0
                and len(skill_results.get('skill')) != 0
            ):


                #Make overview maps and save them
                make_skill_maps.make_skill_maps(skill_results,
                                                prop, name_var,
                                                logger)
                if name_var == 'wl':
                    tabledatum = prop.datum
                else:
                    tabledatum= None

                pd.DataFrame(
                    {
                        'ID': skill_results['station_id'],
                        'NODE': skill_results['node'],
                        'obs_water_depth': skill_results['obs_depth'],
                        'mod_water_depth': skill_results['mod_depth'],
                        'rmse': list(zip(*skill_results['skill']))[0],
                        'r': list(zip(*skill_results['skill']))[1],
                        'bias': list(zip(*skill_results['skill']))[2],
                        'bias_perc': list(zip(*skill_results['skill']))[3],
                        'bias_dir': list(zip(*skill_results['skill']))[4],
                        'central_freq': list(zip(*skill_results['skill']))[5],
                        'central_freq_pass_fail': list(zip(*skill_results['skill']))[6],
                        'pos_outlier_freq': list(zip(*skill_results['skill']))[7],
                        'pos_outlier_freq_pass_fail': list(zip(*skill_results['skill']))[8],
                        'neg_outlier_freq': list(zip(*skill_results['skill']))[9],
                        'neg_outlier_freq_pass_fail': list(zip(*skill_results['skill']))[10],
                        'bias_standard_dev': list(zip(*skill_results['skill']))[11],
                        'target_error_range': list(zip(*skill_results['skill']))[12],
                        'datum': tabledatum,
                        'Y': skill_results['Y'],
                        'X': skill_results['X'],
                        'start_date': prop.start_date_full,
                        'end_date': prop.end_date_full,
                    }
                ).to_csv(
                    r'' + f'{prop.data_skill_1d_table_path}/'
                          f'skill_{prop.ofs}_'
                    f'{variable}_{prop.whichcast}_{prop.ofsfiletype}.csv'
                )

                logger.info(
                    'Summary skill table for prop.ofs %s and variable %s '
                    'is created successfully',
                    prop.ofs,
                    variable,
                )

            else:
                logger.error(
                    'Fail to create summary skill table for OFS: %s and '
                    'variable: %s',
                    prop.ofs,
                    variable,
                )
        else:
            logger.error(
                'Fail to create summary skill table for OFS: %s and '
                'variable: %s',
                prop.ofs,
                variable,
            )

    # Now collect forecast horizon time series, if ya want!
    if (prop.horizonskill == True and
        prop.whichcast == 'forecast_b'):
        # Get all model time series, and put them in a big 'ol CSV file
        # Check to see if this has already been done
        logger.info('Starting forecast horizon skill! This is going to '
                    'take a while...\n')
        try:
            get_forecast_hours.make_horizon_series(prop, logger)
            # Now get obs time series and put that in the CSV file, too.
            get_forecast_hours.merge_obs_series_scalar(prop, logger)
        except Exception as e_x:
            logger.error('Exception caught in get_forecast_hours after '
                         'calling it from get_skill. Error: %s', e_x)
        prop.horizonskill = False
        logger.info('Completed forecast horizon skill!')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python get_skill.py', usage='%(prog)s',
        description='Run skill assessment'
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the prop.ofs_Extents folder, '
        'you can also create your own shapefile, add it to the '
        'prop.ofs_Extents folder and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=False,
        help='Use /home as the default. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e',
        '--EndDate_full',
        required=True,
        help="End Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-w',
        '--Whichcast',
        required=False,
        help='nowcast, forecast_a, '
             'forecast_b(it is the forecast between cycles)',
    )
    parser.add_argument(
        '-d',
        '--Datum',
        required=True,
        help="datum: 'MHHW', 'MHW', 'MLW', 'MLLW', 'NAVD88', 'XGEOID20B', 'IGLD85','LWD'"
    )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ",
    )
    parser.add_argument(
        '-t', '--FileType', required=True,
        help="OFS output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        help="'CO-OPS', 'NDBC', 'USGS',", )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.Whichcast
    prop1.datum = args.Datum.upper()
    prop1.ofsfiletype = args.FileType.lower()

    ''' Make all station owners default, unless user specifies station owners '''
    if args.Station_Owner is None:
        prop1.stationowner = ['co-ops','ndbc','usgs']
    elif args.FileType is not None:
        prop1.stationowner = args.Station_Owner.lower()
    else:
        print('Check station owner argument! Abort.')
        sys.exit(-1)

    prop1.forecast_hr = None
    if prop1.whichcast == 'forecast_a':
        prop1.forecast_hr = args.Forecast_Hr

    get_skill(prop1, None)
