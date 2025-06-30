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

from pathlib import Path
from datetime import datetime
import sys
import argparse
import logging
import logging.config
import os
import warnings
import pandas as pd
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from skill_assessment import get_skill
from obs_retrieval import (utils, station_ctl_file_extract)
from model_processing import (model_properties, get_fcst_cycle, parse_ofs_ctlfile)
from visualization import (plotting_scalar, plotting_vector)


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
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl"
    elif prop.ofsfiletype == 'stations':
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl"
    else:
        logger.error("Invalid OFS file type.")
        return None

    if not os.path.isfile(filename):
        for i in prop.whichcasts:
            prop.whichcast = i.lower()
            logger.info(f'Running scripts for whichcast = {i}')

            if prop.start_date_full.find('T') == -1:
                prop.start_date_full = prop.start_date_full_before
                prop.end_date_full = prop.end_date_full_before

            get_skill.get_skill(prop, logger)

    # If file exists, use method A to parse it
    if os.path.isfile(filename):
        if os.path.getsize(filename):
            return parse_ofs_ctlfile.parse_ofs_ctlfile(filename)
        else:
            logger.info("%s ctl file is blank!", name_var)
            logger.info("For GLOFS, salt and cu ctl files may be blank. "
                        "If running with a single station provider/owner, "
                        "ctl files may also be blank.")
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
        read_station_ctl_file = station_ctl_file_extract. \
            station_ctl_file_extract(
            r"" + prop.control_files_path + "/" + prop.ofs + "_" + \
                var_info[1] + "_station.ctl"
        )
        logger.info(
            "Station ctl file (%s_%s_station.ctl) found in get_title. ",
            prop.ofs,
            var_info[1]
        )
    except FileNotFoundError:
        logger.error("Station ctl file not found.")
        sys.exit(-1)

    for i in range(len(read_ofs_ctl_file[1])):
        now_fores_paired = []
        for cast in prop.whichcasts:
            paired_data = None
            # Here we try to open the paired data set, if not found, create.
            prop.whichcast = cast.lower()
            if (os.path.isfile(
                    f"{prop.data_skill_1d_pair_path}/"
                    f"{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_"
                    f"{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int"
                ) is False):
                logger.error(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) not found in %s. ",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast,
                    prop.visuals_1d_station_path)
                logger.info(
                    "Calling Skill Assessment module for whichcast %s. ",
                    prop.whichcast
                )

                if prop.ofsfiletype == 'fields' or read_ofs_ctl_file[1][i] >= 0:
                    get_skill.get_skill(prop, logger)


            if (os.path.isfile(
                    f"{prop.data_skill_1d_pair_path}/"
                    f"{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_"
                    f"{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int"
                ) is False):
                logger.error(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) not found in %s. ",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast,
                    prop.visuals_1d_station_path)

            else:
                paired_data = pd.read_csv(
                    r"" + f"{prop.data_skill_1d_pair_path}/"
                    f"{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_"
                    f"{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int",
                    delim_whitespace=True, names=var_info[2],
                    header=0) #change to skip header for human readability
                #print(read_ofs_ctl_file[-1][i])
                paired_data['DateTime'] = pd.to_datetime(
                    paired_data[['year', 'month', 'day', 'hour', 'minute']])
                logger.info(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) found in %s",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast, prop.visuals_1d_station_path)
            if paired_data is not None:
                # NEW! subsample time series if using 6-minute resolution from stations files
                deltat = (paired_data['DateTime'].iloc[-1] - paired_data['DateTime'].iloc[0]).days
                if (prop.ofsfiletype == 'stations'
                    and (deltat > 185 #or var_info[1] == 'cu'
                         )):
                    paired_data = paired_data.loc[paired_data.groupby(["year","month","day","hour"])
                                                  ["minute"].idxmin()]
                now_fores_paired.append(paired_data)

        if len(now_fores_paired) > 0:
            try:
                if var_info[1] == 'wl' or var_info[1] == 'temp' or var_info[
                    1] == 'salt':
                    logger.info(
                        "Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop.whichcast)
                    plotting_scalar.oned_scalar_plot(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                    plotting_scalar.oned_scalar_diff_plot(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                elif var_info[1] == 'cu':
                    logger.info(
                        "Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i],
                        read_ofs_ctl_file[1][i],
                        prop.whichcast)
                    plotting_vector.oned_vector_plot1(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                    logger.info(
                        "Trying to build wind rose %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    plotting_vector.oned_vector_plot2b(
                        plotting_vector.oned_vector_plot2a(now_fores_paired, logger),
                        var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop, logger)
                    logger.info(
                        "Trying to build stick %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    plotting_vector.oned_vector_plot3(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                    logger.info(
                        "Trying to build stick %s plot for vector difference: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    plotting_vector.oned_vector_diff_plot3(
                        now_fores_paired, var_info[1],
                        [read_ofs_ctl_file[-1][i],
                         read_station_ctl_file[0][i][2],
                         read_station_ctl_file[0][i][1].split('_')[-1]],
                        read_ofs_ctl_file[1][i],
                        prop,logger)
                    if deltat < 45:
                        logger.info(
                            "Trying to build stick %s plot for vector difference: \
        %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                            read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                        plotting_vector.oned_vector_diff_plot3(
                            now_fores_paired, var_info[1],
                            [read_ofs_ctl_file[-1][i],
                             read_station_ctl_file[0][i][2],
                             read_station_ctl_file[0][i][1].split('_')[-1]],
                            read_ofs_ctl_file[1][i],
                            prop,logger)
            except Exception as ex:
                logger.info(
                    "Fail to create the plot  \
    ---  %s ...Continuing to next plot", ex)


def create_1dplot(prop, logger):
    '''
    This is the main function for plotting 1d paired datasets
    Specify defaults (can be overridden with command line options)
    '''
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s", config_file)
        logger.info("Using log config %s", log_config_file)

    logger.info("--- Starting Visualization Process ---")

    dir_params = utils.Utils().read_config_section("directories", logger)

    # Do forecast_a start and end date reshuffle
    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is None:
            error_message = (
                "prop.forecast_hr is required if prop.whichcast is "
                "forecast_a. Abort!")
            logger.error(error_message)
            sys.exit(-1)
        elif prop.forecast_hr is not None:
            try:
                int(prop.forecast_hr[:-2])
            except ValueError:
                error_message = (f"Please check Forecast Hr format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)
            if prop.forecast_hr[-2:] == 'hr':
                prop.start_date_full, prop.end_date_full =\
                get_fcst_cycle.get_fcst_cycle(prop,logger)
                logger.info(f"Forecast_a: end date reassigned to "
                                 f"{prop.end_date_full}")
            else:
                error_message = (f"Please check Forecast Hr (hr) format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)

    # Start Date and End Date validation
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        datetime.strptime(prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = (f"Please check Start Date - "
                         f"{prop.start_date_full}, End Date - "
                         f"{prop.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if datetime.strptime(
            prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ") > datetime.strptime(
        prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ"):
        error_message = (f"End Date {prop.end_date_full} "
                         f"is before Start Date {prop.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # prop.path validation
    ofs_extents_path = os.path.join(
        prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = (f"ofs_extents/ folder is not found. "
                         f"Please check prop.path - {prop.path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f"{ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shape_file):
        error_message = (f"Shapefile {prop.ofs} is not found at "
                         f"the folder {ofs_extents_path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    # datum validation
    if prop.datum.lower() not in ('mhw', 'mlw', 'mllw',
        'navd88', 'igld85', 'lwd'):
        error_message = f"Datum {prop.datum} is not valid. Abort!"
        logger.error(error_message)
        sys.exit(-1)
    # GLOFS datum validation
    if (prop.datum.lower() not in ('igld85', 'lwd') and prop.ofs in
        ['leofs','loofs','lmhofs','lsofs']):
        error_message = f"Use only LWD or IGLD85 datums for {prop.ofs}!"
        logger.error(error_message)
        sys.exit(-1)
    # Non-GLOFS datum validation
    if (prop.datum.lower() in ('igld85', 'lwd') and prop.ofs not in
        ['leofs','loofs','lmhofs','lsofs']):
        error_message = f"Do not use LWD or IGLD85 datums for {prop.ofs}!"
        logger.error(error_message)
        sys.exit(-1)


    prop.control_files_path = os.path.join(
        prop.path, dir_params["control_files_dir"])
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["observations_dir"],
        dir_params["1d_station_dir"], )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["model_dir"],
        dir_params["1d_node_dir"], )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["1d_pair_dir"], )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["stats_dir"], )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["visual_dir"], )
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    # Replace brackets, if present
    prop.whichcasts = prop.whichcasts.replace("[", "")
    prop.whichcasts = prop.whichcasts.replace("]", "")
    prop.whichcasts = prop.whichcasts.split(",")
    prop.stationowner = prop.stationowner.replace("[", "")
    prop.stationowner = prop.stationowner.replace("]", "")
    prop.stationowner = prop.stationowner.split(",")

    for variable in ['water_level', 'water_temperature', 'salinity',
                     'currents']:
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
if __name__ == "__main__":
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python ofs_inventory_station.py", usage="%(prog)s",
        description="OFS Inventory Station", )
    parser.add_argument(
        "-o", "--OFS", required=True, help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""", )
    parser.add_argument(
        "-p", "--Path", required=False,
        help="Inventory File path where ofs_extents/ folder is located", )
    parser.add_argument(
        "-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-e", "--EndDate_full", required=False,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-d", "--Datum", required=True, help="datum: 'MHHW', 'MHW', \
        'MLW', 'MLLW', 'NAVD88'")
    parser.add_argument(
        "-ws", "--Whichcasts", required=True,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )
    parser.add_argument(
        "-t", "--FileType", required=False,
        help="OFS output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        "-f",
        "--Forecast_Hr",
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ", )
    parser.add_argument(
        "-so",
        "--Station_Owner",
        required=False,
        help="'CO-OPS', 'NDBC', 'USGS',", )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts.lower()
    prop1.datum = args.Datum

    ''' Make stations files default, unless user specifies fields '''
    if args.FileType is None:
        prop1.ofsfiletype = 'stations'
    elif args.FileType is not None:
        prop1.ofsfiletype = args.FileType.lower()
    else:
        print('Check OFS file type argument! Abort.')
        sys.exit(-1)

    ''' Do forecast_a to assess a single forecast cycle'''
    if 'forecast_a' in prop1.whichcasts:
        if args.Forecast_Hr is None:
            print('No forecast cycle input -- defaulting to 00Z. Continuing...')
            prop1.forecast_hr = '00hr'
        elif args.Forecast_Hr is not None:
            prop1.forecast_hr = args.Forecast_Hr

    ''' Enforce end date for whichcasts other than forecast_a'''
    if args.EndDate_full is None and 'forecast_a' not in prop1.whichcasts:
        print('If not using forecast_a, you must set an end date! Abort.')
        sys.exit(-1)

    ''' Make all station owners default, unless user specifies station owners '''
    if args.Station_Owner is None:
        prop1.stationowner = 'co-ops,ndbc,usgs'
    elif args.Station_Owner is not None:
        prop1.stationowner = args.Station_Owner.lower()
    else:
        print('Check station owner argument! Abort.')
        sys.exit(-1)

    logger = create_1dplot(prop1, None)

    logger.info("Finished create_1dplot!")
