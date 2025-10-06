# -*- coding: utf-8 -*-
"""
Created on Tue Apr  8 10:05:25 2025

@author: PL
"""

import sys
import os
import time
from pathlib import Path
import urllib.request
from urllib.error import HTTPError
import socket
import argparse
import logging
import logging.config
from datetime import datetime, timedelta
import numpy as np

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import model_properties
from obs_retrieval import utils


TIMEOUT_SEC = 60 # default API timeout in seconds
socket.setdefaulttimeout(TIMEOUT_SEC)

def parameter_validation(argu_list, logger):
    """ Parameter validation """

    start_date, end_date, path, ofs, whichcast, filetype, ofs_extents_path = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(argu_list[4]),
        str(argu_list[5]),
        str(argu_list[6])
        )

    # start_date and end_date validation
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Please check Start Date -
        {start_date}, End Date - '{end_date}'. Abort!"""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f"""End Date {end_date} is before Start Date
        {start_date}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # path validation
    if not os.path.exists(ofs_extents_path):
        error_message = f"""ofs_extents/ folder is not found. Please
        check path - {path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # ofs validation
    if not os.path.isfile(f"{ofs_extents_path}/{ofs}.shp"):
        error_message = f"""Shapefile {ofs}.shp is not found at the
        folder {ofs_extents_path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # filetype validation
    if filetype not in ['stations', 'fields']:
        error_message = f"Filetype should be fields or stations: {filetype}!"
        logger.error(error_message)
        sys.exit(-1)

    # whichcast validation
    if ('nowcast' not in whichcast and
        'forecast_b' not in whichcast and
        'forecast_a' not in whichcast and
        'all' not in whichcast):
        error_message = f"Incorrect whichcast: {whichcast}! Exiting."
        logger.error(error_message)
        sys.exit(-1)
    if len([whichcast]) > 1:
        logger.error("There is >1 whichcast! Program takes 1 whichcast only.")
        sys.exit(-1)

def get_ofs_cycle(prop,logger):
    '''
    Returns strings of the model cycles, forecast horizons, and forecast/
    nowcast hours for a given OFS. These are used to build file names for URL
    retrieval.
    '''
    logger.info('Starting OFS cycle info retrieval...')

    hrstrings = None

    # Need to know forecast cycle hours (e.g. 00Z) and forecast length (hours)
    if prop.ofs in ("cbofs","dbofs","gomofs","ciofs","leofs","lmhofs","loofs",
                    "lsofs"):
        fcstcycles = ['00', '06', '12', '18']
    elif prop.ofs in ("creofs","ngofs2","sfbofs","tbofs", "sscofs"):
        fcstcycles = ['03', '09', '15', '21']
    elif prop.ofs in ("stofs_3d_atl", "stofs_3d_pac"):
        fcstcycles = ['12']
    else:
        fcstcycles = ['03']

    #Now need to know forecast length in hours
    if prop.ofs in ("cbofs", "ciofs", "creofs","dbofs", "ngofs2", "sfbofs",
                    "tbofs", "stofs_3d_pac"):
        fcstlength = 48
    elif prop.ofs in ("gomofs", "wcofs", "sscofs"):
        fcstlength = 72
    elif prop.ofs in ("stofs_3d_atl"):
        fcstlength = 96
    else:
        fcstlength = 120

    # Get hour strings & time steps (dt)
    if prop.ofs in ("cbofs", "ciofs", "creofs","dbofs", "sfbofs", "tbofs",
                    "leofs", "lmhofs", "loofs", "lsofs", "sscofs"):
        d_t = 1
    elif prop.ofs in ("stofs_3d_atl", "stofs_3d_pac"):
        d_t = 12
    else:
        d_t = 3
    if prop.whichcast == 'forecast_a':
        # Select one forecast cycle for forecast_a
        if prop.forecast_hr[:-2] in fcstcycles:
            fcstcycles = [str(prop.forecast_hr[:-2]).zfill(2)]
        else:
            logger.error(f"Model cycle incorrect for forecast_a and {prop.ofs}!")
            sys.exit(-1)
        hrstrings = np.linspace(d_t,fcstlength,int(fcstlength/d_t)).astype(int). \
                                astype(str)
    elif prop.whichcast in ['nowcast', 'forecast_b']:
        hrstrings = np.linspace(d_t,int(24/len(fcstcycles)),
                            int(24/len(fcstcycles)/d_t)).astype(int).astype(str)
    return fcstcycles, hrstrings

def dates_range(start_date, end_date, ofs, whichcast):
    """
    This function uses the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates = []

    # For WCOFS nowcast, we need to look an extra day ahead
    if ofs == 'wcofs' and whichcast == 'nowcast':
        offset = 2
    else:
        offset = 1

    # Subtract a day off the start date to make sure we have a complete time
    # series
    startdt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") -\
                timedelta(days=1)
    enddt = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

    for i in range(
        int((enddt - startdt).days) + offset
    ):
        date = startdt + timedelta(days=i)
        dates.append(date.strftime("%m/%d/%y"))

    return dates


def list_of_dir(prop, basepath, logger):
    """
    This function takes the output of dates_range, which is a
    list of dates, and creates a list of directories where model output is
    stored.
    """

    dir_list = []
    if prop.whichcast != 'forecast_a':
        dates = dates_range(prop.start_date_full, prop.end_date_full, prop.ofs,
                            prop.whichcast)
    else:
        dates = dates_range(prop.start_date_full, prop.start_date_full,
                            prop.ofs, prop.whichcast)
    dates_len = len(dates)
    #### After 12/31/24, directory structure changes! Now we need to sort
    #a dir list that might have two different formats.
    datethreshold = datetime.strptime("12/31/24","%m/%d/%y")
    logger.info(f"Starting list of directories for {basepath}")
    ####
    for date_index in range(0, dates_len):
        year = datetime.strptime(dates[date_index], "%m/%d/%y").year
        month = datetime.strptime(dates[date_index], "%m/%d/%y").month
        # Add stofs directory structure
        if prop.ofs in ["stofs_3d_atl", "stofs_2d_global", "stofs_3d_pac"]:
           day = datetime.strptime(dates[date_index], "%m/%d/%y").day
           model_dir = f"{prop.model_path}/{prop.ofs}.{year}{month:02}{day:02}"
        else:
           # Do old directory structure
           if datetime.strptime(dates[date_index], "%m/%d/%y") <= datethreshold:
              model_dir = f"{basepath}/{year}{month:02}"
           # Do new directory structure
           elif datetime.strptime(dates[date_index], "%m/%d/%y") > datethreshold:
              day = datetime.strptime(dates[date_index], "%m/%d/%y").day
              model_dir = f"{basepath}/{year}/{month:02}/{day:02}"
           # Whoops! I'm out
           else:
              logger.error("Check the date -- can't find model output dir!")
              sys.exit(-1)
        model_dir = Path(model_dir).as_posix()
        #if model_dir not in dir_list:
        dir_list.append(model_dir)
        logger.info("Found model output dir: %s", model_dir)
    return dir_list, dates

def create_directories(dir_list,logger):
    """Creates directories from a list of directory names."""
    for dir_name in dir_list:
        try:
            os.makedirs(dir_name)
            logger.info(f"Directory '{dir_name}' created successfully.")
        except FileExistsError:
            logger.info(f"Directory '{dir_name}' already exists.")
        except Exception as e_x:
            logger.error(f"Error creating directory '{dir_name}': {e_x}")
            sys.exit(-1)

def make_file_list(prop, dates, dir_list, logger):
    '''
    Returns a list of file paths to download. File list is different
    depending on file type (fields vs stations), OFS, and whichcast (nowcast,
    forecast_a, forecast_b)
    '''

    # First get cycle & forecast horizon info for the OFS
    fcstcycles, hrstrings = get_ofs_cycle(prop,logger)

    # Date that file names change on the NODD
    datechange = datetime.strptime('09/01/2024','%m/%d/%Y')

    # Set up empty variable to append to
    file_list = []

    if prop.whichcast in ['forecast_b', 'forecast_a']:
        if prop.ofsfiletype == 'fields'and prop.ofs not in ("stofs_3d_atl",
                                                            "stofs_3d_pac"):
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    for hrstring in hrstrings:
                        hrstring = hrstring.zfill(3)
                        if datetime.strptime(datei,'%Y%m%d') < datechange:
                            file_name = f"nos.{prop.ofs}.fields.f{hrstring}." \
                                f"{datei}.t{cycle}z.nc"
                            file_name = os.path.join(dir_list[i],file_name). \
                                replace("\\","/")
                            file_list.append(file_name)
                        elif datetime.strptime(datei,'%Y%m%d') >= datechange:
                            file_name = f"{prop.ofs}.t{cycle}z.{datei}.fields." \
                                f"f{hrstring}.nc"
                            file_name = os.path.join(dir_list[i],file_name). \
                                replace("\\","/")
                            file_list.append(file_name)
        elif prop.ofsfiletype == 'fields'and prop.ofs in ("stofs_3d_atl",
                                                          "stofs_3d_pac"):
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    for hrstring in hrstrings:
                        hrstring = hrstring.zfill(3)
                        hrstring0 = str(int(hrstring)-11).zfill(3)

                        file_name = f"{prop.ofs}.t{cycle}z.field2d_f{hrstring0}" \
                            f"_{hrstring}.nc"
                        file_name = os.path.join(dir_list[i],file_name). \
                            replace("\\","/")
                        file_list.append(file_name)
                        for var_name in {"horizontalVelX", "horizontalVelY" ,
                                         "salinity","temperature","zCoordinates"}:
                            file_name = f"{prop.ofs}.t{cycle}z.fields.{var_name}" \
                                f"_f{hrstring0}_{hrstring}.nc"
                            file_name = os.path.join(dir_list[i],file_name). \
                                replace("\\","/")
                            file_list.append(file_name)
        elif prop.ofsfiletype == 'stations':
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    if datetime.strptime(datei,'%Y%m%d') < datechange:
                        file_name = f"nos.{prop.ofs}.stations.forecast." \
                            f"{datei}.t{cycle}z.nc"
                        file_name = os.path.join(dir_list[i],file_name). \
                            replace("\\","/")
                        file_list.append(file_name)
                    elif datetime.strptime(datei,'%Y%m%d') >= datechange:
                        file_name = f"{prop.ofs}.t{cycle}z.{datei}.stations." \
                            f"forecast.nc"
                        file_name = os.path.join(dir_list[i],file_name). \
                            replace("\\","/")
                        file_list.append(file_name)
    elif prop.whichcast == 'nowcast':
        if prop.ofsfiletype == 'fields'and prop.ofs not in ("stofs_3d_atl",
                                                            "stofs_3d_pac"):
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    for hrstring in hrstrings:
                        hrstring = hrstring.zfill(3)
                        if datetime.strptime(datei,'%Y%m%d') < datechange:
                            file_name = f"nos.{prop.ofs}.fields.n{hrstring}." \
                                f"{datei}.t{cycle}z.nc"
                            file_name = os.path.join(dir_list[i],file_name)
                            file_list.append(file_name)
                        elif datetime.strptime(datei,'%Y%m%d') >= datechange:
                            file_name = f"{prop.ofs}.t{cycle}z.{datei}.fields." \
                                f"n{hrstring}.nc"
                            file_name = os.path.join(dir_list[i],file_name). \
                                replace("\\","/")
                            file_list.append(file_name)
        elif prop.ofsfiletype == 'fields'and prop.ofs in ("stofs_3d_atl",
                                                          "stofs_3d_pac"):
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    for hrstring in {'12','24'}:
                        hrstring = hrstring.zfill(3)
                        hrstring0 = str(int(hrstring)-11).zfill(3)

                        file_name = f"{prop.ofs}.t{cycle}z.field2d_n{hrstring0}" \
                            f"_{hrstring}.nc"
                        file_name = os.path.join(dir_list[i],file_name). \
                            replace("\\","/")
                        file_list.append(file_name)
                        for var_name in {"horizontalVelX", "horizontalVelY",
                                         "salinity","temperature","zCoordinates"}:
                            file_name = f"{prop.ofs}.t{cycle}z.fields.{var_name}" \
                                f"_n{hrstring0}_{hrstring}.nc"
                            file_name = os.path.join(dir_list[i],file_name). \
                                replace("\\","/")
                            file_list.append(file_name)
        elif prop.ofsfiletype == 'stations':
            for i, datei in enumerate(dates):
                datei = datetime.strptime(datei,'%m/%d/%y').strftime('%Y%m%d')
                for cycle in fcstcycles:
                    if datetime.strptime(datei,'%Y%m%d') < datechange:
                        file_name = f"nos.{prop.ofs}.stations.nowcast." \
                            f"{datei}.t{cycle}z.nc"
                        file_name = os.path.join(dir_list[i],file_name)
                        file_list.append(file_name)
                    elif datetime.strptime(datei,'%Y%m%d') >= datechange:
                        file_name = f"{prop.ofs}.t{cycle}z.{datei}.stations." \
                            f"nowcast.nc"
                        file_name = os.path.join(dir_list[i],file_name). \
                            replace("\\","/")
                        file_list.append(file_name)
    elif prop.whichcast == 'all':
        logger.error('This option is not ready yet!')
        sys.exit(-1)
    else:
        logger.error("Not sure how you got this far, but whichcast is wrong.")
        sys.exit(-1)

    logger.info("Created list of files for downloading. Here they are:")
    for j in file_list:
        logger.info(j)

    return file_list

def list_of_urls(file_list, logger):
    """
    This function take a file list and builds the URLs for all downloads.
    """

    # Retrieve urls from config file
    url_params = utils.Utils().read_config_section("urls", logger)
    url_root = url_params["nodd_s3"]

    logger.info(f"Starting URL building for {url_root}...")

    url_list = []
    for file in file_list:
        url = (
            f"{url_root}"
            f"{file}"
        )
        url_list.append(url)
        #url_list_backup.append(url_backup)

    logger.info("Completed URL building!")
    return url_list


def download_data(prop, list_of_urls1, dir_list, logger):
    """
    This function gets the model output files from the NODD using the list
    of URLs.
    """

    # Set up save path
    savepath = dir_list[0][:].split(prop.ofs)[0]

    # First try the NODD and see if it's responding
    try:
        logger.info("Try NODD S3 download...")
        urllib.request.urlretrieve(
            list_of_urls1[0].replace("\\","/"), (savepath +\
            list_of_urls1[0].split('.com')[-1]).replace("//","/")
            )
        logger.info("NODD is responding! Keep going -->")
        list_of_urls_main = list_of_urls1
    except (ValueError, HTTPError, Exception) as e_x:
        logger.info("NODD S3 is not responding! I'm out.")
        logger.error(f"Exception: {e_x}")
        sys.exit(-1)
        #list_of_urls = list_of_urls2

    for mod_dat in list_of_urls_main:
        try:
            logger.info(f"Downloading model data: {mod_dat}")
            if not os.path.isfile((savepath +\
                mod_dat.split('.com')[-1]).replace("//","/")):
                urllib.request.urlretrieve(
                    mod_dat.replace("\\","/"), (savepath +\
                        mod_dat.split('.com')[-1]).replace("//","/")
                    )
        except (ValueError, HTTPError, Exception) as ex:
            error_message = f"""Error: {str (ex)}. Download failed {mod_dat}!!"""
            logger.error(error_message)
            #sys.exit(-1)

def get_model_data(prop, logger):
    """
    Main function that calls all of the subroutines. Program gets OFS model
    output from the NODD by building file lists and URLs for a given OFS,
    file type, and whichcast.
    """
    # Specify defaults (can be overridden with command line options)
    if logger is None:
        log_config_file = "conf/logging.conf"
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using log config %s", log_config_file)
    logger.info("--- Starting the program ---")

    ##################### Parameter validation#################################
    dir_params = utils.Utils().read_config_section("directories", logger)

    if dir_params['home'] == '/path/to/sa_homedir/':
        logger.error("HOMEDIR NOT SET IN conf/ofs_dps.conf FILE!")
        logger.error("Program will sleep for 10 seconds to allow user to kill it (ctl-c).")
        time.sleep(10)
        logger.info("Setting homedir to user input -p argument. Model data will be downloaded to %sexample_data/", prop.path)
        time.sleep(3)
        dir_params["home"]=Path(prop.path)
        dir_params["model_historical_dir"]=Path(os.path.join(prop.path,'example_data'))

    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    argu_list = (prop.start_date_full,
                 prop.end_date_full,
                 prop.path,
                 prop.ofs,
                 prop.whichcast,
                 prop.ofsfiletype,
                 ofs_extents_path)
    parameter_validation(argu_list, logger)
    logger.info("Parameter validation complete!")

    ############### Directory & path set-up -->################################
    # Root path for saving files
    prop.model_save_path = os.path.join(
        dir_params["model_historical_dir"], prop.ofs, dir_params["netcdf_dir"]
    )
    prop.model_save_path = Path(prop.model_save_path).as_posix()
    # Path to files on the NODD
    prop.model_nodd_path = os.path.join(
        prop.ofs, dir_params["netcdf_dir"]
    )
    prop.model_nodd_path = Path(prop.model_nodd_path).as_posix()
    logger.info("Successfully set up paths.")

    ###### Directories, file lists, and URLs -- oh my##########################
    # Get directory list for NODD
    dir_list_nodd, dates = list_of_dir(prop,prop.model_nodd_path,logger)
    # Get directory list for saving
    dir_list_save, dates = list_of_dir(prop,prop.model_save_path,logger)
    # Set up directory tree locally
    create_directories(dir_list_save, logger)
    # Get list of files
    file_list = make_file_list(prop, dates, dir_list_nodd, logger)
    # Get list of URLs for NODD downloads using nodd dir list & file list
    url_list = list_of_urls(file_list, logger)

    ###########################################################################
    # I think we're ready now -- let's go
    logger.info(
        "Begin downloading files from the internets! <dial-up modem sounds>"
    )

    try:
        download_data(
            prop,
            url_list,
            dir_list_save,
            logger
        )
        logger.info("Model data all downloaded.")
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed downloading files."""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    logger.info("Program completed! Party mode engage")

### Execution:
if __name__ == "__main__":

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python get_model_data.py",
        usage="%(prog)s",
        description="download model output data",
    )
    parser.add_argument(
        "-o",
        "--OFS",
        required=True,
        help="Choose from the list on the ofs_extents folder, you can also "
             "create your own shapefile, add it top the ofs_extents folder and "
             "call it here",
    )
    parser.add_argument(
        "-p",
        "--Path",
        required=True,
        help="Use working directory. User can specify path"
        )
    parser.add_argument(
        "-s",
        "--StartDate_full",
        required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'"
        )
    parser.add_argument(
        "-e",
        "--EndDate_full",
        required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'"
        )
    parser.add_argument(
        "-t",
        "--FileType",
        required=True,
        help="OFS output file type to use: 'fields' or 'stations'"
        )
    parser.add_argument(
        "-ws",
        "--Whichcast",
        required=True,
        help="whichcast: 'Nowcast','Forecast_A','Forecast_B', all",
        )
    parser.add_argument(
        "-f",
        "--Forecast_Hr",
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ", )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.Whichcast.lower()
    prop1.ofsfiletype = args.FileType.lower()

    # Do forecast_a to assess a single forecast cycle
    if 'forecast_a' in prop1.whichcast:
        if args.Forecast_Hr is None:
            print('No forecast cycle input for forecast_a! Exiting...')
            sys.exit(-1)
        elif args.Forecast_Hr is not None:
            prop1.forecast_hr = args.Forecast_Hr

    # Enforce data retention times for fields files
    if prop1.ofsfiletype == 'fields':
        if ('forecast' in prop1.whichcast and
            datetime.strptime(prop1.start_date_full,"%Y-%m-%dT%H:%M:%SZ") <
            (datetime.now() - timedelta(days=60))):
            ERRORMESSAGE = "THERE ARE NO AVAILABLE FIELDS FILES FOR THE " \
                           "DATES ENTERED. FORECAST FIELDS FILES ARE " \
                           "RETAINED FOR 60 DAYS BEFORE TODAY'S DATE. " \
                           "ADJUST YOUR START DATE AND PLEASE TRY AGAIN."
            print(ERRORMESSAGE)
            sys.exit(-1)

        if ('nowcast' in prop1.whichcast and
            datetime.strptime(prop1.start_date_full,"%Y-%m-%dT%H:%M:%SZ") <
            (datetime.now() - timedelta(days=365*2))):
            ERRORMESSAGE = "THERE ARE NO AVAILABLE FIELDS FILES FOR THE " \
                           "DATES ENTERED. NOWCAST FIELDS FILES ARE " \
                           "RETAINED FOR 2 YEARS BEFORE TODAY'S DATE. " \
                           "ADJUST YOUR START DATE AND PLEASE TRY AGAIN."
            print(ERRORMESSAGE)
            sys.exit(-1)

    get_model_data(
        prop1,
        None
    )
