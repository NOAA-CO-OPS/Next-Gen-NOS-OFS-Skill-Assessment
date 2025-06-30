"""
This script has a series of functions for creating
a list of files to be concatenated.
The first function creates the range of dates, which is
then used to create a list of directories in which the
model netcdf files are stored
The third function (list_of_files) lists all files inside
these folder and order them based on the kind of product
input by the user (nowcast, forecast_a, or forecast_b)

Note! There are special considerations for WCOFS, because it runs once
per day at 03:00 UTC and has three-hourly output for fields files.
Below, the code retrieves an extra day of output for WCOFS.
This extra day helps the skill
assessment stitch together more complete time series -- without it,
there are often time series gaps.

"""

import os
import sys
from datetime import datetime, timedelta
from os import listdir
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils

def dates_range(start_date, end_date, ofs, whichcast):
    """
    This function uses the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates = []
    # For WCOFS nowcast, we need to look an extra day ahead for nowcast, and an
    # extra day behind for forecast_b
    if ofs == 'wcofs':
        offset = 2
        if whichcast == 'forecast_b':
            ddays = -1 #Look behind one day with offset
        elif whichcast == 'nowcast':
            ddays = 0 #Look ahead one day with offset
    else: #No looking behind or ahead
        offset = 1
        ddays = 0
    for i in range(
        int((datetime.strptime(end_date, "%Y%m%d%H")
                - datetime.strptime(start_date, "%Y%m%d%H")).days) + offset
        ):
        date = datetime.strptime(start_date, "%Y%m%d%H") + \
            timedelta(days=(i+ddays))
        dates.append(date.strftime("%m/%d/%y"))

    return dates


def list_of_dir(prop, logger):
    """
    This function takes the output of dates_range, which is a
    list of dates, and creates a list of directories based on model_path
    model_path is the path to where the model data is stored,
    that will change from server to server.
    """

    dir_list = []
    if prop.whichcast != 'forecast_a':
        dates = dates_range(prop.startdate, prop.enddate, prop.ofs,
                            prop.whichcast)
    else:
        dates = dates_range(prop.startdate, prop.startdate, prop.ofs,
                            prop.whichcast)
    dates_len = len(dates)

    #### After 12/31/24, directory structure changes! Now we need to sort
    #a dir list that might have two different formats.
    datethreshold = datetime.strptime("12/31/24","%m/%d/%y")
    logger.info("Starting model output directory search...")
    ####
    for date_index in range(0, dates_len):
        year = datetime.strptime(dates[date_index], "%m/%d/%y").year
        month = datetime.strptime(dates[date_index], "%m/%d/%y").month
        # Do old directory structure
        if datetime.strptime(dates[date_index], "%m/%d/%y") <= datethreshold:
            model_dir = f"{prop.model_path}/{year}{month:02}"
        # Do new directory structure
        elif datetime.strptime(dates[date_index], "%m/%d/%y") > datethreshold:
            day = datetime.strptime(dates[date_index], "%m/%d/%y").day
            model_dir = f"{prop.model_path}/{year}/{month:02}/{day:02}"
        # Whoops! I'm out
        else:
            logger.error("Check the date -- can't find model output dir!")
            sys.exit(-1)
        model_dir = Path(model_dir).as_posix()

        # Switch to backup directory if files are not in primary directory
        if not os.path.exists(model_dir) or not os.listdir(model_dir):
            logger.info(
                "Model data path " + model_dir + " not found, or is empty. ")
            logger.info("Trying backup dir...")
            dir_params = utils.Utils().read_config_section(
                "directories",logger)
            prop.model_path = os.path.join(
                dir_params["model_historical_dir_backup"],
                prop.ofs,dir_params["netcdf_dir"])
            # Do old directory structure
            if datetime.strptime(dates[date_index],"%m/%d/%y")<=datethreshold:
                model_dir = f"{prop.model_path}/{year}{month:02}"
            # Do new directory structure
            elif datetime.strptime(dates[date_index],"%m/%d/%y")>datethreshold:
                day = datetime.strptime(dates[date_index], "%m/%d/%y").day
                model_dir = f"{prop.model_path}/{year}/{month:02}/{day:02}"
            if not os.path.exists(model_dir):
                logger.error(
                    "Model data path " + model_dir + " not found. Abort!")
                sys.exit(-1)
        if model_dir not in dir_list:
            dir_list.append(model_dir)
            logger.info("Found model output dir: %s", model_dir)

    return dir_list


def list_of_files(prop, dir_list):
    """
    This function takes the output of list_of_dir and lists all
    files inside each directory.
    These files are then sorted according to their model temporal order.
    This is important for ensuring that the data is concatenated
    correctly (in the correct temporal order)
    Sorting is different if we are concatenating nowcast or
    Forecast model files (see the if statements below)
    """
    list_files = []
    dir_list_len = len(dir_list)
    for i_index in range(0, dir_list_len):

        if prop.whichcast == "nowcast":
            '''
            New file format:
            cbofs.t00z.20240901.fields.n001.nc
            Old file format:
            nos.cbofs.fields.n001.20240901.t00z.nc

            '''
            all_files = listdir(dir_list[i_index])
            files = []
            hr_cyc_day = []
            if prop.ofs == 'wcofs':
                ndays = 1
            else:
                ndays = 0
            for af_name in all_files:
                spltstr = af_name.split(".")
                # First do old file names
                if "nos." in af_name:
                    if "fields.n" in af_name and prop.ofsfiletype == 'fields':
                        checkstr = spltstr[-4][-3:] + spltstr[-2][1:3] +\
                            spltstr[-3][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d") +
                                 timedelta(days=ndays))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
                    elif ('stations.n' in af_name and
                          prop.ofsfiletype == 'stations'):
                        checkstr = '999' + spltstr[-2][1:3] + spltstr[-3][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d") +
                                 timedelta(days=ndays))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)


                #Now do new file names
                elif "nos." not in af_name:
                    if "fields.n" in af_name and prop.ofsfiletype == 'fields':
                        checkstr = spltstr[-2][-3:] + spltstr[-5][1:3] +\
                            spltstr[-4][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") <=
                                 datetime.strptime(prop.enddate[:-2], "%Y%m%d")
                                 + timedelta(days=ndays))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
                    elif ("stations.n" in af_name and
                          prop.ofsfiletype == 'stations'):
                        checkstr = '999' + spltstr[-5][1:3] + spltstr[-4][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") <=
                                 datetime.strptime(prop.enddate[:-2], "%Y%m%d")
                                 + timedelta(days=ndays))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
            files = [dir_list[i_index] + "//" + i for i in files]
            tupfiles = tuple(zip(hr_cyc_day,files))
            # Sort by forecast/nowcast hour, then model run cycle, then day
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
            # Unzip, get sorted file list back
            files = list(zip(*tupfiles))[1]
            files = list(files)


        elif prop.whichcast == "forecast_a":
            '''
            New file format:
            cbofs.t00z.20240901.fields.f001.nc
            Old file format:
            nos.cbofs.fields.f001.20240901.t00z.nc

            '''
            all_files = listdir(dir_list[i_index])
            files = []
            hr_cyc_day = []
            cycle_z = prop.forecast_hr[:-2] + 'z'
            for af_name in all_files:
                spltstr = af_name.split(".")
                # First do old file names
                if "nos." in af_name:
                    if ("fields.f" in af_name and
                        prop.ofsfiletype == 'fields' and
                        cycle_z in af_name):
                        checkstr = spltstr[-4][-3:] + spltstr[-2][1:3] +\
                            spltstr[-3][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") ==
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
                    elif ('stations.f' in af_name and
                          prop.ofsfiletype == 'stations' and
                          cycle_z in af_name):
                        checkstr = '999' + spltstr[-2][1:3] + spltstr[-3][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") ==
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)


                #Now do new file names
                elif "nos." not in af_name:
                    if ("fields.f" in af_name and
                        prop.ofsfiletype == 'fields' and
                        cycle_z in af_name):
                        checkstr = spltstr[-2][-3:] + spltstr[-5][1:3] +\
                            spltstr[-4][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") ==
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
                    elif ("stations.f" in af_name and
                          prop.ofsfiletype == 'stations' and
                          cycle_z in af_name):
                        checkstr = '999' + spltstr[-5][1:3] + spltstr[-4][-2:]
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") ==
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d"))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)

            files = [dir_list[i_index] + "//" + i for i in files]
            tupfiles = tuple(zip(hr_cyc_day,files))
            # Sort by forecast/nowcast hour, then model run cycle, then day
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
            # Unzip, get sorted file list back
            files = list(zip(*tupfiles))[1]
            files = list(files)


        elif prop.whichcast == "forecast_b":
            '''
            New file format:
            cbofs.t00z.20240901.fields.f001.nc
            Old file format:
            nos.cbofs.fields.f001.20240901.t00z.nc

            '''
            all_files = listdir(dir_list[i_index])
            files = []
            hr_cyc_day = []
            if prop.ofs == 'wcofs':
                ndays = 1
            else:
                ndays = 0
            for af_name in all_files:
                spltstr = af_name.split(".")
                #Old file names
                if "nos." in af_name:
                    if "fields.f" in af_name and prop.ofsfiletype == 'fields':
                        if "f0" in af_name:
                            checkstr = (spltstr[-4][-3:] + spltstr[-2][1:3]
                                        + spltstr[-3][-2:])
                            if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d") -
                                 timedelta(days=ndays))
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d"))
                                and checkstr[0:3] != '000'
                                ):
                                if (prop.ofs == "wcofs"
                                    and int(checkstr[0:3]) >= 1
                                    and int(checkstr[0:3]) < 25):
                                    hr_cyc_day.append(checkstr)
                                    files.append(af_name)
                                elif (int(checkstr[0:3]) >= 1
                                      and int(checkstr[0:3]) < 7):
                                    hr_cyc_day.append(checkstr)
                                    files.append(af_name)
                    elif ("stations.f" in af_name and
                          prop.ofsfiletype == 'stations'):
                        #if "f0" in af_name:
                        checkstr = ('999' + spltstr[-2][1:3]
                                    + spltstr[-3][-2:])
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d") -
                                 timedelta(days=ndays))
                            and (datetime.strptime(spltstr[-3],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d"))
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
                # New file names
                elif "nos." not in af_name:
                    if "fields.f" in af_name and prop.ofsfiletype == 'fields':
                        if "f0" in af_name:
                            checkstr = (spltstr[-2][-3:] + spltstr[-5][1:3]
                                        + spltstr[-4][-2:])
                            if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d") -
                                 timedelta(days=ndays))
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d"))
                                and checkstr[0:3] != '000'
                                ):
                                if (prop.ofs == "wcofs"
                                    and int(checkstr[0:3]) >= 1
                                    and int(checkstr[0:3]) < 25):
                                    hr_cyc_day.append(checkstr)
                                    files.append(af_name)
                                elif (int(checkstr[0:3]) >= 1
                                      and int(checkstr[0:3]) < 7):
                                    hr_cyc_day.append(checkstr)
                                    files.append(af_name)
                    elif ("stations.f" in af_name and
                          prop.ofsfiletype == 'stations'):
                        #if "f0" in af_name:
                        checkstr = ('999' + spltstr[-5][1:3]
                                    + spltstr[-4][-2:])
                        if (checkstr not in hr_cyc_day
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") >=
                                 datetime.strptime
                                 (prop.startdate[:-2], "%Y%m%d") -
                                 timedelta(days=ndays))
                            and (datetime.strptime(spltstr[-4],"%Y%m%d") <=
                                 datetime.strptime
                                 (prop.enddate[:-2], "%Y%m%d"))
                            and checkstr[0:3] != '000'
                            ):
                            hr_cyc_day.append(checkstr)
                            files.append(af_name)
            files = [dir_list[i_index] + "//" + i for i in files]
            tupfiles = tuple(zip(hr_cyc_day,files))
            # Sort by forecast/nowcast hour, then model run cycle, then day
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
            tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
            # Unzip, get sorted file list back
            files = list(zip(*tupfiles))[1]
            files = list(files)


        # Append files to master
        list_files.append(files)

    list_files = sum(list_files, [])

    return list_files
