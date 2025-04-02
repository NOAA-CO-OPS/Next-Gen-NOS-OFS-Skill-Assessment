"""
-*- coding: utf-8 -*-

Documentation for Scripts get_station_observations.py

Directory Location:   /path/to/ofs_dps/server/bin/obs_retrieval

Technical Contact(s): Name:  FC

Abstract:

   This is the final station observation data function.
   This function calls the Tides and Currents, NDBC, and USGS retrieval
   function in loop for all stations found in the
   ofs_inventory_stations(OFS, Start_Date, End_Date, Path) and variables
   ['water_level', 'water_temperature', 'salinity', 'currents'].
   The output is a .obs file for each station with DateTime and OBS
   and a final control file (.ctl)

Language:  Python 3.8

Estimated Execution Time: <10min

Scripts/Programs Called:
1) ofs_inventory_stations(OFS, Start_Date, End_Date, Path)
   This script is only called if inventory_all_{OFS}.csv is not found
   in SCI_SA/Control_Files directory
2) retrieve_T_and_C_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve Tides and Currents station data
3) retrieve_NDBC_year_station(Station, Year, Variable)
    This script is used to retrieve NDBC station data that is stored as
    yearly files
4) retrieve_NDBC_month_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve NDBC station data that is stored as
    monthly files
5) retrieve_NDBC_RT_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve the most recent (up to real time) NDBC
    station data.
6) retrieve_USGS_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve USGS station data
7) write_obs_ctlfile((Start_Date, End_Date, Datum, Path, OFS))
    This script is used in case the station control file is not found
8) station_ctl_file_extract(ctlfile_Path)
    This script is used to read the station control file and extract the
    necessary information
9) format_obs_timeseries.py
    This script is used to format the time series that will be saved

usage: python write_obs_ctlfile.py

ofs write Station Control File

optional arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Choose from the list on the ofs_Extents folder, you
                        can also create your own shapefile, add it top the
                        ofs_Extents folder and call it here
  -p PATH, --path PATH  Inventary File path
  -s STARTDATE_FULL, --StartDate_full STARTDATE_FULL
                        Start Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20220115-05:05:05'
  -e ENDDATE_FULL, --EndDate_full ENDDATE_FULL
                        End Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20230808-05:05:05'
  -d DATUM, --datum DATUM
                        datum: 'MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW',
                        'MLLW', 'NAVD', 'IGLD', 'LWD', 'STND'

Output:
1) station_timeseries
    /data/observations/1d_station
    .obs file with DateTime, Depth of observation, Observed variable for
    each station found
2) station_control_file
    /Control_Files
    .ctl file that has the final station information including station name,
    id, lat, lon, datum, depth
3) observation data
    /data/observations/1d_station
    .obs file that has all the observations from start date to end date

Author Name:  FC       Creation Date:  08/04/2023

Revisions:
    Date          Author             Description
    07-20-2023    MK           Modified the scripts to add config,
                                logging, try/except and argparse features
    08-01-2023    FC   Modified this script to be get data
                                       from station control file ONLY
    09-06-2023    MK       Modified the code to match PEP-8 standard.
    08-26-2024    AJK            Fix issues with OS path conventions.

"""
TIMEOUT_SEC = 60 # default API timeout in seconds
import socket
socket.setdefaulttimeout(TIMEOUT_SEC)
import argparse
import logging
import logging.config
import os
from datetime import datetime, timedelta
import sys
import pandas as pd

from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from obs_retrieval import retrieve_t_and_c_station
from obs_retrieval import retrieve_ndbc_year_station
from obs_retrieval import retrieve_ndbc_month_station
from obs_retrieval import retrieve_ndbc_rt_station
from obs_retrieval import retrieve_usgs_station
from obs_retrieval import write_obs_ctlfile
from obs_retrieval import station_ctl_file_extract
from obs_retrieval import format_obs_timeseries
from obs_retrieval import utils
from obs_retrieval import retrieve_properties

def parameter_validation(argu_list, logger):
    """ Parameter validation """

    start_date, end_date, path, ofs, ofs_extents_path, datum = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(argu_list[4]),
        str(argu_list[5]))

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

    # datum validation
    if datum not in ('MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW', 'MLLW',
        'NAVD', 'IGLD', 'LWD', 'STND'):
        error_message = f"""Datum {datum} is not valid. Abort!"""
        logger.error(error_message)
        sys.exit(-1)


def get_station_observations(start_date_full, end_date_full, datum, path, ofs, logger):
    """
    This is the final function.
    This function calls the Tides and Currents, NDBC, and USGS retrieval
    function in loop for all stations found for the
    ofs_inventory(ofs, start_date, end_date, path) and variables
    ['water_level', 'water_temperature', 'salinity', 'currents'].
    The output is a csv file for each station with DateTime and OBS
    """
    # Specify defaults (can be overridden with command line options)

    if logger is None:
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using log config %s", log_config_file)
    logger.info("--- Starting Station Observation Process ---")

    dir_params = utils.Utils().read_config_section("directories", logger)

    # parameter validation
    ofs_extents_path = os.path.join(path, dir_params['ofs_extents_dir'])

    argu_list = (start_date_full, end_date_full, path, ofs, ofs_extents_path, datum)
    parameter_validation(argu_list, logger)

    control_files_path = os.path.join(path, dir_params["control_files_dir"])
    os.makedirs(control_files_path, exist_ok=True)

    data_observations_1d_station_path = os.path.join(
        path,
        dir_params["data_dir"],
        dir_params["observations_dir"],
        dir_params["1d_station_dir"],
    )
    os.makedirs(data_observations_1d_station_path, exist_ok=True)

    # This is adding +- 3 days to make sure when the data is sliced
    # it has data from beginning to end
    start_date_full = start_date_full.replace("-", "")
    end_date_full = end_date_full.replace ( "-" , "" )
    start_date_full = start_date_full.replace("Z", "")
    end_date_full = end_date_full.replace ( "Z" , "" )
    start_date = start_date_full.split("T")[0]
    end_date = end_date_full.split("T")[0]
    start_date_full = start_date_full.replace("T", "-")
    end_date_full = end_date_full.replace ( "T" , "-" )

    start_dt = datetime.strptime(start_date, "%Y%m%d") - timedelta(days=3)
    end_dt = datetime.strptime(end_date, "%Y%m%d") + timedelta(days=3)

    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    # This outer loop is used to download all data for all variables
    # Inside this loop there is another loop that will go over each line
    # in the station ctl file and will try to download the data from TandC,
    # USGS, and NDBC based on the station data source

    retrieve_input = retrieve_properties.RetrieveProperties()

    for variable in ["water_level", "water_temperature", "salinity", "currents"]:

        if variable == "water_level":
            name_var = "wl"
            logger.info("Creating Water Level station control file.")

        elif variable == "water_temperature":
            name_var = "temp"
            logger.info("Creating Water Temperature station control file.")

        elif variable == "salinity":
            name_var = "salt"
            logger.info("Creating Salinity station control file.")

        elif variable == "currents":
            name_var = "cu"
            logger.info("Creating Currents station control file.")

        # This will try to read the station ctl file for the given ofs and for
        # all variables. If not found then it will create it using write_obs_ctlfile.py
        try:
            read_station_ctl_file = station_ctl_file_extract.station_ctl_file_extract(
                r"" + control_files_path + "/" + ofs + "_" + name_var + "_station.ctl"
            )
            logger.info(
                "Station ctl file (%s_%s_station.ctl) found in %s. "
                "If you instead want to create a new Inventory file, please change "
                "the name/delete the current %s_%s_station.ctl",
                ofs,
                name_var,
                control_files_path,
                ofs,
                name_var,
            )
        except FileNotFoundError:
            try:
                logger.info(
                    "Station ctl file not found. Creating station ctl file!. "
                    "This might take a couple of minutes"
                )
                write_obs_ctlfile.write_obs_ctlfile(
                    start_date, end_date, datum, path, ofs, logger
                )
                read_station_ctl_file = (
                    station_ctl_file_extract.station_ctl_file_extract(
                        r""
                        + control_files_path
                        + "/"
                        + ofs
                        + "_"
                        + name_var
                        + "_station.ctl"
                    )
                )
                logger.info("Station ctl file created successfully")
            except Exception as ex:
                logger.error(
                    "Errors happened when creating station ctl files -- %s.",
                    str(ex)
                )
                raise Exception("Error happened when creating station ctl files") from ex

        logger.info("Downloading data found in the station ctl files")


        for i in range(len(read_station_ctl_file[0])):
            # These are common for all variable in ['water_level',
            # 'water_temperature', 'salinity', 'currents']
            station_id = read_station_ctl_file[0][i][0]
            # This is just a test, where if no data is found this variable
            # does not get updated and the if statement at the end ensures
            # that no data is saved
            formatted_series = 'NoDataFound'
            if (
                read_station_ctl_file[0][i][3] == "TC"
                or read_station_ctl_file[0][i][3] == "TAC"
                or read_station_ctl_file[0][i][3] == "COOPS"
                or read_station_ctl_file[0][i][3] == "CO-OPS"
            ):
                try:
                    retrieve_input.station = str(station_id)
                    retrieve_input.start_date = start_date
                    retrieve_input.end_date = end_date
                    retrieve_input.variable = variable
                    retrieve_input.datum = datum


                    timeseries = retrieve_t_and_c_station.retrieve_t_and_c_station(
                        retrieve_input, logger)

                    if timeseries is None:
                        logger.info(
                            "Fail first try to extract COOPS %s data for station %s",
                            variable, station_id)
                        continue # Modified by XC to enable the code to continue when data extraction fails
                    else:
                        timeseries = timeseries[timeseries["OBS"].notna()]

                    if variable == "water_level":
                        # apply datum shift (if any)
                        # This is only important for water level, this
                        # shift will be applied to the water level time
                        # series if datum_shift != zero
                        datum_shift = read_station_ctl_file[1][i][2]

                        if isinstance(timeseries, pd.DataFrame) is False:
                            ### Here we a just trying a couple of vastly used datums
                            all_datums = [
                                "NAVD",
                                "MSL",
                                "MLLW",
                                "IGLD",
                                "LWD",
                                "MHHW",
                                "MHW",
                                "MTL",
                                "DTL",
                                "MLW",
                                "STND",
                            ]
                            length = len(all_datums)
                            for dat in range(0, length):
                                try:
                                    retrieve_input.station = str(
                                        station_id
                                        )
                                    retrieve_input.start_date = start_date
                                    retrieve_input.end_date = end_date
                                    retrieve_input.variable = variable
                                    retrieve_input.datum = all_datums[dat]
                                    timeseries = retrieve_t_and_c_station.retrieve_t_and_c_station(
                                        retrieve_input,
                                        logger,
                                    )
                                    if timeseries is not None:
                                        logger.info(
                                            "This (%s) is the datum for which data was found. "
                                            "If that is not what was expected, please revise.",
                                            all_datums[dat],
                                        )
                                        break
                                except ValueError:
                                    logger.info("Fail # %s when when trying multiple datums:"
                                                "COOPS %s data for station %s and datum %s",
                                                dat, variable, station_id, all_datums[dat]
                                    )
                                    pass

                        # Applying datum shift in case it was specified
                        if (
                            datum_shift.isalpha() is False
                            and datum_shift != "NAVD88"
                            and float(datum_shift) != 0.0
                        ):
                            timeseries["OBS"] = timeseries["OBS"] + float(
                                datum_shift
                            )
                            logger.info(
                                "A datum shift of %s meters was "
                                "applied to the water level data for "
                                "station %s as specified in the %s_wl_station.",
                                datum_shift,
                                station_id,
                                ofs,
                            )

                        else:
                            logger.info(
                                "A datum shift of %s was read for "
                                "station %s (see: %s_wl_station). "
                                "No datum shift applied.",
                                datum_shift,
                                station_id,
                                ofs,
                            )

                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )

                    elif (
                        variable == "currents"
                    ):  # different format than scalar variables
                        formatted_series = format_obs_timeseries.vector(
                            timeseries, start_date_full, end_date_full
                        )

                    else:
                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )
                except ValueError:
                    logger.info("Fail when getting COOPS %s data for station %s",
                                variable, station_id
                    )

            elif read_station_ctl_file[0][i][3] == "USGS":
                try:
                    retrieve_input.station = str(station_id)
                    retrieve_input.start_date = start_date
                    retrieve_input.end_date = end_date
                    retrieve_input.variable = variable
                    timeseries = retrieve_usgs_station.retrieve_usgs_station(
                        retrieve_input, logger
                    )
                    if timeseries is None:
                        continue
                    timeseries = timeseries[timeseries["OBS"].notna()]

                    if variable == "water_level":  # apply datum shift (if any)
                        # This is only important for water level, this shift will
                        # be applied to the water level time series if datum_shift != zero
                        datum_shift = read_station_ctl_file[1][i][2]

                        if (
                            datum_shift.isalpha() is False
                            and datum_shift != "NAVD88"
                            and float(datum_shift) != 0.0
                        ):
                            timeseries["OBS"] = timeseries["OBS"] + float(
                                datum_shift
                            )
                            logger.info(
                                "A datum shift of %s meters was applaied "
                                "to the water level data for station %s "
                                "as specified in the %s_wl_station.",
                                datum_shift,
                                station_id,
                                ofs,
                            )
                        else:
                            logger.info(
                                "A datum shift of %s was read for "
                                "station %s (see: %s_wl_station). "
                                "No datum shift applied.",
                                datum_shift,
                                station_id,
                                ofs,
                            )

                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )

                    elif (
                        variable == "currents"
                    ):  # different format than scalar variables
                        formatted_series = format_obs_timeseries.vector(
                            timeseries, start_date_full, end_date_full
                        )

                    else:
                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )
                except ValueError:
                    logger.info("Fail when getting COOPS %s data for station %s",
                                variable, station_id
                    )

            elif read_station_ctl_file[0][i][3] == "NDBC":
                try:
                    data_station = []
                    for yr_index in range( start_dt.year , end_dt.year + 1 ):
                        if yr_index != datetime.now().year:
                            retrieve_input.station = str(station_id)
                            retrieve_input.year = yr_index
                            retrieve_input.variable = variable
                            try:
                                timeseries = \
                                    retrieve_ndbc_year_station.retrieve_ndbc_year_station(
                                    retrieve_input , logger
                                    )
                                data_station.append( timeseries )
                            except:
                                if start_dt.year != end_dt.year:
                                    date_this_yr = datetime( yr_index, 1 , 1 )
                                elif start_dt.year == end_dt.year:
                                    date_this_yr = datetime( yr_index, start_dt.month , 1 )
                                #date_this_yr.month <= end_dt.month and 
                                while date_this_yr.year <= end_dt.year and date_this_yr.year >= datetime.now().year-1 :
                                    retrieve_input.station = str( station_id )
                                    retrieve_input.year = yr_index
                                    retrieve_input.variable = variable
                                    retrieve_input.month_num = date_this_yr.month
                                    retrieve_input.month = date_this_yr.strftime( "%b" )
                                    try:
                                        timeseries = \
                                            retrieve_ndbc_month_station.retrieve_ndbc_month_station(
                                             retrieve_input , logger
                                            )
                                        if timeseries is not None:
                                            data_station.append(timeseries)
                                    except:
                                        try:
                                            retrieve_input.month = datetime.strptime(
                                                str(date_this_yr.month),"%m"
                                                ).strftime("%b")
                                            timeseries = \
                                                retrieve_ndbc_rt_station.retrieve_ndbc_rt_station(
                                                    retrieve_input , logger
                                                )
                                            if timeseries is not None:
                                                data_station.append(timeseries)
                                        except:
                                            pass

                                    date_this_yr += timedelta( days=32 )
            
                        elif yr_index == datetime.now().year:
                            date_this_yr = datetime( end_dt.year , 1 , 1 )
                            while date_this_yr.month <= end_dt.month and date_this_yr.year <= end_dt.year:
                                retrieve_input.station = str( station_id )
                                retrieve_input.year = yr_index
                                retrieve_input.variable = variable
                                retrieve_input.month_num = date_this_yr.month
                                retrieve_input.month = date_this_yr.strftime( "%b" )

                                try:
                                    timeseries = \
                                        retrieve_ndbc_month_station.retrieve_ndbc_month_station(
                                         retrieve_input , logger
                                        )
                                    if timeseries is not None:
                                        data_station.append(timeseries)
                                except:
                                    try:
                                        retrieve_input.month = datetime.strptime(
                                            str(date_this_yr.month),"%m"
                                            ).strftime("%b")

                                        timeseries = \
                                            retrieve_ndbc_rt_station.retrieve_ndbc_rt_station(
                                                retrieve_input , logger
                                            )

                                        if timeseries is not None:
                                            data_station.append(timeseries)
                                    except:
                                        pass

                                date_this_yr += timedelta( days=32 )

                    if data_station[0] is None:
                        continue
                    # at this point the station data (year+month+RT) has
                    # been downloaded and saved as data_station
                    # the else statement below is used to organize these
                    # broken timeseries into something temporaly adequate
                    # and create the ctlfile

                    data_station = pd.concat(data_station).sort_values(
                    by="DateTime").drop_duplicates()
                    
                    mask = (data_station["DateTime"] >= start_dt) & (
                        data_station["DateTime"] <= end_dt
                    )

                    data_station = data_station.loc[mask]

                    if data_station.empty:
                        continue

                    timeseries = data_station
                    timeseries = timeseries[timeseries["OBS"].notna()]
                    if (
                        variable == "water_level"
                    ):  # apply datum shift (if any)
                        # This is only important for water level, this shift
                        # will be applied to the water level time series if datum_shift != zero
                        datum_shift = read_station_ctl_file[1][i][2]

                        if (
                            datum_shift.isalpha() is False
                            and datum_shift != "NAVD88"
                            and float(datum_shift) != 0.0
                        ):
                            timeseries["OBS"] = timeseries["OBS"] + float(
                                datum_shift
                            )
                            logger.info(
                                "A datum shift of %s meters was "
                                "appalied to the water level data"
                                " for station %s as specified in "
                                "the %s_wl_station.",
                                datum_shift,
                                station_id,
                                ofs,
                            )
                        else:
                            logger.info(
                                "A datum shift of %s was read "
                                "for station %s "
                                "(see: %s_wl_station). "
                                "No datum shift applied.",
                                datum_shift,
                                station_id,
                                ofs,
                            )

                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )
                    elif (
                        variable == "currents"
                    ):  # different format than scalar variables
                        formatted_series = format_obs_timeseries.vector(
                            timeseries, start_date_full, end_date_full
                        )
                    else:
                        formatted_series = format_obs_timeseries.scalar(
                            timeseries, start_date_full, end_date_full
                        )
                except Exception:
                    logger.info("Fail when getting NDBC %s data for station %s",
                                variable, station_id
                    )

            else:
                logger.error(
                    "The second item on the first line of each station "
                    "in the %s_%s_station.ctl should be written as "
                    "ID_variable_ofs_DataSouce(TC, NDBC,or USGS)",
                    ofs,
                    name_var,
                )
                logger.error(
                    "Data source %s in %s_%s_station.ctl not supported",
                    read_station_ctl_file[0][i][3],
                    ofs,
                    name_var,
                )
                return

            # This section writes the station.ctl file with all the
            # data found and formatted in the ctl_file list
            try:
                # This only happens if the formatted series is actually created
                if formatted_series != 'NoDataFound':
                    obs_path = os.path.join(data_observations_1d_station_path, str(station_id+"_"+ofs+"_"+name_var+"_station.obs"))
                    with open(obs_path,
                    #with open(
                    #    r""
                    #    + data_observations_1d_station_path
                    #    + "/"
                    #    + station_id
                    #    + "_"
                    #    + ofs
                    #    + "_"
                    #    + name_var
                    #    + "_station.obs",
                        "w", encoding="utf-8"
                    ) as output:
                        for i in formatted_series:
                            output.write(str(i) + "\n")
                        logger.info(
                            "%s_%s_station.obs created successfully", ofs, name_var
                        )
                else:
                    logger.info("Formatted %s time series not found for station %s",
                                variable, station_id
                    )
            except FileNotFoundError as ex:
                logger.error(
                    "Saving station failed. Please check the directory "
                    "path: %s -- %s.",
                    data_observations_1d_station_path,
                    str(ex),
                )


### Execution:
if __name__ == "__main__":

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python write_obs_ctlfile.py",
        usage="%(prog)s",
        description="ofs write Station Control File",
    )

    parser.add_argument(
        "-o",
        "--ofs",
        required=True,
        help="Choose from the list on the ofs_Extents folder, you can also "
             "create your own shapefile, add it top the ofs_Extents folder and "
             "call it here",
    )
    parser.add_argument("-p", "--path", required=True,
                        help="Inventary File path")
    parser.add_argument("-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument("-e", "--EndDate_full", required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-d",
        "--datum",
        required=True,
        help="datum: 'MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW', 'MLLW', "
             "'NAVD', 'STND'",
    )

    args = parser.parse_args()

    get_station_observations(
        args.StartDate_full,
        args.EndDate_full,
        args.datum,
        args.path,
        args.ofs,
        None
    )
