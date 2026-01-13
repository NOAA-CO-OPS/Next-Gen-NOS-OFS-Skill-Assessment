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
2) retrieve_t_and_c_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve Tides and Currents station data
3) retrieve_ndbc_year_station(Station, Year, Variable)
    This script is used to retrieve NDBC station data that is stored as
    yearly files
4) retrieve_NDBC_month_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve NDBC station data that is stored as
    monthly files
5) retrieve_NDBC_RT_station(Station, Year, Variable, Month_Num, Month)
    This script is used to retrieve the most recent (up to real time) NDBC
    station data.
6) retrieve_usgs_station(Station, Start_Date, End_Date, Variable, Datum)
    This script is used to retrieve USGS station data
7) write_obs_ctlfile((Start_Date, End_Date, Datum, Path, OFS))
    This script is used in case the station control file is not found
8) station_ctl_file_extract(ctlfile_Path)
    This script is used to read the station control file and extract the
    necessary information
9) scalar() and vector() from format_obs_timeseries module
    These functions are used to format the time series that will be saved

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
import argparse
import socket

from ofs_skill.obs_retrieval.get_station_observations import get_station_observations

# Import directly from module to avoid circular import

# parse_arguments_to_list is now in utils module

TIMEOUT_SEC = 120 # default API timeout in seconds
socket.setdefaulttimeout(TIMEOUT_SEC)


### Execution:
if __name__ == '__main__':

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python write_obs_ctlfile.py',
        usage='%(prog)s',
        description='ofs write Station Control File',
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the ofs_Extents folder, you can also '
             'create your own shapefile, add it top the ofs_Extents folder and '
             'call it here',
    )
    parser.add_argument('-p', '--Path', required=True,
                        help='Inventary File path')
    parser.add_argument('-s', '--StartDate_full', required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument('-e', '--EndDate_full', required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        '-d',
        '--Datum',
        required=True,
        help="prop.datum: 'MHHW', 'MHW', 'MLW', 'MLLW', 'NAVD88', 'LWD', "
        "'IGLD85', 'xgeoid20b'",
    )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        help="'CO-OPS', 'NDBC', 'USGS',", )
    parser.add_argument(
        '-vs',
        '--Var_Selection',
        required=False,
        help='Which variables do you want to skill assess? Options are: '
            'water_level, water_temperature, salinity, and currents. Choose '
            'any combination. Default (no argument) is all variables.')

    args = parser.parse_args()

    prop1 = ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.datum = args.Datum.upper()

    # Make all station owners default, unless user specifies station owners
    if args.Station_Owner is None:
        prop1.stationowner = 'co-ops,ndbc,usgs'
    elif args.FileType is not None:
        prop1.stationowner = args.Station_Owner.lower()

    #Handle variable selection
    if args.Var_Selection is None:
        # Default: include all vars
        prop1.var_list = 'water_level,water_temperature,salinity,currents'
    elif args.Var_Selection is not None:
        prop1.var_list = args.Var_Selection.lower()

    get_station_observations(prop1, None)
