"""
-*- coding: utf-8 -*-

Documentation for Scripts ofs_inventory_stations.py

Script Name: ofs_inventory_stations.py

Technical Contact(s): Name:  FC

Abstract:

   This script is used to create a final inventory file, by combining
   all individual inventory dataframes
   (T_C, NDBC, USGS...), and removing duplicates.
   Duplicates are removed based on location (lat, and long).
   Stations with the same lat and long
   (2 decimal degree precision). Precedent is given to Tides
   and Currents stations over NDBC, and NDBC over USGS.
   The final inventory is saved as a .csv file under /Control_Files

Language:  Python 3.8

Estimated Execution Time: < 4min

Scripts/Programs Called:
 ofs_geometry(ofs,path)
 --- This is called to create the inputs for the following scripts

 inventory_T_C(lat1,lat2,lon1,lon2)
 --- This is to create the Tides and Currents inventory

 inventory_NDBC(lat1,lat2,lon1,lon2)
 --- This is to create the NDBC inventory

 inventory_USGS(lat1,lat2,lon1,lon2,start_date,end_date)
 --- This is to create the USGS inventory

Usage: python ofs_inventory.py

OFS Inventory

Arguments:
 -h, --help            show this help message and exit
 -o ofs, --ofs OFS     Choose from the list on the ofs_extents/ folder, you
                       can also create your own shapefile, add it top the
                       ofs_extents/ folder and call it here
 -p PATH, --path PATH  Inventary File Path
 -s STARTDATE, --StartDate STARTDATE
                       Start Date
 -e ENDDATE, --EndDate ENDDATE
                       End Date
Output:
Name                 Description
inventory_all_{}.csv This is a simple .csv file that has all stations
                     available (ID, X, Y, Source, Name)
dataset_final        Pandas Dataframe with ID, X, Y, Source, and
                      Name info for all stations withing lat and lon 1 and 2

Author Name:  FC       Creation Date:  06/23/2023

Revisions:
Date          Author     Description
07-20-2023    MK   Modified the scripts to add config, logging,
                         try/except and argparse features

08-10-2023    MK   Modified the scripts to match the PEP-8
                         standard and code best practices
02-28-2024    AJK        Added inventory filter function

Remarks:
      The output from this script is used by for retrieving data.
      Only the data found in the
      dataset_final/inventory_all_{}.csv will be considered for download
      inventory_all_{}.csv can be edited manually if the user wants to
      include extra stations

"""
# Libraries:
from datetime import datetime
import sys
import argparse
import logging
import logging.config
import os
import pandas as pd
from shapely.geometry import Point, Polygon


# Scripts:
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import ofs_geometry
from obs_retrieval import inventory_t_c_station
from obs_retrieval import inventory_ndbc_station
from obs_retrieval import inventory_usgs_station
from obs_retrieval import utils
from obs_retrieval import filter_inventory

def parameter_validation(argu_list, logger):
    """ Parameter validation """

    start_date, end_date, path, ofs, ofs_extents_path = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(argu_list[4]))

    # start_date and end_date validation
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
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


def retrieving_inventories(geo, start_date, end_date, ofs, logger):
    """ Retrieving Inventories """
    lat1, lat2, lon1, lon2 = geo[-4], geo[-3], geo[-2], geo[-1]

    logger.info(
        "Retrieving Tides and Currents inventory for %s from %s to %s",
        ofs, start_date, end_date
    )

    t_c = inventory_t_c_station.inventory_t_c_station(
        lat1, lat2, lon1, lon2, logger
    )

    logger.info("Finished retrieving Tides and Currents inventory!")

    logger.info(
        "Retrieving NDBC inventory for %s from %s to %s",
        ofs, start_date, end_date
    )
    ndbc = inventory_ndbc_station.inventory_ndbc_station(
        lat1, lat2, lon1, lon2, logger
    )
    logger.info("Finished retrieving NDBC inventory!")

    logger.info(
        "Retrieving USGS inventory for %s from %s to %s",
        ofs, start_date, end_date
    )
    argu_list = (lat1, lat2, lon1, lon2)
    usgs = inventory_usgs_station.inventory_usgs_station(
        argu_list, start_date, end_date, logger
    )

    logger.info("Finished retrieving USGS inventory!")

    return get_inventory_datasets(geo, t_c, usgs, ndbc, logger)


def get_inventory_datasets(geo, t_c, usgs, ndbc, logger):
    """
     Then these inventories are concatenated in order of priority
     t_c,usgs,ndbc.
     If there is any duplicated data (same lat and lon with lat and long
     rounded to 2 decimals) t_c takes precedent over usgs, which takes
     precedent over ndbc.
     The only diference between dataset and dataset_2 is that dataset_2 has
     lat and lon rounded to 2 decimal degrees,
     that is necessary to find duplicates
    """

    logger.info("Merging Inventories")

    dataset = pd.concat([t_c, usgs, ndbc], ignore_index=True)
    dataset_2 = dataset.round(decimals=2)
    dataset_2 = dataset_2.drop_duplicates(
        subset=["X", "Y"], keep="first"
    )

    i_1 = dataset_2.set_index("ID").index
    i_2 = dataset.set_index("ID").index

    # This is where we go back to dataset (which has the precise lat and lon,
    # which we want to keep) and
    # only keep those rows (stations) that
    # were not removed in the dataset_2.drop_duplicates(subset=['X', 'Y'],
    # keep='first')

    dataset_final = dataset[i_2.isin(i_1)]
    dataset_final = dataset_final.reset_index(drop=True)

    # This loop creates a set of x and y "Point" and test if it falls inside
    # poly if true the index is saved on a list (index_true) that is then
    # used to filter dataset_final

    index_true = []
    for i in range(len(dataset_final["ID"])):
        if Point(dataset_final["X"][i],
                 dataset_final["Y"][i]).within(Polygon(geo[0])) is True:
            index_true.append(i)

    return dataset_final.iloc[index_true]


def ofs_inventory_stations(ofs, start_date, end_date, path, logger):
    """ Specify defaults (can be overridden with command line options) """

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

    logger.info("--- Starting Inventory Retrieval Process ---")

    dir_params = utils.Utils().read_config_section(
        "directories", logger
    )

    # parameter validation
    ofs_extents_path = os.path.join(path, dir_params['ofs_extents_dir'])

    argu_list = (start_date, end_date, path, ofs, ofs_extents_path)
    parameter_validation(argu_list, logger)

    control_files_path = os.path.join(
        path,dir_params["control_files_dir"])
    os.makedirs(control_files_path,exist_ok = True)

    try:
        geo = ofs_geometry.ofs_geometry(ofs, path, logger)

        dataset_final = retrieving_inventories(
            geo, start_date, end_date, ofs, logger
        )

        logger.info("Searching for duplicate stations in inventory file")
        dataset_final = filter_inventory.filter_inventory(dataset_final)  #filter duplicate NDBC stations
        logger.info("Duplicate station filter complete!")

        dataset_final.to_csv(
            r"" + control_files_path + "/inventory_all_" + ofs + ".csv"
        )

        logger.info(
            "Final Inventory saved as: %s/inventory_all_%s.csv",
            control_files_path, ofs
        )
        return dataset_final
    except Exception as ex:
        logger.error(
            "Error happened when creating inventory "
            "file %s/inventory_all_%s.csv -- %s.",
            control_files_path, ofs, str(ex))

        raise Exception("Error happened at ofs_inventory_stations") from ex


# Execution:
if __name__ == "__main__":
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python ofs_inventory_station.py",
        usage="%(prog)s",
        description="OFS Inventory Station",
    )

    parser.add_argument(
        "-o",
        "--OFS",
        required=True,
        help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""",
    )
    parser.add_argument(
        "-p",
        "--Path",
        required=True,
        help="Inventary File path where ofs_extents/ folder is located",
    )
    parser.add_argument(
        "-s",
        "--StartDate",
        required=True,
        help="Start Date: YYYYMMDD e.g. '20230701'",
    )
    parser.add_argument(
        "-e",
        "--EndDate",
        required=True,
        help="End Date: YYYYMMDD e.g. '20230722'",
    )

    args = parser.parse_args()
    ofs_inventory_stations(
        args.OFS.lower(),
        args.StartDate,
        args.EndDate,
        args.Path,
        None)
