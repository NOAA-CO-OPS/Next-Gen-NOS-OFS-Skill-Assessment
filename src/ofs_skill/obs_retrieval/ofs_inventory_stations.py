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
import argparse
import logging
import logging.config
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from ofs_skill.obs_retrieval import (
    utils,
)
from ofs_skill.obs_retrieval.filter_inventory import filter_inventory
from ofs_skill.obs_retrieval.inventory_ndbc_station import inventory_ndbc_station
from ofs_skill.obs_retrieval.inventory_t_c_station import inventory_t_c_station
from ofs_skill.obs_retrieval.inventory_usgs_station import inventory_usgs_station
from ofs_skill.obs_retrieval.ofs_geometry import ofs_geometry


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
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Please check Start Date -
        {start_date}, End Date - '{end_date}'. Abort!"""
        logger.error(error_message)
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
    if not os.path.isfile(f'{ofs_extents_path}/{ofs}.shp'):
        error_message = f"""Shapefile {ofs}.shp is not found at the
        folder {ofs_extents_path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)


def retrieving_inventories(geo, start_date, end_date, ofs, stationowner,
                           logger):
    """ Retrieving Inventories """
    lat1, lat2, lon1, lon2 = geo[-4], geo[-3], geo[-2], geo[-1]

    t_c_future = None
    usgs_future = None
    ndbc_future = None

    with ThreadPoolExecutor(max_workers=3) as executor:
        if 'co-ops' in stationowner:
            logger.info(
                'Retrieving Tides and Currents inventory for %s from %s to %s',
                ofs, start_date, end_date
            )
            t_c_future = executor.submit(
                inventory_t_c_station, lat1, lat2, lon1, lon2, logger
            )

        if 'ndbc' in stationowner:
            logger.info(
                'Retrieving NDBC inventory for %s from %s to %s',
                ofs, start_date, end_date
            )
            ndbc_future = executor.submit(
                inventory_ndbc_station, lat1, lat2, lon1, lon2, logger
            )

        if 'usgs' in stationowner:
            logger.info(
                'Retrieving USGS inventory for %s from %s to %s',
                ofs, start_date, end_date
            )
            argu_list = (lat1, lat2, lon1, lon2)
            usgs_future = executor.submit(
                inventory_usgs_station, argu_list, start_date, end_date, logger
            )

    # Collect results (blocks until each future completes)
    t_c = t_c_future.result() if t_c_future else None
    usgs = usgs_future.result() if usgs_future else None
    ndbc = ndbc_future.result() if ndbc_future else None

    if t_c is not None:
        logger.info('Finished retrieving Tides and Currents inventory!')
    if ndbc is not None:
        logger.info('Finished retrieving NDBC inventory!')
    if usgs is not None:
        logger.info('Finished retrieving USGS inventory!')

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
     that is necessary to find duplicates.

     Variable availability columns (has_wl, has_temp, has_salt, has_cu) are
     preserved to enable efficient data retrieval by skipping stations that
     don't have data for a given variable.
    """

    logger.info('Merging Inventories')

    # Ensure all dataframes have the variable availability columns
    var_cols = ['has_wl', 'has_temp', 'has_salt', 'has_cu']
    for df in [t_c, usgs, ndbc]:
        if df is not None:
            for col in var_cols:
                if col not in df.columns:
                    df[col] = True  # Default to True for backwards compatibility

    dataset = pd.concat([t_c, usgs, ndbc], ignore_index=True)

    # For duplicate removal, only deduplicate within the same source.
    # Different sources (CO-OPS, USGS, NDBC) may provide different
    # variables at the same location and should all be preserved.
    dataset_2 = dataset.copy()
    dataset_2['X_round'] = dataset_2['X'].round(2)
    dataset_2['Y_round'] = dataset_2['Y'].round(2)

    # Group by source AND rounded coordinates to only remove
    # duplicates within the same data source
    agg_dict = {
        'ID': 'first',
        'X': 'first',
        'Y': 'first',
        'Name': 'first',
        'has_wl': 'max',  # max of bool = OR
        'has_temp': 'max',
        'has_salt': 'max',
        'has_cu': 'max',
    }
    dataset_dedup = dataset_2.groupby(
        ['Source', 'X_round', 'Y_round'], as_index=False
    ).agg(agg_dict)

    # Convert back to bool
    for col in var_cols:
        dataset_dedup[col] = dataset_dedup[col].astype(bool)

    # Drop the temporary rounded columns
    dataset_final = dataset_dedup.drop(columns=['X_round', 'Y_round'])
    dataset_final = dataset_final.reset_index(drop=True)

    # This loop creates a set of x and y "Point" and test if it falls inside
    # poly if true the index is saved on a list (index_true) that is then
    # used to filter dataset_final

    logger.info('Starting OFS boundary filter.')
    ofs_polygon = Polygon(geo[0])
    ofs_polygon_buffered = ofs_polygon.buffer(0.04)

    points = gpd.GeoSeries(
        [Point(x, y) for x, y in zip(dataset_final['X'], dataset_final['Y'])]
    )
    mask = points.within(ofs_polygon_buffered)

    # Log stations outside the boundary
    for idx in dataset_final.index[~mask]:
        logger.debug(
            'Station %s is %s degrees outside of the OFS shapefile.',
            dataset_final['ID'].iloc[idx],
            points.iloc[idx].distance(ofs_polygon),
        )

    result = dataset_final[mask].reset_index(drop=True)
    logger.info('Finished OFS boundary filter.')

    # Log variable availability summary
    logger.info(
        'Merged inventory: %d stations total, '
        '%d with water_level, %d with water_temperature, '
        '%d with salinity, %d with currents',
        len(result),
        result['has_wl'].sum(),
        result['has_temp'].sum(),
        result['has_salt'].sum(),
        result['has_cu'].sum(),
    )

    return result


def ofs_inventory_stations(ofs, start_date, end_date, path, stationowner,
                           logger):
    """ Specify defaults (can be overridden with command line options) """

    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent.parent/\
                           log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting Inventory Retrieval Process ---')

    dir_params = utils.Utils().read_config_section(
        'directories', logger)
    station_list = []
    if 'list' in stationowner:
        station_list = (utils.Utils().
                        read_config_section('station_IDs', logger)\
                        ['station_id_list']).replace(',','').split(' ')

    # parameter validation
    ofs_extents_path = os.path.join(path, dir_params['ofs_extents_dir'])

    argu_list = (start_date, end_date, path, ofs, ofs_extents_path)
    parameter_validation(argu_list, logger)

    control_files_path = os.path.join(
        path,dir_params['control_files_dir'])
    os.makedirs(control_files_path,exist_ok = True)

    try:
        geo = ofs_geometry(ofs, path, logger)

        dataset_final = retrieving_inventories(
            geo, start_date, end_date, ofs, stationowner, logger
        )

        logger.info('Searching for duplicate stations in inventory file')
        #filter duplicate NDBC stations
        dataset_final = filter_inventory(dataset_final,
                                                          station_list,
                                                          logger)
        logger.info('Duplicate station filter complete!')

        dataset_final.to_csv(
            r'' + control_files_path + '/inventory_all_' + ofs + '.csv'
        )

        logger.info(
            'Final Inventory saved as: %s/inventory_all_%s.csv',
            control_files_path, ofs
        )
        return dataset_final
    except Exception as ex:
        logger.error(
            'Error happened when creating inventory '
            'file %s/inventory_all_%s.csv -- %s.',
            control_files_path, ofs, str(ex))

        raise Exception('Error happened at ofs_inventory_stations') from ex


# Execution:
if __name__ == '__main__':
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python ofs_inventory_station.py',
        usage='%(prog)s',
        description='OFS Inventory Station',
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""",
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=True,
        help='Inventary File path where ofs_extents/ folder is located',
    )
    parser.add_argument(
        '-s',
        '--StartDate',
        required=True,
        help="Start Date: YYYYMMDD e.g. '20230701'",
    )
    parser.add_argument(
        '-e',
        '--EndDate',
        required=True,
        help="End Date: YYYYMMDD e.g. '20230722'",
    )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        default = 'co-ops,ndbc,usgs',
        help="'CO-OPS','NDBC','USGS',", )


    args = parser.parse_args()
    ofs_inventory_stations(
        args.OFS.lower(),
        args.StartDate,
        args.EndDate,
        args.Path,
        args.Station_Owner.lower(),
        None)
