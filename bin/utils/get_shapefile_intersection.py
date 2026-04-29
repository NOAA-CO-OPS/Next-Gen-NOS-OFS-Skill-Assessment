import os
import sys
import argparse
import logging
import logging.config
from pathlib import Path

import geopandas as gpd

from ofs_skill.obs_retrieval.filter_inventory import filter_inventory
from ofs_skill.obs_retrieval.ofs_inventory_stations import retrieving_inventories
from ofs_skill.obs_retrieval.ofs_geometry import ofs_geometry

def get_shapefile_intersection(shp1, shp2, home_path, stationowner, start_date, end_date, logger=None):
    """
    Takes two shapefiles, finds their overlapping areas,
    and writes the result to a new shapefile. Then a new inventory is
    collected
    """

    # --- Logger Setup ---
    if logger is None:
        log_config_file = os.path.join(home_path, 'conf', 'logging.conf')

        if not os.path.isfile(log_config_file):
            print(f"CRITICAL ERROR: Log config file not found at {log_config_file}")
            sys.exit(-1)

        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info(f"Using log config {log_config_file}")

    logger.info('--- Starting Overlap Process ---')

    # --- Shapefile Setup & Loading ---
    shape_path = os.path.join(home_path, 'ofs_extents')

    logger.info(f"Loading first shapefile: {shp1}...")
    try:
        shp1_path = os.path.join(shape_path, f"{shp1}.shp")
        gdf1 = gpd.read_file(shp1_path)
    except Exception as e:
        logger.error(f"Error loading {shp1_path}: {e}")
        sys.exit(-1)

    logger.info(f"Loading second shapefile: {shp2}...")
    try:
        shp2_path = os.path.join(shape_path, f"{shp2}.shp")
        gdf2 = gpd.read_file(shp2_path)
    except Exception as e:
        logger.error(f"Error loading {shp2_path}: {e}")
        sys.exit(-1)

    # --- Spatial Intersection ---
    if gdf1.crs != gdf2.crs:
        logger.warning("CRS mismatch detected. Reprojecting the second shapefile to match the first...")
        gdf2 = gdf2.to_crs(gdf1.crs)

    logger.info("Calculating intersection...")
    intersection_gdf = gpd.overlay(gdf1, gdf2, how='intersection')

    if intersection_gdf.empty:
        logger.info("No overlapping areas were found between the two shapefiles. No output created.")
        return

    # --- Save New Shapefile ---
    logger.info(f"Saving overlapping areas to {shape_path}...")
    new_ofs = f"{shp1}_{shp2}_overlap"

    try:
        intersection_gdf = intersection_gdf.rename(columns={'Shape_Leng_2': 'Shp_Lng_2'})
        shp3_path = os.path.join(shape_path, f"{new_ofs}.shp")
        intersection_gdf.to_file(shp3_path)
        logger.info(f"Successfully saved {new_ofs}.shp!")
    except Exception as e:
        logger.error(f"Error saving to {shape_path}: {e}")
        sys.exit(-1)

    # --- Inventory Retrieval ---
    try:
        logger.info('Initializing geometry and retrieving inventories...')
        geo = ofs_geometry(new_ofs, home_path, logger, None)

        dataset_final = retrieving_inventories(
            geo, start_date, end_date, new_ofs, stationowner, logger, config_file=None
        )

        logger.info('Searching for and filtering duplicate stations in inventory file...')
        dataset_final = filter_inventory(dataset_final, [], logger)
        logger.info('Duplicate station filter complete!')

        control_files_path = os.path.join(home_path, 'control_files')
        inventory_file_path = os.path.join(control_files_path, f"inventory_all_{new_ofs}.csv")

        dataset_final.to_csv(inventory_file_path)
        logger.info(f"Final Inventory saved as: {inventory_file_path}")

    except Exception as ex:
        logger.error(f"Error creating inventory file inventory_all_{new_ofs}.csv -- {ex}")
        raise Exception('Error happened at ofs_inventory_stations') from ex


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find the overlapping area between two OFS shapefiles and save to a new shapefile."
    )

    # Define standard POSIX command-line arguments
    parser.add_argument('-o1', '--ofs1',
                        required=True,
                        help="First OFS to overlap")
    parser.add_argument('-o2', '--ofs2',
                        required=True,
                        help="Second OFS to overlap")
    parser.add_argument('-p', '--home_path',
                        required=True,
                        help="Path to directory where package is installed")
    parser.add_argument('-so', '--station_owner',
                        required=False,
                        default='co-ops,ndbc,usgs,chs',
                        help="Input station provider to use: 'CO-OPS', 'NDBC', 'USGS', 'CHS'")
    parser.add_argument('-s', '--start_date',
                        required=False,
                        help="Assessment start date: YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument('-e', '--end_date',
                        required=False,
                        help="Assessment end date: YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")

    args = parser.parse_args()

    # Run the function
    get_shapefile_intersection(
        shp1=args.ofs1.lower(),
        shp2=args.ofs2.lower(),
        home_path=args.home_path,
        stationowner=args.station_owner,
        start_date=args.start_date,
        end_date=args.end_date,
        logger=None
    )
