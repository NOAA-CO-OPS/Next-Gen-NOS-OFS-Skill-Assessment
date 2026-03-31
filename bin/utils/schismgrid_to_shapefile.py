"""
Created on Tue Oct 8 2025

@author: AA - FC

"""
from __future__ import annotations

import logging.config
import os
import subprocess
import sys
from pathlib import Path
import ocsmesh
import geopandas as gpd

parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(parent_dir))


def create_SCHISM_mesh_extent_shapefile(mesh_path: str, shapefile_name: str):
    """
    Reads a mesh file (.gr3 or .2dm), extracts its boundary polygons, and saves
    them as a shapefile in the specified relative output directory.

    Args:
        mesh_path (str): The file path to the input mesh (.gr3 or .2dm).
        shapefile_name (str): The desired name for the output shapefile
        (e.g., "stofs_3d_pac.shp").

    """

    # Specify defaults (can be overridden with command line options)
    log_config_file = 'conf/logging.conf'
    log_config_file = (Path(__file__).parent.parent.parent /
                       log_config_file).resolve()

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using log config %s', log_config_file)
    logger.info('--- Starting the program ---')


    # Construct the full output directory path
    output_path_full = parent_dir/'ofs_extents'
    
    # Construct the full path for the output shapefile
    output_file_path = output_path_full/shapefile_name

    # Mesh Processing Logic
    logger.info(f'Attempting to open mesh file: {mesh_path}')
    try:
        # Assuming the mesh file uses WGS 84 (EPSG:4326)
        SCHISM_mesh = ocsmesh.Mesh.open(mesh_path, crs=4326)
    except FileNotFoundError:
        logger.error(
            f'Mesh file not found at: {mesh_path}. Please check the path.')
        return
    except Exception as e:
        logger.error(f'Error opening mesh file {mesh_path}: {e}')
        return

    logger.info('Mesh loaded successfully. Extracting boundary polygons...')

    # Extract polygons from the mesh triangulation/quadrilaterals
    poly_SCHISM = ocsmesh.utils.get_mesh_polygons(SCHISM_mesh.msh_t)

    # Create the GeoDataFrame
    # We use gpd.GeoSeries to handle the list of shapely polygons
    logger.info('Creating GeoDataFrame...')
    gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(poly_SCHISM),
        crs=4326,
    )

    # Save the Shapefile
    logger.info(f'Saving shapefile to: {output_file_path}')
    try:
        # GeoPandas handles saving the .shp, .shx, .dbf, etc. files
        gdf.to_file(output_file_path)
        logger.info('Successfully created SCHISM extent shapefile.')
    except Exception as e:
        logger.error(f'Error saving shapefile: {e}')
        return


if __name__ == '__main__':

    # 1. Setup argument parsing for command line usage
    import argparse

    parser = argparse.ArgumentParser(
        description=('Extracts the extent of a SCHISM mesh (.gr3 or .2dm) and '
                     'saves it as a shapefile.'),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        'mesh_path',
        type=str,
        help=('The file path to the input mesh file '
              '(e.g., /path/to/my_mesh.gr3 or my_mesh.2dm).'),
    )

    parser.add_argument(
        'output_name',
        type=str,
        help='The name for the output shapefile (e.g., output.shp)',
    )

    args = parser.parse_args()

    # 2. Call the main function with parsed arguments

    # Check if the provided mesh file exists before proceeding
    if not os.path.exists(args.mesh_path):
        print(
            f"Input mesh file '{args.mesh_path}' not found. Exiting.")
        sys.exit(1)

    # Call the main function
    create_SCHISM_mesh_extent_shapefile(
        mesh_path=args.mesh_path,
        shapefile_name=args.output_name,
    )
