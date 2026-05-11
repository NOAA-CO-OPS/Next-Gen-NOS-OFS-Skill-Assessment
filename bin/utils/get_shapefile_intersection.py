"""
Compute the spatial intersection of two OFS shapefiles and build an
observation-station inventory restricted to the overlap region.

Outputs:
    ofs_extents/{ofs1}_{ofs2}_overlap.shp
    control_files/inventory_all_{ofs1}_{ofs2}_overlap.csv

@author: PL
"""
from __future__ import annotations

import argparse
import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd

from ofs_skill.obs_retrieval.filter_inventory import filter_inventory
from ofs_skill.obs_retrieval.ofs_geometry import ofs_geometry
from ofs_skill.obs_retrieval.ofs_inventory_stations import retrieving_inventories


def _resolve_ofs_shapefile(ofs_name, shape_path):
    """Resolve {ofs_name}.shp inside shape_path and reject any value that
    escapes that directory via ``..`` or an absolute prefix."""
    base = Path(shape_path).resolve()
    candidate = (base / f'{ofs_name}.shp').resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise ValueError(
            f'OFS name {ofs_name!r} resolves outside {base}'
        ) from exc
    return candidate


def get_shapefile_intersection(shp1, shp2, home_path, stationowner, logger=None):
    """Find overlap between two OFS shapefiles, write it as a new shapefile,
    then retrieve an observation-station inventory inside the overlap."""

    # --- Logger Setup ---
    if logger is None:
        log_config_file = os.path.join(home_path, 'conf', 'logging.conf')

        if not os.path.isfile(log_config_file):
            print(f'CRITICAL ERROR: Log config file not found at {log_config_file}')
            sys.exit(1)

        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting Overlap Process ---')

    # --- Shapefile Setup & Loading ---
    shape_path = os.path.join(home_path, 'ofs_extents')

    try:
        shp1_path = _resolve_ofs_shapefile(shp1, shape_path)
        shp2_path = _resolve_ofs_shapefile(shp2, shape_path)
    except ValueError as exc:
        logger.error('Invalid OFS argument: %s', exc)
        sys.exit(1)

    logger.info('Loading first shapefile: %s...', shp1)
    try:
        gdf1 = gpd.read_file(shp1_path)
    except (OSError, ValueError) as exc:
        logger.error('Error loading %s: %s', shp1_path, exc)
        sys.exit(1)

    logger.info('Loading second shapefile: %s...', shp2)
    try:
        gdf2 = gpd.read_file(shp2_path)
    except (OSError, ValueError) as exc:
        logger.error('Error loading %s: %s', shp2_path, exc)
        sys.exit(1)

    # --- Spatial Intersection ---
    if gdf1.crs != gdf2.crs:
        logger.warning(
            'CRS mismatch detected. Reprojecting the second shapefile to '
            'match the first...'
        )
        gdf2 = gdf2.to_crs(gdf1.crs)

    logger.info('Calculating intersection...')
    intersection_gdf = gpd.overlay(gdf1, gdf2, how='intersection')

    if intersection_gdf.empty:
        logger.info(
            'No overlapping areas were found between the two shapefiles. '
            'No output created.'
        )
        return

    # Downstream ofs_geometry keeps only the largest polygon — warn when the
    # intersection produces multiple features or a mixed geometry type so
    # disconnected legitimate pieces aren't silently dropped without notice.
    if len(intersection_gdf) > 1:
        logger.warning(
            'Intersection produced %d separate features; downstream '
            'ofs_geometry will use the largest polygon only.',
            len(intersection_gdf),
        )
    if 'GeometryCollection' in set(intersection_gdf.geom_type.unique()):
        logger.warning(
            'Intersection contains a GeometryCollection (mixed geometry '
            'types). Inspect the output before relying on the inventory.'
        )

    # Pin the output to EPSG:4326. ofs_geometry reads point coordinates
    # without consulting the .prj file, so we cannot rely on it to handle
    # non-WGS84 inputs correctly.
    if intersection_gdf.crs is None or intersection_gdf.crs.to_epsg() != 4326:
        intersection_gdf = intersection_gdf.to_crs(4326)

    # --- Save New Shapefile ---
    logger.info('Saving overlapping areas to %s...', shape_path)
    new_ofs = f'{shp1}_{shp2}_overlap'

    try:
        # Drop the overlay-suffixed metadata columns (OBJECTID_1/_2,
        # Shape_Leng_1/_2, Shape_Area_1/_2, BUFF_DIST_1/_2, ...) before
        # write. The DBF format silently truncates field names to 10 chars,
        # which can collide _1/_2 pairs into a single column. The overlap
        # polygon is the only attribute any downstream consumer needs.
        intersection_gdf = intersection_gdf[['geometry']]
        shp3_path = os.path.join(shape_path, f'{new_ofs}.shp')
        intersection_gdf.to_file(shp3_path)
        logger.info('Successfully saved %s.shp!', new_ofs)
    except (OSError, ValueError) as exc:
        logger.error('Error saving to %s: %s', shape_path, exc)
        sys.exit(1)

    # --- Inventory Retrieval ---
    # retrieving_inventories accepts %Y-%m-%d directly. The format differs
    # from the rest of the codebase because we bypass
    # ofs_inventory_stations.parameter_validation, which expects %Y%m%d.
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = start_date
    try:
        logger.info('Initializing geometry and retrieving inventories...')
        geo = ofs_geometry(new_ofs, home_path, logger, None)

        dataset_final = retrieving_inventories(
            geo, start_date, end_date, new_ofs, stationowner, logger,
            config_file=None,
        )

        logger.info(
            'Searching for and filtering duplicate stations in inventory '
            'file...'
        )
        dataset_final = filter_inventory(dataset_final, [], logger)
        logger.info('Duplicate station filter complete!')

        control_files_path = os.path.join(home_path, 'control_files')
        inventory_file_path = os.path.join(
            control_files_path, f'inventory_all_{new_ofs}.csv'
        )

        if os.path.exists(inventory_file_path):
            logger.warning(
                'Overwriting existing inventory file: %s',
                inventory_file_path,
            )

        dataset_final.to_csv(inventory_file_path)
        logger.info('Final Inventory saved as: %s', inventory_file_path)

    except Exception as ex:
        logger.exception(
            'Error creating inventory file inventory_all_%s.csv', new_ofs
        )
        raise RuntimeError(
            f'Inventory retrieval failed for {new_ofs}'
        ) from ex


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            'Find the overlapping area between two OFS shapefiles, save '
            'the intersection as a new shapefile, and build an '
            'observation-station inventory inside the overlap.'
        ),
    )

    parser.add_argument(
        '-o1', '--ofs1',
        required=True,
        help='First OFS to overlap',
    )
    parser.add_argument(
        '-o2', '--ofs2',
        required=True,
        help='Second OFS to overlap',
    )
    parser.add_argument(
        '-p', '--home_path',
        required=True,
        help='Path to directory where package is installed',
    )
    parser.add_argument(
        '-so', '--station_owner',
        required=False,
        default='co-ops,ndbc,usgs,chs',
        help=(
            'Comma-separated station providers to include. Valid choices: '
            "'co-ops', 'ndbc', 'usgs', 'chs' "
            "(default: 'co-ops,ndbc,usgs,chs')."
        ),
    )

    args = parser.parse_args()

    get_shapefile_intersection(
        shp1=args.ofs1.lower(),
        shp2=args.ofs2.lower(),
        home_path=args.home_path,
        stationowner=args.station_owner,
        logger=None,
    )
