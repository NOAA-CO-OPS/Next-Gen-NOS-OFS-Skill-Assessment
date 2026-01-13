"""
Extract geometric extent from OFS shapefile.

This module reads a shapefile of the OFS extent and extracts the polygon
boundaries and min/max lat/lon coordinates. Used to filter station inventory
to within the OFS domain.
"""

import os
from logging import Logger

import shapefile

from ofs_skill.obs_retrieval import utils


def get_response_1(
    first: dict
) -> tuple[float, float, float, float, list[tuple[float, float]]]:
    """
    Extract largest polygon from shapefile (first search method).

    Searches through nested coordinate structures to find the largest polygon
    and extracts its bounding box.

    Args:
        first: GeoInterface dictionary from shapefile

    Returns:
        Tuple of (min_lat, max_lat, min_lon, max_lon, polygon_coords)
        Returns None if coordinate structure is invalid
    """
    i_1, i_2, size = [], [], []
    for i in range(len(first['coordinates'])):

        if len(first['coordinates'][i]) > 1:
            for j in range(len(first['coordinates'][i])):
                i_1.append(i)
                i_2.append(j)
                size.append(len(first['coordinates'][i][j]))

        else:
            i_1.append(i)
            i_2.append(0)
            size.append(len(first['coordinates'][i][0]))

    # This is the largest polygon found:
    ofs_mask = first['coordinates'][i_1[size.index(max(size))]][
        i_2[size.index(max(size))]
    ]

    # This little loop here is just to grab the largest and smallest lat and lon
    xx_list, yy_list = [], []
    for i in ofs_mask[:]:
        if isinstance(i, tuple):
            xx_list.append(i[0])
            yy_list.append(i[1])
        else:
            return None

    return (min(yy_list), max(yy_list), min(xx_list), max(xx_list), ofs_mask)


def get_response_2(
    first: dict
) -> tuple[float, float, float, float, list[tuple[float, float]]]:
    """
    Extract largest polygon from shapefile (second search method).

    Alternative search method for simpler coordinate structures. Returns
    min/max lat/lon and largest polygon.

    Args:
        first: GeoInterface dictionary from shapefile

    Returns:
        Tuple of (min_lat, max_lat, min_lon, max_lon, polygon_coords)
        Returns None if coordinate structure is invalid
    """
    i_1, size = [], []
    for i in range(len(first['coordinates'])):
        i_1.append(i)
        size.append(len(first['coordinates'][i]))

    # This is the largest polygon found:
    ofs_mask = first['coordinates'][i_1[size.index(max(size))]]

    # This little loop here is just to grab the largest and smallest lat and lon
    xx_list, yy_list = [], []
    for i in ofs_mask[:]:
        if isinstance(i, tuple):
            xx_list.append(i[0])
            yy_list.append(i[1])
        else:
            return None

    return (min(yy_list), max(yy_list), min(xx_list), max(xx_list), ofs_mask)


def ofs_geometry(
    ofs: str,
    path: str,
    logger: Logger
) -> tuple[list[tuple[float, float]], float, float, float, float]:
    """
    Read OFS shapefile and extract geometric extent.

    This function reads a shapefile of the OFS extent and extracts the polygon
    mask and bounding box coordinates. If multiple polygons exist in the
    shapefile, the largest one is selected.

    Args:
        ofs: OFS name (must match .shp filename in ofs_extents folder)
        path: Base path containing ofs_extents directory
        logger: Logger instance

    Returns:
        Tuple of (ofs_mask, lat_1, lat_2, lon_1, lon_2) where:
            - ofs_mask: List of (lon, lat) tuples defining polygon
            - lat_1: Minimum latitude
            - lat_2: Maximum latitude
            - lon_1: Minimum longitude
            - lon_2: Maximum longitude

    Raises:
        Exception: If shapefile cannot be read or processed

    Note:
        This script grabs the largest polygon in the shapefile. Ideally there
        would be only 1 polygon, however that is not always the case. If data
        retrieval is incomplete, check that the shapefile covers the entire
        study area.
    """
    try:
        dir_params = utils.Utils().read_config_section('directories', logger)
        ofs_extents_path = os.path.join(
            path,
            dir_params['ofs_extents_dir'],
        )

        shape = shapefile.Reader(r'' + ofs_extents_path + '/' + ofs + '.shp')
        first = shape.shapeRecords()[0].shape.__geo_interface__

        # This little loop here is just to make sure we grab the largest polygon
        # in the shapefile in case there is more than one polygon
        # (this is true for the ofs shapefile masks on Tides and Currents due to
        # poor resolution of the shapefiles available.
        # which ends up creating multiple polygons (parts of the mesh that are
        # not connected to the rest of the mesh).

        # i_1,i_2,size are saving the indexes and size of all the polygons, then
        # at the end we grab the index of the largest polygon with:
        # [i_1[size.index(max(size))]] [i_2[size.index(max(size))]]

        response_1 = get_response_1(first)
        if response_1 is not None:
            lat_1, lat_2, lon_1, lon_2, ofs_mask = (
                response_1[0],
                response_1[1],
                response_1[2],
                response_1[3],
                response_1[4],
            )
        else:
            response_2 = get_response_2(first)
            lat_1, lat_2, lon_1, lon_2, ofs_mask = (
                response_2[0],
                response_2[1],
                response_2[2],
                response_2[3],
                response_2[4],
            )

    except Exception as ex:
        raise Exception(
            'Errors happened when reading a shapefile of the ofs '
            + 'extents and creating outputs for '
            + ofs_extents_path
            + '/'
            + ofs
            + '.shp -- '
            + str(ex)
        ) from ex

    logger.info('ofs_geometry.py ran sucessfully')

    return ofs_mask, lat_1, lat_2, lon_1, lon_2
