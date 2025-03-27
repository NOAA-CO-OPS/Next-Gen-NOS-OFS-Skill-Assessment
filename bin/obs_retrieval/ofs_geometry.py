"""
 -*- coding: utf-8 -*-

 Documentation for Scripts ofs_geometry.py

 Script Name: ofs_geometry.py

 Technical Contact(s): Name:  FC

 Abstract:

               The only input to this function is a string with the ofs name
               {e.g. "cbofs"} and the Path. This name must match the .shp
               file name in the ofs_extents/ folder.
               This function reads a shapefile of the OFS extent and outputs
               the polygon "ofs_mask" that can be used to filter the inventory
               of station with the ofs extent
               This function also outputs max/min lat and long of a ofs, which
               is then used by the other scripts.

 Language:  Python 3.8

 Estimated Execution Time: < 5sec

 Author Name:  FC       Creation Date:  06/23/2023

 Revisions:
 Date          Author     Description
 07-20-2023    MK   Modified the scripts to add config, logging,
                          try/except and argparse features

 08-10-2023    MK   Modified the scripts to match the PEP-8
                          standard and code best practices

 Remarks:
       This script grabs the largest polygon in the shapefile. Ideally there
       would be only 1 polygon, however that is not
       always the case. Thus, this script goes over all polygons in the
       shapefile and grabs the largest.

       If the data retrival is incomplete (you know there should be a station
       but the scripts failed to retrieve it),
       the reason could be the shapefile.

       Make sure your OFS shapefile actually covers the entire study area.
       Try to have a single polygon shapefile.
"""

import os
import shapefile
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def get_response_1(first):
    """Get Response"""
    i_1, i_2, size = [], [], []
    for i in range(len(first["coordinates"])):

        if len(first["coordinates"][i]) > 1:
            for j in range(len(first["coordinates"][i])):
                i_1.append(i)
                i_2.append(j)
                size.append(len(first["coordinates"][i][j]))

        else:
            i_1.append(i)
            i_2.append(0)
            size.append(len(first["coordinates"][i][0]))

    # This is the largest polygon found:
    ofs_mask = first["coordinates"][i_1[size.index(max(size))]][
        i_2[size.index(max(size))]
    ]

    # This little loop here is just to grab the largest and smallest lat and lon
    xx_list, yy_list = [], []
    for i in ofs_mask[:]:
        if isinstance( i , tuple ) :
            xx_list.append(i[0])
            yy_list.append(i[1])
        else:
            return None

    return (min(yy_list), max(yy_list), min(xx_list), max(xx_list), ofs_mask)


def get_response_2(first):
    """Get Response"""
    i_1, size = [], []
    for i in range(len(first["coordinates"])):
        i_1.append(i)
        size.append(len(first["coordinates"][i]))

    # This is the largest polygon found:
    ofs_mask = first["coordinates"][i_1[size.index(max(size))]]

    # This little loop here is just to grab the largest and smallest lat and lon
    xx_list, yy_list = [], []
    for i in ofs_mask[:]:
        if isinstance( i , tuple ):
            xx_list.append( i [0] )
            yy_list.append( i [1] )
        else:
            return None

    return (min(yy_list), max(yy_list), min(xx_list), max(xx_list), ofs_mask)


def ofs_geometry(ofs, path, logger):
    """
    The only input to this function is a string with the ofs name
    {e.g. "cbofs"} and the Path. This name must match the .shp file name in
    the ofs_extent folder.
    This function reads a shapefile of the OFS extent and outputs the polygon
    "ofs_mask" that can be used to filter the inventory of station with the
    ofs extent
    This function also outputs max/min lat and long of a ofs, which is then
    used by the other scripts.
    """

    try:
        dir_params = utils.Utils().read_config_section("directories", logger)
        ofs_extents_path = os.path.join(
            path,
            dir_params["ofs_extents_dir"],
        )

        shape = shapefile.Reader(r"" + ofs_extents_path + "/" + ofs + ".shp")
        first = shape.shapeRecords()[0].shape.__geo_interface__

        # This little loop here is just to make sure we grab the largest polygon
        # in the shapefile in case there is more than one polygon
        # (this is true for the ofs shapefile masks on Tides and Currents due to
        # poor resolution of the shapefiles available.
        # which ends up creating multiple polygons (parts of the mesh that are not
        # connected to the rest of the mesh).

        # i_1,i_2,size are saving the indexes and size of all the polygons, then at
        # the end we grab the index of the largest polygon with:
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
            "Errors happened when reading a shapefile of the ofs "
            + "extents and creating outputs for "
            + ofs_extents_path
            + "/"
            + ofs
            + ".shp -- "
            + str(ex)
        ) from ex

    logger.info("ofs_geometry.py ran sucessfully")

    return ofs_mask, lat_1, lat_2, lon_1, lon_2
