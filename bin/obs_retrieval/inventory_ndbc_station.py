"""
-*- coding: utf-8 -*-

Documentation for Scripts inventory_ndbc_station.py

Script Name: inventory_ndbc_station.py

Technical Contact(s): Name:  FC

Abstract:

              This script is used to create a dataframe with all NOAA NDBC
              Stations. This dataframe contains all stations within
              lat1,lat2,lon1,lon2

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
      The inputs for this function can either be entered manually, or
      by the ouputs of ofs_geometry
      The output from this script is used by ofs_inventory.py to create the
      final data inventory
"""

import urllib.request
import urllib.error
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def get_inventory(data):
    """ Get Inventory """
    stat_id, list_lon, list_lat, name_list = [], [], [], []
    datalen = len(data)
    for i in range(0, datalen):
        station = data[i].split('"')[1]
        lon = data[i][data[i].find("lng="):].split('"')[1]
        lat = data[i][data[i].find("lat="):].split('"')[1]
        name = data[i][data[i].find("name="):].split('"')[1]

        list_lon.append(lon)
        list_lat.append(lat)
        stat_id.append(station)
        name_list.append(name)

    inventory_ndbc = pd.DataFrame(
        {
            "ID": stat_id,
            "X": pd.to_numeric(list_lon),
            "Y": pd.to_numeric(list_lat),
            "Source": "NDBC",
            "Name": name_list,
        }
    )

    return inventory_ndbc


def inventory_ndbc_station(lat1, lat2, lon1, lon2, logger):
    """
    This function will go over
    https://www.ndbc.noaa.gov/metadata/stationmetadata.xml and
    retrieve all the station id and lat and lon
    """

    lat1, lat2, lon1, lon2 = (
        float(lat1),
        float(lat2),
        float(lon1),
        float(lon2),
    )

    url_params = utils.Utils().read_config_section(
        "urls", logger
    )

    # Retrieve url from config file
    url = url_params["ndbc_station_metadata_url"]

    try:
        logger.info("Calling NDBC service for inventory...")
        with urllib.request.urlopen(url) as response:
            data = response.read()
    except urllib.error.HTTPError as ex:
        logger.error(
            "NDBC data download failed at %s -- %s",
            url, str(ex)
        )
        return None

    data = data.decode("utf-8")
    data = data.split("<station id=")[1:]

    inventory_ndbc = get_inventory(data)

    inventory_ndbc = inventory_ndbc[inventory_ndbc["X"].between(lon1, lon2)]
    inventory_ndbc_final = inventory_ndbc[
        inventory_ndbc["Y"].between(lat1, lat2)
    ]

    logger.info("inventory_ndbc_station.py run successfully")

    return inventory_ndbc_final
