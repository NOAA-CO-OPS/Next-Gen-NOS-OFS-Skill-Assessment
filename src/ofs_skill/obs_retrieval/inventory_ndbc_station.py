"""
Create inventory of NOAA NDBC (National Data Buoy Center) stations.

This module retrieves and parses the NDBC station metadata XML to create
an inventory of all buoy stations within specified geographic bounds.
"""

import urllib.error
import urllib.request
from logging import Logger
from typing import Optional

import pandas as pd

from ofs_skill.obs_retrieval import utils


def get_inventory(data: list[str]) -> pd.DataFrame:
    """
    Parse NDBC station metadata from XML splits.

    Args:
        data: List of XML station elements split from metadata

    Returns:
        DataFrame with station inventory information
    """
    stat_id, list_lon, list_lat, name_list = [], [], [], []
    datalen = len(data)
    for i in range(0, datalen):
        station = data[i].split('"')[1]
        lon = data[i][data[i].find('lng='):].split('"')[1]
        lat = data[i][data[i].find('lat='):].split('"')[1]
        name = data[i][data[i].find('name='):].split('"')[1]

        list_lon.append(lon)
        list_lat.append(lat)
        stat_id.append(station)
        name_list.append(name)

    inventory_ndbc = pd.DataFrame(
        {
            'ID': stat_id,
            'X': pd.to_numeric(list_lon),
            'Y': pd.to_numeric(list_lat),
            'Source': 'NDBC',
            'Name': name_list,
        }
    )

    return inventory_ndbc


def inventory_ndbc_station(
    lat1: float,
    lat2: float,
    lon1: float,
    lon2: float,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Create inventory of all NDBC stations within geographic bounds.

    This function retrieves the NDBC station metadata XML and filters
    stations to those within the specified lat/lon bounding box.

    Args:
        lat1: Minimum latitude
        lat2: Maximum latitude
        lon1: Minimum longitude
        lon2: Maximum longitude
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - ID: Station ID
            - X: Longitude
            - Y: Latitude
            - Source: Data source ('NDBC')
            - Name: Station name
        Returns None if metadata download fails.

    Note:
        The inputs for this function can either be entered manually or
        obtained from ofs_geometry output. This output is used by
        ofs_inventory.py to create the final data inventory.
    """
    lat1, lat2, lon1, lon2 = (
        float(lat1),
        float(lat2),
        float(lon1),
        float(lon2),
    )

    url_params = utils.Utils().read_config_section('urls', logger)

    # Retrieve url from config file
    url = url_params['ndbc_station_metadata_url']

    try:
        logger.info('Calling NDBC service for inventory...')
        with urllib.request.urlopen(url) as response:
            data = response.read()
    except urllib.error.HTTPError as ex:
        logger.error('NDBC data download failed at %s -- %s', url, str(ex))
        return None

    data = data.decode('utf-8')
    data = data.split('<station id=')[1:]

    inventory_ndbc = get_inventory(data)

    inventory_ndbc = inventory_ndbc[inventory_ndbc['X'].between(lon1, lon2)]
    inventory_ndbc_final = inventory_ndbc[
        inventory_ndbc['Y'].between(lat1, lat2)
    ]

    logger.info('inventory_ndbc_station.py run successfully')

    return inventory_ndbc_final
