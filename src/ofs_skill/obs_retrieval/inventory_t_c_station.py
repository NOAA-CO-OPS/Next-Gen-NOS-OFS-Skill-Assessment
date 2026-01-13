"""
Create inventory of NOAA Tides and Currents (CO-OPS) stations.

This module queries the CO-OPS Metadata API to retrieve all available
stations within specified geographic bounds. It handles multiple variable
types (water level, temperature, currents, salinity) and consolidates
the results into a single inventory DataFrame.
"""

import json
import urllib.error
import urllib.request
from logging import Logger
from typing import Optional

import pandas as pd

from ofs_skill.obs_retrieval import utils


def get_inventory(
    station_type: str,
    url_params: dict[str, str],
    variable: str,
    logger: Logger
) -> Optional[dict]:
    """
    Retrieve station inventory from CO-OPS Metadata API.

    Args:
        station_type: CO-OPS station type ('waterlevels', 'watertemp',
                     'met', 'currents', 'physocean')
        url_params: Dictionary with URL configuration
        variable: Variable name for logging
        logger: Logger instance

    Returns:
        Dictionary with station data from API, or None if request fails
    """
    station_url = (
        url_params['co_ops_mdapi_base_url']
        + '/webapi/stations.json?type='
        + station_type
        + '&units=english'
    )

    logger.info(f'Calling CO-OPS MDAPI for inventory: {station_type}')
    try:
        with urllib.request.urlopen(station_url) as url:
            inventory = json.load(url)
    except urllib.error.HTTPError as ex:
        logger.error(
            'CO-OPS station %s data download failed at %s -- %s.',
            variable,
            station_url,
            str(ex),
        )
        return None
    return inventory


def inventory_t_c_station(
    lat_1: float,
    lat_2: float,
    lon_1: float,
    lon_2: float,
    logger: Logger
) -> pd.DataFrame:
    """
    Create inventory of all CO-OPS stations within geographic bounds.

    This function queries the CO-OPS Metadata API for multiple variable
    types and consolidates results into a single inventory. Duplicates
    are removed based on station ID.

    Args:
        lat_1: Minimum latitude
        lat_2: Maximum latitude
        lon_1: Minimum longitude
        lon_2: Maximum longitude
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - ID: Station ID
            - X: Longitude
            - Y: Latitude
            - Source: Data source ('CO-OPS')
            - Name: Station name

    Note:
        The inputs for this function can either be entered manually or
        obtained from ofs_geometry output. This output is used by
        ofs_inventory.py to create the final data inventory.
    """
    url_params = utils.Utils().read_config_section('urls', logger)

    lat_1, lat_2, lon_1, lon_2 = (
        float(lat_1),
        float(lat_2),
        float(lon_1),
        float(lon_2),
    )

    id_list, lon_list, lat_list, name_list = [], [], [], []
    for variable in [
        'water_level',
        'water_temperature',
        'currents',
        'salinity',
    ]:

        if variable == 'water_level':
            station_type = 'waterlevels'
        elif variable == 'water_temperature':
            station_type = 'watertemp'
        elif variable in ('wind', 'air_pressure'):
            station_type = 'met'
        elif variable == 'currents':
            station_type = 'currents'
        elif variable == 'salinity':
            station_type = 'physocean'

        inventory = get_inventory(station_type, url_params, variable, logger)

        if inventory is not None:
            for i in range(0, len(inventory['stations'])):
                if (lon_1 < inventory['stations'][i]['lng'] < lon_2) & (
                    lat_1 < inventory['stations'][i]['lat'] < lat_2
                ):
                    id_list.append(inventory['stations'][i]['id'])
                    lon_list.append(inventory['stations'][i]['lng'])
                    lat_list.append(inventory['stations'][i]['lat'])
                    name_list.append(inventory['stations'][i]['name'])

    inventory_t_c_final = pd.DataFrame(
        {
            'ID': id_list,
            'X': pd.to_numeric(lon_list),
            'Y': pd.to_numeric(lat_list),
            'Source': 'CO-OPS',
            'Name': name_list,
        }
    )

    inventory_t_c_final = inventory_t_c_final.drop_duplicates(
        subset=['ID'], keep='first'
    )

    logger.info('inventory_t_c_station.py run successfully')

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)

    return inventory_t_c_final
