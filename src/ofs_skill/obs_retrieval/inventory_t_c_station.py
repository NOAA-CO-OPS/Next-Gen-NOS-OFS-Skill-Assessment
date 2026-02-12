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

    logger.info('Calling CO-OPS MDAPI for inventory: %s', station_type)
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
    are removed based on station ID, while preserving information about
    which variables are available at each station.

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
            - has_wl: True if water_level data available
            - has_temp: True if water_temperature data available
            - has_salt: True if salinity data available
            - has_cu: True if currents data available

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

    # Track stations and their available variables
    stations_dict: dict[str, dict] = {}

    variable_config = [
        ('water_level', 'waterlevels', 'has_wl'),
        ('water_temperature', 'watertemp', 'has_temp'),
        ('salinity', 'physocean', 'has_salt'),
        ('currents', 'currents', 'has_cu'),
    ]

    for variable, station_type, var_col in variable_config:
        inventory = get_inventory(station_type, url_params, variable, logger)

        if inventory is not None:
            for i in range(0, len(inventory['stations'])):
                station = inventory['stations'][i]
                if (lon_1 < station['lng'] < lon_2) & (
                    lat_1 < station['lat'] < lat_2
                ):
                    station_id = station['id']
                    if station_id not in stations_dict:
                        stations_dict[station_id] = {
                            'ID': station_id,
                            'X': station['lng'],
                            'Y': station['lat'],
                            'Name': station['name'],
                            'has_wl': False,
                            'has_temp': False,
                            'has_salt': False,
                            'has_cu': False,
                        }
                    stations_dict[station_id][var_col] = True

    if not stations_dict:
        return pd.DataFrame(columns=[
            'ID', 'X', 'Y', 'Source', 'Name',
            'has_wl', 'has_temp', 'has_salt', 'has_cu'
        ])

    inventory_t_c_final = pd.DataFrame(list(stations_dict.values()))
    inventory_t_c_final['Source'] = 'CO-OPS'
    inventory_t_c_final['X'] = pd.to_numeric(inventory_t_c_final['X'])
    inventory_t_c_final['Y'] = pd.to_numeric(inventory_t_c_final['Y'])

    # Reorder columns
    inventory_t_c_final = inventory_t_c_final[[
        'ID', 'X', 'Y', 'Source', 'Name',
        'has_wl', 'has_temp', 'has_salt', 'has_cu'
    ]]

    logger.info('inventory_t_c_station.py run successfully')
    logger.info(
        'Found %d CO-OPS stations: %d with water_level, '
        '%d with water_temperature, %d with salinity, %d with currents',
        len(inventory_t_c_final),
        inventory_t_c_final['has_wl'].sum(),
        inventory_t_c_final['has_temp'].sum(),
        inventory_t_c_final['has_salt'].sum(),
        inventory_t_c_final['has_cu'].sum(),
    )

    return inventory_t_c_final
