"""
Create inventory of CHS (Canadian Hydrographic Service) stations.

This module retrieves and parses the CHS station metadata XML to create
an inventory of all buoy stations within specified geographic bounds.

@author: PWL
Created on Wed Feb  4 16:58:00 2026
"""

from logging import Logger
from typing import Optional

import pandas as pd
from searvey._chs_api import get_chs_stations


def inventory_chs_station(
    lat1: float,
    lat2: float,
    lon1: float,
    lon2: float,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Create inventory of all CHS stations within geographic bounds.

    This function retrieves the CHS station metadata XML and filters
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
            - Source: Data source ('CHS')
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

    try:
        logger.info('Calling CHS service for inventory...')
        data = get_chs_stations(
            lon_min=lon1, lon_max=lon2,
            lat_min=lat1, lat_max=lat2,
        )
    except Exception as ex:
        logger.error('CHS data download failed! Error: %s', ex)
        return None

    if data.empty:
        logger.info('No CHS stations found within bounding box')
        return None

    codes_per_station = data['timeSeries'].apply(
        lambda ts_list: {ts['code'] for ts in ts_list}
        if isinstance(ts_list, list) else set()
    )

    inventory_chs = pd.DataFrame(
        {
            'ID': data['id'],
            'X': data['longitude'],
            'Y': data['latitude'],
            'Source': 'CHS',
            'Name': data['officialName'],
            'has_wl': codes_per_station.apply(
                lambda c: 'wlo' in c),
            'has_temp': codes_per_station.apply(
                lambda c: bool(c & {'wt1', 'wt2'})),
            'has_salt': codes_per_station.apply(
                lambda c: bool(c & {'ws1', 'ws2'})),
            'has_cu': codes_per_station.apply(
                lambda c: bool(c & {'wcs1', 'wcs2'})
                and bool(c & {'wcd1', 'wcd2'})),
        }
    )

    logger.info('inventory_chs_station.py ran successfully')

    return inventory_chs
