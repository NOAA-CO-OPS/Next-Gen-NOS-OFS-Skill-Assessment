"""
Create inventory of NOAA NDBC (National Data Buoy Center) stations.

This module retrieves and parses the NDBC active stations XML to create
an inventory of all buoy stations within specified geographic bounds.

Uses activestations.xml which includes data availability flags (met,
currents, waterquality) to set accurate has_* variable flags.
"""

import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from logging import Logger
from typing import Optional

import pandas as pd

from ofs_skill.obs_retrieval import utils

# URL path for active stations (relative to ndbc_noaa_url)
_ACTIVE_STATIONS_PATH = 'activestations.xml'


def inventory_ndbc_station(
    lat1: float,
    lat2: float,
    lon1: float,
    lon2: float,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Create inventory of all NDBC stations within geographic bounds.

    This function retrieves the NDBC active stations XML which includes
    data availability flags, and filters stations to the bounding box.

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
            - has_wl: Has water level data (False - TIDE is rare on buoys)
            - has_temp: Has temperature data (from met flag)
            - has_salt: Has salinity data (from waterquality flag)
            - has_cu: Has current data (from currents flag)
        Returns None if metadata download fails.
    """
    lat1, lat2, lon1, lon2 = (
        float(lat1),
        float(lat2),
        float(lon1),
        float(lon2),
    )

    url_params = utils.Utils().read_config_section('urls', logger)
    base_url = url_params['ndbc_noaa_url'].rstrip('/')
    url = f'{base_url}/{_ACTIVE_STATIONS_PATH}'

    try:
        logger.info('Calling NDBC service for inventory...')
        with urllib.request.urlopen(url) as response:
            data = response.read()
    except (urllib.error.HTTPError, urllib.error.URLError) as ex:
        logger.error('NDBC data download failed at %s -- %s', url, str(ex))
        return None

    try:
        root = ET.fromstring(data)
    except ET.ParseError as ex:
        logger.error('Failed to parse NDBC XML: %s', str(ex))
        return None

    records = []
    for station in root.iter('station'):
        sid = station.get('id', '')
        lat_str = station.get('lat', '')
        # activestations.xml uses 'lon', stationmetadata.xml uses 'lng'
        lon_str = station.get('lon') or station.get('lng', '')
        name = station.get('name', '')

        if not sid or not lat_str or not lon_str:
            continue

        try:
            lat = float(lat_str)
            lon = float(lon_str)
        except ValueError:
            continue

        # Filter to bounding box
        if not (lat1 <= lat <= lat2 and lon1 <= lon <= lon2):
            continue

        met = station.get('met', 'n').lower() == 'y'
        currents = station.get('currents', 'n').lower() == 'y'
        waterquality = station.get('waterquality', 'n').lower() == 'y'

        records.append({
            'ID': sid,
            'X': lon,
            'Y': lat,
            'Source': 'NDBC',
            'Name': name,
            # TIDE data is rare on NDBC buoys; no XML flag exists for it.
            # CO-OPS stations typically cover water level in the same areas.
            'has_wl': False,
            # Temperature (WTMP/OTMP) is available on most met-equipped buoys
            'has_temp': met or waterquality,
            # Salinity (SAL) is in ocean mode, indicated by waterquality flag
            'has_salt': waterquality,
            # Current data (ADCP) is indicated by currents flag
            'has_cu': currents,
        })

    if not records:
        logger.warning('No NDBC stations found in bounding box.')
        return pd.DataFrame(columns=[
            'ID', 'X', 'Y', 'Source', 'Name',
            'has_wl', 'has_temp', 'has_salt', 'has_cu'
        ])

    inventory = pd.DataFrame(records)

    # Filter out stations with no relevant data
    has_any = inventory[['has_wl', 'has_temp', 'has_salt', 'has_cu']].any(axis=1)
    n_before = len(inventory)
    inventory = inventory[has_any].reset_index(drop=True)

    logger.info(
        'inventory_ndbc_station.py run successfully - %d stations '
        '(temp=%d, salt=%d, cu=%d, removed %d with no data)',
        len(inventory),
        inventory['has_temp'].sum(),
        inventory['has_salt'].sum(),
        inventory['has_cu'].sum(),
        n_before - len(inventory),
    )

    return inventory
