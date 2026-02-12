"""
Create inventory of USGS stream gauge stations.

This module uses the modern USGS Water Data API (via dataretrieval) to
discover water and atmospheric stations within specified geographic bounds.
It replaces the decommissioned NWIS inventory endpoint.

Parameter availability is checked via searvey to set accurate has_wl,
has_temp, has_salt, has_cu flags, preventing unnecessary retrieval calls.
"""

import os
from logging import Logger

import geopandas as gpd
import pandas as pd
from dataretrieval import waterdata
from searvey.usgs import get_station_parameter_availability

# Environment variable for USGS API key (same as searvey uses)
_USGS_API_KEY_ENV = 'API_USGS_PAT'


# Site type codes for water-related monitoring locations
WATER_SITE_TYPES = {
    'OC', 'OC-CO',     # Ocean / Coastal
    'ES',               # Estuary
    'LK',               # Lake
    'ST', 'ST-CA',      # Stream / Canal
    'ST-DCH', 'ST-TS',  # Ditch / Tidal stream
    'WE',               # Wetland
}

# Max stations per parameter availability API call.
# Kept below 100 to avoid 403 "query exceeding server limits" errors.
_PARAM_AVAIL_CHUNK_SIZE = 80


def _check_parameter_availability(site_nos, logger):
    """Query searvey for which parameters each station has.

    Returns a DataFrame with columns: site_no, has_water_level,
    has_temperature, has_salinity, has_currents.
    Processes in chunks to avoid oversized API requests.
    """
    site_list = list(site_nos)
    chunks = [
        site_list[i:i + _PARAM_AVAIL_CHUNK_SIZE]
        for i in range(0, len(site_list), _PARAM_AVAIL_CHUNK_SIZE)
    ]
    results = []
    for idx, chunk in enumerate(chunks):
        logger.info(
            'Checking parameter availability: chunk %d/%d (%d stations)',
            idx + 1, len(chunks), len(chunk)
        )
        try:
            avail = get_station_parameter_availability(site_nos=chunk)
            results.append(avail)
        except Exception as ex:
            logger.warning(
                'Parameter availability check failed for chunk %d: %s',
                idx + 1, ex
            )
            # Fall back to all-True for this chunk
            fallback = pd.DataFrame({
                'site_no': chunk,
                'has_water_level': True,
                'has_temperature': True,
                'has_salinity': True,
                'has_currents': True,
            })
            results.append(fallback)
    return pd.concat(results, ignore_index=True)


def inventory_usgs_station(
    argu_list: list[float],
    start_date: str,
    end_date: str,
    logger: Logger
) -> pd.DataFrame:
    """
    Create inventory of USGS stations within geographic bounds.

    Uses the modern USGS Water Data API via dataretrieval's
    get_monitoring_locations() with bounding box query, then checks
    parameter availability via searvey to set accurate variable flags.

    Note:
        start_date and end_date are accepted for API compatibility with
        the caller (retrieving_inventories) but are not used by the
        modern USGS Water Data API for station discovery.
    """
    empty_df = pd.DataFrame(columns=[
        'ID', 'X', 'Y', 'Source', 'Name',
        'has_wl', 'has_temp', 'has_salt', 'has_cu'
    ])

    lat_1, lat_2, lon_1, lon_2 = (
        float(argu_list[0]),
        float(argu_list[1]),
        float(argu_list[2]),
        float(argu_list[3]),
    )

    logger.info('Calling USGS Water Data API for inventory...')

    try:
        # Query monitoring locations by bounding box
        # bbox format: [lon_min, lat_min, lon_max, lat_max]
        sites, _ = waterdata.get_monitoring_locations(
            bbox=[lon_1, lat_1, lon_2, lat_2],
        )
    except Exception as ex:
        logger.error('USGS station discovery failed: %s', str(ex))
        return empty_df

    if sites.empty:
        logger.warning('No USGS stations found in bounding box.')
        return empty_df

    # Filter to water-related site types
    stations = sites[sites['site_type_code'].isin(WATER_SITE_TYPES)].copy()
    logger.info(
        'Found %d water-related stations out of %d total in bounding box',
        len(stations), len(sites)
    )

    if stations.empty:
        logger.warning('No water-related USGS stations found.')
        return empty_df

    # Extract lat/lon from geometry
    if isinstance(stations, gpd.GeoDataFrame) and 'geometry' in stations.columns:
        lons = stations.geometry.x.values
        lats = stations.geometry.y.values
    else:
        logger.error('USGS stations missing geometry data.')
        return empty_df

    # Extract site_no from monitoring_location_id
    # e.g., "USGS-01669850" -> "01669850"
    site_nos = stations['monitoring_location_id'].str.replace(
        'USGS-', '', regex=False
    ).values

    # Build output DataFrame
    inventory = pd.DataFrame({
        'ID': site_nos,
        'X': pd.to_numeric(lons),
        'Y': pd.to_numeric(lats),
        'Source': 'USGS',
        'Name': stations['monitoring_location_name'].values,
    })

    # Check if USGS API key is available for parameter availability queries.
    # Without a key, USGS limits to 50 requests/hour. The param availability
    # check uses ~35 requests for a large domain, leaving very few for actual
    # data retrieval. Skip it to preserve the budget for data retrieval.
    has_api_key = bool(os.environ.get(_USGS_API_KEY_ENV, '').strip())

    if has_api_key:
        logger.info(
            'Checking parameter availability for %d USGS stations...',
            len(site_nos)
        )
        avail = _check_parameter_availability(site_nos, logger)

        # Merge parameter availability flags
        avail = avail.rename(columns={
            'has_water_level': 'has_wl',
            'has_temperature': 'has_temp',
            'has_salinity': 'has_salt',
            'has_currents': 'has_cu',
        })
        inventory = inventory.merge(
            avail[['site_no', 'has_wl', 'has_temp', 'has_salt', 'has_cu']],
            left_on='ID', right_on='site_no', how='left',
        )
        inventory.drop(columns=['site_no'], inplace=True)

        # Fill any missing availability info with False (conservative)
        for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
            inventory[col] = inventory[col].fillna(False)

        # Filter out stations that have no data for any variable
        has_any = inventory[['has_wl', 'has_temp', 'has_salt', 'has_cu']].any(axis=1)
        n_before = len(inventory)
        inventory = inventory[has_any].reset_index(drop=True)
        logger.info(
            'After parameter filtering: %d stations with data '
            '(removed %d with no data)',
            len(inventory), n_before - len(inventory)
        )
    else:
        logger.warning(
            'No USGS API key found (%s env var not set). '
            'Skipping parameter availability check to conserve the '
            '50 requests/hour rate limit for data retrieval. '
            'Set the API key for faster, more targeted retrieval.',
            _USGS_API_KEY_ENV
        )
        # Without param availability, set all flags to True
        for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
            inventory[col] = True

    logger.info(
        'inventory_USGS_station.py run successfully - found %d stations '
        '(wl=%d, temp=%d, salt=%d, cu=%d)',
        len(inventory),
        inventory['has_wl'].sum(),
        inventory['has_temp'].sum(),
        inventory['has_salt'].sum(),
        inventory['has_cu'].sum(),
    )
    return inventory
