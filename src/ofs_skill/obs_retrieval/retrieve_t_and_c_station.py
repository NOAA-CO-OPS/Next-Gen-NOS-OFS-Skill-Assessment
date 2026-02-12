"""
Retrieve time series observations from NOAA Tides and Currents (CO-OPS) API.

This module provides functions to retrieve tidal observations, water level,
temperature, salinity, currents, and other oceanographic data from NOAA CO-OPS
stations, as well as tidal predictions and nearest station finding capabilities.

The retrieval is performed in 30-day chunks to handle API limitations, with
automatic retry logic for temperature and salinity using backup URLs.
"""

import json
import math
import urllib.request
from datetime import datetime, timedelta
from logging import Logger
from typing import Optional
from urllib.error import HTTPError

import pandas as pd

from ofs_skill.obs_retrieval import t_and_c_properties, utils


def retrieve_t_and_c_station(
    retrieve_input: object,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve time series observations from NOAA Tides and Currents station.

    This function loops between the start and end date, gathering data
    in 30-day pieces. If the last 30-day period does not end exactly at
    the end date (which is very likely), data will be masked between
    start_dt_0 and end_dt_0.

    Args:
        retrieve_input: Object with attributes:
            - station: Station ID
            - start_date: Start date in YYYYMMDD format
            - end_date: End date in YYYYMMDD format
            - variable: Variable type ('water_level', 'water_temperature',
                       'currents', 'salinity', 'wind', 'air_pressure')
            - datum: Vertical datum (for water_level)
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - DateTime: Observation timestamps
            - DEP01: Observation depth in meters
            - OBS: Observation values
            - DIR: Direction values (for currents/wind only)
        Returns None if no data retrieved.

    Raises:
        HTTPError: If API request fails after retries
    """
    variable = retrieve_input.variable

    t_c = t_and_c_properties.TidesandCurrentsProperties()

    # Retrieve url from config file
    url_params = utils.Utils().read_config_section('urls', logger)
    t_c.mdapi_url = url_params['co_ops_mdapi_base_url']
    t_c.api_url = url_params['co_ops_api_base_url']

    t_c.start_dt_0 = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    t_c.start_dt = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var, t_c.total_dir = [], [], []

    while t_c.start_dt <= t_c.end_dt:
        date_i = (
            t_c.start_dt.strftime('%Y') +
            t_c.start_dt.strftime('%m') +
            t_c.start_dt.strftime('%d')
        )
        date_f = (
            (t_c.start_dt + t_c.delta).strftime('%Y')
            + (t_c.start_dt + t_c.delta).strftime('%m')
            + (t_c.start_dt + t_c.delta).strftime('%d')
        )

        if variable == 'water_level':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product={variable}&datum='
                f'{retrieve_input.datum}&time_zone=gmt&units='
                f'metric&format=json'
            )

        elif variable == 'water_temperature':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product='
                f'{variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

            t_c.station_url_2 = (
                f'{t_c.api_url}/datagetter?product='
                f'{variable}&application='
                f'NOS.COOPS.TAC.PHYSOCEAN&begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&time_zone=GMT&units='
                f'metric&interval=6&format=json'
            )

        elif variable == 'salinity':

            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product='
                f'{variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

            t_c.station_url_2 = (
                f'{t_c.api_url}/datagetter?product='
                f'{variable}&application='
                f'NOS.COOPS.TAC.PHYSOCEAN&begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&time_zone=GMT&units='
                f'metric&interval=6&format=json'
            )

        elif variable == 'currents':
            t_c.station_url = (
                f'{t_c.api_url}/datagetter?begin_date='
                f'{date_i}&end_date={date_f}&station='
                f'{retrieve_input.station}&product={variable}&time_zone='
                f'gmt&units=metric&format=json'
            )

        if variable in {'water_temperature', 'salinity'}:
            try:
                with urllib.request.urlopen(t_c.station_url) as url:
                    obs = json.load(url)
                logger.info(
                    'CO-OPS station %s contacted for %s retrieval.',
                    retrieve_input.station, variable)
            except HTTPError as ex_1:
                error_msg = get_HTTP_error(ex_1)
                logger.error(
                    'CO-OPS %s observation retrieval failed for station %s! '
                    'HTTP %s %s\n%s',
                    variable, retrieve_input.station, ex_1.code, ex_1.reason,
                    error_msg
                )
                logger.error(f'Exception caught: {ex_1}')
                try:
                    with urllib.request.urlopen(t_c.station_url_2) as url_2:
                        obs = json.load(url_2)
                    logger.info(
                        'CO-OPS backup station %s contacted for %s retrieval.',
                        retrieve_input.station, variable)
                except HTTPError as ex_2:
                    error_msg = get_HTTP_error(ex_2)
                    logger.error(
                        'Backup CO-OPS %s observation retrieval failed for '
                        'station %s! HTTP %s %s\n%s',
                        variable, retrieve_input.station, ex_2.code,
                        ex_2.reason, error_msg
                    )
                    logger.error('Exception caught: %s', ex_2)
                    t_c.start_dt += t_c.delta
                    continue
        else:
            try:
                with urllib.request.urlopen(t_c.station_url) as url:
                    obs = json.load(url)
                logger.info(
                    'CO-OPS station %s contacted for %s retrieval.',
                    retrieve_input.station, variable)
            except HTTPError as ex:
                error_msg = get_HTTP_error(ex)
                logger.error(
                    'CO-OPS %s observation retrieval failed for station %s! '
                    'HTTP %s %s\n%s',
                    variable, retrieve_input.station, ex.code, ex.reason,
                    error_msg
                )
                logger.error('Exception caught: %s', ex)
                t_c.start_dt += t_c.delta
                continue

        t_c.date, t_c.var, t_c.drt = [], [], []
        if 'data' in obs.keys():
            for i in range(len(obs['data'])):
                t_c.date.append(obs['data'][i]['t'])

                if variable in {'water_level',
                                'water_temperature',
                                'air_pressure'}:
                    t_c.var.append(obs['data'][i]['v'])

                elif variable == 'salinity':
                    t_c.var.append(obs['data'][i]['s'])

                elif variable == 'currents':
                    # Convert speed from cm/s to m/s
                    t_c.var.append(float(obs['data'][i]['s']) / 100)
                    t_c.drt.append(obs['data'][i]['d'])

                elif variable == 'wind':
                    t_c.var.append(obs['data'][i]['s'])
                    t_c.drt.append(obs['data'][i]['d'])

            t_c.total_date.append(t_c.date)
            t_c.total_var.append(t_c.var)
            if variable in {'wind', 'currents'}:
                t_c.total_dir.append(t_c.drt)

        t_c.start_dt += t_c.delta

    # Retrieve observation depth from metadata API
    t_c.depth = 0.0
    t_c.depth_url = None
    try:
        t_c.station_url = (
            f'{t_c.mdapi_url}/webapi/stations/{str(retrieve_input.station)}'
            f'/bins.json'
            f'?units=metric'
        )
        with urllib.request.urlopen(t_c.station_url) as url:
            t_c.depth_url = json.load(url)
            logger.info(
                'CO-OPS %s depth retrieval complete for station %s.',
                variable, retrieve_input.station)
    except HTTPError as ex:
        logger.error('CO-OPS %s observation retrieval failed!', variable)
        logger.error('Exception caught: %s', ex)

    if (
        t_c.depth_url is not None
        and t_c.depth_url['bins'] is not None
        and t_c.depth_url['real_time_bin'] is not None
        and t_c.depth_url['bins'][t_c.depth_url['real_time_bin'] - 1][
            'depth'] is not None
    ):
        t_c.depth = float(
            t_c.depth_url['bins'][t_c.depth_url['real_time_bin'] - 1]['depth']
        )

    t_c.total_date = sum(t_c.total_date, [])
    t_c.total_var = sum(t_c.total_var, [])

    if variable in {'wind', 'currents'}:
        t_c.total_dir = sum(t_c.total_dir, [])

        obs = pd.DataFrame(
            {
                'DateTime': pd.to_datetime(t_c.total_date),
                'DEP01': pd.to_numeric(t_c.depth),
                'DIR': pd.to_numeric(t_c.total_dir),
                'OBS': pd.to_numeric(t_c.total_var),
            }
        )

    else:
        obs = pd.DataFrame(
            {
                'DateTime': pd.to_datetime(t_c.total_date),
                'DEP01': pd.to_numeric(t_c.depth),
                'OBS': pd.to_numeric(t_c.total_var),
            }
        )

    mask = (obs['DateTime'] >= t_c.start_dt_0) & (
        obs['DateTime'] <= t_c.end_dt_0)
    obs = obs.loc[mask]

    if len(obs.DateTime) > 0:
        obs = obs.sort_values(by='DateTime').drop_duplicates()
        return obs

    return None


def get_HTTP_error(ex: HTTPError) -> str:
    """
    Parse HTTP error to show CO-OPS API error message.

    Args:
        ex: HTTPError exception from urllib

    Returns:
        Formatted error message string from API response, or default message
    """
    try:
        error_body = ex.read().decode(errors='replace')
        # Attempt to parse JSON if it's JSON formatted
        try:
            error_json = json.loads(error_body)
            error_msg = error_json.get('error', {}).get('message', error_body)
        except json.JSONDecodeError:
            error_msg = error_body
    except Exception:
        error_msg = 'No additional error message available.'

    return error_msg


def retrieve_tidal_predictions(
    retrieve_input: object,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve tidal predictions from CO-OPS API.

    Similar to water_level retrieval but uses product=predictions.

    Args:
        retrieve_input: Object with attributes:
            - station: Station ID
            - start_date: Start date in YYYYMMDDHHMMSS format
            - end_date: End date in YYYYMMDDHHMMSS format
            - datum: Vertical datum reference
        logger: Logger instance

    Returns:
        DataFrame with columns:
            - DateTime: Prediction timestamps
            - TIDE: Predicted tidal values
        Returns None if no data available.
        Returns False if station doesn't support predictions.
    """
    t_c = t_and_c_properties.TidesandCurrentsProperties()

    url_params = utils.Utils().read_config_section('urls', logger)
    t_c.api_url = url_params['co_ops_api_base_url']

    t_c.start_dt_0 = datetime.strptime(
        retrieve_input.start_date, '%Y%m%d%H%M%S'
    )
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, '%Y%m%d%H%M%S')
    t_c.start_dt = datetime.strptime(retrieve_input.start_date, '%Y%m%d%H%M%S')
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, '%Y%m%d%H%M%S')

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var = [], []

    while t_c.start_dt <= t_c.end_dt:
        date_i = t_c.start_dt.strftime('%Y%m%d%%20%H:%M')
        date_f = (t_c.start_dt + t_c.delta).strftime('%Y%m%d%%20%H:%M')

        t_c.station_url = (
            f'{t_c.api_url}/datagetter?begin_date={date_i}&end_date={date_f}'
            f'&station={retrieve_input.station}&product=predictions'
            f'&datum={retrieve_input.datum}&time_zone=gmt&units=metric'
            f'&format=json'
        )

        try:
            with urllib.request.urlopen(t_c.station_url) as url:
                obs = json.load(url)
            logger.info(
                'CO-OPS station %s contacted for tidal predictions.',
                retrieve_input.station
            )
        except HTTPError as ex:
            logger.warning(
                'CO-OPS tidal predictions retrieval failed for %s: %s',
                retrieve_input.station, ex
            )
            t_c.start_dt += t_c.delta
            continue

        # Check for API error message (station doesn't support predictions)
        if 'error' in obs:
            error_msg = obs['error'].get('message', str(obs['error']))
            logger.debug(
                'CO-OPS API error for station %s: %s',
                retrieve_input.station, error_msg
            )
            # Return False to indicate station doesn't support predictions
            return False

        if 'predictions' in obs.keys():
            for i in range(len(obs['predictions'])):
                t_c.total_date.append(obs['predictions'][i]['t'])
                t_c.total_var.append(obs['predictions'][i]['v'])

        t_c.start_dt += t_c.delta

    if not t_c.total_date:
        logger.warning(
            'No tidal prediction data returned from API for station %s',
            retrieve_input.station
        )
        return None

    obs_df = pd.DataFrame({
        'DateTime': pd.to_datetime(t_c.total_date),
        'TIDE': pd.to_numeric(t_c.total_var),
    })

    logger.debug(
        'Tidal data retrieved: %d points from %s to %s',
        len(obs_df), obs_df['DateTime'].min(), obs_df['DateTime'].max()
    )
    logger.debug('Requested range: %s to %s', t_c.start_dt_0, t_c.end_dt_0)

    mask = (obs_df['DateTime'] >= t_c.start_dt_0) & (
        obs_df['DateTime'] <= t_c.end_dt_0
    )
    obs_df = obs_df.loc[mask]

    if len(obs_df.DateTime) > 0:
        obs_df = obs_df.sort_values(by='DateTime').drop_duplicates()
        return obs_df

    logger.warning(
        'Tidal data was retrieved but all %d points were outside requested '
        'time range for station %s',
        len(t_c.total_date), retrieve_input.station
    )

    logger.debug('Exiting tide retrieval.')
    return None


def find_nearest_tidal_stations(
    lat: float,
    lon: float,
    logger: Logger,
    max_stations: int = 10
) -> list[tuple[str, str, float]]:
    """
    Find the nearest CO-OPS stations with tidal predictions.

    Uses the CO-OPS metadata API to get stations with predictions capability
    and calculates distances using Haversine formula.

    Args:
        lat: Latitude of target location
        lon: Longitude of target location
        logger: Logger instance
        max_stations: Maximum number of stations to return (default: 10)

    Returns:
        List of tuples (station_id, station_name, distance_km) sorted by
        distance, or empty list if none found.
    """
    url_params = utils.Utils().read_config_section('urls', logger)
    mdapi_url = url_params['co_ops_mdapi_base_url']

    # Get list of stations with tidal predictions
    stations_url = f'{mdapi_url}/webapi/stations.json?type=tidepredictions'

    try:
        with urllib.request.urlopen(stations_url) as url:
            stations_data = json.load(url)
    except Exception as ex:
        logger.warning('Could not retrieve CO-OPS tidal stations list: %s', ex)
        return []

    if 'stations' not in stations_data or not stations_data['stations']:
        logger.warning('No tidal prediction stations found in CO-OPS API '
                       'response')
        return []

    # Calculate distance using Haversine distance
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great circle distance in km using Haversine formula."""
        R = 6371  # Earth's radius in km
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        return 2 * R * math.asin(math.sqrt(a))

    # Calculate distances for all stations
    station_distances = []
    for station in stations_data['stations']:
        try:
            slat = float(station['lat'])
            slon = float(station['lng'])
            dist = haversine(lat, lon, slat, slon)
            station_distances.append(
                (station['id'], station.get('name', 'Unknown'), dist)
            )
        except (KeyError, ValueError, TypeError):
            continue

    # Sort by distance and return top N
    station_distances.sort(key=lambda x: x[2])
    return station_distances[:max_stations]


def find_nearest_tidal_station(
    lat: float,
    lon: float,
    logger: Logger
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Find the single nearest CO-OPS station with tidal predictions.

    Args:
        lat: Latitude of target location
        lon: Longitude of target location
        logger: Logger instance

    Returns:
        Tuple of (station_id, station_name, distance_km) or
        (None, None, None) if not found.
    """
    stations = find_nearest_tidal_stations(lat, lon, logger, max_stations=1)
    if stations:
        station_id, station_name, distance = stations[0]
        logger.info(
            'Found nearest tidal station: %s (%s) at %.1f km',
            station_id, station_name, distance
        )
        return station_id, station_name, distance
    return None, None, None
