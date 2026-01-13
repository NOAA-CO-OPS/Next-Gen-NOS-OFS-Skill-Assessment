"""
Retrieve USGS stream gauge station observations.

USGS data retrieval is complicated because there are multiple parameter codes
for the same variable. This module tries all relevant codes to ensure data
is retrieved when available.

Supported variables:
    - water_level: Multiple datum codes (NAVD88, NGVD, IGLD)
    - water_temperature: Multiple sensor codes
    - salinity: Multiple unit codes (all equivalent)
    - currents: Multiple velocity codes with unit conversions
"""

import json
import urllib.request
from datetime import datetime
from logging import Logger
from typing import Optional
from urllib.error import HTTPError

import pandas as pd

from ofs_skill.obs_retrieval import usgs_properties, utils


def retrieve_usgs_station(
    retrieve_input: object,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve USGS stream gauge station observations.

    This function tries multiple USGS parameter codes for each variable
    type to ensure data retrieval. Different codes represent different
    sensors, datums, or units, which are handled appropriately.

    Args:
        retrieve_input: Object with attributes:
            - station: USGS station ID
            - start_date: Start date in YYYYMMDD format
            - end_date: End date in YYYYMMDD format
            - variable: Variable type ('water_level', 'water_temperature',
                       'salinity', 'currents')
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - DateTime: Observation timestamps
            - DEP01: Observation depth (usually 0.0 for USGS)
            - OBS: Observation values in standard units
            - DIR: Direction (for currents, usually 0.0)
            - Datum: Vertical datum (for water_level only)
        Returns None if no data available for any parameter code.

    Note:
        - Water level converted to meters and includes datum info
        - Temperature converted to Celsius if in Fahrenheit
        - Current velocities converted to m/s from various units
    """
    usgs = usgs_properties.USGSProperties()

    # Retrieve url from config file
    usgs.base_url = utils.Utils().read_config_section(
        'urls', logger)['usgs_nwis_url']

    usgs.start = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    usgs.end = datetime.strptime(retrieve_input.end_date, '%Y%m%d')

    usgs.start_year = usgs.start.strftime('%Y')
    usgs.start_month = usgs.start.strftime('%m')
    usgs.start_day = usgs.start.strftime('%d')

    usgs.end_year = usgs.end.strftime('%Y')
    usgs.end_month = usgs.end.strftime('%m')
    usgs.end_day = usgs.end.strftime('%d')

    usgs.start_str = f'{usgs.start_year}-{usgs.start_month}-{usgs.start_day}'
    usgs.end_str = f'{usgs.end_year}-{usgs.end_month}-{usgs.end_day}'

    usgs.obs_final = None

    if retrieve_input.variable == 'water_level':
        # Various USGS water level parameter codes with different datums
        list_of_codes = [
            '62622',  # Reservoir water surface elevation above datum
            '62620',  # Reservoir water surface elevation above NAVD 1988
            '72279',  # Water level, above NAVD88
            '63161',  # Reservoir water surface elevation above NGVD 1929
            '63160',  # Stream water level elevation above NAVD 1988
            '62617',  # Reservoir water surface elevation above NGVD
            '62615',  # Reservoir water surface elevation
            '62616',  # Reservoir water surface elevation above local datum
            '62614',  # Lake or reservoir water surface elevation above NGVD
            '72214',  # Reservoir water surface elevation above IGLD 1985
            '72215',  # Reservoir water surface elevation above IGLD 1985
            '00065',  # Gage height
        ]

        for code in list_of_codes:
            usgs.url = (
                f'{usgs.base_url}?sites='
                f'{retrieve_input.station}&parameterCd='
                f'{code}&st'
                f'artDT='
                f'{usgs.start_str}'
                f'T00:00:00.000-04:00&endDT='
                f'{usgs.end_str}'
                f'T23:59:59.999-04:00&siteStatus=all&format=json'
            )

            try:
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    'Retrieve USGS data failed for %s at '
                    'at %s. Exception: %s',
                    retrieve_input.variable,
                    usgs.url, ex
                )
                continue

            if (
                len(obs['value']['timeSeries']) > 0
                and len(obs['value']['timeSeries'][0]['values']) > 0
            ):
                data = obs['value']['timeSeries'][0]['values'][0]['value']
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = pd.to_datetime(
                        data[i]['dateTime']).tz_convert(None)
                    date.append(fixed_date)
                    var.append(float(data[i]['value']))

                obs = pd.DataFrame(
                    {
                        'DateTime': pd.to_datetime(date),
                        'DEP01': pd.to_numeric(0.0),
                        'OBS': pd.to_numeric(var),
                    }
                )

                # Convert feet to meters for most codes
                if code in {
                    '00065',
                    '62620',
                    '72279',
                    '63160',
                    '062615',
                    '62614',
                    '72214',
                }:
                    obs['OBS'] = obs['OBS'] * 0.3048
                    obs['Datum'] = 'NAVD88'

                if code in {'62616', '62614'}:
                    obs['Datum'] = 'NGVD'

                if code in {'72214', '72215'}:
                    obs['Datum'] = 'IGLD'

                usgs.obs_final = obs
                break

    elif retrieve_input.variable == 'water_temperature':
        # Various temperature parameter codes
        list_of_codes = [
            '00010',  # Temperature, water, degrees Celsius
            '00011',  # Temperature, water, degrees Fahrenheit
            '99976',  # Temperature, water, degrees Celsius
            '99980',  # Temperature, water, degrees Celsius
            '99984',  # Temperature, water, degrees Celsius
        ]
        logger.info('Calling USGS API for %s...', retrieve_input.variable)

        for code in list_of_codes:
            usgs.url = (
                f'{usgs.base_url}?'
                f'sites={retrieve_input.station}&parameterCd='
                f'{code}&st'
                f'artDT='
                f'{usgs.start_str}'
                f'T00:00:00.000-00:00&endDT='
                f'{usgs.end_str}'
                f'T23:59:59.999-00:00&siteStatus=all&format=json'
            )

            try:
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    'Retrieve USGS data failed for %s at '
                    'at %s. Exception: %s',
                    retrieve_input.variable,
                    usgs.url, ex
                )
                continue

            if (
                len(obs['value']['timeSeries']) > 0
                and len(obs['value']['timeSeries'][0]['values']) > 0
            ):
                data = obs['value']['timeSeries'][0]['values'][0]['value']
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]['dateTime'].split('-')[:-1]
                    fixed_date = (
                        fixed_date[0] + fixed_date[1] + fixed_date[2]
                    )
                    date.append(fixed_date)
                    var.append(float(data[i]['value']))

                obs = pd.DataFrame(
                    {
                        'DateTime': pd.to_datetime(date),
                        'DEP01': pd.to_numeric(0.0),
                        'OBS': pd.to_numeric(var),
                    }
                )

                # Convert Fahrenheit to Celsius
                if code == '00011':
                    obs['OBS'] = (obs['OBS'] - 32) * (5 / 9)

                usgs.obs_final = obs
                break

    elif retrieve_input.variable == 'salinity':
        # Various salinity parameter codes (all essentially equivalent)
        list_of_codes = [
            '00480',  # Salinity, water, ppth
            '00096',  # Salinity, water, mg/ml
            '70305',  # Salinity, water, g/l
            '72401',  # Salinity, water, PSU
            '90860',  # Salinity, water, PSU
            '90862',  # Salinity, water, PSS
        ]
        logger.info('Calling USGS API for %s...', retrieve_input.variable)

        for code in list_of_codes:
            usgs.url = (
                f'{usgs.base_url}?'
                f'sites={retrieve_input.station}&parameterCd='
                f'{code}&st'
                f'artDT='
                f'{usgs.start_str}'
                f'T00:00:00.000-00:00&endDT='
                f'{usgs.end_str}'
                f'T23:59:59.999-00:00&siteStatus=all&format=json'
            )

            try:
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    'Retrieve USGS data failed for %s at '
                    'at %s. Exception: %s',
                    retrieve_input.variable,
                    usgs.url, ex
                )
                continue

            if (
                len(obs['value']['timeSeries']) > 0
                and len(obs['value']['timeSeries'][0]['values']) > 0
            ):
                data = obs['value']['timeSeries'][0]['values'][0]['value']
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]['dateTime'].split('-')[:-1]
                    fixed_date = (
                        fixed_date[0] + fixed_date[1] + fixed_date[2]
                    )
                    date.append(fixed_date)
                    var.append(float(data[i]['value']))

                obs = pd.DataFrame(
                    {
                        'DateTime': pd.to_datetime(date),
                        'DEP01': pd.to_numeric(0.0),
                        'OBS': pd.to_numeric(var),
                    }
                )
                usgs.obs_final = obs
                break

    elif retrieve_input.variable == 'currents':
        # Various current velocity parameter codes
        list_of_codes = [
            '72255',  # Stream velocity, ft/s
            '00055',  # Stream velocity, ft/s
        ]
        logger.info('Calling USGS API for %s...', retrieve_input.variable)

        for code in list_of_codes:
            url = (
                f'{usgs.base_url}?'
                f'sites={retrieve_input.station}&parameterCd='
                f'{code}&st'
                f'artDT='
                f'{usgs.start_str}'
                f'T00:00:00.000-04:00&endDT='
                f'{usgs.end_str}'
                f'T23:59:59.999-04:00&siteStatus=all&format=json'
            )

            try:
                with urllib.request.urlopen(url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    'Retrieve USGS data failed for %s at '
                    'at %s. Exception: %s',
                    retrieve_input.variable,
                    usgs.url, ex
                )
                continue

            if (
                len(obs['value']['timeSeries']) > 0
                and len(obs['value']['timeSeries'][0]['values']) > 0
            ):
                data = obs['value']['timeSeries'][0]['values'][0]['value']
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]['dateTime'].split('-')[:-1]
                    fixed_date = (
                        fixed_date[0] + fixed_date[1] + fixed_date[2]
                    )
                    date.append(fixed_date)
                    var.append(float(data[i]['value']))

                obs = pd.DataFrame(
                    {
                        'DateTime': pd.to_datetime(date),
                        'DEP01': pd.to_numeric(0.0),
                        'DIR': pd.to_numeric(0.0),
                        'OBS': pd.to_numeric(var),
                    }
                )

                # Convert various units to m/s
                if code in {
                    '72255',
                    '00055',
                    '72168',
                    '72254',
                    '72322',
                    '81904',
                }:  # ft/s to m/s
                    obs['OBS'] = obs['OBS'] * 0.3048

                elif code == '70232':  # knots to m/s
                    obs['OBS'] = obs['OBS'] * 0.5144

                elif code in {'72294', '72321'}:  # mph to m/s
                    obs['OBS'] = obs['OBS'] * 0.44704

                usgs.obs_final = obs
                break

    return usgs.obs_final
