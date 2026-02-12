"""
Retrieve USGS stream gauge station observations.

This module uses SEARVEY to retrieve USGS instantaneous values data via
the modern Water Data API. It replaces direct NWIS IV API calls.

USGS data retrieval is complicated because there are multiple parameter codes
for the same variable. This module tries all relevant codes to ensure data
is retrieved when available.

Supported variables:
    - water_level: Multiple datum codes (NAVD88, NGVD, IGLD)
    - water_temperature: Multiple sensor codes
    - salinity: Multiple unit codes (all equivalent)
    - currents: Multiple velocity codes with unit conversions
"""

import os
from datetime import datetime
from logging import Logger
from typing import Optional

import pandas as pd
from searvey.usgs import (
    USGS_CURRENT_CODES,
    USGS_SALINITY_CODES,
    USGS_TEMPERATURE_CODES,
    USGS_WATER_LEVEL_CODES,
    get_usgs_station_data,
)

# Track whether we've already warned about rate limiting this session
_warned_rate_limit = False


# Codes where the unit is feet, requiring conversion to meters
_FEET_TO_METERS_CODES = {
    '00065', '62620', '72279', '63160', '62615', '62614', '72214',
    '63158',
}

# Datum assignment by code
_IGLD_CODES = {'72214', '72215'}
_NGVD_CODES = {'62616', '62614', '63161', '62617', '63158'}

# Temperature codes in Fahrenheit
_FAHRENHEIT_CODES = {'00011'}

# Current codes in ft/s
_FT_PER_SEC_CODES = {'72255', '00055', '72168', '72254', '72322', '81904'}
# Current codes in knots
_KNOT_CODES = {'70232'}
# Current codes in mph
_MPH_CODES = {'72294', '72321'}


def retrieve_usgs_station(
    retrieve_input: object,
    logger: Logger
) -> Optional[pd.DataFrame]:
    """
    Retrieve USGS stream gauge station observations via searvey.

    This function fetches all available data for a station and filters
    to the relevant parameter codes for the requested variable.

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
    start = datetime.strptime(retrieve_input.start_date, '%Y%m%d')
    end = datetime.strptime(retrieve_input.end_date, '%Y%m%d')
    period = max(1, (end - start).days)
    station = str(retrieve_input.station)
    variable = retrieve_input.variable

    # Map variable to relevant parameter codes
    if variable == 'water_level':
        relevant_codes = USGS_WATER_LEVEL_CODES
    elif variable == 'water_temperature':
        relevant_codes = USGS_TEMPERATURE_CODES
    elif variable == 'salinity':
        relevant_codes = USGS_SALINITY_CODES
    elif variable == 'currents':
        relevant_codes = USGS_CURRENT_CODES
    else:
        return None

    # Fetch data for the station
    logger.info('Calling USGS API via searvey for %s...', variable)
    try:
        data = get_usgs_station_data(
            usgs_code=station,
            endtime=end,
            period=period,
        )
    except Exception as ex:
        global _warned_rate_limit
        error_str = str(ex)
        if '403' in error_str or 'rate' in error_str.lower():
            if not _warned_rate_limit:
                _warned_rate_limit = True
                has_key = bool(
                    os.environ.get('API_USGS_PAT', '').strip()
                )
                if not has_key:
                    logger.warning(
                        'USGS API rate limit reached (50 requests/hour '
                        'without API key). Remaining USGS stations will '
                        'be skipped. Set the API_USGS_PAT environment '
                        'variable to increase the limit to 1000/hour.'
                    )
                else:
                    logger.warning(
                        'USGS API rate limit reached. Remaining USGS '
                        'stations may fail. Consider reducing the number '
                        'of stations or spacing out requests.'
                    )
            return None
        logger.error(
            'Retrieve USGS data failed for %s station %s: %s',
            variable, station, ex
        )
        return None

    if data.empty:
        return None

    # Reset MultiIndex to flat columns
    data = data.reset_index()

    # Filter to relevant parameter codes for this variable
    data_filtered = data[data['code'].isin(relevant_codes)]

    if data_filtered.empty:
        return None

    # Use the first available code that has data
    first_code = data_filtered['code'].iloc[0]
    data_for_code = data_filtered[data_filtered['code'] == first_code].copy()

    # Build output DataFrame
    obs = pd.DataFrame({
        'DateTime': pd.to_datetime(data_for_code['datetime'].values),
        'DEP01': 0.0,
        'OBS': pd.to_numeric(data_for_code['value'].values),
    })

    # Apply unit conversions and set metadata based on variable
    if variable == 'water_level':
        if first_code in _FEET_TO_METERS_CODES:
            obs['OBS'] = obs['OBS'] * 0.3048

        if first_code in _IGLD_CODES:
            obs['Datum'] = 'IGLD'
        elif first_code in _NGVD_CODES:
            obs['Datum'] = 'NGVD'
        else:
            obs['Datum'] = 'NAVD88'

    elif variable == 'water_temperature':
        if first_code in _FAHRENHEIT_CODES:
            obs['OBS'] = (obs['OBS'] - 32) * (5 / 9)

    elif variable == 'currents':
        if first_code in _FT_PER_SEC_CODES:
            obs['OBS'] = obs['OBS'] * 0.3048
        elif first_code in _KNOT_CODES:
            obs['OBS'] = obs['OBS'] * 0.5144
        elif first_code in _MPH_CODES:
            obs['OBS'] = obs['OBS'] * 0.44704
        obs['DIR'] = 0.0

    return obs
