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
from typing import Any

import pandas as pd
from searvey.usgs import (
    USGS_CURRENT_CODES,
    USGS_SALINITY_CODES,
    USGS_TEMPERATURE_CODES,
    USGS_WATER_LEVEL_CODES,
    get_usgs_station_data,
)

from ofs_skill.obs_retrieval.utils import redact_secrets

# Track whether we've already warned about rate limiting this session
_warned_rate_limit = False


# Codes where the unit is feet, requiring conversion to meters
_FEET_TO_METERS_CODES = {
    '00065', '62620', '72279', '63160', '62615', '62614', '72214',
    '63158',
}

# Water-level code preference within each datum family. USGS is migrating
# stream gages to 63160 "Stream level, NAVD88" (new time series per station,
# target Dec 2026 — issue #133), so true-datum elevation codes must win over
# legacy gage height (00065) whenever a station serves both.
_NAVD88_WL_CODES = ['63160', '62620', '72279', '62615', '63161', '62622',
                    '62617']
_NGVD_WL_CODES = ['63158', '62614', '62616']
_IGLD_WL_CODES = ['72214', '72215']
# Gage height: referenced to the local gage datum, not a true vertical
# datum. Assumed ~NAVD88 (historical behavior).
_GAGE_HEIGHT_WL_CODES = ['00065']

# A higher-priority code is only selected when its best sensor series
# spans at least this fraction of the time span of the best-covered
# candidate. During the 63160 migration a just-activated series can cover
# a sliver of the requested window while the legacy series covers all of
# it; without this guard the sparse series would silently win on
# priority alone.
_MIN_RELATIVE_COVERAGE = 0.5

# Datum label by parameter-code family. Codes in neither set are labeled
# NAVD88: per the USGS parameter-code dictionary every remaining
# elevation code is NAVD88-referenced, and gage height (00065) is
# assumed ~NAVD88 as a last resort.
_IGLD_CODES = set(_IGLD_WL_CODES)
_NGVD_CODES = set(_NGVD_WL_CODES)

# Temperature codes in Fahrenheit
_FAHRENHEIT_CODES = {'00011'}

# Current codes in ft/s
_FT_PER_SEC_CODES = {'72255', '00055', '72168', '72254', '72322', '81904'}
# Current codes in knots
_KNOT_CODES = {'70232'}
# Current codes in mph
_MPH_CODES = {'72294', '72321'}

# Specific conductance code requiring conversion to PSU
_SPECIFIC_CONDUCTANCE_CODES = {'00095'}

# Preferred salinity codes (actual salinity, not conductance)
_PREFERRED_SALINITY_CODES = {'00480', '72401', '90860', '90862', '00096', '70305'}


def _water_level_code_priority(requested_datum: str) -> list[str]:
    """Return water-level parameter codes in preference order.

    Codes in the family matching the requested datum come first (a direct
    match needs no datum conversion downstream), then NAVD88 codes (the
    only family with a vdatum conversion path to the tidal datums), then
    gage height (assumed ~NAVD88, so it also converts). NGVD and IGLD
    series rank last for non-matching requests: the pipeline has no
    conversion path for them, so selecting one would leave the station
    with an unusable UNKNOWN datum offset.
    """
    datum_upper = (requested_datum or '').upper()
    if 'IGLD' in datum_upper or 'LWD' in datum_upper:
        preferred = _IGLD_WL_CODES
    elif 'NGVD' in datum_upper:
        preferred = _NGVD_WL_CODES
    else:
        preferred = []
    base = (_NAVD88_WL_CODES + _GAGE_HEIGHT_WL_CODES + _NGVD_WL_CODES
            + _IGLD_WL_CODES)
    return preferred + [code for code in base if code not in preferred]


def _series_key_column(data_filtered: pd.DataFrame) -> str | None:
    """Column distinguishing sensor series within a parameter code.

    The new Water Data API uses time_series_id; the legacy NWIS API
    used option.
    """
    if 'time_series_id' in data_filtered.columns:
        return 'time_series_id'
    if 'option' in data_filtered.columns:
        return 'option'
    return None


def _select_water_level_series(
    data_filtered: pd.DataFrame,
    requested_datum: str,
    station: str,
    logger: Logger
) -> tuple[str, pd.DataFrame]:
    """Select the parameter code and single sensor series to use.

    Coverage is measured as the time span of a series' valid samples, on
    the exact single-sensor series the caller would receive: a code can
    carry several sensors, and an aggregate count would overstate what a
    single series delivers. Span rather than sample count, so a
    lower-rate series covering the window is not beaten by a high-rate
    one (USGS sensors report at 6, 15, or 60 minute intervals).

    Walks the datum-aware priority order, skipping codes whose best
    series spans less than ``_MIN_RELATIVE_COVERAGE`` of the
    best-covered candidate. Falls back to the first code in API order if
    no priority code is present (future-proofing for new searvey
    water-level codes).

    Returns the selected code and its best-covered series, not yet
    deduplicated on timestamps.
    """
    key_col = _series_key_column(data_filtered)
    valid = data_filtered.dropna(subset=['value'])
    group_cols = ['code', key_col] if key_col else ['code']
    spans = valid.groupby(group_cols)['datetime'].agg(['min', 'max'])
    spans['span'] = spans['max'] - spans['min']

    if key_col and not spans.empty:
        code_span = spans['span'].groupby(level='code').max()
        best_series_key = spans['span'].groupby(level='code').idxmax()
    else:
        code_span = spans['span']
        best_series_key = None
    best_span = code_span.max() if not code_span.empty else pd.Timedelta(0)

    codes_present = set(data_filtered['code'])
    selected = None
    for code in _water_level_code_priority(requested_datum):
        if code not in codes_present:
            continue
        span = code_span.get(code, pd.Timedelta(0))
        if span >= _MIN_RELATIVE_COVERAGE * best_span:
            selected = code
            break
        logger.info(
            'USGS station %s: skipping water-level code %s (best series '
            'spans %s vs %s for the best-covered code) — likely a newly '
            'established series with partial coverage.',
            station, code, span, best_span
        )
    if selected is None:
        selected = data_filtered['code'].iloc[0]
        logger.info(
            'USGS station %s: no known water-level code present; falling '
            'back to code %s (first in API order).', station, selected
        )

    series = data_filtered[data_filtered['code'] == selected].copy()
    if key_col is not None:
        if best_series_key is not None and selected in best_series_key.index:
            series_id = best_series_key[selected][1]
        else:
            # No valid samples for this code: keep the first series
            series_id = series[key_col].iloc[0]
        series = series[series[key_col] == series_id]

    logger.info(
        'USGS station %s: selected water-level code %s (series spanning '
        '%s).', station, selected,
        code_span.get(selected, pd.Timedelta(0))
    )
    return selected, series


def retrieve_usgs_station(
    retrieve_input: Any,
    logger: Logger
) -> pd.DataFrame | None:
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
            - datum: Requested vertical datum (optional; drives
                     water-level parameter-code preference)
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
            variable, station, redact_secrets(str(ex)),
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

    # For salinity, prefer actual salinity codes over specific conductance
    if variable == 'salinity':
        preferred = data_filtered[data_filtered['code'].isin(_PREFERRED_SALINITY_CODES)]
        if not preferred.empty:
            data_filtered = preferred

    if variable == 'water_level':
        # Pick the best available code rather than the first one the API
        # happens to return: stations transitioning to 63160 (issue #133)
        # serve both the new NAVD88 series and legacy gage height.
        selected_code, data_for_code = _select_water_level_series(
            data_filtered,
            getattr(retrieve_input, 'datum', ''),
            station,
            logger,
        )
        if selected_code in _GAGE_HEIGHT_WL_CODES:
            logger.warning(
                'USGS station %s water level uses gage height (00065), '
                'which is referenced to the local gage datum; assuming '
                'NAVD88.', station
            )
    else:
        # Use the first available code that has data
        selected_code = data_filtered['code'].iloc[0]
        data_for_code = (
            data_filtered[data_filtered['code'] == selected_code].copy()
        )

        # Filter to a single time series to avoid duplicate timestamps.
        # The new Water Data API uses time_series_id to distinguish
        # sensors; the legacy NWIS API used option. Try both.
        if 'time_series_id' in data_for_code.columns:
            first_ts = data_for_code['time_series_id'].iloc[0]
            data_for_code = data_for_code[
                data_for_code['time_series_id'] == first_ts
            ]
        elif 'option' in data_for_code.columns:
            first_option = data_for_code['option'].iloc[0]
            data_for_code = data_for_code[
                data_for_code['option'] == first_option
            ]

    # Drop any remaining duplicate timestamps, keeping first value
    data_for_code = data_for_code.drop_duplicates(subset='datetime', keep='first')

    # Build output DataFrame
    obs = pd.DataFrame({
        'DateTime': pd.to_datetime(data_for_code['datetime'].values),
        'DEP01': 0.0,
        'OBS': pd.to_numeric(data_for_code['value'].values),
    })

    # Apply unit conversions and set metadata based on variable
    if variable == 'water_level':
        if selected_code in _FEET_TO_METERS_CODES:
            obs['OBS'] = obs['OBS'] * 0.3048

        if selected_code in _IGLD_CODES:
            obs['Datum'] = 'IGLD'
        elif selected_code in _NGVD_CODES:
            obs['Datum'] = 'NGVD'
        else:
            obs['Datum'] = 'NAVD88'

    elif variable == 'water_temperature':
        if selected_code in _FAHRENHEIT_CODES:
            obs['OBS'] = (obs['OBS'] - 32) * (5 / 9)

    elif variable == 'salinity':
        if selected_code in _SPECIFIC_CONDUCTANCE_CODES:
            logger.info(
                'Station %s reports specific conductance at 25 Celsius (code %s) — '
                'converting to salinity PSU', station, selected_code
            )
            obs['OBS'] = obs['OBS'] * 0.00064

    elif variable == 'currents':
        if selected_code in _FT_PER_SEC_CODES:
            obs['OBS'] = obs['OBS'] * 0.3048
        elif selected_code in _KNOT_CODES:
            obs['OBS'] = obs['OBS'] * 0.5144
        elif selected_code in _MPH_CODES:
            obs['OBS'] = obs['OBS'] * 0.44704
        obs['DIR'] = 0.0

    return obs
