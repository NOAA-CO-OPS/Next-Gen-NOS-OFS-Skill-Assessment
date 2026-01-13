"""
Observation Time Series Formatting

Format observation data from pandas DataFrames into standardized
text format for skill assessment.
"""

import math
from datetime import datetime

import numpy as np
import pandas as pd


def format_scalar(
    timeseries: pd.DataFrame,
    start_date_full: str,
    end_date_full: str
) -> list[str]:
    """
    Format scalar observation data (water level, temperature, salinity).

    Converts pandas DataFrame to fixed-width formatted strings suitable
    for model-observation pairing.

    Parameters
    ----------
    timeseries : pd.DataFrame
        DataFrame with 'DateTime' and 'OBS' columns
    start_date_full : str
        Start date in format 'YYYYMMDD-HH:MM:SS'
    end_date_full : str
        End date in format 'YYYYMMDD-HH:MM:SS'

    Returns
    -------
    List[str]
        List of formatted strings, one per observation

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'DateTime': pd.date_range('2025-01-01', periods=3, freq='H'),
    ...     'OBS': [1.23, 1.45, 1.67]
    ... })
    >>> formatted = format_scalar(df, '20250101-00:00:00', '20250101-02:00:00')
    >>> print(formatted[0])
    2460676.50000000 2025  1  1  0  0    1.2300

    Notes
    -----
    Output format (fixed-width columns):
        julian_date year month day hour minute value

    - julian_date: Julian date (float, 13.8 format)
    - year: 4-digit year
    - month, day, hour, minute: 2-digit integers
    - value: Observation value (9.4 format)

    Missing data (values < -999 or > 999) are converted to NaN.
    """
    # Parse date range
    start_dt_full = datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
    end_dt_full = datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')

    # Filter to date range
    mask = (
        (timeseries['DateTime'] >= start_dt_full) &
        (timeseries['DateTime'] <= end_dt_full)
    )
    timeseries = timeseries.loc[mask].copy()

    # Calculate Julian date
    julian = pd.array(timeseries['DateTime']).to_julian_date()
    julian = julian.round(4)

    # Extract date components
    year = pd.to_datetime(timeseries['DateTime']).dt.strftime('%Y').to_numpy()
    month = pd.to_datetime(timeseries['DateTime']).dt.strftime('%m').to_numpy()
    day = pd.to_datetime(timeseries['DateTime']).dt.strftime('%d').to_numpy()
    hour = pd.to_datetime(timeseries['DateTime']).dt.strftime('%H').to_numpy()
    minute = pd.to_datetime(timeseries['DateTime']).dt.strftime('%M').to_numpy()

    # Filter out missing data values (< -999 or > 999)
    timeseries.loc[timeseries['OBS'] < -999, 'OBS'] = np.nan
    timeseries.loc[timeseries['OBS'] > 999, 'OBS'] = np.nan

    obs = timeseries['OBS'].to_numpy()

    # Format as fixed-width strings
    formatted_series = []
    for i in range(len(obs)):
        formatted_series.append(
            f'{float(julian[i]):13.8f} {int(year[i]):4d} {int(month[i]):2d} {int(day[i]):2d} {int(hour[i]):2d} {int(minute[i]):2d} {float(obs[i]):9.4f}'
        )

    return formatted_series


def format_vector(
    timeseries: pd.DataFrame,
    start_date_full: str,
    end_date_full: str
) -> list[str]:
    """
    Format vector observation data (currents).

    Converts current speed and direction to u/v components and formats
    as fixed-width strings.

    Parameters
    ----------
    timeseries : pd.DataFrame
        DataFrame with 'DateTime', 'OBS' (speed), and 'DIR' (direction) columns
    start_date_full : str
        Start date in format 'YYYYMMDD-HH:MM:SS'
    end_date_full : str
        End date in format 'YYYYMMDD-HH:MM:SS'

    Returns
    -------
    List[str]
        List of formatted strings, one per observation

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'DateTime': pd.date_range('2025-01-01', periods=2, freq='H'),
    ...     'OBS': [0.5, 0.6],  # m/s
    ...     'DIR': [90, 180]     # degrees clockwise from North
    ... })
    >>> formatted = format_vector(df, '20250101-00:00:00', '20250101-01:00:00')

    Notes
    -----
    Output format:
        julian_date year month day hour minute speed direction u v

    Direction convention:
    - Clockwise from North (meteorological convention)
    - u = speed * sin(direction)
    - v = speed * cos(direction)

    Missing data (values < -999 or > 999) are converted to NaN.
    """
    # Parse date range
    start_dt_full = datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
    end_dt_full = datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')

    # Filter to date range
    mask = (
        (timeseries['DateTime'] >= start_dt_full) &
        (timeseries['DateTime'] <= end_dt_full)
    )
    timeseries = timeseries.loc[mask].copy()

    # Calculate Julian date
    julian = pd.array(timeseries['DateTime']).to_julian_date()
    julian = julian.round(4)

    # Extract date components
    year = pd.to_datetime(timeseries['DateTime']).dt.strftime('%Y').to_numpy()
    month = pd.to_datetime(timeseries['DateTime']).dt.strftime('%m').to_numpy()
    day = pd.to_datetime(timeseries['DateTime']).dt.strftime('%d').to_numpy()
    hour = pd.to_datetime(timeseries['DateTime']).dt.strftime('%H').to_numpy()
    minute = pd.to_datetime(timeseries['DateTime']).dt.strftime('%M').to_numpy()

    # Filter out missing data values
    timeseries.loc[timeseries['OBS'] < -999, 'OBS'] = np.nan
    timeseries.loc[timeseries['OBS'] > 999, 'OBS'] = np.nan
    timeseries.loc[timeseries['DIR'] < -999, 'DIR'] = np.nan
    timeseries.loc[timeseries['DIR'] > 999, 'DIR'] = np.nan

    obs = timeseries['OBS'].to_numpy()  # Speed
    ang = timeseries['DIR'].to_numpy()  # Direction

    # Convert to u,v components
    # Direction is clockwise from North, so:
    # u = speed * sin(direction)
    # v = speed * cos(direction)
    u, v = [], []
    for i in range(len(ang)):
        u.append(float(obs[i]) * math.sin(math.radians(float(ang[i]))))
        v.append(float(obs[i]) * math.cos(math.radians(float(ang[i]))))

    # Format as fixed-width strings
    formatted_series = []
    for i in range(len(obs)):
        formatted_series.append(
            f'{float(julian[i]):13.8f} {int(year[i]):4d} {int(month[i]):2d} {int(day[i]):2d} {int(hour[i]):2d} {int(minute[i]):2d} {float(obs[i]):9.4f} {float(ang[i]):9.4f} {float(u[i]):9.4f} {float(v[i]):9.4f}'
        )

    return formatted_series


# Legacy function names for backward compatibility
def scalar(timeseries: pd.DataFrame, start_date_full: str, end_date_full: str) -> list[str]:
    """Legacy function name - use format_scalar() instead."""
    return format_scalar(timeseries, start_date_full, end_date_full)


def vector(timeseries: pd.DataFrame, start_date_full: str, end_date_full: str) -> list[str]:
    """Legacy function name - use format_vector() instead."""
    return format_vector(timeseries, start_date_full, end_date_full)
