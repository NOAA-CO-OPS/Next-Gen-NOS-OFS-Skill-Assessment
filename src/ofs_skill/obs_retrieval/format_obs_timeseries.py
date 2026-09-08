"""
Observation Time Series Formatting

Format observation data from pandas DataFrames into standardized
text format for skill assessment.
"""

import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def format_scalar(
    timeseries: pd.DataFrame,
    start_date_full: str,
    end_date_full: str,
    lookback_hours: int = 24,
) -> list[str]:
    """Format scalar observations into fixed-width ``.obs`` lines.

    Args:
        timeseries: DataFrame with ``DateTime`` and ``OBS`` columns.
        start_date_full: Window start (``YYYYMMDD-HH:MM:SS`` style used
            by the pipeline).
        end_date_full: Window end.
        lookback_hours: Extra hours before ``start_date_full`` to include
            (default 24; use ``0`` to disable).

    Returns:
        List of fixed-width strings ready to write to an ``.obs`` file.
        Each line is::

            julian_date year month day hour minute value
            {:13.8f}    {:4d} {:2d}  {:2d} {:2d}  {:2d}   {:9.4f}

    Note:
        Missing data is filtered before formatting: ``OBS`` values below
        ``-999`` or above ``999`` are converted to ``NaN``.
    """
    # Parse date range
    start_dt_full = datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
    end_dt_full = datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')

    # Filter to date range (lookback ensures overlap between consecutive casts)
    mask = (
        (timeseries['DateTime'] >= start_dt_full - timedelta(hours=lookback_hours)) &
        (timeseries['DateTime'] <= end_dt_full)
    )
    timeseries = timeseries.loc[mask].copy()

    # Calculate Julian date. Round at the precision the fixed-width
    # writer emits (8 decimals): the historical round(4) quantized to an
    # 8.64 s grid, which made strictly 6-minute series show elapsed-day
    # steps wobbling between 0.0041 and 0.0043 (issue #200).
    julian = pd.array(timeseries['DateTime']).to_julian_date()
    julian = julian.round(8)

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
    end_date_full: str,
    lookback_hours: int = 24,
) -> list[str]:
    """Format current observations into fixed-width ``.obs`` lines.

    Converts speed/direction to ``u``/``v`` (meteorological convention:
    clockwise from North) and emits one fixed-width string per timestep.

    Args:
        timeseries: DataFrame with ``DateTime``, ``OBS`` (speed), ``DIR``.
        start_date_full: Window start (``YYYYMMDD-HH:MM:SS``).
        end_date_full: Window end.
        lookback_hours: Extra hours before start (default 24; ``0`` disables).

    Returns:
        List of fixed-width strings ready to write to an ``.obs`` file.
        Each line is::

            julian_date year month day hour minute speed direction u v
            {:13.8f}    {:4d} {:2d}  {:2d} {:2d}  {:2d}   {:9.4f} {:9.4f} {:9.4f} {:9.4f}

        where ``u = speed * sin(radians(direction))`` and
        ``v = speed * cos(radians(direction))``.

    Note:
        Missing data is filtered before formatting: ``OBS`` (speed) and
        ``DIR`` values below ``-999`` or above ``999`` are converted to
        ``NaN``.
    """
    # Parse date range
    start_dt_full = datetime.strptime(start_date_full, '%Y%m%d-%H:%M:%S')
    end_dt_full = datetime.strptime(end_date_full, '%Y%m%d-%H:%M:%S')

    # Filter to date range (lookback ensures overlap between consecutive casts)
    mask = (
        (timeseries['DateTime'] >= start_dt_full - timedelta(hours=lookback_hours)) &
        (timeseries['DateTime'] <= end_dt_full)
    )
    timeseries = timeseries.loc[mask].copy()

    # Calculate Julian date. Round at the precision the fixed-width
    # writer emits (8 decimals): the historical round(4) quantized to an
    # 8.64 s grid, which made strictly 6-minute series show elapsed-day
    # steps wobbling between 0.0041 and 0.0043 (issue #200).
    julian = pd.array(timeseries['DateTime']).to_julian_date()
    julian = julian.round(8)

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
    """Alias for :func:`format_scalar` (legacy name).

    Args:
        timeseries: DataFrame with ``DateTime`` and ``OBS``.
        start_date_full: Window start.
        end_date_full: Window end.

    Returns:
        Fixed-width observation lines from :func:`format_scalar`.
    """
    return format_scalar(timeseries, start_date_full, end_date_full)


def vector(timeseries: pd.DataFrame, start_date_full: str, end_date_full: str) -> list[str]:
    """Alias for :func:`format_vector` (legacy name).

    Args:
        timeseries: DataFrame with ``DateTime``, ``OBS``, ``DIR``.
        start_date_full: Window start.
        end_date_full: Window end.

    Returns:
        Fixed-width observation lines from :func:`format_vector`.
    """
    return format_vector(timeseries, start_date_full, end_date_full)
