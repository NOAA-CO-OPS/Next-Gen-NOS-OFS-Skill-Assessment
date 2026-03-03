"""
Extrema extraction: high/low water, max flood/ebb, and slack water.

Replaces the legacy ``extremes`` and ``slack`` Fortran subroutines.
Identifies local maxima/minima in water level and current time series,
and detects slack-water events.
"""
from __future__ import annotations

import logging

import numpy as np
from scipy.signal import argrelextrema

logger = logging.getLogger(__name__)


def extract_water_level_extrema(
    time: np.ndarray,
    water_level: np.ndarray,
    min_separation_hours: float = 4.0,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Extract high-water and low-water extrema from a water level series.

    Uses :func:`scipy.signal.argrelextrema` with a minimum separation
    constraint to avoid detecting spurious local peaks.

    Parameters
    ----------
    time : np.ndarray
        Timestamps (datetime64 or DatetimeIndex).
    water_level : np.ndarray
        Water level values.
    min_separation_hours : float, optional
        Minimum time between consecutive extrema of the same type
        (default 4.0 hours).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    dict
        ``"high_water_times"`` : np.ndarray of timestamps.
        ``"high_water_amplitudes"`` : np.ndarray of water levels at HW.
        ``"low_water_times"`` : np.ndarray of timestamps.
        ``"low_water_amplitudes"`` : np.ndarray of water levels at LW.

    Raises
    ------
    ValueError
        If *time* and *water_level* have different lengths or fewer than
        3 points.
    """
    _log = logger or logging.getLogger(__name__)

    time = np.asarray(time)
    water_level = np.asarray(water_level, dtype=float)

    if len(time) != len(water_level):
        raise ValueError(
            f"time ({len(time)}) and water_level ({len(water_level)}) must "
            f"have the same length."
        )
    if len(time) < 3:
        raise ValueError('At least 3 data points are required.')

    # Estimate sampling interval in hours
    dt_hours = _median_dt_hours(time)
    order = max(1, int(min_separation_hours / dt_hours))

    hw_idx = argrelextrema(water_level, np.greater, order=order)[0]
    lw_idx = argrelextrema(water_level, np.less, order=order)[0]

    _log.info(
        'Extrema extraction: %d HW, %d LW (order=%d samples, dt=%.3f h).',
        len(hw_idx), len(lw_idx), order, dt_hours,
    )

    return {
        'high_water_times': time[hw_idx],
        'high_water_amplitudes': water_level[hw_idx],
        'low_water_times': time[lw_idx],
        'low_water_amplitudes': water_level[lw_idx],
    }


def extract_current_extrema(
    time: np.ndarray,
    speed: np.ndarray,
    direction: np.ndarray,
    principal_direction: float,
    min_separation_hours: float = 4.0,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Extract max-flood and max-ebb current extrema.

    Projects current vectors onto the along-channel axis defined by
    *principal_direction*, then finds positive maxima (flood) and
    negative minima (ebb).

    Parameters
    ----------
    time : np.ndarray
        Timestamps.
    speed : np.ndarray
        Current speed (m/s).
    direction : np.ndarray
        Current direction in degrees True.
    principal_direction : float
        Principal flood direction in degrees True.
    min_separation_hours : float, optional
        Minimum separation between extrema (default 4.0 hours).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    dict
        ``"flood_times"`` : np.ndarray of timestamps at max flood.
        ``"flood_speeds"`` : np.ndarray of speeds at max flood.
        ``"flood_directions"`` : np.ndarray of directions at max flood.
        ``"ebb_times"`` : np.ndarray of timestamps at max ebb.
        ``"ebb_speeds"`` : np.ndarray of speeds at max ebb.
        ``"ebb_directions"`` : np.ndarray of directions at max ebb.

    Raises
    ------
    ValueError
        If input arrays have mismatched lengths.
    """
    _log = logger or logging.getLogger(__name__)

    time = np.asarray(time)
    speed = np.asarray(speed, dtype=float)
    direction = np.asarray(direction, dtype=float)

    if not (len(time) == len(speed) == len(direction)):
        raise ValueError(
            f"time ({len(time)}), speed ({len(speed)}), and direction "
            f"({len(direction)}) must have the same length."
        )
    if len(time) < 3:
        raise ValueError('At least 3 data points are required.')

    # Along-channel component: positive = flood, negative = ebb
    angle_diff = np.radians(direction - principal_direction)
    along_channel = speed * np.cos(angle_diff)

    dt_hours = _median_dt_hours(time)
    order = max(1, int(min_separation_hours / dt_hours))

    # Flood: maxima of along_channel (positive peaks)
    flood_idx = argrelextrema(along_channel, np.greater, order=order)[0]
    # Keep only positive along-channel values
    flood_idx = flood_idx[along_channel[flood_idx] > 0]

    # Ebb: minima of along_channel (negative troughs)
    ebb_idx = argrelextrema(along_channel, np.less, order=order)[0]
    # Keep only negative along-channel values
    ebb_idx = ebb_idx[along_channel[ebb_idx] < 0]

    _log.info(
        'Current extrema: %d flood, %d ebb events.',
        len(flood_idx), len(ebb_idx),
    )

    return {
        'flood_times': time[flood_idx],
        'flood_speeds': speed[flood_idx],
        'flood_directions': direction[flood_idx],
        'ebb_times': time[ebb_idx],
        'ebb_speeds': speed[ebb_idx],
        'ebb_directions': direction[ebb_idx],
    }


def find_slack_water(
    time: np.ndarray,
    speed: np.ndarray,
    threshold_knots: float = 0.5,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Identify slack-water events where current speed is below a threshold.

    Parameters
    ----------
    time : np.ndarray
        Timestamps (datetime64 or DatetimeIndex).
    speed : np.ndarray
        Current speed (m/s).
    threshold_knots : float, optional
        Speed threshold in knots (default 0.5).  Converted to m/s
        internally (1 knot = 0.514444 m/s).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    dict
        ``"slack_events"`` : list of dict, each with ``"start_time"``,
        ``"end_time"``, ``"duration_hours"``.

    Raises
    ------
    ValueError
        If *time* and *speed* have different lengths.
    """
    _log = logger or logging.getLogger(__name__)

    time = np.asarray(time)
    speed = np.asarray(speed, dtype=float)

    if len(time) != len(speed):
        raise ValueError(
            f"time ({len(time)}) and speed ({len(speed)}) must have the "
            f"same length."
        )

    # Convert threshold to m/s
    threshold_ms = threshold_knots * 0.514444

    below = speed < threshold_ms
    events: list[dict] = []

    # Find contiguous runs of below-threshold speed
    if not np.any(below):
        _log.info('No slack-water events found (threshold=%.2f kn).',
                   threshold_knots)
        return {'slack_events': events}

    # Detect edges of below-threshold regions
    diff = np.diff(below.astype(int))
    starts = np.where(diff == 1)[0] + 1
    ends = np.where(diff == -1)[0] + 1

    # Handle edge cases: series starts or ends below threshold
    if below[0]:
        starts = np.concatenate(([0], starts))
    if below[-1]:
        ends = np.concatenate((ends, [len(below)]))

    for s, e in zip(starts, ends):
        t_start = time[s]
        t_end = time[min(e, len(time) - 1)]
        # Duration in hours
        dt = (np.datetime64(t_end) - np.datetime64(t_start)) / np.timedelta64(1, 'h')
        events.append({
            'start_time': t_start,
            'end_time': t_end,
            'duration_hours': float(dt),
        })

    _log.info(
        'Found %d slack-water events (threshold=%.2f kn = %.4f m/s).',
        len(events), threshold_knots, threshold_ms,
    )
    return {'slack_events': events}


def _median_dt_hours(time: np.ndarray) -> float:
    """Estimate the median sampling interval in hours."""
    time = np.asarray(time, dtype='datetime64[ns]')
    diffs = np.diff(time)
    median_ns = np.median(diffs.astype(float))
    return float(median_ns) / 3.6e12  # ns to hours
