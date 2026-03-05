"""
Data preprocessing: gap filling and time interval conversion.

Replaces the ``equal_interval`` Fortran subroutine from the legacy NOS OFS
Skill Assessment code.  Converts irregularly-spaced or gapped time series to
equally-spaced grids suitable for harmonic analysis.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def to_equal_interval(
    time: pd.DatetimeIndex,
    values: np.ndarray,
    target_interval: str = '6min',
    max_gap_hours: float = 6.0,
    method: str = 'cubic',
    logger: logging.Logger | None = None,
) -> tuple[pd.DatetimeIndex, np.ndarray]:
    """
    Convert a time series to an equally-spaced grid with gap-aware filling.

    Small gaps (shorter than *max_gap_hours*) are filled by interpolation;
    large gaps are left as NaN.  This is the Python equivalent of the
    ``equal_interval`` Fortran subroutine.

    Parameters
    ----------
    time : pd.DatetimeIndex
        Timestamps of the input observations (need not be equally spaced).
    values : np.ndarray
        Observed values corresponding to *time*.
    target_interval : str, optional
        Target output interval as a pandas frequency string (default ``"6min"``).
    max_gap_hours : float, optional
        Maximum gap size (in hours) that will be interpolated.  Gaps larger
        than this are left as NaN (default 6.0).
    method : str, optional
        Interpolation method passed to :meth:`pandas.Series.interpolate`
        (default ``"cubic"``).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    new_index : pd.DatetimeIndex
        Equally-spaced time index.
    filled_values : np.ndarray
        Values on the new grid, with small gaps filled and large gaps as NaN.

    Raises
    ------
    ValueError
        If *time* and *values* have different lengths, or if *time* has
        fewer than two points.
    """
    if len(time) != len(values):
        raise ValueError(
            f"time ({len(time)}) and values ({len(values)}) must have the "
            f"same length."
        )
    if len(time) < 2:
        raise ValueError('At least two data points are required.')

    _log = logger or logging.getLogger(__name__)

    # Build a regular grid spanning the input range
    new_index = pd.date_range(
        start=time[0], end=time[-1], freq=target_interval
    )
    _log.info(
        'Reindexing %d points to %d-point regular grid (%s interval).',
        len(time), len(new_index), target_interval,
    )

    # Place original data onto the new grid (unmatched slots become NaN)
    series = pd.Series(values, index=time)
    reindexed = series.reindex(new_index)

    # Compute the maximum number of consecutive NaN samples to fill
    interval_seconds = pd.Timedelta(target_interval).total_seconds()
    max_gap_samples = int(max_gap_hours * 3600.0 / interval_seconds)

    # Interpolate only within small gaps
    filled = reindexed.interpolate(method=method, limit=max_gap_samples)

    n_filled = int(reindexed.isna().sum() - filled.isna().sum())
    n_remaining_gaps = int(filled.isna().sum())
    _log.info(
        'Filled %d gap samples; %d NaN samples remain (large gaps).',
        n_filled, n_remaining_gaps,
    )

    return filled.index, filled.values
