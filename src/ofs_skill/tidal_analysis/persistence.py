"""
Persistence forecasting: tide + persisted non-tidal offset.

Replaces the legacy ``persistence.f`` Fortran subroutine.  Generates a
persistence forecast by adding the mean non-tidal residual from a recent
window to the tidal prediction over a forward horizon.
"""
from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)


def build_persistence_forecast(
    time: np.ndarray,
    observed: np.ndarray,
    predicted_tide: np.ndarray,
    forecast_horizon_hours: float = 24.0,
    offset_window_hours: float = 6.0,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    Build a persistence forecast (tidal prediction + persisted residual).

    At each initialization time *i*, the mean non-tidal residual over the
    past *offset_window_hours* is computed and added to the tidal
    prediction over the following *forecast_horizon_hours*.

    Parameters
    ----------
    time : np.ndarray
        Timestamps (datetime64 or DatetimeIndex), equally spaced.
    observed : np.ndarray
        Observed water levels.
    predicted_tide : np.ndarray
        Tidal predictions (same length as *observed*).
    forecast_horizon_hours : float, optional
        Forward forecast horizon in hours (default 24.0).
    offset_window_hours : float, optional
        Lookback window for computing the mean residual (default 6.0).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    np.ndarray
        Persistence forecast, same length as input.  Elements outside
        valid forecast ranges are NaN.

    Raises
    ------
    ValueError
        If input arrays have mismatched lengths or fewer than 2 points.
    """
    _log = logger or logging.getLogger(__name__)

    time = np.asarray(time, dtype='datetime64[ns]')
    observed = np.asarray(observed, dtype=float)
    predicted_tide = np.asarray(predicted_tide, dtype=float)

    if not (len(time) == len(observed) == len(predicted_tide)):
        raise ValueError(
            f"time ({len(time)}), observed ({len(observed)}), and "
            f"predicted_tide ({len(predicted_tide)}) must have the same "
            f"length."
        )
    if len(time) < 2:
        raise ValueError('At least 2 data points are required.')

    # Estimate sampling interval in hours
    dt_ns = np.median(np.diff(time).astype(float))
    dt_hours = dt_ns / 3.6e12

    window_samples = max(1, int(offset_window_hours / dt_hours))
    horizon_samples = max(1, int(forecast_horizon_hours / dt_hours))

    n = len(time)
    residual = observed - predicted_tide
    forecast = np.full(n, np.nan)

    _log.info(
        'Persistence forecast: window=%d samples (%.1f h), '
        'horizon=%d samples (%.1f h).',
        window_samples, offset_window_hours,
        horizon_samples, forecast_horizon_hours,
    )

    for i in range(window_samples, n):
        # Mean residual over the lookback window
        window_start = max(0, i - window_samples)
        window_residual = residual[window_start:i]
        finite_mask = np.isfinite(window_residual)
        if not np.any(finite_mask):
            continue
        mean_offset = np.mean(window_residual[finite_mask])

        # Apply to the forecast horizon
        horizon_end = min(i + horizon_samples, n)
        forecast[i:horizon_end] = predicted_tide[i:horizon_end] + mean_offset

    valid_count = int(np.sum(np.isfinite(forecast)))
    _log.info(
        'Persistence forecast complete: %d of %d points filled.',
        valid_count, n,
    )
    return forecast
