"""
Signal filtering for separating tidal and non-tidal components.

Replaces the legacy ``foufil`` and ``svd`` Fortran subroutines.  Provides
FFT-based and IIR (Butterworth) low-pass filters for extracting the
non-tidal residual from an observed time series.
"""
from __future__ import annotations

import logging

import numpy as np
from scipy.fft import fft, ifft
from scipy.signal import butter, filtfilt

logger = logging.getLogger(__name__)


def fourier_lowpass_filter(
    data: np.ndarray,
    dt_hours: float,
    cutoff_hours: float = 25.0,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    FFT-based low-pass filter that removes tidal-frequency energy.

    Zeroes all Fourier coefficients with frequencies above ``1/cutoff_hours``
    and returns the inverse FFT.  Requires equally-spaced, gap-free data.

    Parameters
    ----------
    data : np.ndarray
        Equally-spaced, gap-free time series.
    dt_hours : float
        Sample interval in hours (e.g. 0.1 for 6-min data).
    cutoff_hours : float, optional
        Low-pass cutoff period in hours (default 25.0).  Frequencies with
        period shorter than this are removed.
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    np.ndarray
        Filtered (low-passed) time series, same length as *data*.

    Raises
    ------
    ValueError
        If *data* contains NaN values or has fewer than 4 points.
    """
    _log = logger or logging.getLogger(__name__)

    if len(data) < 4:
        raise ValueError('data must have at least 4 points for FFT filtering.')
    if np.any(np.isnan(data)):
        raise ValueError('data must not contain NaN values for FFT filtering.')

    n = len(data)
    freqs = np.fft.fftfreq(n, d=dt_hours)  # cycles per hour
    cutoff_freq = 1.0 / cutoff_hours

    spectrum = fft(data)
    spectrum[np.abs(freqs) > cutoff_freq] = 0.0
    filtered = np.real(ifft(spectrum))

    _log.info(
        'Fourier low-pass: cutoff=%.1f h, removed %d of %d frequency bins.',
        cutoff_hours,
        int(np.sum(np.abs(freqs) > cutoff_freq)),
        n,
    )
    return filtered


def butterworth_lowpass(
    data: np.ndarray,
    dt_hours: float,
    cutoff_hours: float = 25.0,
    order: int = 4,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    Zero-phase Butterworth low-pass filter.

    Uses :func:`scipy.signal.butter` and :func:`scipy.signal.filtfilt` for
    forward-backward (zero-phase) filtering.  Better edge handling than the
    Fourier method.

    Parameters
    ----------
    data : np.ndarray
        Equally-spaced, gap-free time series.
    dt_hours : float
        Sample interval in hours.
    cutoff_hours : float, optional
        Low-pass cutoff period in hours (default 25.0).
    order : int, optional
        Filter order (default 4).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    np.ndarray
        Filtered (low-passed) time series, same length as *data*.

    Raises
    ------
    ValueError
        If *data* contains NaN values or has fewer than ``3 * order + 1``
        points (minimum for ``filtfilt``).
    """
    _log = logger or logging.getLogger(__name__)

    min_length = 3 * order + 1
    if len(data) < min_length:
        raise ValueError(
            f"data must have at least {min_length} points for a Butterworth "
            f"filter of order {order}."
        )
    if np.any(np.isnan(data)):
        raise ValueError(
            'data must not contain NaN values for Butterworth filtering.'
        )

    # Nyquist frequency in cycles per hour
    nyquist = 1.0 / (2.0 * dt_hours)
    cutoff_freq = 1.0 / cutoff_hours

    # Normalized cutoff (0 to 1 where 1 = Nyquist)
    wn = cutoff_freq / nyquist

    b, a = butter(order, wn, btype='low')
    filtered = filtfilt(b, a, data)

    _log.info(
        'Butterworth low-pass: order=%d, cutoff=%.1f h (Wn=%.4f).',
        order, cutoff_hours, wn,
    )
    return filtered


def compute_nontidal_residual(
    observed: np.ndarray,
    predicted_tide: np.ndarray,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    Compute the non-tidal residual by subtracting the predicted tide.

    Parameters
    ----------
    observed : np.ndarray
        Observed water levels.
    predicted_tide : np.ndarray
        Tidal prediction (same length as *observed*).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    np.ndarray
        Non-tidal residual (``observed - predicted_tide``).

    Raises
    ------
    ValueError
        If *observed* and *predicted_tide* have different lengths.
    """
    _log = logger or logging.getLogger(__name__)

    if len(observed) != len(predicted_tide):
        raise ValueError(
            f"observed ({len(observed)}) and predicted_tide "
            f"({len(predicted_tide)}) must have the same length."
        )

    residual = observed - predicted_tide
    _log.info(
        'Non-tidal residual: mean=%.4f, std=%.4f.',
        np.nanmean(residual), np.nanstd(residual),
    )
    return residual
