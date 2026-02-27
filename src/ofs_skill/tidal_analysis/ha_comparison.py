"""
Harmonic constant comparison between model and accepted values.

Computes per-constituent amplitude differences, phase differences, and
vector differences for comparing model-derived harmonic constants against
CO-OPS accepted values or other reference sets.

The vector difference formula follows NOS convention::

    Vd = sqrt(Am² + Aa² - 2·Am·Aa·cos(Δg))

where Am/Aa are model/accepted amplitudes and Δg is the phase difference.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compare_harmonic_constants(
    model_amp: np.ndarray,
    model_phase: np.ndarray,
    accepted_amp: np.ndarray,
    accepted_phase: np.ndarray,
    constituents: list[str] | None = None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """
    Compare model-derived harmonic constants against accepted values.

    Parameters
    ----------
    model_amp : np.ndarray
        Model amplitudes (metres or m/s).
    model_phase : np.ndarray
        Model phases (degrees, Greenwich epoch).
    accepted_amp : np.ndarray
        Accepted (reference) amplitudes.
    accepted_phase : np.ndarray
        Accepted (reference) phases (degrees).
    constituents : list of str, optional
        Constituent names.  If ``None``, generic labels ``C0, C1, ...``
        are used.
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    pd.DataFrame
        Columns: ``Constituent``, ``Accepted_Amp``, ``Model_Amp``,
        ``Amp_Diff``, ``Accepted_Phase``, ``Model_Phase``, ``Phase_Diff``,
        ``Vector_Diff``.

    Raises
    ------
    ValueError
        If input arrays have mismatched lengths.
    """
    _log = logger or logging.getLogger(__name__)

    model_amp = np.asarray(model_amp, dtype=float)
    model_phase = np.asarray(model_phase, dtype=float)
    accepted_amp = np.asarray(accepted_amp, dtype=float)
    accepted_phase = np.asarray(accepted_phase, dtype=float)

    lengths = {len(model_amp), len(model_phase),
               len(accepted_amp), len(accepted_phase)}
    if len(lengths) != 1:
        raise ValueError(
            'All input arrays must have the same length.  Got lengths: '
            f'model_amp={len(model_amp)}, model_phase={len(model_phase)}, '
            f'accepted_amp={len(accepted_amp)}, '
            f'accepted_phase={len(accepted_phase)}.'
        )

    n = len(model_amp)
    if constituents is None:
        constituents = [f'C{i}' for i in range(n)]
    elif len(constituents) != n:
        raise ValueError(
            f'constituents list ({len(constituents)}) must match array '
            f'length ({n}).'
        )

    # Amplitude difference
    amp_diff = model_amp - accepted_amp

    # Phase difference wrapped to [-180, 180]
    phase_diff = (model_phase - accepted_phase + 180.0) % 360.0 - 180.0

    # Vector difference: sqrt(Am² + Aa² - 2·Am·Aa·cos(Δg))
    delta_rad = np.radians(phase_diff)
    vector_diff = np.sqrt(
        model_amp ** 2
        + accepted_amp ** 2
        - 2.0 * model_amp * accepted_amp * np.cos(delta_rad)
    )

    df = pd.DataFrame({
        'Constituent': constituents,
        'Accepted_Amp': accepted_amp,
        'Model_Amp': model_amp,
        'Amp_Diff': amp_diff,
        'Accepted_Phase': accepted_phase,
        'Model_Phase': model_phase,
        'Phase_Diff': phase_diff,
        'Vector_Diff': vector_diff,
    })

    # Summary statistics using NaN-safe arithmetic
    valid_mask = np.isfinite(vector_diff)
    n_valid = int(np.sum(valid_mask))
    n_missing = n - n_valid

    if n_valid > 0:
        mean_vd = float(np.nanmean(vector_diff))
        _log.info(
            'HA comparison: %d of %d constituents have valid vector diff '
            '(mean=%.4f). %d constituents unresolved.',
            n_valid, n, mean_vd, n_missing,
        )
    else:
        _log.warning(
            'HA comparison: all %d constituents have NaN vector diff. '
            'No valid comparison could be computed.',
            n,
        )

    return df
