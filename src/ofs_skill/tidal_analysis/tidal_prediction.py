"""
Tidal prediction from harmonic constants.

Implements the NOS standard prediction formula::

    h = H0 + sum{ f * H * cos[a*t + (V0+u) - kappa] }

by wrapping :func:`utide.reconstruct`.  Replaces the legacy ``pred.f``
Fortran program.

Two entry points are provided:

* :func:`predict_tide` — predict from a UTide coefficient structure
  (as returned by :func:`~ofs_skill.tidal_analysis.harmonic_analysis`).
* :func:`predict_from_constants` — predict from plain amplitude/phase
  dictionaries (e.g. CO-OPS accepted harmonic constants).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from utide import reconstruct
from utide.utilities import Bunch

from .constituents import CONSTITUENT_SPEEDS

logger = logging.getLogger(__name__)


def predict_tide(
    time: pd.DatetimeIndex,
    coef: object,
    constit: list[str] | None = None,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    Generate tidal predictions from a UTide coefficient structure.

    This is the Python equivalent of ``pred.f``.  Nodal corrections (*f*,
    *u*) and equilibrium arguments (*V0*) are computed internally by UTide.

    Parameters
    ----------
    time : pd.DatetimeIndex
        Prediction times (UTC).
    coef : utide Bunch
        Coefficient structure from :func:`harmonic_analysis` (the
        ``"coef"`` key of the returned dict) or built via
        :func:`_build_coef_from_constants`.
    constit : list of str, optional
        Subset of constituents to include in prediction.  If ``None``,
        all constituents present in *coef* are used.
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    np.ndarray
        Predicted tidal heights (or along-channel current speeds).
    """
    _log = logger or logging.getLogger(__name__)
    _log.info('Generating tidal predictions for %d time steps.', len(time))

    result = reconstruct(t=time, coef=coef, constit=constit)
    return result.h


def predict_from_constants(
    time: pd.DatetimeIndex,
    amplitudes: dict[str, float],
    phases: dict[str, float],
    mean_level: float,
    latitude: float,
    logger: logging.Logger | None = None,
) -> np.ndarray:
    """
    Generate predictions from amplitude/phase dictionaries.

    Useful when working with CO-OPS accepted harmonic constants retrieved
    via the Tides & Currents API (``product=harcon``) rather than a fresh
    harmonic analysis.

    Parameters
    ----------
    time : pd.DatetimeIndex
        Prediction times (UTC).
    amplitudes : dict
        ``{constituent_name: amplitude}`` in metres (or m/s for currents).
    phases : dict
        ``{constituent_name: phase_lag}`` in degrees (Greenwich epoch).
    mean_level : float
        Mean water level H0 (metres above datum).
    latitude : float
        Station latitude in decimal degrees.
    logger : logging.Logger, optional
        Logger instance.

    Returns
    -------
    np.ndarray
        Predicted tidal heights.
    """
    coef = _build_coef_from_constants(amplitudes, phases, mean_level, latitude)
    return predict_tide(time, coef, logger=logger)


def _build_coef_from_constants(
    amplitudes: dict[str, float],
    phases: dict[str, float],
    mean_level: float,
    latitude: float,
) -> Bunch:
    """
    Build a synthetic UTide Bunch coefficient structure from dictionaries.

    This constructs the minimal set of attributes that
    :func:`utide.reconstruct` needs to produce a prediction.

    Parameters
    ----------
    amplitudes : dict
        ``{constituent_name: amplitude}``.
    phases : dict
        ``{constituent_name: phase_lag_degrees}``.
    mean_level : float
        Mean value (H0).
    latitude : float
        Station latitude.

    Returns
    -------
    utide.utilities.Bunch
        Coefficient structure accepted by :func:`utide.reconstruct`.
    """
    # Use only constituents present in both dicts
    names = sorted(set(amplitudes.keys()) & set(phases.keys()))
    if not names:
        raise ValueError('No common constituents found in amplitudes and phases.')

    # Build the constituent index array that UTide uses internally.
    # UTide's _ut_constants stores ~146 constituents; we look up each
    # name to get the canonical index (lind).  Fall back to sequential
    # indices if the lookup table is unavailable.
    try:
        from utide._ut_constants import ut_constants

        const_names = [n.strip() for n in ut_constants['const']['name']]
        lind = np.array([const_names.index(n) for n in names])
    except Exception:
        lind = np.arange(len(names))

    coef = Bunch()
    coef.name = np.array(names)
    coef.A = np.array([amplitudes[n] for n in names])
    coef.g = np.array([phases[n] for n in names])
    coef.mean = mean_level
    coef.slope = 0.0
    coef.aux = Bunch()
    coef.aux.lat = latitude
    # Use UTide's own frequency table when available for consistency
    # with its nodal correction calculations.
    try:
        from utide._ut_constants import ut_constants

        const_names = [n.strip() for n in ut_constants['const']['name']]
        const_freqs = ut_constants['const']['freq']  # cycles per hour
        coef.aux.frq = np.array(
            [float(const_freqs[const_names.index(n)]) for n in names]
        )
    except Exception:
        coef.aux.frq = np.array(
            [CONSTITUENT_SPEEDS.get(n, 0.0) / 360.0 for n in names]
        )
    coef.aux.lind = lind
    coef.aux.reftime = 0.0  # reference time (UTide uses datenum epoch)
    coef.aux.opt = Bunch()
    coef.aux.opt.twodim = False
    coef.aux.opt.nodsatlint = False
    coef.aux.opt.nodsatnone = False
    coef.aux.opt.gwchlint = False
    coef.aux.opt.gwchnone = False
    coef.aux.opt.nodiagn = True  # skip SNR filtering for synthetic coefs
    coef.aux.opt.notrend = True  # no trend removal for synthetic coefs
    coef.aux.opt.prefilt = []

    return coef
