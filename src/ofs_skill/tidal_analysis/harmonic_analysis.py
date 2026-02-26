"""
Core harmonic analysis module wrapping UTide to match NOS Fortran conventions.

Replaces the legacy ``harm29d.f``, ``harm15.f``, and ``lsqha.f`` programs.
UTide's iteratively-reweighted least-squares solver provides a single,
unified interface that automatically handles short, standard, and long
records.

Record-length guidance (per NOS CO-OPS Technical Memorandum 0021 and
https://tidesandcurrents.noaa.gov/about_harmonic_constituents.html):

* **15–28 days** — equivalent to ``harm15.f``.  Only ~9 constituents can
  be computed directly; the remaining standard 37 must be inferred.
* **29–179 days** — equivalent to ``harm29d.f``.  ~10 constituents
  computed directly; 14+ inferred.  The minimum recommended for routine
  skill assessment.
* **≥ 180 days (~6 months)** — equivalent to ``lsqha.f``.  Progressively
  more of the full 37 constituents can be resolved directly.
* **≥ 365 days (1 year)** — a full year is needed to directly observe all
  37 standard NOS constituents (NOAA CO-OPS recommendation).

Reference sources for the harmonic-constant comparison table differ by
variable (NOS CS 17 / CS 24):

* **Water levels** — reference = CO-OPS accepted harmonic constants
  retrieved from the Tides & Currents API (``product=harcon``).  These are
  derived from years of observations at permanent tide stations.
* **Currents** — reference = harmonic analysis of the *observation* time
  series from the same run period, because CO-OPS does not maintain
  long-term accepted constants for current stations (deployments are
  typically temporary).

References
----------
- Codiga, D.L. (2011). Unified Tidal Analysis and Prediction Using the
  UTide Matlab Functions.  Technical Report 2011-01, URI-GSO.
- Zhang et al. (2006). NOAA Technical Report NOS CS 24.
- Hess et al. (2003). NOAA Technical Report NOS CS 17.
- NOAA Technical Memorandum NOS CO-OPS 0021, Tidal Current Analysis
  Procedures.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from utide import solve

from .constituents import NOS_37_CONSTITUENTS

logger = logging.getLogger(__name__)


def harmonic_analysis(
    time: pd.DatetimeIndex,
    values: np.ndarray,
    latitude: float,
    constit: list[str] | None = None,
    method: str = 'auto',
    min_duration_days: float = 15.0,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Perform harmonic analysis on a water level or current time series.

    This is the Python replacement for ``harm29d.f`` (29-day Fourier HA),
    ``harm15.f`` (15-day Fourier HA), and ``lsqha.f`` (least-squares HA).
    Under the hood it calls :func:`utide.solve` with NOS-convention defaults.

    Parameters
    ----------
    time : pd.DatetimeIndex
        Timestamps of observations (UTC).
    values : np.ndarray
        Observed values (water level in metres, or current speed).
    latitude : float
        Station latitude in decimal degrees (needed for nodal corrections).
    constit : list of str, optional
        Constituent names to resolve.  If ``None`` (default), the NOS
        standard 37 constituents are requested; UTide will automatically
        drop any that cannot be separated given the record length.
    method : str, optional
        ``"auto"`` (default) — UTide selects internally.  Provided for
        forward-compatibility if explicit method selection is added later.
    min_duration_days : float, optional
        Minimum record length required (default 15.0 days).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    dict
        ``"coef"`` : UTide coefficient structure (Bunch) —
            contains ``name``, ``A``, ``g``, ``A_ci``, ``g_ci``, ``mean``.
        ``"constituents"`` : :class:`pandas.DataFrame` —
            columns ``Name``, ``Amplitude``, ``Phase``, ``SNR``.
        ``"mean"`` : float — mean water level H0.
        ``"method_used"`` : str — description of effective method class.
        ``"requested_constituents"`` : list of str — constituents requested.
        ``"resolved_constituents"`` : list of str — constituents UTide solved.
        ``"dropped_constituents"`` : list of str — requested but not resolved.

    Raises
    ------
    ValueError
        If the record is shorter than *min_duration_days* or if *values*
        contains no finite data.
    """
    _log = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------
    if len(time) != len(values):
        raise ValueError(
            f"time ({len(time)}) and values ({len(values)}) must have the "
            f"same length."
        )

    finite_mask = np.isfinite(values)
    if not np.any(finite_mask):
        raise ValueError('values contains no finite data.')

    duration_days = (time[-1] - time[0]).total_seconds() / 86400.0
    if duration_days < min_duration_days:
        raise ValueError(
            f"Record length {duration_days:.1f} days is less than the "
            f"minimum {min_duration_days} days required for harmonic analysis."
        )

    # ------------------------------------------------------------------
    # Record-length advisory warnings (NOS CO-OPS recommendations)
    # ------------------------------------------------------------------
    _warn_record_length(duration_days, _log)

    # ------------------------------------------------------------------
    # Constituent list
    # ------------------------------------------------------------------
    if constit is None:
        constit = list(NOS_37_CONSTITUENTS)

    # Filter out constituents not recognized by UTide (e.g. M1, 2MK3)
    try:
        from utide._ut_constants import ut_constants
        utide_names = {n.strip() for n in ut_constants['const']['name']}
        unsupported = [c for c in constit if c not in utide_names]
        if unsupported:
            _log.warning(
                'Dropping %d constituents not in UTide: %s',
                len(unsupported), unsupported,
            )
            constit = [c for c in constit if c in utide_names]
    except Exception:
        pass  # If we can't check, let UTide raise its own error

    requested_constituents = list(constit)
    method_label = _classify_method(duration_days)
    _log.info(
        'Running harmonic analysis: %.1f-day record, %d constituents '
        "requested, method class '%s'.",
        duration_days, len(constit), method_label,
    )

    # ------------------------------------------------------------------
    # Call UTide solver
    # ------------------------------------------------------------------
    coef = solve(
        t=time,
        u=values,
        lat=latitude,
        constit=constit,
        method='ols',           # ordinary least squares (closest to legacy)
        conf_int='linear',      # linear confidence intervals
        Rayleigh_min=0.9,       # constituent separation criterion
    )

    # ------------------------------------------------------------------
    # Package results
    # ------------------------------------------------------------------
    # SNR proxy: amplitude / half-width of 95 % confidence interval
    snr = coef.A / np.where(coef.A_ci > 0, coef.A_ci, np.inf)

    results_df = pd.DataFrame({
        'Name': coef.name,
        'Amplitude': coef.A,
        'Phase': coef.g,
        'SNR': snr,
    })

    # Track which constituents were resolved vs dropped by UTide
    resolved_names = list(coef.name)
    dropped = [c for c in requested_constituents if c not in resolved_names]

    mean_level = float(coef.mean) if hasattr(coef, 'mean') else np.nan
    _log.info(
        'Harmonic analysis complete. Mean=%.4f, %d of %d requested '
        'constituents resolved.',
        mean_level, len(resolved_names), len(requested_constituents),
    )
    if dropped:
        _log.info(
            'Constituents not resolved (insufficient record length to '
            'separate via Rayleigh criterion): %s',
            dropped,
        )

    return {
        'coef': coef,
        'constituents': results_df,
        'mean': mean_level,
        'method_used': method_label,
        'requested_constituents': requested_constituents,
        'resolved_constituents': resolved_names,
        'dropped_constituents': dropped,
    }


def _classify_method(duration_days: float) -> str:
    """Return a human-readable label for the effective HA method class.

    Classification follows the legacy Fortran program thresholds:
      < 20 days  → harm15 (short record, ~9 constituents computed)
      20–179 days → harm29d (standard, ~10 constituents computed)
      ≥ 180 days → lsqha  (long record, progressively more resolved)
    """
    if duration_days < 20:
        return 'short_record'
    elif duration_days < 180:
        return 'standard'
    else:
        return 'long_record_lsq'


def _warn_record_length(duration_days: float, _log: logging.Logger) -> None:
    """Emit advisory warnings based on NOS CO-OPS record-length guidance.

    Thresholds per NOS CO-OPS Tech Memo 0021 and the NOAA "About Harmonic
    Constituents" page (tidesandcurrents.noaa.gov):

    * 15–28 days  → only ~9 constituents directly computed (harm15-class)
    * 29–179 days → ~10 constituents directly computed (harm29d-class)
    * 180–364 days → many more resolved, but not all 37
    * ≥ 365 days  → full 37 NOS constituents directly observable
    """
    if duration_days < 29:
        _log.warning(
            'Record length %.1f days is below the 29-day recommendation '
            '(NOS CO-OPS Tech Memo 0021). Only ~9 constituents can be '
            'computed directly; remaining constituents will be inferred '
            'or dropped. Consider using at least 29 days of data.',
            duration_days,
        )
    elif duration_days < 180:
        _log.warning(
            'Record length %.1f days is below 180 days (~6 months). '
            'Only ~10 of the NOS standard 37 constituents can be computed '
            'directly (equivalent to harm29d). For a more complete '
            'constituent resolution, 180+ days of data are recommended '
            '(NOS CO-OPS Tech Memo 0021).',
            duration_days,
        )
    elif duration_days < 365:
        _log.info(
            'Record length %.1f days (≥ 180 days). Many constituents '
            'resolvable directly (equivalent to lsqha). A full year '
            '(365 days) is needed to directly observe all 37 NOS '
            'standard constituents.',
            duration_days,
        )
    else:
        _log.info(
            'Record length %.1f days (≥ 1 year). Sufficient to directly '
            'resolve all 37 NOS standard constituents.',
            duration_days,
        )
