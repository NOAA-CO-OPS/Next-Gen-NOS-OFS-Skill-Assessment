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
            f'time ({len(time)}) and values ({len(values)}) must have the '
            f'same length.'
        )

    finite_mask = np.isfinite(values)
    if not np.any(finite_mask):
        raise ValueError('values contains no finite data.')

    duration_days = (time[-1] - time[0]).total_seconds() / 86400.0
    if duration_days < min_duration_days:
        raise ValueError(
            f'Record length {duration_days:.1f} days is less than the '
            f'minimum {min_duration_days} days required for harmonic analysis.'
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

    # When constit is an explicit list, UTide does NOT apply the Rayleigh
    # criterion — it fits everything regardless, producing physically
    # meaningless amplitudes for constituents that cannot be resolved.
    #
    # Two pre-filters:
    # 1. Period filter: remove constituents whose period > record length
    #    (handles SA, SSA, MSM, etc.)
    # 2. Rayleigh filter: remove constituents that are too closely spaced
    #    in frequency to be separated (handles S2/K2/T2/R2 aliasing, etc.)
    constit = _filter_by_period(constit, duration_days, _log)
    constit = _filter_by_rayleigh(constit, duration_days, _log=_log)

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


def _filter_by_rayleigh(
    constit: list[str],
    duration_days: float,
    rayleigh_min: float = 0.9,
    _log: logging.Logger | None = None,
) -> list[str]:
    """Remove constituents that cannot be separated by the Rayleigh criterion.

    For each pair of constituents, the Rayleigh number is::

        R = T_hours * |f1 - f2|

    where *f* is in cycles per hour.  When R < *rayleigh_min*, the two
    constituents cannot be independently resolved.  This function finds
    groups of mutually inseparable constituents and keeps only the
    highest-priority one (earliest in the input *constit* list, which
    follows the NOS importance ordering).

    Parameters
    ----------
    constit : list of str
        Candidate constituent names (already period-filtered).
    duration_days : float
        Record length in days.
    rayleigh_min : float
        Minimum Rayleigh number (default 0.9).
    _log : logging.Logger, optional
        Logger for diagnostics.

    Returns
    -------
    list of str
        Filtered constituent list.
    """
    from collections import defaultdict

    from .constituents import CONSTITUENT_SPEEDS

    _log = _log or logging.getLogger(__name__)

    T_hours = duration_days * 24.0
    min_delta_f = rayleigh_min / T_hours  # cycles per hour

    # Get frequencies for known constituents
    freqs: dict[str, float] = {}
    for c in constit:
        speed = CONSTITUENT_SPEEDS.get(c)
        if speed is not None:
            freqs[c] = speed / 360.0  # deg/hr → cycles/hr

    # Build adjacency graph of inseparable pairs
    adj: dict[str, set[str]] = defaultdict(set)
    names_with_freq = [c for c in constit if c in freqs]
    for i, c1 in enumerate(names_with_freq):
        for c2 in names_with_freq[i + 1:]:
            if abs(freqs[c1] - freqs[c2]) < min_delta_f:
                adj[c1].add(c2)
                adj[c2].add(c1)

    # Find connected components via BFS
    visited: set[str] = set()
    to_drop: set[str] = set()
    for c in names_with_freq:
        if c in visited:
            continue
        # BFS from this node
        component: list[str] = []
        queue = [c]
        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            component.append(node)
            queue.extend(adj[node] - visited)

        if len(component) > 1:
            # Sort by position in the original constit list (earlier = keep)
            component.sort(key=lambda x: constit.index(x))
            kept = component[0]
            dropped = component[1:]
            to_drop.update(dropped)
            _log.warning(
                'Inseparable constituents (Rayleigh < %.1f for %.0f-day '
                'record): %s. Keeping %s, dropping %s.',
                rayleigh_min, duration_days, component, kept, dropped,
            )

    if to_drop:
        _log.warning(
            'Dropped %d constituents due to Rayleigh criterion: %s',
            len(to_drop), sorted(to_drop),
        )

    # Preserve original order; include unknowns that lack frequency info
    return [c for c in constit if c not in to_drop]


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


def _filter_by_period(
    constit: list[str],
    duration_days: float,
    _log: logging.Logger,
) -> list[str]:
    """Remove constituents whose period exceeds the record length.

    When an explicit constituent list is passed to UTide ``solve()``, the
    Rayleigh criterion is **not** applied — UTide fits everything requested.
    Constituents with periods longer than the record produce physically
    meaningless amplitudes (sometimes millions of units) because the solver
    cannot constrain them.  This helper drops those constituents before the
    call to ``solve()``.

    Parameters
    ----------
    constit : list of str
        Candidate constituent names.
    duration_days : float
        Record length in days.
    _log : logging.Logger
        Logger for diagnostics.

    Returns
    -------
    list of str
        Filtered constituent list.
    """
    from .constituents import CONSTITUENT_SPEEDS

    kept: list[str] = []
    removed: list[str] = []
    for c in constit:
        speed = CONSTITUENT_SPEEDS.get(c)
        if speed is None or speed == 0:
            kept.append(c)  # unknown → let UTide decide
            continue
        period_days = 360.0 / (speed * 24.0)
        if period_days > duration_days:
            removed.append(c)
        else:
            kept.append(c)

    if removed:
        _log.warning(
            'Dropping %d constituents whose period exceeds the %.1f-day '
            'record: %s',
            len(removed), duration_days, removed,
        )

    return kept


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
            'Record length %.1f days (>= 180 days). Many constituents '
            'resolvable directly (equivalent to lsqha). A full year '
            '(365 days) is needed to directly observe all 37 NOS '
            'standard constituents.',
            duration_days,
        )
    else:
        _log.info(
            'Record length %.1f days (>= 1 year). Sufficient to directly '
            'resolve all 37 NOS standard constituents.',
            duration_days,
        )
