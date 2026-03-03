"""
Current analysis: principal direction and 2D harmonic analysis.

Replaces the legacy ``prcmp`` Fortran subroutine.  Computes the principal
current direction from velocity covariance eigenvectors and performs 2D
harmonic analysis via UTide to extract tidal ellipse parameters.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from utide import solve

logger = logging.getLogger(__name__)


def compute_principal_direction(
    u: np.ndarray,
    v: np.ndarray,
    logger: logging.Logger | None = None,
) -> float:
    """
    Compute the principal current direction from velocity components.

    The principal direction is the orientation of the major axis of the
    velocity covariance ellipse, determined from the eigenvector
    corresponding to the largest eigenvalue.

    Parameters
    ----------
    u : np.ndarray
        East–west velocity component (positive eastward).
    v : np.ndarray
        North–south velocity component (positive northward).
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    float
        Principal direction in degrees True (0–360, measured clockwise
        from north).

    Raises
    ------
    ValueError
        If *u* and *v* have different lengths or fewer than 2 points.
    """
    _log = logger or logging.getLogger(__name__)

    u = np.asarray(u, dtype=float)
    v = np.asarray(v, dtype=float)

    if len(u) != len(v):
        raise ValueError(
            f'u ({len(u)}) and v ({len(v)}) must have the same length.'
        )
    if len(u) < 2:
        raise ValueError('At least 2 data points are required.')

    # Remove NaN pairs
    mask = np.isfinite(u) & np.isfinite(v)
    if np.sum(mask) < 2:
        raise ValueError('Fewer than 2 finite data points after NaN removal.')

    u_clean = u[mask]
    v_clean = v[mask]

    # Covariance matrix of (u, v)
    cov = np.cov(u_clean, v_clean)
    eigenvalues, eigenvectors = np.linalg.eigh(cov)

    # Major axis eigenvector (largest eigenvalue — last column from eigh)
    major_vec = eigenvectors[:, -1]  # [u_component, v_component]

    # Convert to compass bearing (degrees True, clockwise from north)
    # atan2(east, north) gives angle clockwise from north
    direction_deg = np.degrees(np.arctan2(major_vec[0], major_vec[1]))

    # Normalize to [0, 360)
    direction_deg = direction_deg % 360.0

    _log.info('Principal current direction: %.1f deg True.', direction_deg)
    return float(direction_deg)


def current_harmonic_analysis(
    time: pd.DatetimeIndex,
    u: np.ndarray,
    v: np.ndarray,
    latitude: float,
    constit: list[str] | None = None,
    logger: logging.Logger | None = None,
) -> dict:
    """
    Perform 2D harmonic analysis on current velocity components.

    Wraps :func:`utide.solve` with ``u`` and ``v`` for complex (2D)
    analysis, extracting tidal ellipse parameters for each constituent.

    Parameters
    ----------
    time : pd.DatetimeIndex
        Timestamps of observations (UTC).
    u : np.ndarray
        East–west velocity component (m/s, positive eastward).
    v : np.ndarray
        North–south velocity component (m/s, positive northward).
    latitude : float
        Station latitude in decimal degrees.
    constit : list of str, optional
        Constituent names to resolve.  If ``None``, UTide selects
        automatically based on record length.
    logger : logging.Logger, optional
        Logger instance for diagnostic messages.

    Returns
    -------
    dict
        ``"coef"`` : UTide coefficient structure (Bunch).
        ``"ellipses"`` : :class:`pandas.DataFrame` with columns
            ``Name``, ``Lsmaj``, ``Lsmin``, ``theta``, ``g``.
        ``"principal_direction"`` : float — principal current direction
            in degrees True.

    Raises
    ------
    ValueError
        If inputs have mismatched lengths or no finite data.
    """
    _log = logger or logging.getLogger(__name__)

    u = np.asarray(u, dtype=float)
    v = np.asarray(v, dtype=float)

    if len(time) != len(u) or len(time) != len(v):
        raise ValueError(
            f'time ({len(time)}), u ({len(u)}), and v ({len(v)}) must have '
            f'the same length.'
        )

    mask = np.isfinite(u) & np.isfinite(v)
    if not np.any(mask):
        raise ValueError('u and v contain no finite data.')

    _log.info(
        'Running 2D harmonic analysis: %d time steps, lat=%.2f.',
        len(time), latitude,
    )

    # UTide 2D solve
    solve_kwargs = dict(
        t=time,
        u=u,
        v=v,
        lat=latitude,
        method='ols',
        conf_int='linear',
        Rayleigh_min=0.9,
    )
    if constit is not None:
        solve_kwargs['constit'] = constit

    coef = solve(**solve_kwargs)

    # Extract ellipse parameters
    ellipses = pd.DataFrame({
        'Name': coef.name,
        'Lsmaj': coef.Lsmaj,
        'Lsmin': coef.Lsmin,
        'theta': coef.theta,
        'g': coef.g,
    })

    principal_dir = compute_principal_direction(u, v, logger=_log)

    _log.info(
        '2D HA complete: %d constituents resolved, principal dir=%.1f deg.',
        len(ellipses), principal_dir,
    )

    return {
        'coef': coef,
        'ellipses': ellipses,
        'principal_direction': principal_dir,
    }
