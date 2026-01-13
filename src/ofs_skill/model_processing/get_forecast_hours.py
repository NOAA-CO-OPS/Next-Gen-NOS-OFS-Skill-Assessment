"""
Forecast Horizon Management

Functions for managing forecast cycle hours, forecast lengths, and
processing forecast horizon time series for skill assessment.
"""

from __future__ import annotations

import numpy as np


def get_forecast_hours(ofs: str) -> tuple[int, np.ndarray]:
    """
    Get forecast cycle hours and maximum forecast length for an OFS.

    Parameters
    ----------
    ofs : str
        OFS identifier (e.g., 'cbofs', 'ngofs2')

    Returns
    -------
    fcstlength : int
        Maximum forecast length in hours
    fcstcycles : np.ndarray or int
        Array of forecast cycle hours (e.g., [0, 6, 12, 18])
        or single integer for models with one cycle

    Examples
    --------
    >>> fcstlength, fcstcycles = get_forecast_hours('cbofs')
    >>> print(f"Length: {fcstlength}h, Cycles: {fcstcycles}")
    Length: 48h, Cycles: [ 0  6 12 18]

    >>> fcstlength, fcstcycles = get_forecast_hours('stofs_3d_atl')
    >>> print(f"Length: {fcstlength}h, Cycle: {fcstcycles}")
    Length: 96h, Cycle: 12

    Notes
    -----
    **Forecast Cycle Hours:**
    - Most OFS: 00Z, 06Z, 12Z, 18Z
    - CREOFS, NGOFS2, SFBOFS, SSCOFS: 03Z, 09Z, 15Z, 21Z
    - STOFS_3D: 12Z only

    **Forecast Lengths:**
    - CBOFS, CIOFS, CREOFS, DBOFS, NGOFS2, SFBOFS, TBOFS: 48h
    - GOMOFS, WCOFS, SSCOFS: 72h
    - STOFS_3D_ATL: 96h
    - STOFS_3D_PAC: 48h
    - Great Lakes OFS: 120h
    """
    ofs_lower = ofs.lower()

    # Define forecast cycle hours for each OFS group
    if ofs_lower in (
        'cbofs', 'dbofs', 'gomofs', 'ciofs', 'leofs', 'lmhofs', 'loofs',
        'lsofs', 'tbofs',
    ):
        fcstcycles = np.array([0, 6, 12, 18])
    elif ofs_lower in ('creofs', 'ngofs2', 'sfbofs', 'sscofs'):
        fcstcycles = np.array([3, 9, 15, 21])
    elif ofs_lower in ('stofs_3d_atl', 'stofs_3d_pac'):
        fcstcycles = 12
    else:
        fcstcycles = 3

    # Define forecast length in hours for each OFS group
    if ofs_lower in (
        'cbofs', 'ciofs', 'creofs', 'dbofs', 'ngofs2', 'sfbofs', 'tbofs',
    ):
        fcstlength = 48
    elif ofs_lower in ('gomofs', 'wcofs', 'sscofs'):
        fcstlength = 72
    elif ofs_lower in ('stofs_3d_atl',):
        fcstlength = 96
    elif ofs_lower in ('stofs_3d_pac',):
        fcstlength = 48
    else:
        fcstlength = 120

    return fcstlength, fcstcycles
