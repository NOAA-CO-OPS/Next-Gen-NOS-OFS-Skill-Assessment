"""
Forecast Cycle Management

Functions for determining and validating forecast cycle times and lengths
for different OFS systems.
"""

import logging
from datetime import datetime, timedelta

import numpy as np


def get_fcst_hours(ofs):
    '''
    Just what the name says -- gets model forecast cycle hours and forecast
    length (max horizon) in hours.
    Called by do_horizon_skill_utils.get_horizon_filenames

    Parameters
    ----------
    ofs: string, model OFS

    Returns
    -------
    fcstlength: max length of forecast in hours for OFS
    fcstcycles: list of forecast cycle hours for OFS

    '''

    # Need to know forecast cycle hours (e.g. 00Z) and forecast length (hours)
    if ofs in (
        'cbofs', 'dbofs', 'gomofs', 'ciofs', 'leofs',
        'lmhofs', 'loofs', 'loofs2', 'lsofs', 'tbofs',
        'necofs'
    ):
        fcstcycles = np.array([0, 6, 12, 18])
    elif ofs in ('creofs', 'ngofs2', 'sfbofs', 'sscofs'):
        fcstcycles = np.array([3, 9, 15, 21])
    elif ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        fcstcycles = np.array([12])
    else:
        fcstcycles = np.array([3])
    # Now need to know forecast length in hours
    if ofs in (
        'cbofs', 'ciofs', 'creofs', 'dbofs', 'ngofs2', 'sfbofs',
        'tbofs', 'stofs_3d_pac',
    ):
        fcstlength = 48
    elif ofs in ('gomofs', 'wcofs', 'sscofs', 'necofs'):
        fcstlength = 72
    elif ofs in ('stofs_3d_atl'):
        fcstlength = 96
    else:  # Default / catch-all
        fcstlength = 120

    return fcstlength, fcstcycles


def get_fcst_dates(
    ofs: str,
    start_date_full: str,
    forecast_hr: str,
    logger: logging.Logger
) -> tuple[str, str]:
    """
    Assign forecast cycle and compute end date for forecast runs.

    This function is used in forecast_a runs to assign the correct forecast
    horizon for the input OFS, ensure the forecast cycle hour is valid,
    and compute the end date based on the forecast length.

    Parameters
    ----------
    ofs : str
        OFS identifier (e.g., 'cbofs', 'ngofs2')
    start_date_full : str
        Start date in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    forecast_hr : str
        Requested forecast hour with 'hr' suffix (e.g., '00hr', '06hr')
    logger : logging.Logger
        Logger instance for tracking adjustments

    Returns
    -------
    fcst_start : str
        Adjusted forecast start date/time in ISO format
    fcst_end : str
        Computed forecast end date/time in ISO format

    Notes
    -----
    **Forecast Cycle Hours:**

    - CBOFS, DBOFS, GOMOFS, CIOFS, LEOFS, LMHOFS, LOOFS, LSOFS, TBOFS:
      00Z, 06Z, 12Z, 18Z
    - CREOFS, NGOFS2, SFBOFS, SSCOFS:
      03Z, 09Z, 15Z, 21Z
    - STOFS_3D_ATL, STOFS_3D_PAC:
      12Z only
    - Others:
      03Z default

    **Forecast Lengths:**

    - CBOFS, CIOFS, CREOFS, DBOFS, NGOFS2, SFBOFS, TBOFS: 48 hours
    - GOMOFS, WCOFS: 72 hours
    - STOFS_3D_ATL: 96 hours
    - STOFS_3D_PAC: 48 hours
    - Others: 120 hours

    If the requested forecast hour doesn't match a valid cycle for the OFS,
    the function automatically adjusts to the nearest valid cycle hour.

    Examples
    --------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> start, end = get_fcst_dates('cbofs', '2025-07-15T05:00:00Z', '06hr', logger)
    >>> print(f"Start: {start}")
    Start: 2025-07-15T06:00:00Z
    >>> print(f"End: {end}")
    End: 2025-07-17T06:00:00Z

    >>> # Invalid cycle hour gets adjusted
    >>> start, end = get_fcst_dates('cbofs', '2025-07-15T05:00:00Z', '05hr', logger)
    INFO: Adjusted input forecast cycle hour from 05 to 06 for cbofs
    >>> print(f"Start: {start}")
    Start: 2025-07-15T06:00:00Z

    See Also
    --------
    get_fcst_hours : Get list of forecast hours for a run
    """
    logger.info('Starting cycle and end date assignment for forecast_a...')

    # Define forecast cycle hours for each OFS group
    fcstlength, fcstcycles = get_fcst_hours(ofs)

    # Convert forecast cycle ints to str
    fcstcycless = [f'{item:02}' for item in fcstcycles]

    # Verify forecast hour input and adjust if necessary
    requested_hour = forecast_hr[:-2]  # Remove 'hr' suffix
    sdate = start_date_full.split('T')[0]  # Extract date part
    ftime = f'T{forecast_hr[:-2]}:00:00Z'
    sdate = sdate + ftime
    sdatetime = datetime.strptime(sdate, '%Y-%m-%dT%H:%M:%SZ')
    if requested_hour not in fcstcycless:
        if fcstcycles[0] == 0:
            fcstcycles = np.append(fcstcycles, 24)
            fcstcycless.append('00')
        elif fcstcycles[0] == 3 and len(fcstcycles) > 1:
            fcstcycles = np.concatenate(([-3], fcstcycles))
            fcstcycless.insert(0, '21')
        # Find nearest valid cycle hour
        requested_hour_int = int(requested_hour)
        dist = np.array([item - requested_hour_int for item in fcstcycles])
        # Adjust start date to match forecast cycle
        sdatetime = sdatetime + \
            timedelta(hours=int(dist[np.nanargmin(np.abs(dist))]))
        # Display warning
        forecast_hr = sdatetime.strftime('%H')
        logger.info(
            f'Adjusted input forecast cycle hour from {requested_hour} to '
            f'{forecast_hr} for {ofs}')
    fcst_start = datetime.strftime(sdatetime, '%Y-%m-%dT%H:%M:%SZ')

    # Calculate end date based on forecast length
    edate = sdatetime + timedelta(hours=fcstlength)
    fcst_end = datetime.strftime(edate, '%Y-%m-%dT%H:%M:%SZ')

    logger.info(f'Forecast cycle: {forecast_hr}Z')
    logger.info(f'Forecast length: {fcstlength} hours')
    logger.info(f'Forecast period: {fcst_start} to {fcst_end}')
    logger.info('Completed cycle and end date assignment for forecast_a!')

    return fcst_start, fcst_end
