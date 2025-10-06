# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 10:23:25 2024

@author: PL
"""

from datetime import datetime, timedelta
import numpy as np

def get_fcst_cycle(ofs,start_date_full,forecast_hr,logger):
    '''This module/function is used in forecast_a runs to assign the correct
    forecast hoizon for the input OFS, make sure the forecast cycle hour is
    correct, and reassign the end date to correspond to the forecast horizon.

    '''

    logger.info('Starting cycle and end date assignment for forecast_a...')

    # Need to know forecast cycle hours (e.g. 00Z) and forecast length (hours)
    if ofs in ("cbofs","dbofs","gomofs","ciofs","leofs","lmhofs","loofs",
                    "lsofs"):
        fcstcycles = np.array([0, 6, 12, 18])
        fcstcycless = ['00', '06', '12', '18']
    elif ofs in ("creofs","ngofs2","sfbofs","tbofs"):
        fcstcycles = np.array([3, 9, 15, 21])
        fcstcycless = ['03', '09', '15', '21']
    elif ofs in ("stofs_3d_atl", "stofs_3d_pac"):
        fcstcycles = 12
        fcstcycless = ['12']
    else:
        fcstcycles = 3
        fcstcycless = ['03']

    #Now need to know forecast length in hours
    if ofs in ("cbofs", "ciofs", "creofs","dbofs", "ngofs2", "sfbofs",
                    "tbofs"):
        fcstlength = 48
    elif ofs in ("gomofs", "wcofs"):
        fcstlength = 72
    elif ofs in ("stofs_3d_atl"):
        fcstlength = 96
    elif ofs in ("stofs_3d_pac"):
        fcstlength = 48
    else:
        fcstlength = 120

    # Verify forecast hour input and change it if necessary
    if forecast_hr[:-2] not in fcstcycless:
        pfh = forecast_hr[:-2]
        pfhint = int(str(int(pfh)))
        dist = abs(fcstcycles - pfhint)
        mindex = np.nanargmin(dist)
        forecast_hr = fcstcycless[mindex] + 'hr'
        logger.info(
            f'Adjusted input forecast cycle hour from {pfh} to '
            f'{forecast_hr[:-2]} for {ofs}')

    # Make start date correspond to forecast cycle selection
    sdate = start_date_full
    ftime = 'T' + forecast_hr[:-2] + ':00:00Z'
    sdate = sdate.split('T')
    fcst_start = sdate[0] + ftime
    # Make end date correspond to forecast cycle selection
    edate = datetime.strptime(fcst_start,"%Y-%m-%dT%H:%M:%SZ") + timedelta(hours = fcstlength)
    fcst_end = datetime.strftime(edate,"%Y-%m-%dT%H:%M:%SZ")

    logger.info('Completed cycle and end date assignment for forecast_a!')

    return fcst_start, fcst_end
