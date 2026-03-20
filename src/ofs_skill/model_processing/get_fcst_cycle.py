"""
Forecast Cycle Management

Functions for determining and validating forecast cycle times and lengths
for different OFS systems.
"""

from datetime import UTC, datetime, timedelta

import boto3
import numpy as np
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from ofs_skill.obs_retrieval import utils


def get_s3_bucket(ofs):
    """Select appropriate S3 bucket config name from OFS.

    Parameters
    ----------
    ofs : str
        OFS model name (e.g., 'cbofs', 'stofs_3d_atl').

    Returns
    -------
    str
        Config key for the S3 bucket URL.
    """
    if ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        url_root = 'nodd_s3_stofs3d'
    elif ofs == 'stofs_2d_global':
        url_root = 'nodd_s3_stofs2d'
    else:
        url_root = 'nodd_s3'
    return url_root


def get_most_recent_file_date(bucket_name, ofs, logger):
    """
    Finds the most recent model station file date in an S3 bucket.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket.
    ofs : str
        OFS model name (e.g., 'cbofs', 'stofs_3d_atl').
    logger : logging.Logger
        Logger instance for logging messages.

    Returns
    -------
    str or None
        The formatted date string (e.g., '2025-03-15T12:00:00Z'),
        or None if no files are found.
    """
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    prefix = ofs
    if ofs == 'stofs_3d_atl':
        prefix = 'STOFS-3D-Atl/'
    elif ofs == 'stofs_3d_pac':
        prefix = 'STOFS-3D-Pac/'

    # Sort objects with 'station' in their name by LastModified timestamp in ascending order
    # and get the last one
    if 'stofs' not in ofs:
        # Date-targeted approach: check the most recent days first to avoid
        # paginating through the entire bucket prefix (which can be very slow).
        all_filt_objects = []
        for days_back in range(7):
            date_str = datetime.strftime(
                datetime.now(UTC) - timedelta(days=days_back), '%Y/%m/%d')
            targeted_prefix = f'{prefix}/netcdf/{date_str}/'
            try:
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name,
                                           Prefix=targeted_prefix)
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if 'station' in obj['Key']:
                                all_filt_objects.append(obj)
            except ClientError as e:
                logger.error(f'Error listing S3 objects: {e}')
                continue
            if all_filt_objects:
                break
        if not all_filt_objects:
            logger.error('No station files found in S3 bucket for the last 7 days')
            return None
        all_filt_objects.sort(key=lambda obj: obj['LastModified'])
        most_recent_object = all_filt_objects[-1]
        split_name = most_recent_object['Key'].split('/')
        date = split_name[2]+'-'+split_name[3]+'-'+split_name[4]+'T'+split_name[-1].split('.')[1][1:-1]+':00:00Z'
    else:
        # STOFS uses different path structure - no 'netcdf' subdirectory
        # Bucket structure: STOFS-3D-Atl/stofs_3d_atl.YYYYMMDD/filename.nc
        # Build prefix/file path
        MAX_LOOKBACK_DAYS = 30
        dir_found = False
        counter = -1
        while not dir_found and counter < MAX_LOOKBACK_DAYS:
            counter += 1
            date_to_check = datetime.strftime(datetime.now(UTC) - \
                                  timedelta(hours=counter*24),'%Y%m%d')
            folder_path = prefix + ofs + '.' + date_to_check + '/'
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=folder_path,
                    MaxKeys=1
                )
                # If 'Contents' is in the response, the folder has stuff in it
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'] != folder_path:
                            dir_found = True
                            date = datetime.strftime(datetime.strptime(date_to_check,'%Y%m%d'),'%Y-%m-%dT12:00:00Z')
            except ClientError as e:
                logger.error(f'Error checking folder existence: {e}')
                return None
        if not dir_found:
            logger.error(f'No STOFS data found in the last {MAX_LOOKBACK_DAYS} days')
            return None
    return date

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


def get_fcst_dates(prop, logger):
    """
    Assign forecast cycle and compute end date for forecast runs.

    This function is used in forecast_a runs to assign the correct forecast
    horizon for the input OFS, ensure the forecast cycle hour is valid,
    and compute the end date based on the forecast length.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - ofs : str, OFS identifier (e.g., 'cbofs', 'ngofs2')
        - start_date_full : str, Start date in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        - forecast_hr : str, Requested forecast hour with 'z' suffix (e.g., '00z', '06z')
    logger : logging.Logger
        Logger instance for tracking adjustments

    Returns
    -------
    fcst_start : str
        Adjusted forecast start date/time in ISO format
    fcst_end : str
        Computed forecast end date/time in ISO format

    If the requested forecast hour doesn't match a valid cycle for the OFS,
    the function automatically adjusts to the nearest valid cycle hour.

    Examples
    --------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> start, end = get_fcst_dates(prop, logger)
    >>> print(f"Start: {start}")
    Start: 2025-07-15T06:00:00Z
    >>> print(f"End: {end}")
    End: 2025-07-17T06:00:00Z

    See Also
    --------
    get_fcst_hours : Get list of forecast hours for a run
    """
    logger.info('Starting cycle and end date assignment for forecast_a...')

    # Check if S3 fallback is enabled
    try:
        conf_settings = utils.Utils().read_config_section('settings', logger)
        use_s3_fallback = conf_settings.get('use_s3_fallback', 'False').lower() in ('true', '1', 'yes')
    except (KeyError, FileNotFoundError):
        use_s3_fallback = False

    # Define forecast cycle hours for each OFS group
    fcstlength, fcstcycles = get_fcst_hours(prop.ofs)

    # Convert forecast cycle ints to str
    fcstcycles_str = [f'{item:02}' for item in fcstcycles]

    # Verify forecast hour input
    if prop.forecast_hr[-1:].lower() == 'z' or not use_s3_fallback:
        try:
            sdate = prop.start_date_full.split('T')[0]  # Extract date part
        except AttributeError:
            if not use_s3_fallback and prop.forecast_hr == 'now':
                logger.error('Please enable S3 fallback in the ofs_dps.conf '
                             'file to run forecast_a in "now" mode!')
            else:
                logger.error('If running forecast_a with a specific cycle, '
                             'you must specify a start date. Try again!')
            raise SystemExit(1)
        try:
            int(prop.forecast_hr[:-1])
        except ValueError:
            logger.warning('Cannot run forecast_a in "now" mode without using '
                           'S3 fallback enabled! Changing forecast cycle to '
                           '00Z, or nearest cycle to 00Z...')
            prop.forecast_hr = '00z' # Set this as temporary default, then autotune below

        # Verify forecast hour input and adjust if necessary
        requested_hour = prop.forecast_hr[:-1]  # Remove 'z' suffix

        ftime = f'T{prop.forecast_hr[:-1]}:00:00Z'
        sdate = sdate + ftime
        sdatetime = datetime.strptime(sdate, '%Y-%m-%dT%H:%M:%SZ')
        if requested_hour not in fcstcycles_str:
            if fcstcycles[0] == 0:
                fcstcycles = np.append(fcstcycles, 24)
                fcstcycles_str.append('00')
            elif fcstcycles[0] == 3 and len(fcstcycles) > 1:
                fcstcycles = np.concatenate(([-3], fcstcycles))
                fcstcycles_str.insert(0, '21')
            # Find nearest valid cycle hour
            requested_hour_int = int(requested_hour)
            dist = np.array([item - requested_hour_int for item in fcstcycles])
            # Adjust start date to match forecast cycle
            sdatetime = sdatetime + \
                timedelta(hours=int(dist[np.nanargmin(np.abs(dist))]))
            # Display warning
            prop.forecast_hr = sdatetime.strftime('%H')
            logger.warning(
                f'Adjusted input forecast cycle hour from {requested_hour} to '
                f'{prop.forecast_hr} for {prop.ofs}')
        fcst_start = datetime.strftime(sdatetime, '%Y-%m-%dT%H:%M:%SZ')

    elif 'now' in prop.forecast_hr and use_s3_fallback:
        # Get most recent forecast cycle
        url_params = utils.Utils().read_config_section('urls', logger)
        # Select appropriate S3 bucket URL based on OFS
        url_root = url_params[get_s3_bucket(prop.ofs)]
        bucket_name = url_root.split('//')[1].split('.')[0]
        fcst_start = get_most_recent_file_date(bucket_name,prop.ofs,logger)

    # Calculate end date based on forecast length
    try:
        edate = datetime.strptime(fcst_start,'%Y-%m-%dT%H:%M:%SZ') + \
            timedelta(hours=fcstlength)
    except TypeError:
        logger.error('No start date found for most recent forecast model '
                     'output! Please enter a start date in the command line '
                     'interface, and try again.')
        raise SystemExit(1)
    fcst_end = datetime.strftime(edate, '%Y-%m-%dT%H:%M:%SZ')

    logger.info(f'Forecast cycle: {prop.forecast_hr}')
    logger.info(f'Forecast length: {fcstlength} hours')
    logger.info(f'Forecast period: {fcst_start} to {fcst_end}')
    logger.info('Completed cycle and end date assignment for forecast_a!')

    return fcst_start, fcst_end
