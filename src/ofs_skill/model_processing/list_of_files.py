"""
Model File Listing and Discovery Module

This module provides functions for creating lists of model netCDF files to be concatenated
and processed for skill assessment. It handles both local file discovery and S3 URL
construction for remote model data access.

The main functions create date ranges, construct directory paths, and list files based on
the OFS type, forecast cycle, and temporal parameters.

Note: There are special considerations for WCOFS, which runs once per day at 03:00 UTC
and has three-hourly output for fields files. The code retrieves an extra day of output
for WCOFS to help stitch together more complete time series and avoid gaps.

Functions
---------
construct_s3_url : Converts local file paths to S3 URLs for NODD bucket access
dates_range : Creates a date range between start and end dates
construct_expected_files : Generates expected file names when files are not available locally
list_of_dir : Creates list of directories containing model netCDF files
list_of_files : Lists and sorts all files inside model directories
"""

import os
from datetime import datetime, timedelta
from logging import Logger
from os import listdir
from pathlib import Path
from typing import Any, Optional

from ofs_skill.obs_retrieval import utils


def construct_s3_url(local_path: str, prop: Any, logger: Logger) -> Optional[str]:
    """
    Convert a local file path to an S3 URL for the NODD bucket.

    Parameters
    ----------
    local_path : str
        Local file path (e.g., '../example_data/cbofs/netcdf/202411/file.nc')
    prop : ModelProperties
        ModelProperties object containing OFS name
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    str or None
        S3 URL string (e.g., 'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/202411/file.nc')
        Returns None if URL construction fails

    Examples
    --------
    >>> s3_url = construct_s3_url('../example_data/cbofs/netcdf/202411/file.nc', prop, logger)
    >>> print(s3_url)
    'https://noaa-nos-ofs-pds.s3.amazonaws.com/cbofs/netcdf/202411/file.nc'
    """
    try:
        # Get URL configuration
        url_params = utils.Utils().read_config_section('urls', logger)

        # Normalize path separators
        local_path = Path(local_path).as_posix()

        # Extract the OFS-relative path (everything after the OFS name)
        # Format: {base_path}/{ofs}/netcdf/{rest_of_path}
        path_parts = local_path.split('/')

        # Find where 'netcdf' appears in the path
        try:
            netcdf_idx = path_parts.index('netcdf')
            # Reconstruct from OFS name onwards
            ofs_relative_path = '/'.join([prop.ofs, 'netcdf'] + path_parts[netcdf_idx + 1:])
        except ValueError:
            # Fallback: try to find OFS name in path
            try:
                ofs_idx = path_parts.index(prop.ofs)
                ofs_relative_path = '/'.join(path_parts[ofs_idx:])
            except ValueError:
                logger.error(f'Cannot determine OFS-relative path from: {local_path}')
                return None

        # Select appropriate S3 bucket URL based on OFS
        if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
            url_root = url_params['nodd_s3_stofs3d']
            # STOFS uses different path structure - no 'netcdf' subdirectory
            # Bucket structure: STOFS-3D-Atl/stofs_3d_atl.YYYYMMDD/filename.nc
            ofs_relative_path = ofs_relative_path.replace('stofs_3d_atl/', 'STOFS-3D-Atl/')
            ofs_relative_path = ofs_relative_path.replace('stofs_3d_pac/', 'STOFS-3D-Pac/')
        elif prop.ofs == 'stofs_2d_global':
            url_root = url_params['nodd_s3_stofs2d']
        else:
            url_root = url_params['nodd_s3']

        # Construct full S3 URL
        s3_url = f'{url_root}{ofs_relative_path}'

        return s3_url

    except Exception as e:
        logger.error(f'Error constructing S3 URL for {local_path}: {e}')
        return None


def dates_range(start_date: str, end_date: str, ofs: str, whichcast: str) -> list[str]:
    """
    Generate a list of dates between start and end dates.

    This function creates all dates between start_date and end_date, which is useful
    when listing all folders (one per date) where model data is stored.

    Special handling for WCOFS and STOFS models:
    - WCOFS nowcast: looks an extra day ahead
    - WCOFS forecast_b: looks an extra day behind
    - STOFS forecasts: looks an extra day behind

    Parameters
    ----------
    start_date : str
        Start date in format 'YYYYMMDDHH'
    end_date : str
        End date in format 'YYYYMMDDHH'
    ofs : str
        OFS model name (e.g., 'cbofs', 'wcofs', 'stofs_3d_atl')
    whichcast : str
        Forecast type ('nowcast', 'forecast_a', 'forecast_b')

    Returns
    -------
    list of str
        List of dates in 'MM/DD/YY' format

    Examples
    --------
    >>> dates = dates_range('2024010100', '2024010300', 'cbofs', 'nowcast')
    >>> print(dates)
    ['01/01/24', '01/02/24', '01/03/24']
    """
    dates = []
    # For WCOFS nowcast, we need to look an extra day ahead for nowcast, and an
    # extra day behind for forecast_b
    if ofs == 'wcofs' or ofs == 'stofs_3d_atl' or ofs == 'stofs_3d_pac':
        if whichcast == 'forecast_b' and ofs == 'wcofs':
            offset = 2
            ddays = -1  # Look behind one day with offset
        elif whichcast != 'nowcast' and ofs != 'wcofs':
            offset = 2
            ddays = -1  # Look behind one day with offset
        elif whichcast == 'nowcast':
            offset = 2
            ddays = 0  # Look ahead one day with offset
    else:  # No looking behind or ahead
        offset = 1
        ddays = 0

    for i in range(
        int((datetime.strptime(end_date, '%Y%m%d%H')
             - datetime.strptime(start_date, '%Y%m%d%H')).days) + offset):
        date = datetime.strptime(start_date, '%Y%m%d%H') + \
            timedelta(days=(i + ddays))
        dates.append(date.strftime('%m/%d/%y'))

    return dates


def construct_expected_files(prop: Any, dir_path: str, logger: Logger) -> list[str]:
    """
    Construct expected file names for a directory when files are not available locally.

    This is used when S3 fallback is enabled to generate the file names that would
    exist if the data were downloaded.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing OFS configuration
    dir_path : str
        Directory path (e.g., '../example_data/cbofs/netcdf/2025/12/15')
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    list of str
        List of expected file paths

    Notes
    -----
    - Handles both old (pre-9/1/2024) and new file naming conventions
    - File format changed on September 1, 2024
    - Old format: nos.{ofs}.{filetype}.{cast}.{date}.t{cycle}z.nc
    - New format: {ofs}.t{cycle}z.{date}.{filetype}.{cast}.nc

    Examples
    --------
    >>> files = construct_expected_files(prop, '../cbofs/netcdf/2025/12/15', logger)
    """
    files = []
    dir_path = Path(dir_path).as_posix()
    # Extract date from directory path
    # Path format: .../netcdf/YYYY/MM/DD or .../netcdf/YYYYMM or .../netcdf/{ofs}.YYYYMMDD (STOFS)
    path_parts = dir_path.split('/')

    date_str = None
    try:
        # Try STOFS format first: {ofs}.YYYYMMDD
        if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac', 'stofs_2d_global'):
            # STOFS directory format: .../netcdf/stofs_3d_atl.20251228
            dir_name = path_parts[-1]
            if '.' in dir_name:
                ofs_part, date_part = dir_name.rsplit('.', 1)
                if len(date_part) == 8 and date_part.isdigit():
                    date_str = date_part
                else:
                    logger.error(f'Cannot parse STOFS date from directory: {dir_path}')
                    return files
            else:
                logger.error(f'Cannot parse STOFS date from directory: {dir_path}')
                return files
        # Try new format: YYYY/MM/DD
        elif len(path_parts) >= 3:
            year = path_parts[-3]
            month = path_parts[-2]
            day = path_parts[-1]
            if len(year) == 4 and len(month) == 2 and len(day) == 2:
                date_str = f'{year}{month}{day}'
            else:
                # Old format: YYYYMM
                year_month = path_parts[-1]
                if len(year_month) == 6:
                    date_str = year_month
                else:
                    logger.error(f'Cannot parse date from directory: {dir_path}')
                    return files
        else:
            logger.error(f'Invalid directory path format: {dir_path}')
            return files
    except Exception as e:
        logger.error(f'Error parsing directory path {dir_path}: {e}')
        return files

    # If date_str couldn't be parsed, return empty list
    if date_str is None:
        logger.error(f'Unable to extract date from path: {dir_path}')
        return files

    # Get forecast cycles based on OFS
    if prop.ofs in ('cbofs', 'dbofs', 'gomofs', 'ciofs', 'leofs', 'lmhofs', 'loofs',
                    'lsofs', 'tbofs', 'necofs'):
        fcstcycles = ['00', '06', '12', '18']
    elif prop.ofs in ('creofs', 'ngofs2', 'sfbofs', 'sscofs'):
        fcstcycles = ['03', '09', '15', '21']
    elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        fcstcycles = ['12']
    else:
        fcstcycles = ['03']

    # Determine file type indicator
    if prop.whichcast == 'nowcast':
        cast_type = 'nowcast'
    elif prop.whichcast in ['forecast_a', 'forecast_b']:
        cast_type = 'forecast'
    else:
        cast_type = prop.whichcast

    # Construct file names based on format (new format after 9/1/2024)
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    datechange = datetime.strptime('09/01/2024', '%m/%d/%Y')

    # Get hour strings based on OFS and whichcast
    if prop.ofs in ('cbofs', 'ciofs', 'creofs', 'dbofs', 'sfbofs', 'tbofs',
                    'leofs', 'lmhofs', 'loofs', 'lsofs', 'sscofs'):
        d_t = 1
    elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        d_t = 12
    else:
        d_t = 3

    # Get forecast length
    if prop.ofs in ('cbofs', 'ciofs', 'creofs', 'dbofs', 'ngofs2', 'sfbofs',
                    'tbofs', 'stofs_3d_pac'):
        fcstlength = 48
    elif prop.ofs in ('gomofs', 'wcofs', 'sscofs', 'necofs'):
        fcstlength = 72
    elif prop.ofs in ('stofs_3d_atl'):
        fcstlength = 96
    else:
        fcstlength = 120

    if prop.ofsfiletype == 'stations':
        for cycle in fcstcycles:

            if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
                filename = f'{prop.ofs}.t{cycle}z.points.cwl.temp.salt.vel.nc'
            else:
                if date_obj >= datechange:
                    # New format: cbofs.t00z.20251215.stations.nowcast.nc
                    filename = f'{prop.ofs}.t{cycle}z.{date_str}.stations.{cast_type}.nc'
                else:
                    # Old format: nos.cbofs.stations.nowcast.20251215.t00z.nc
                    filename = f'nos.{prop.ofs}.stations.{cast_type}.{date_str}.t{cycle}z.nc'

            filepath = f'{dir_path}//{filename}'
            files.append(filepath)

    elif prop.ofsfiletype == 'fields':
        # STOFS 3D models use different file naming pattern
        if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
            # STOFS 3D variables
            stofs_vars = ['out2d','horizontalVelX', 'horizontalVelY', 'salinity',
                          'temperature', 'zCoordinates']

            # Determine cast prefix and hour ranges
            if prop.whichcast == 'nowcast':
                cast_prefix = 'n'
                # Nowcast: 12-hour ranges up to 24 hours
                hour_ranges = [(1, 12), (13, 24)]
            else:
                cast_prefix = 'f'
                # Forecast: 12-hour ranges up to forecast length
                hour_ranges = []
                for end_hr in range(12, fcstlength + 1, 12):
                    start_hr = end_hr - 11
                    hour_ranges.append((start_hr, end_hr))

            for cycle in fcstcycles:
                for start_hr, end_hr in hour_ranges:
                    hr_range = f'{cast_prefix}{str(start_hr).zfill(3)}_{str(end_hr).zfill(3)}'
                    '''
                    # 2D field file
                    filename = f'{prop.ofs}.t{cycle}z.field2d_{hr_range}.nc'
                    filepath = f'{dir_path}//{filename}'
                    files.append(filepath)
                    '''
                    # 3D field files for each variable
                    for var_name in stofs_vars:
                        filename = f'{prop.ofs}.t{cycle}z.fields.{var_name}_{hr_range}.nc'
                        filepath = f'{dir_path}//{filename}'
                        files.append(filepath)

        else:
            # Standard OFS file naming
            if prop.whichcast == 'nowcast':
                # Nowcast uses n001, n002, etc.
                cast_prefix = 'n'
                if prop.ofs == 'wcofs':
                    max_hours = int(24/len(fcstcycles))
                else:
                    max_hours = int(24/len(fcstcycles))
                hrstrings = [str(h).zfill(3) for h in range(d_t, max_hours + 1, d_t)]
            elif prop.whichcast == 'forecast_a':
                # Forecast_a uses f001, f002, etc. up to forecast length
                cast_prefix = 'f'
                hrstrings = [str(h).zfill(3) for h in range(d_t, fcstlength + 1, d_t)]
            elif prop.whichcast == 'forecast_b':
                # Forecast_b uses f001-f006 (or f001-f024 for WCOFS)
                cast_prefix = 'f'
                if prop.ofs == 'wcofs':
                    max_hours = 24
                else:
                    max_hours = 6
                hrstrings = [str(h).zfill(3) for h in range(d_t, max_hours + 1, d_t)]
            else:
                cast_prefix = 'n'
                hrstrings = ['001']

            for cycle in fcstcycles:
                for hrstring in hrstrings:
                    if date_obj >= datechange:
                        # New format: cbofs.t00z.20251215.fields.n001.nc
                        filename = f'{prop.ofs}.t{cycle}z.{date_str}.fields.{cast_prefix}{hrstring}.nc'
                    else:
                        # Old format: nos.cbofs.fields.n001.20251215.t00z.nc
                        filename = f'nos.{prop.ofs}.fields.{cast_prefix}{hrstring}.{date_str}.t{cycle}z.nc'

                    filepath = f'{dir_path}//{filename}'
                    files.append(filepath)

    return files


def list_of_dir(prop: Any, logger: Logger) -> list[str]:
    """
    Create a list of directories containing model netCDF files.

    Takes the output of dates_range and creates a list of directories based on model_path.
    The model_path is the path to where the model data is stored.

    Handles directory structure changes:
    - Before 12/31/2024: {model_path}/{YYYYMM}
    - After 12/31/2024: {model_path}/{YYYY}/{MM}/{DD}
    - STOFS models: {model_path}/{ofs}.{YYYYMMDD}

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing model configuration
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    list of str
        List of directory paths containing model netCDF files

    Notes
    -----
    - Checks for directory existence and falls back to backup directory if configured
    - If S3 fallback is enabled, continues even if local directories don't exist
    - Logs warnings when directories are not found

    Examples
    --------
    >>> dir_list = list_of_dir(prop, logger)
    >>> print(dir_list)
    ['../cbofs/netcdf/2025/01/01', '../cbofs/netcdf/2025/01/02']
    """
    # Check if S3 fallback is enabled
    try:
        conf_settings = utils.Utils().read_config_section('settings', logger)
        use_s3_fallback = conf_settings.get('use_s3_fallback', 'False').lower() in ('true', '1', 'yes')
    except Exception:
        use_s3_fallback = False

    dir_list = []
    if prop.whichcast != 'forecast_a':
        dates = dates_range(prop.startdate, prop.enddate, prop.ofs,
                            prop.whichcast)
    else:
        dates = dates_range(prop.startdate, prop.startdate, prop.ofs,
                            prop.whichcast)
    dates_len = len(dates)

    # After 12/31/24, directory structure changes! Now we need to sort
    # a dir list that might have two different formats.
    datethreshold = datetime.strptime('12/31/24', '%m/%d/%y')
    logger.info('Starting model output directory search...')

    for date_index in range(0, dates_len):
        year = datetime.strptime(dates[date_index], '%m/%d/%y').year
        month = datetime.strptime(dates[date_index], '%m/%d/%y').month
        # Add stofs directory structure
        if prop.ofs == 'stofs_3d_atl' or prop.ofs == 'stofs_2d_global' or prop.ofs == 'stofs_3d_pac':
            day = datetime.strptime(dates[date_index], '%m/%d/%y').day
            model_dir = f'{prop.model_path}/{prop.ofs}.{year}{month:02}{day:02}'
        else:
            # Do old directory structure
            if datetime.strptime(dates[date_index], '%m/%d/%y') <= datethreshold:
                model_dir = f'{prop.model_path}/{year}{month:02}'
            # Do new directory structure
            elif datetime.strptime(dates[date_index], '%m/%d/%y') > datethreshold:
                day = datetime.strptime(dates[date_index], '%m/%d/%y').day
                model_dir = f'{prop.model_path}/{year}/{month:02}/{day:02}'
            # Whoops! I'm out
            else:
                logger.error("Check the date -- can't find model output dir!")
                raise SystemExit(-1)
        model_dir = Path(model_dir).as_posix()

        # Switch to backup directory if files are not in primary directory
        if not os.path.exists(model_dir) or not os.listdir(model_dir):
            logger.info(
                'Model data path ' + model_dir + ' not found, or is empty. ')

            # Always try backup directory first before S3 fallback
            logger.info('Trying backup dir...')
            dir_params = utils.Utils().read_config_section(
                'directories', logger)
            backup_model_path = os.path.join(
                dir_params['model_historical_dir_backup'],
                prop.ofs, dir_params['netcdf_dir'])

            # Construct backup directory path based on OFS type and date
            if prop.ofs in ('stofs_3d_atl', 'stofs_2d_global', 'stofs_3d_pac'):
                day = datetime.strptime(dates[date_index], '%m/%d/%y').day
                backup_model_dir = f'{backup_model_path}/{prop.ofs}.{year}{month:02}{day:02}'
            elif datetime.strptime(dates[date_index], '%m/%d/%y') <= datethreshold:
                backup_model_dir = f'{backup_model_path}/{year}{month:02}'
            else:
                day = datetime.strptime(dates[date_index], '%m/%d/%y').day
                backup_model_dir = f'{backup_model_path}/{year}/{month:02}/{day:02}'

            backup_model_dir = Path(backup_model_dir).as_posix()

            # Check if backup directory exists and has files
            if os.path.exists(backup_model_dir) and os.listdir(backup_model_dir):
                logger.info('Found model data in backup dir: %s', backup_model_dir)
                model_dir = backup_model_dir
            elif use_s3_fallback:
                # Backup also not found, fall back to S3
                logger.info('Backup dir not found either. S3 fallback enabled - will use expected directory path for URL construction')
            else:
                # No S3 fallback and backup not found - error out
                logger.error(
                    'Model file path ' + model_dir + ' not found, and backup '
                    + backup_model_dir + ' also not found. Abort!')
                raise SystemExit(-1)

        if model_dir not in dir_list:
            dir_list.append(model_dir)
            if os.path.exists(model_dir):
                logger.info('Found model output dir: %s', model_dir)
            else:
                logger.info('Expected model output dir (will use S3): %s', model_dir)

    return dir_list


def list_of_files(prop: Any, dir_list: list[str], logger: Logger) -> list[str]:
    """
    List and sort all model files inside specified directories.

    Takes the output of list_of_dir and lists all files inside each directory.
    Files are sorted according to their model temporal order to ensure correct
    concatenation.

    Sorting is different for nowcast vs forecast model files based on:
    1. Forecast/nowcast hour (first 3 digits of checkstr)
    2. Model run cycle (middle 2 digits)
    3. Day (last 2 digits)

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing model configuration
    dir_list : list of str
        List of directories to search for model files
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    list of str
        Sorted list of model file paths (local paths or S3 URLs)

    Raises
    ------
    SystemExit
        If no files are found and S3 fallback is not enabled

    Notes
    -----
    - Handles both old and new file naming conventions
    - Special handling for STOFS, WCOFS file patterns
    - When S3 fallback is enabled, returns S3 URLs for missing local files
    - Filters duplicate files based on cycle/hour/day combinations

    Examples
    --------
    >>> file_list = list_of_files(prop, dir_list, logger)
    >>> print(len(file_list))
    48
    """
    # Check if S3 fallback is enabled
    try:
        conf_settings = utils.Utils().read_config_section('settings', logger)
        use_s3_fallback = conf_settings.get('use_s3_fallback', 'False').lower() in ('true', '1', 'yes')
    except Exception:
        use_s3_fallback = False

    try:
        list_files = []
        dir_list_len = len(dir_list)
        for i_index in range(0, dir_list_len):

            # Check if directory exists; if not and S3 fallback enabled, construct expected file names
            if not os.path.exists(dir_list[i_index]):
                if use_s3_fallback:
                    logger.info(f'Directory not found locally, will construct expected file names: {dir_list[i_index]}')
                    # Generate expected file names for this directory
                    files = construct_expected_files(prop, dir_list[i_index], logger)
                    list_files.append(files)
                    continue  # Skip to next directory
                else:
                    logger.error(f'Directory does not exist: {dir_list[i_index]}')
                    continue

            if prop.whichcast == 'nowcast':
                # New file format:
                # cbofs.t00z.20240901.fields.n001.nc
                # Old file format:
                # nos.cbofs.fields.n001.20240901.t00z.nc

                all_files = listdir(dir_list[i_index])
                files = []
                hr_cyc_day = []
                if prop.ofs == 'wcofs':
                    ndays = 1
                else:
                    ndays = 0
                for af_name in all_files:
                    spltstr = af_name.split('.')
                    # First do old file names
                    if 'nos.' in af_name:
                        if 'fields.n' in af_name and prop.ofsfiletype == 'fields':
                            checkstr = spltstr[-4][-3:] + spltstr[-2][1:3] + \
                                spltstr[-3][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') <=
                                     datetime.strptime
                                     (prop.enddate[:-2], '%Y%m%d') +
                                     timedelta(days=ndays))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif ('stations.n' in af_name and
                              prop.ofsfiletype == 'stations'):
                            checkstr = '999' + spltstr[-2][1:3] + spltstr[-3][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') <=
                                     datetime.strptime
                                     (prop.enddate[:-2], '%Y%m%d') +
                                     timedelta(days=ndays))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)

                    # Now do new file names
                    elif 'nos.' not in af_name:
                        if 'fields.n' in af_name and prop.ofsfiletype == 'fields':
                            checkstr = spltstr[-2][-3:] + spltstr[-5][1:3] + \
                                spltstr[-4][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') <=
                                     datetime.strptime(prop.enddate[:-2], '%Y%m%d')
                                     + timedelta(days=ndays))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif ('stations.n' in af_name and
                              prop.ofsfiletype == 'stations'):
                            checkstr = '999' + spltstr[-5][1:3] + spltstr[-4][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') <=
                                     datetime.strptime(prop.enddate[:-2], '%Y%m%d')
                                     + timedelta(days=ndays))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):  # add stofs filename format
                            if 'n0' in af_name and 'fields' in af_name:  # skiping filed2d post process
                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]
                                if ((int(checkstr2) - 1 >= int(prop.startdate[-2:]))
                                    and (int(checkstr1) - 1 <= int(prop.enddate[-2:]))
                                    ):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)
                            elif prop.ofsfiletype == 'stations':
                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]
                                if ((int(checkstr2) - 1 >= int(prop.startdate[-2:]))
                                    and (int(checkstr1) - 1 <= int(prop.enddate[-2:]))
                                    ):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)

                files = [dir_list[i_index] + '//' + i for i in files]

                # Only sort if we have files
                if len(files) > 0:
                    tupfiles = tuple(zip(hr_cyc_day, files))
                    # Sort by forecast/nowcast hour, then model run cycle, then day
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
                    # Unzip, get sorted file list back
                    files = list(zip(*tupfiles))[1]
                    files = list(files)
                elif use_s3_fallback:
                    # No files found after filtering, generate expected file names
                    logger.info(f'Directory exists but no {prop.whichcast} files found. Constructing expected file names: {dir_list[i_index]}')
                    files = construct_expected_files(prop, dir_list[i_index], logger)

            elif prop.whichcast == 'forecast_a':
                # New file format:
                # cbofs.t00z.20240901.fields.f001.nc
                # Old file format:
                # nos.cbofs.fields.f001.20240901.t00z.nc

                all_files = listdir(dir_list[i_index])
                files = []
                hr_cyc_day = []
                cycle_z = prop.forecast_hr[:-2] + 'z'
                for af_name in all_files:
                    spltstr = af_name.split('.')
                    # First do old file names
                    if 'nos.' in af_name:
                        if ('fields.f' in af_name and
                            prop.ofsfiletype == 'fields' and
                            cycle_z in af_name):
                            checkstr = spltstr[-4][-3:] + spltstr[-2][1:3] + \
                                spltstr[-3][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') ==
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif ('stations.f' in af_name and
                              prop.ofsfiletype == 'stations' and
                              cycle_z in af_name):
                            checkstr = '999' + spltstr[-2][1:3] + spltstr[-3][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') ==
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)

                    # Now do new file names
                    elif 'nos.' not in af_name:
                        if ('fields.f' in af_name and
                            prop.ofsfiletype == 'fields' and
                            cycle_z in af_name):
                            checkstr = spltstr[-2][-3:] + spltstr[-5][1:3] + \
                                spltstr[-4][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') ==
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif ('stations.f' in af_name and
                              prop.ofsfiletype == 'stations' and
                              cycle_z in af_name):
                            checkstr = '999' + spltstr[-5][1:3] + spltstr[-4][-2:]
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') ==
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d'))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):   # add stofs filename format
                            if 'f0' in af_name and 'fields' in af_name:  # skiping filed2d post process
                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]

                                if (int(checkstr2) - 1 >= int(prop.startdate[-2:])):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)

                            elif prop.ofsfiletype == 'stations':
                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]

                                if (int(checkstr2) - 1 >= int(prop.startdate[-2:])):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)

                files = [dir_list[i_index] + '//' + i for i in files]

                # Only sort if we have files
                if len(files) > 0:
                    tupfiles = tuple(zip(hr_cyc_day, files))
                    # Sort by forecast/nowcast hour, then model run cycle, then day
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
                    # Unzip, get sorted file list back
                    files = list(zip(*tupfiles))[1]
                    files = list(files)
                elif use_s3_fallback:
                    # No files found after filtering, generate expected file names
                    logger.info(f'Directory exists but no {prop.whichcast} files found. Constructing expected file names: {dir_list[i_index]}')
                    files = construct_expected_files(prop, dir_list[i_index], logger)

            elif prop.whichcast == 'forecast_b':
                # New file format:
                # cbofs.t00z.20240901.fields.f001.nc
                # Old file format:
                # nos.cbofs.fields.f001.20240901.t00z.nc

                all_files = listdir(dir_list[i_index])
                files = []
                hr_cyc_day = []
                if prop.ofs == 'wcofs':
                    ndays = 1
                else:
                    ndays = 0
                for af_name in all_files:
                    spltstr = af_name.split('.')
                    # Old file names
                    if 'nos.' in af_name:
                        if 'fields.f' in af_name and prop.ofsfiletype == 'fields':
                            if 'f0' in af_name:
                                checkstr = (spltstr[-4][-3:] + spltstr[-2][1:3]
                                            + spltstr[-3][-2:])
                                if (checkstr not in hr_cyc_day
                                    and (datetime.strptime(spltstr[-3], '%Y%m%d') >=
                                         datetime.strptime
                                         (prop.startdate[:-2], '%Y%m%d') -
                                         timedelta(days=ndays))
                                    and (datetime.strptime(spltstr[-3], '%Y%m%d') <=
                                         datetime.strptime
                                         (prop.enddate[:-2], '%Y%m%d'))
                                    and checkstr[0:3] != '000'
                                    ):
                                    if (prop.ofs == 'wcofs'
                                        and int(checkstr[0:3]) >= 1
                                        and int(checkstr[0:3]) < 25):
                                        hr_cyc_day.append(checkstr)
                                        files.append(af_name)
                                    elif (int(checkstr[0:3]) >= 1
                                          and int(checkstr[0:3]) < 7):
                                        hr_cyc_day.append(checkstr)
                                        files.append(af_name)
                        elif ('stations.f' in af_name and
                              prop.ofsfiletype == 'stations'):
                            checkstr = ('999' + spltstr[-2][1:3]
                                        + spltstr[-3][-2:])
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d') -
                                     timedelta(days=ndays))
                                and (datetime.strptime(spltstr[-3], '%Y%m%d') <=
                                     datetime.strptime
                                     (prop.enddate[:-2], '%Y%m%d'))
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                    # New file names
                    elif 'nos.' not in af_name:
                        if 'fields.f' in af_name and prop.ofsfiletype == 'fields':
                            if 'f0' in af_name:
                                checkstr = (spltstr[-2][-3:] + spltstr[-5][1:3]
                                            + spltstr[-4][-2:])
                                if (checkstr not in hr_cyc_day
                                    and (datetime.strptime(spltstr[-4], '%Y%m%d') >=
                                         datetime.strptime
                                         (prop.startdate[:-2], '%Y%m%d') -
                                         timedelta(days=ndays))
                                    and (datetime.strptime(spltstr[-4], '%Y%m%d') <=
                                         datetime.strptime
                                         (prop.enddate[:-2], '%Y%m%d'))
                                    and checkstr[0:3] != '000'
                                    ):
                                    if (prop.ofs == 'wcofs'
                                        and int(checkstr[0:3]) >= 1
                                        and int(checkstr[0:3]) < 25):
                                        hr_cyc_day.append(checkstr)
                                        files.append(af_name)
                                    elif (int(checkstr[0:3]) >= 1
                                          and int(checkstr[0:3]) < 7):
                                        hr_cyc_day.append(checkstr)
                                        files.append(af_name)
                        elif ('stations.f' in af_name and
                              prop.ofsfiletype == 'stations'):
                            checkstr = ('999' + spltstr[-5][1:3]
                                        + spltstr[-4][-2:])
                            if (checkstr not in hr_cyc_day
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') >=
                                     datetime.strptime
                                     (prop.startdate[:-2], '%Y%m%d') -
                                     timedelta(days=ndays))
                                and (datetime.strptime(spltstr[-4], '%Y%m%d') <=
                                     datetime.strptime
                                     (prop.enddate[:-2], '%Y%m%d'))
                                and checkstr[0:3] != '000'
                                ):
                                hr_cyc_day.append(checkstr)
                                files.append(af_name)
                        elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):    # add stofs filename format
                            if 'f0' in af_name and 'fields' in af_name:  # skiping filed2d post process:
                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]

                                if ((int(checkstr2) - 1 >= int(prop.startdate[-2:]))
                                    and (int(checkstr1) - 1 <= int(prop.enddate[-2:]))
                                    ):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)
                            elif prop.ofsfiletype == 'stations':

                                # Split the string based on underscores and periods
                                spltstr = af_name.split('_')
                                # Extract the values
                                checkstr1 = spltstr[-2][-2:]
                                checkstr2 = spltstr[-1].split('.')[0][1:3]

                                if ((int(checkstr2) - 1 >= int(prop.startdate[-2:]))
                                    and (int(checkstr1) - 1 <= int(prop.enddate[-2:]))
                                    ):
                                    files.append(af_name)
                                    hr_cyc_day.append(checkstr1)

                files = [dir_list[i_index] + '//' + i for i in files]

                # Only sort if we have files
                if len(files) > 0:
                    tupfiles = tuple(zip(hr_cyc_day, files))
                    # Sort by forecast/nowcast hour, then model run cycle, then day
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][0:3])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-4:-2])))
                    tupfiles = tuple(sorted(tupfiles, key=lambda x: (x[0][-2:])))
                    # Unzip, get sorted file list back
                    files = list(zip(*tupfiles))[1]
                    files = list(files)
                elif use_s3_fallback:
                    # No files found after filtering, generate expected file names
                    logger.info(f'Directory exists but no {prop.whichcast} files found. Constructing expected file names: {dir_list[i_index]}')
                    files = construct_expected_files(prop, dir_list[i_index], logger)

            # Append files to master
            list_files.append(files)

        list_files = sum(list_files, [])

    except Exception as e:
        logger.error('Problem with list_of_files, check model directory')
        logger.error('Exception message: %s', e)

    # Ensure list_files is properly flattened (handle any edge cases)
    # This must happen OUTSIDE try/except so it runs even if there was an exception
    flattened = []
    for item in list_files:
        if isinstance(item, list):
            flattened.extend(item)
        else:
            flattened.append(item)
    list_files = flattened

    if list_files == []:
        logger.error('Problem in list_of_files.py; no files found! Aborting program')
        raise SystemExit()

    # Now check individual files and use S3 fallback if enabled
    if use_s3_fallback:
        logger.info('S3 fallback is enabled - checking for missing local files...')
        final_list = []
        missing_count = 0
        for file_path in list_files:
            # Normalize path for checking
            normalized_path = file_path.replace('//', '/')
            if os.path.isfile(normalized_path):
                # File exists locally, use it
                final_list.append(file_path)
            else:
                # File doesn't exist locally, construct S3 URL
                s3_url = construct_s3_url(file_path, prop, logger)
                if s3_url:
                    logger.info(f'Local file not found, using S3: {os.path.basename(file_path)}')
                    final_list.append(s3_url)
                    missing_count += 1
                else:
                    logger.error(f'Could not construct S3 URL for: {file_path}')
                    final_list.append(file_path)  # Keep original, will fail downstream

        if missing_count > 0:
            logger.info(f'Using S3 URLs for {missing_count} missing local files')
        else:
            logger.info('All model files found locally')

        return final_list
    else:
        logger.info('S3 fallback is disabled - using only local files')
        return list_files
