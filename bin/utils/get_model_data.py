"""
Created on Tue Apr  8 10:05:25 2025

@author: PL
"""
from __future__ import annotations

import argparse
import logging.config
import os
import socket
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse

import numpy as np

from ofs_skill.model_processing import get_fcst_cycle, model_properties
from ofs_skill.model_processing.list_of_files import (
    get_nodd_prefix_map,
    swap_path_prefix,
)
from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.utils import get_parallel_config

TIMEOUT_SEC = 60  # default API timeout in seconds
socket.setdefaulttimeout(TIMEOUT_SEC)

# STOFS-3D writes one points (station) file per cycle containing both the
# nowcast and forecast periods.
STOFS_3D_POINTS_FILENAME = 'points.cwl.temp.salt.vel.nc'


def parameter_validation(argu_list, logger):
    """ Parameter validation """

    start_date, end_date, path, ofs, whichcast, filetype, ofs_extents_path = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(argu_list[4]),
        str(argu_list[5]),
        str(argu_list[6]),
    )

    # start_date and end_date validation
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Please check Start Date -
        {start_date}, End Date - '{end_date}'. Abort!"""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if start_dt > end_dt:
        error_message = f"""End Date {end_date} is before Start Date
        {start_date}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # path validation
    if not os.path.exists(ofs_extents_path):
        error_message = f"""ofs_extents/ folder is not found. Please
        check path - {path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # ofs validation
    if not os.path.isfile(f'{ofs_extents_path}/{ofs}.shp'):
        error_message = f"""Shapefile {ofs}.shp is not found at the
        folder {ofs_extents_path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)
    if ofs == 'stofs_2d_glo':
        logger.warning('IMPORTANT NOTE: STOFS-2D-Global currently uses a '
                       'copy of the GOMOFS extent file for testing purposes. '
                       'This may cause issues with some workflows!')

    # filetype validation
    if filetype not in ['stations', 'fields']:
        error_message = f'Filetype should be fields or stations: {filetype}!'
        logger.error(error_message)
        sys.exit(-1)

    # whichcast validation
    if (
        'nowcast' not in whichcast and
        'forecast_b' not in whichcast and
        'forecast_a' not in whichcast and
        'all' not in whichcast
    ):
        error_message = f'Incorrect whichcast: {whichcast}! Exiting.'
        logger.error(error_message)
        sys.exit(-1)
    if len([whichcast]) > 1:
        logger.error('There is >1 whichcast! Program takes 1 whichcast only.')
        sys.exit(-1)


def get_ofs_cycle(prop, logger):
    '''
    Returns strings of the model cycles, forecast horizons, and forecast/
    nowcast hours for a given OFS. These are used to build file names for URL
    retrieval.
    '''
    logger.info('Starting OFS cycle info retrieval...')

    hrstrings = None

    # Need to know forecast cycle hours (e.g. 00Z) and forecast length (hours)
    # Define forecast cycle hours for each OFS group
    fcstlength, fcstcycles = get_fcst_cycle.get_fcst_hours(prop.ofs)

    # Forecast cycles from int to str
    fcstcycles = [f'{item:02}' for item in fcstcycles]

    # Get hour strings & field file time step (dt)
    if prop.ofs in (
        'cbofs', 'ciofs', 'creofs', 'dbofs', 'sfbofs', 'tbofs',
        'leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs', 'sscofs', 'secofs'
    ):
        d_t = 1
    elif prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
        d_t = 12
    else:
        d_t = 3
    if prop.whichcast == 'forecast_a':
        # Select one forecast cycle for forecast_a
        if prop.forecast_hr[:-1] in fcstcycles:
            fcstcycles = [str(prop.forecast_hr[:-1]).zfill(2)]
        else:
            logger.error(
                f'Model cycle incorrect for forecast_a and {prop.ofs}!',
            )
            sys.exit(-1)
        hrstrings = np.linspace(d_t, fcstlength, int(fcstlength/d_t)).\
            astype(int).astype(str)
    elif prop.whichcast in ['nowcast', 'forecast_b', 'hindcast']:
        hrstrings = np.linspace(
            d_t, int(24/len(fcstcycles)),
            int(24/len(fcstcycles)/d_t),
        ).astype(int).astype(str)
    return fcstcycles, hrstrings

# pylint: disable-next=too-many-arguments
def dates_range(start_date, end_date, ofs, whichcast, logger, *,
                ofsfiletype=None):
    """
    This function uses the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates = []

    # For WCOFS nowcast, we need to look an extra day ahead
    if ofs == 'wcofs' and whichcast == 'nowcast':
        offset = 2
    # Same for STOFS-3D stations nowcast: the t12z points file covers the
    # preceding 24 hours, so the post-12Z hours of the window's last day
    # live in the NEXT day's file. The read side already looks a day ahead
    # (list_of_files.dates_range); mirror it here so pre-provisioned runs
    # (use_s3_fallback=False) have the file the reader asks for.
    elif (ofs in ('stofs_3d_atl', 'stofs_3d_pac')
          and whichcast == 'nowcast' and ofsfiletype == 'stations'):
        offset = 2
    else:
        offset = 1

    # Subtract a day off the start date to make sure we have a complete time
    # series
    try:
        startdt = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ') -\
                    timedelta(days=1)
        enddt = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        logger.error('Wrong datetime format in get_model_data.dates_range! '
                     'Trying again...')
        try:
            startdt = datetime.strptime(start_date, '%Y%m%d-%H:%M:%S') -\
                        timedelta(days=1)
            enddt = datetime.strptime(end_date, '%Y%m%d-%H:%M:%S')
            logger.info('Datetime format corrected. Continuing...')
        except ValueError:
            logger.error('Cannot convert %s to datetime object!',
                         start_date)

    for i in range(
        int((enddt - startdt).days) + offset,
    ):
        date = startdt + timedelta(days=i)
        dates.append(date.strftime('%m/%d/%y'))

    return dates


def list_of_dir(prop, basepath, logger):
    """
    This function takes the output of dates_range, which is a
    list of dates, and creates a list of directories where model output is
    stored.
    """

    dir_list = []
    ofsfiletype = getattr(prop, 'ofsfiletype', None)
    if prop.whichcast != 'forecast_a':
        dates = dates_range(prop.start_date_full, prop.end_date_full, prop.ofs,
                            prop.whichcast, logger, ofsfiletype=ofsfiletype)
    else:
        dates = dates_range(prop.start_date_full, prop.start_date_full,
                            prop.ofs, prop.whichcast, logger,
                            ofsfiletype=ofsfiletype)
    dates_len = len(dates)
    # After 12/31/24, directory structure changes! Now we need to sort
    # a dir list that might have two different formats.
    datethreshold = datetime.strptime('12/31/24', '%m/%d/%y')
    logger.info(f'Starting list of directories for {basepath}')
    ####
    for date_index in range(0, dates_len):
        year = datetime.strptime(dates[date_index], '%m/%d/%y').year
        month = datetime.strptime(dates[date_index], '%m/%d/%y').month
        # Add stofs directory structure
        if prop.ofs in ['stofs_3d_atl', 'stofs_2d_glo', 'stofs_3d_pac']:
            day = datetime.strptime(dates[date_index], '%m/%d/%y').day
            model_dir = f'{basepath}{prop.model_path}/{prop.ofs}.{year}' +\
                        f'{month:02}{day:02}'
        else:
            # Do old directory structure
            if (
                datetime.strptime(dates[date_index], '%m/%d/%y') <=
                datethreshold
            ):
                model_dir = f'{basepath}/{year}{month:02}'
            # Do new directory structure
            elif (
                datetime.strptime(dates[date_index], '%m/%d/%y') >
                datethreshold
            ):
                day = datetime.strptime(dates[date_index], '%m/%d/%y').day
                model_dir = f'{basepath}/{year}/{month:02}/{day:02}'
            # Whoops! I'm out
            else:
                logger.error("Check the date -- can't find model output dir!")
                sys.exit(-1)
        model_dir = Path(model_dir).as_posix()
        # if model_dir not in dir_list:
        dir_list.append(model_dir)
        logger.info('Found model output dir: %s', model_dir)
    return dir_list, dates


def create_directories(dir_list, logger):
    """Creates directories from a list of directory names."""
    for dir_name in dir_list:
        try:
            os.makedirs(dir_name)
            logger.info(f"Directory '{dir_name}' created successfully.")
        except FileExistsError:
            logger.info(f"Directory '{dir_name}' already exists.")
        except Exception as e_x:
            logger.error(f"Error creating directory '{dir_name}': {e_x}")
            sys.exit(-1)


def make_file_list(prop, dates, dir_list, logger):
    '''
    Returns a list of file paths to download. File list is different
    depending on file type (fields vs stations), OFS, and whichcast (nowcast,
    forecast_a, forecast_b)
    '''
    # First get cycle & forecast horizon info for the OFS
    fcstcycles, hrstrings = get_ofs_cycle(prop, logger)

    # Date that file names change on the NODD
    datechange = datetime.strptime('09/01/2024', '%m/%d/%Y')

    # Set up empty variable to append to
    file_list = []

    # Logic order:
    # forecast_a or forecast_b
    #     fields
    #         stofs_3d_atl or stofs_3d_pac
    #         stof_2d_glo
    #         other OFSs
    #     stations
    #         stofs_2d_glo
    #         other OFSs
    # nowcast
    #     fields
    #         stofs_3d_atl or stofs_3d_pac
    #         stof_2d_glo
    #         other OFSs
    #     stations
    #         stofs_2d_glo
    #         other OFSs
    # whichcast == all (not ready yet)
    # Note no "hindcast" option.
    # TODO: delete the above comment once merged. It's just here to note the change.
    if prop.whichcast in ['forecast_b', 'forecast_a']:
        if prop.ofsfiletype == 'fields':
            if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        for hrstring in hrstrings:
                            hrstring = hrstring.zfill(3)
                            hrstring0 = str(int(hrstring)-11).zfill(3)
                            #skipping field2d files
                            '''
                            file_name = f'{prop.ofs}.t{cycle}z.field2d_' + \
                                f'f{hrstring0}_{hrstring}.nc'
                            file_name = os.path.join(dir_list[i], file_name). \
                                replace('\\', '/')
                            file_list.append(file_name)
                            '''
                            for var_name in {'out2d','horizontalVelX', 'horizontalVelY' ,
                                            'salinity','temperature','zCoordinates'}:
                                file_name = f'{prop.ofs}.t{cycle}z.fields.' + \
                                    f'{var_name}_f{hrstring0}_{hrstring}.nc'
                                file_name = os.path.join(dir_list[i], file_name). \
                                    replace('\\', '/')
                                file_list.append(file_name)
            elif prop.ofs == 'stofs_2d_glo':
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        # For now we're just doing the combined water level ("cwl").
                        # Warnings and errors elsewhere should handle requests for
                        # other variables.
                        file_name = f'{prop.ofs}.t{cycle}z.fields.cwl.nc'
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            else:
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        for hrstring in hrstrings:
                            hrstring = hrstring.zfill(3)
                            if datetime.strptime(datei, '%Y%m%d') < datechange:
                                file_name = f'nos.{prop.ofs}.fields.f{hrstring}.' \
                                    f'{datei}.t{cycle}z.nc'
                                file_name = os.path.join(dir_list[i], file_name). \
                                    replace('\\', '/')
                                file_list.append(file_name)
                            elif datetime.strptime(datei, '%Y%m%d') >= datechange:
                                file_name = f'{prop.ofs}.t{cycle}z.{datei}.' +\
                                    f'fields.f{hrstring}.nc'
                                file_name = os.path.join(dir_list[i], file_name). \
                                    replace('\\', '/')
                                file_list.append(file_name)

        elif prop.ofsfiletype == 'stations':
            if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        file_name = (
                            f'{prop.ofs}.t{cycle}z.{STOFS_3D_POINTS_FILENAME}'
                        )
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            elif prop.ofs == 'stofs_2d_glo':
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        # For now we're just doing the combined water level ("cwl").
                        # Note that this is bias corrected for station files.
                        file_name = f'{prop.ofs}.t{cycle}z.points.cwl.nc'
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            else:
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        if datetime.strptime(datei, '%Y%m%d') < datechange:
                            file_name = f'nos.{prop.ofs}.stations.forecast.' \
                                f'{datei}.t{cycle}z.nc'
                            file_name = os.path.join(dir_list[i], file_name). \
                                replace('\\', '/')
                            file_list.append(file_name)
                        elif datetime.strptime(datei, '%Y%m%d') >= datechange:
                            file_name = f'{prop.ofs}.t{cycle}z.{datei}.stations.' \
                                f'forecast.nc'
                            file_name = os.path.join(dir_list[i], file_name). \
                                replace('\\', '/')
                            file_list.append(file_name)

    elif prop.whichcast == 'nowcast':
        if prop.ofsfiletype == 'fields':
            if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        for hrstring in {'12', '24'}:
                            hrstring = hrstring.zfill(3)
                            hrstring0 = str(int(hrstring)-11).zfill(3)
                            #skipping field2d files
                            '''
                            file_name = f'{prop.ofs}.t{cycle}z.field2d_n' + \
                                f'{hrstring0}_{hrstring}.nc'
                            file_name = os.path.join(dir_list[i], file_name). \
                                replace('\\', '/')
                            file_list.append(file_name)
                            '''
                            for var_name in {'out2d',
                                'horizontalVelX', 'horizontalVelY',
                                'salinity', 'temperature', 'zCoordinates',
                            }:
                                file_name = f'{prop.ofs}.t{cycle}z.fields.' + \
                                    f'{var_name}_n{hrstring0}_{hrstring}.nc'
                                file_name = os.path.join(dir_list[i], file_name). \
                                    replace('\\', '/')
                                file_list.append(file_name)
            elif prop.ofs == 'stofs_2d_glo':
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        # For now we're just doing the combined water level ("cwl").
                        # Warnings and errors elsewhere should handle requests for
                        # other variables.
                        file_name = f'{prop.ofs}.t{cycle}z.fields.cwl.nc'
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            else:
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        for hrstring in hrstrings:
                            hrstring = hrstring.zfill(3)
                            if datetime.strptime(datei, '%Y%m%d') < datechange:
                                file_name = f'nos.{prop.ofs}.fields.n{hrstring}.' \
                                    f'{datei}.t{cycle}z.nc'
                                file_name = os.path.join(dir_list[i], file_name)
                                file_list.append(file_name)
                            elif datetime.strptime(datei, '%Y%m%d') >= datechange:
                                file_name = f'{prop.ofs}.t{cycle}z.{datei}.' + \
                                    f'fields.n{hrstring}.nc'
                                file_name = os.path.join(dir_list[i], file_name). \
                                    replace('\\', '/')
                                file_list.append(file_name)

        elif prop.ofsfiletype == 'stations':
            if prop.ofs in ('stofs_3d_atl', 'stofs_3d_pac'):
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        file_name = (
                            f'{prop.ofs}.t{cycle}z.{STOFS_3D_POINTS_FILENAME}'
                        )
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            elif prop.ofs == 'stofs_2d_glo':
                for i, datei in enumerate(dates):
                    for cycle in fcstcycles:
                        # For now we're just doing the combined water level ("cwl").
                        # Note that this is bias corrected for station files.
                        file_name = f'{prop.ofs}.t{cycle}z.points.cwl.nc'
                        file_list.append(
                            os.path.join(dir_list[i], file_name).replace('\\', '/')
                        )
            else:
                for i, datei in enumerate(dates):
                    datei = datetime.strptime(datei, '%m/%d/%y').strftime('%Y%m%d')
                    for cycle in fcstcycles:
                        if datetime.strptime(datei, '%Y%m%d') < datechange:
                            file_name = f'nos.{prop.ofs}.stations.nowcast.' \
                                f'{datei}.t{cycle}z.nc'
                            file_name = os.path.join(dir_list[i], file_name)
                            file_list.append(file_name)
                        elif datetime.strptime(datei, '%Y%m%d') >= datechange:
                            file_name = f'{prop.ofs}.t{cycle}z.{datei}.stations.' \
                                f'nowcast.nc'
                            file_name = os.path.join(dir_list[i], file_name). \
                                replace('\\', '/')
                            file_list.append(file_name)

    elif prop.whichcast == 'all':
        logger.error('This option is not ready yet!')
        sys.exit(-1)
    else:
        logger.error('Whichcast %s does not work in get_model_data!',
                     prop.whichcast)
        raise Exception

    logger.info('Created list of files for downloading. Here they are:')
    for j in file_list:
        logger.info(j)

    return file_list


def list_of_urls(file_list, prop, logger):
    """
    This function take a file list and builds the URLs for all downloads.
    """

    # Retrieve urls from config file
    _conf = getattr(prop, 'config_file', None)
    url_params = utils.Utils(_conf).read_config_section('urls', logger)
    local_prefix, bucket_prefix = get_nodd_prefix_map(prop, logger)
    url_root = url_params[get_fcst_cycle.get_s3_bucket(prop.ofs)]
    logger.info(f'Starting URL building for {url_root}...')
    # Swap each file's local {ofs}/{netcdf_dir}/ prefix (anchored at the
    # start of the path) for the bucket prefix. This is a no-op for
    # non-STOFS OFS, whose buckets match the local layout.
    url_list = [
        f'{url_root}{swap_path_prefix(file, local_prefix, bucket_prefix)}'
        for file in file_list
    ]
    logger.info('Completed URL building!')
    return url_list


def _url_to_local_path(url, savepath, prefix_map):
    """
    Map a NODD URL to the local path where the file should be saved.

    Only the bucket key (the part of the URL after the bucket host) is
    translated from the bucket layout back to the local layout, anchored
    at the start of the key. The savepath itself is never rewritten, even
    if it happens to contain a bucket prefix such as 'STOFS-3D-Atl/'.

    Parameters
    ----------
    url : str
        Full URL of the file to download.
    savepath : str
        Local base path for saving downloaded files.
    prefix_map : tuple of str
        (local_prefix, bucket_prefix) pair from get_nodd_prefix_map.

    Returns
    -------
    str
        The local file path.
    """
    local_prefix, bucket_prefix = prefix_map
    # urlparse().path is exactly the bucket key regardless of the endpoint
    # host (a '.com' substring split breaks for non-.com endpoints and for
    # keys that themselves contain '.com').
    key = urlparse(url).path.lstrip('/')
    # No-op for non-STOFS OFS (identical prefixes) and for stofs_2d_glo
    # (empty bucket prefix; savepath already carries the local prefix).
    key = swap_path_prefix(key, bucket_prefix, local_prefix)
    return f'{savepath}/{key}'.replace('//', '/')


def _retrieve_atomic(url, local_path):
    """
    Download url to local_path via a temporary '.part' name.

    urllib.request.urlretrieve writes directly to its destination, so an
    interrupted transfer would leave a truncated file at the final path
    that later runs treat as complete (same defect class as issues
    #176/#193, fixed for the S3 cache path in intake_scisa.py). Download
    to '<name>.part' and promote with os.replace() only after the
    transfer finishes; on failure the partial '.part' file is removed.
    """
    part_path = f'{local_path}.part'
    try:
        urllib.request.urlretrieve(url, part_path)
        os.replace(part_path, local_path)
    except BaseException:
        try:
            os.remove(part_path)
        except OSError:
            pass
        raise


def _download_single_file(mod_dat, savepath, logger, prefix_map=('', '')):
    """
    Download a single model output file from the NODD.

    Handles stofs vs non-stofs path construction, skips files that already
    exist locally, and retries up to 3 times on HTTP 503 errors with
    exponential backoff.

    Parameters
    ----------
    mod_dat : str
        Full URL of the file to download.
    savepath : str
        Local base path for saving downloaded files.
    logger : logging.Logger
        Logger instance.
    prefix_map : tuple of str
        (local_prefix, bucket_prefix) pair from get_nodd_prefix_map, used
        to translate the bucket key back to the local directory layout
        (e.g. 'STOFS-3D-Atl/...' -> 'stofs_3d_atl/netcdf/...').

    Returns
    -------
    str or None
        The local file path on success, None on failure.
    """
    max_retries = 3
    backoff_seconds = 1

    try:
        local_path = _url_to_local_path(mod_dat, savepath, prefix_map)

        # Remove any stale partial file left by an interrupted run
        part_path = f'{local_path}.part'
        if os.path.isfile(part_path):
            logger.warning('Removing stale partial download: %s', part_path)
            os.remove(part_path)

        # Skip if file already exists. Safe because downloads are promoted
        # to the final name only after they finish (_retrieve_atomic), so
        # a truncated transfer can never land at the final path.
        if os.path.isfile(local_path):
            logger.info('File already exists, skipping: %s', local_path)
            return local_path

        logger.info('Downloading model data: %s', mod_dat)
        url = mod_dat.replace('\\', '/')

        # Retry loop for transient HTTP errors
        for attempt in range(max_retries):
            try:
                _retrieve_atomic(url, local_path)
                return local_path
            except HTTPError as e:
                if e.code == 503 and attempt < max_retries - 1:
                    wait = backoff_seconds * (2 ** attempt)
                    logger.warning(
                        'HTTP 503 for %s, retrying in %ds (attempt %d/%d)',
                        mod_dat, wait, attempt + 1, max_retries,
                    )
                    time.sleep(wait)
                else:
                    raise

    except Exception as ex:
        logger.error('Error: %s. Download failed %s!', ex, mod_dat)
    # Reached only on failure (the retry loop either returns or raises)
    return None


def _derive_savepath(dir_list, local_prefix):
    """
    Derive the local base path (everything above {ofs}/{netcdf_dir}/)
    from the first save directory.

    Splitting on the first occurrence of the OFS name anywhere in the
    string breaks for working directories that happen to contain the OFS
    name (e.g. /home/user/cbofs_runs/). Instead, locate the trailing,
    path-anchored {ofs}/{netcdf_dir}/ layout component from
    get_nodd_prefix_map and keep everything before it.

    Parameters
    ----------
    dir_list : list of str
        Save directories ({base}/{ofs}/{netcdf_dir}/{date_dir} layout).
    local_prefix : str
        The '{ofs}/{netcdf_dir}/' local prefix from get_nodd_prefix_map.

    Returns
    -------
    str
        The base path, with a trailing '/' when non-empty.
    """
    first_dir = f'{Path(dir_list[0]).as_posix()}/'
    if first_dir.startswith(local_prefix):
        # Relative layout rooted directly at {ofs}/{netcdf_dir}/
        return ''
    anchor = f'/{local_prefix}'
    idx = first_dir.rfind(anchor)
    if idx < 0:
        raise ValueError(
            f"Cannot find layout component '{local_prefix}' in save "
            f"directory '{dir_list[0]}'."
        )
    return first_dir[:idx + 1]


def download_data(prop, list_of_urls1, dir_list, logger):
    """
    This function gets the model output files from the NODD using the list
    of URLs.
    """
    # Prefixes for translating bucket paths back to the local layout
    prefix_map = get_nodd_prefix_map(prop, logger)
    # Set up save path: strip the {ofs}/{netcdf_dir}/{date_dir} layout
    # from the first save directory, anchored at the trailing path
    # component (not the first occurrence of the OFS name, which may also
    # appear in the working directory path).
    savepath = _derive_savepath(dir_list, prefix_map[0])
    # Need to add the local prefix to the savepath for stofs_2d_glo files
    # because the NODD S3 bucket doesn't contain it (unlike other models).
    if prop.ofs == 'stofs_2d_glo':
        savepath = savepath + prefix_map[0]
    # First try the NODD and see if it's responding
    try:
        logger.info('Try NODD S3 download...')
        _retrieve_atomic(
            list_of_urls1[0].replace('\\', '/'),
            _url_to_local_path(list_of_urls1[0], savepath, prefix_map),
        )
        logger.info('NODD is responding! Keep going -->')
        list_of_urls_main = list_of_urls1
    except (ValueError, HTTPError, Exception) as e_x:
        logger.info("NODD S3 is not responding! I'm out.")
        logger.error('First download failed for URL: %s',
                     list_of_urls1[0].replace('\\', '/'))
        logger.error(f'Exception: {e_x}')
        sys.exit(-1)
        # list_of_urls = list_of_urls2

    # Download remaining files in parallel
    parallel_config = get_parallel_config(
        logger, config_file=getattr(prop, 'config_file', None))
    max_workers = parallel_config['model_download_workers']

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _download_single_file, mod_dat, savepath, logger, prefix_map,
            ): mod_dat
            for mod_dat in list_of_urls_main
        }
        for future in as_completed(futures):
            future.result()  # raise exceptions from worker if any


def get_model_data(prop, logger):
    """
    Main function that calls all of the subroutines. Program gets OFS model
    output from the NODD by building file lists and URLs for a given OFS,
    file type, and whichcast.
    """
    # Specify defaults (can be overridden with command line options)
    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent / log_config_file
        ).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)
    logger.info('--- Starting the program ---')

    #Parameter validation
    _conf = getattr(prop, 'config_file', None)
    dir_params = utils.Utils(_conf).read_config_section('directories', logger)

    if dir_params['home'] == '/path/to/sa_homedir/':
        logger.error('HOMEDIR NOT SET! Copy conf/ofs_dps.conf.example to '
                     'conf/ofs_dps.conf and set home= to your working directory.')
        logger.error(
            'Program will sleep for 10 seconds to allow user to kill it '
            '(ctl-c).',)
        time.sleep(10)
        logger.info(
            'Setting homedir to user input -p argument. Model data will be '
            'downloaded to %sexample_data/', prop.path,
        )
        time.sleep(3)
        dir_params['home'] = Path(prop.path)
        dir_params['model_historical_dir'] = Path(
            os.path.join(prop.path, 'example_data'),
        )

    ofs_extents_path = utils.resolve_asset_path(prop.path, dir_params['ofs_extents_dir'])
    argu_list = (
        prop.start_date_full,
        prop.end_date_full,
        prop.path,
        prop.ofs,
        prop.whichcast,
        prop.ofsfiletype,
        ofs_extents_path,
    )
    parameter_validation(argu_list, logger)
    logger.info('Parameter validation complete!')

    # Directory & path set-up -->
    # Root path for saving files
    prop.model_save_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir'],
    )
    prop.model_save_path = Path(prop.model_save_path).as_posix()
    # Path to files on the NODD
    prop.model_nodd_path = os.path.join(
        prop.ofs, dir_params['netcdf_dir'],
    )
    prop.model_nodd_path = Path(prop.model_nodd_path).as_posix()
    logger.info('Successfully set up paths.')

    # Directories, file lists, and URLs -- oh my
    # Get directory list for NODD
    dir_list_nodd, dates = list_of_dir(prop, prop.model_nodd_path, logger)
    # Get directory list for saving
    dir_list_save, dates = list_of_dir(prop, prop.model_save_path, logger)
    # Set up directory tree locally
    create_directories(dir_list_save, logger)
    # Get list of files
    file_list = make_file_list(prop, dates, dir_list_nodd, logger)
    # Get list of URLs for NODD downloads using nodd dir list & file list
    url_list = list_of_urls(file_list, prop, logger)

    # I think we're ready now -- let's go
    logger.info(
        'Begin downloading files from the internets! <dial-up modem sounds>',
    )

    try:
        download_data(
            prop,
            url_list,
            dir_list_save,
            logger,
        )
        logger.info('Model data all downloaded.')
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed downloading files."""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    logger.info('Program completed! Party mode engage')


# Execution:
if __name__ == '__main__':

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python get_model_data.py',
        usage='%(prog)s',
        description='download model output data',
    )
    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the ofs_extents folder, you can also '
             'create your own shapefile, add it top the ofs_extents folder '
             'and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=True,
        help='Use working directory. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help='Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '
        "'2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e',
        '--EndDate_full',
        required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-t',
        '--FileType',
        required=True,
        help="OFS output file type to use: 'fields' or 'stations'",
    )
    parser.add_argument(
        '-ws',
        '--Whichcast',
        required=True,
        help="whichcast: 'Nowcast','Forecast_A','Forecast_B', all",
    )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ", )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file (default: conf/ofs_dps.conf)')

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.config_file = args.config
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcast = args.Whichcast.lower()
    prop1.ofsfiletype = args.FileType.lower()

    # Do forecast_a to assess a single forecast cycle
    if 'forecast_a' in prop1.whichcast:
        if args.Forecast_Hr is None:
            print('No forecast cycle input for forecast_a! Exiting...')
            sys.exit(-1)
        elif args.Forecast_Hr is not None:
            prop1.forecast_hr = args.Forecast_Hr

    # Enforce data retention times for fields files
    if prop1.ofsfiletype == 'fields':
        if (
            'forecast' in prop1.whichcast and
            datetime.strptime(prop1.start_date_full, '%Y-%m-%dT%H:%M:%SZ') <
            (datetime.now() - timedelta(days=60))
        ):
            ERRORMESSAGE = 'THERE ARE NO AVAILABLE FIELDS FILES FOR THE ' \
                           'DATES ENTERED. FORECAST FIELDS FILES ARE ' \
                           "RETAINED FOR 60 DAYS BEFORE TODAY'S DATE. " \
                           'ADJUST YOUR START DATE AND PLEASE TRY AGAIN.'
            print(ERRORMESSAGE)
            sys.exit(-1)

        if (
            'nowcast' in prop1.whichcast and
            datetime.strptime(prop1.start_date_full, '%Y-%m-%dT%H:%M:%SZ') <
            (datetime.now() - timedelta(days=365*2))
        ):
            ERRORMESSAGE = 'THERE ARE NO AVAILABLE FIELDS FILES FOR THE ' \
                           'DATES ENTERED. NOWCAST FIELDS FILES ARE ' \
                           "RETAINED FOR 2 YEARS BEFORE TODAY'S DATE. " \
                           'ADJUST YOUR START DATE AND PLEASE TRY AGAIN.'
            print(ERRORMESSAGE)
            sys.exit(-1)

    get_model_data(
        prop1,
        None,
    )
