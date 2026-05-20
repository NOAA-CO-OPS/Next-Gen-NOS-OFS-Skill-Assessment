"""
Model File Validation Module

This module verifies that all model files needed to run the skill assessment are available
before processing begins. If files are missing, the program will exit and list the files
that are missing. Users can then run get_model_data.py in the utils directory to retrieve
those missing files from the NODD bucket.

Functions
---------
check_model_files : Main validation function that checks for required model files
check_custom_file_list : Verifies files from a provided text file (Local, S3, or HTTP/HTTPS)

Notes
-----
This module runs at the beginning of a call to create_1dplot.py (or other entry points)
to verify model file availability. It uses:
- get_model_data.py to figure out what model files SHOULD be there
- list_of_files.py to figure out what files are ACTUALLY there

Then cross-checks the two lists and reports any missing files with a fatal error.
If all files are found, the program continues.

Author: PWL
Created: Fri Aug 8 08:59:28 2025
"""

import os
import urllib.parse
import urllib.request
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from bin.utils import get_model_data
from ofs_skill.model_processing.list_of_files import list_of_dir, list_of_files
from ofs_skill.obs_retrieval import utils


def check_custom_file_list(file_list_path: str, logger: Logger) -> None:
    """
    Reads a text file containing a list of paths (local, S3 URIs, or HTTPS URLs) and
    verifies that they exist.

    Parameters
    ----------
    file_list_path : str
        Path to the text file containing a list of model file locations.
    logger : Logger
        Logger instance for logging messages.
    """
    if not os.path.exists(file_list_path):
        logger.error(f'Custom file list text file not found: {file_list_path}')
        raise SystemExit(1)

    with open(file_list_path) as f:
        files_to_check = [line.strip() for line in f if line.strip()]

    if not files_to_check:
        logger.error('The custom file list is empty.')
        raise SystemExit(1)

    s3_client = None
    missing_files = []

    for filepath in files_to_check:
        # Check native S3 paths
        if filepath.startswith('s3://'):
            if s3_client is None:
                try:
                    import boto3
                    # If dealing with public NODD buckets, you may need to configure
                    # unsigned requests: boto3.client('s3', config=Config(signature_version=UNSIGNED))
                    s3_client = boto3.client('s3')
                except ImportError:
                    logger.error('boto3 library is required to verify s3:// paths. Please install it.')
                    raise SystemExit(1)

            parsed = urllib.parse.urlparse(filepath)
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')

            try:
                s3_client.head_object(Bucket=bucket, Key=key)
            except Exception:
                # Catching general exception; typically botocore.exceptions.ClientError for 404/403
                missing_files.append(filepath)

        # Check HTTP/HTTPS paths pointing to S3 (or any web server)
        elif filepath.startswith(('http://', 'https://')):
            try:
                # Use a HEAD request to check for file existence without downloading the payload
                req = urllib.request.Request(
                    filepath,
                    headers={'User-Agent': 'Mozilla/5.0'},
                    method='HEAD'
                )
                with urllib.request.urlopen(req, timeout=10) as response:
                    if response.status >= 400:
                        missing_files.append(filepath)
            except (HTTPError, URLError):
                # HTTPError handles 403 Forbidden, 404 Not Found, etc. URLError handles connection failures.
                missing_files.append(filepath)
            except Exception:
                missing_files.append(filepath)

        # Check local paths
        else:
            if not os.path.exists(filepath):
                missing_files.append(filepath)

    if missing_files:
        logger.error('The following custom files are missing:\n{}'.format('\n'.join(missing_files)))
        raise SystemExit(1)
    else:
        logger.info('All custom files from the provided list were verified successfully.')



def check_model_files(prop: Any, logger: Logger) -> None:
    """
    Verify that all necessary model files are present for skill assessment.

    This function compares the expected model files (from get_model_data) with the
    actual files found in the directories (from list_of_files). Any missing files
    trigger a fatal error with a list of missing files.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - whichcasts : list of str
            List of forecast types to check ('nowcast', 'forecast_a', 'forecast_b')
        - ofs : str
            OFS model name (e.g., 'cbofs', 'ngofs2')
        - start_date_full : str
            Start date in format 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYYMMDDHH'
        - end_date_full : str
            End date in format 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYYMMDDHH'
        - ofsfiletype : str
            Type of file ('stations' or 'fields')
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    None
        Function exits program if files are missing, otherwise returns silently

    Raises
    ------
    SystemExit
        If model files are missing or if there are errors in date formatting
    """

    # Check if custom filename input is enabled
    _conf = getattr(prop, 'config_file', None)
    try:
        conf_settings = utils.Utils(_conf).read_config_section('settings', logger)
        use_custom_files = conf_settings.get('use_custom_filenames', 'False').lower() in ('true', '1', 'yes')
        # Retrieve the path to the text file list from config (adjust key as needed)
        custom_file_list_path = conf_settings.get('filename_path', '')
    except Exception:
        use_custom_files = False
        custom_file_list_path = ''

    if use_custom_files:
        logger.info('Custom model file input is enabled! Checking to see if '
                    'files are available...')
        if custom_file_list_path:
            check_custom_file_list(custom_file_list_path, logger)
        else:
            logger.warning('use_custom_filenames is enabled, but no path '
                           'was found in the configuration!')
        return

    # This first chunk handles the main skill assessment
    for cast in prop.whichcasts:
        prop.whichcast = cast
        # Directory params
        _conf = getattr(prop, 'config_file', None)
        dir_params = utils.Utils(_conf).read_config_section('directories', logger)
        prop.model_save_path = os.path.join(dir_params['model_historical_dir'],
                                            prop.ofs, dir_params['netcdf_dir'])

        # First make list of what files SHOULD be in the directories
        try:
            dir_list, dates = get_model_data.list_of_dir(prop,
                                                         prop.model_save_path,
                                                         logger)
        except Exception as e_x:
            logger.error('Error in get_model_data: %s! '
                         'Unable to check if model files are present.', e_x)
            return

        dates = dates[1:]  # Chop off extra first date, not needed here
        dir_list = dir_list[1:]  # Chop off extra first dir, not needed here
        file_path_list = get_model_data.make_file_list(prop, dates, dir_list,
                                                        logger)
        file_wish = []
        file_wish.append([i.split('/')[-1] for i in file_path_list])

        # Now see what is actually available. Need to reformat dates before
        # using list_of_files.py
        startdatesave = prop.start_date_full
        enddatesave = prop.end_date_full
        if 'T' in prop.start_date_full and 'Z' in prop.start_date_full:
            prop.start_date_full = prop.start_date_full.replace('-', '')
            prop.end_date_full = prop.end_date_full.replace('-', '')
            prop.start_date_full = prop.start_date_full.replace('Z', '')
            prop.end_date_full = prop.end_date_full.replace('Z', '')
            prop.start_date_full = prop.start_date_full.replace('T', '-')
            prop.end_date_full = prop.end_date_full.replace('T', '-')
        try:
            prop.startdate = (datetime.strptime(
                prop.start_date_full.split('-')[0], '%Y%m%d')).strftime(
                '%Y%m%d') + '00'
            prop.enddate = (datetime.strptime(
                prop.end_date_full.split('-')[0], '%Y%m%d')).strftime(
                '%Y%m%d') + '23'
        except ValueError as e_x:
            logger.error(f'Date format problem in check_model_files: {e_x}')
            logger.error('Unable to check if model files are present.')
            return

        prop.model_path = os.path.join(dir_params['model_historical_dir'],
                                       prop.ofs, dir_params['netcdf_dir'])
        prop.model_path = Path(prop.model_path).as_posix()
        dir_list = list_of_dir(prop, logger)
        try:
            file_actual_path_list = list_of_files(prop, dir_list, logger)
        except Exception as e_x:
            logger.error('Error in list_of_files: %s! '
                         'Unable to check if model files are present.', e_x)
            return

        if len(file_actual_path_list) == 0:
            logger.error('No model output files! Exiting...')
            raise SystemExit(1)

        file_actual = []
        file_actual.append([i.split('/')[-1] for i in file_actual_path_list])

        # Now cross-check wish_list and actual_list. If files are missing,
        # display missing files in log.
        if list(set(file_wish[0]).difference(file_actual[0])):
            missing_files = list(set(file_wish[0]) -
                                 set(file_actual[0]))
            logger.warning('Oops, you are missing model files! The missing '
                         'files are: \n{}'.format('\n'.join(map(
                                                    str, missing_files))))
            logger.warning('Continuing despite missing local files - S3 fallback '
                      'will attempt to retrieve them during processing.')

        else:
            logger.info('Located all necessary model files for %s!', cast)
            # Reset dates before returning
            prop.start_date_full = startdatesave
            prop.end_date_full = enddatesave
