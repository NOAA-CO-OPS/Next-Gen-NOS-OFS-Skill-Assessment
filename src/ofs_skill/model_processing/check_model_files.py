"""
Model File Validation Module

This module verifies that all model files needed to run the skill assessment are available
before processing begins. If files are missing, the program will exit and list the files
that are missing. Users can then run get_model_data.py in the utils directory to retrieve
those missing files from the NODD bucket.

Functions
---------
check_model_files : Main validation function that checks for required model files

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
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Any

from bin.utils import get_model_data
from ofs_skill.model_processing.list_of_files import list_of_dir, list_of_files
from ofs_skill.obs_retrieval import utils


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

    Notes
    -----
    - Handles both ISO format dates (with 'T' and 'Z') and simple format dates
    - Creates a wish list of files that should exist based on configuration
    - Creates an actual list of files that do exist in the directories
    - Cross-checks and reports any discrepancies
    - Resets date formats to original values before returning

    Examples
    --------
    >>> check_model_files(prop, logger)
    INFO:root:Located all necessary model files for nowcast!
    """
    # This first chunk handles the main skill assessment
    for cast in prop.whichcasts:
        prop.whichcast = cast
        # Directory params
        dir_params = utils.Utils().read_config_section('directories', logger)
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
            file_actual_path_list = list_of_files(prop,
                                                  dir_list,
                                                  logger)
        except Exception as e_x:
            logger.error('Error in list_of_files: %s! '
                         'Unable to check if model files are present.', e_x)
            return

        file_actual = []
        file_actual.append([i.split('/')[-1] for i in file_actual_path_list])

        # Now cross-check wish_list and actual_list. If files are missing,
        # display missing files in log.
        if list(set(file_wish[0]).difference(file_actual[0])):
            missing_files = list(set(file_wish[0]) -
                                 set(file_actual[0]))
            logger.error('Oops, you are missing model files! The missing '
                         'files are: \n{}'.format('\n'.join(map(
                                                    str, missing_files))))
            logger.error('NOTE: If S3 fallback is enabled, these files will '
                        'be retrieved from NODD during processing.')
            logger.warning('Continuing despite missing local files - S3 fallback '
                          'will attempt to retrieve them during processing.')
            # Don't exit - let S3 fallback handle missing files
            # raise SystemExit(1)
        else:
            logger.info('Located all necessary model files for %s!', cast)
            # Reset dates before returning
            prop.start_date_full = startdatesave
            prop.end_date_full = enddatesave
