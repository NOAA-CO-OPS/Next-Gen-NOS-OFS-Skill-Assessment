"""
Created on Fri Aug  8 08:59:28 2025

@author: PWL

This little module runs at the beginning of a call to create_1dplot.py to
verify that all model files needed to run the skill assessment are available.

If files are missing, the program will exit, and list the files that are
missing. The user can then run get_model_data.py in the utils directory to
retrieve those missing files from the NODD bucket. Hooray for buckets!

"""
import os
import sys
from datetime import datetime
from pathlib import Path

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import list_of_files

#from model_processing import get_forecast_hours
from obs_retrieval import utils
from utils import get_model_data


def check_model_files(prop,logger):
    '''
    Main function -- it takes only prop & logger as inputs and borrows
    existing magic from:
        get_model_data.py to figure out what model files SHOULD be there, and
        list_of_files.py to figure what files are ACTUALLY there.

    Then the two lists are cross-checked. Any missing files are returned
    to the log with a fatal error. If all files are found, the program will
    continue.

    Things get slightly more complicated if using the forecast horizon
    skill option -- that needs to be updated.
    '''
    # This first chunk handles the main skill assessment
    for cast in prop.whichcasts:
        prop.whichcast=cast
        # Directory params
        dir_params = utils.Utils().read_config_section('directories', logger)
        prop.model_save_path = os.path.join(dir_params['model_historical_dir'],
                                            prop.ofs,dir_params['netcdf_dir'])
        #First make list of what files SHOULD be in the directories
        try:
            dir_list, dates = get_model_data.list_of_dir(prop,
                                                         prop.model_save_path,
                                                         logger)
        except Exception as e_x:
            logger.error('Error in get_model_data: %s! '
                         'Unable to check if model files are present.', e_x)
            return
        dates = dates[1:] #Chop off extra first date, not needed here
        dir_list = dir_list[1:] # Chop off extra first dir, not needed here
        file_path_list = get_model_data.make_file_list(prop,dates,dir_list,
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
        dir_list = list_of_files.list_of_dir(prop, logger)
        try:
            file_actual_path_list = list_of_files.list_of_files(prop,
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
            missing_files = list(set(file_wish[0]) - \
                                 set(file_actual[0]))
            logger.error('Oops, you are missing model files! The missing '
                         'files are: \n{}'.format('\n'.join(map(\
                                                    str, missing_files))))
            sys.exit(-1)
        else:
            logger.info('Located all necessary model files for %s!', cast)
            # Reset dates before returning
            prop.start_date_full = startdatesave
            prop.end_date_full = enddatesave
