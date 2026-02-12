#!/bin/bash
# FILENAME:  insert_skill_stats.py
# CREATED:   2024-06-25
#
# PURPOSE:   Insert the OFS skill assessment statistics into
#            a database (presently SqLite)
#
# REVISION HISTORY:
#
#
import argparse
import logging
import logging.config
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils


def get_skill_files(ofs,filepath,logger):
    '''
    Gets a list of all skill tables (csv format) within the specified
    filepath for a given OFS
    '''
    try:
        allfiles = os.listdir(filepath) # Get all file names
        allfiles[0]
    except FileNotFoundError:
        logger.error('File path (%s) for the skill stats is incorrect!',
                     filepath)
        sys.exit()
    except IndexError:
        logger.error('No files found in skill stats directory: %s. Good day.',
                     filepath)
        sys.exit()

    ofs_files = [] # Collect OFS file names here
    for file in allfiles: # Loop through each file name using iterator 'file'
        if ofs in file and 'skill' in file and '.csv' in file and\
            '2d' not in file:
            ofs_files.append(file)
    return ofs_files

# Main method
def main(skill_stats_file_path,db_path,period,ofs,logger):
    # Parse metadata (OFS, product, type) from filename
    # Sample filenames
    # skill_cbofs_currents_forecast_b.csv
    # skill_cbofs_currents_nowcast.csv
    # skill_cbofs_salinity_forecast_b.csv
    # skill_cbofs_salinity_nowcast.csv
    # skill_cbofs_water_level_forecast_b.csv
    # skill_cbofs_water_level_nowcast.csv
    # skill_cbofs_water_temperature_forecast_b.csv
    # skill_cbofs_water_temperature_nowcast.csv
    dir_params = utils.Utils().read_config_section('directories', logger)
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(Path(dir_params['home']),
                                       log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            print('Cannot find logging config file! Bye.')
            return
        # Check if config file exists
        if not os.path.isfile(config_file):
            print('Cannot find main config file! Bye.')
            return

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    # Input arguments -->
    # Path to database file
    if not os.path.exists(db_path):
        print(
            'Error: Sqlite database file path not found: ' + db_path)
        return

    # Assign table type
    db_table = f'{period}_skill_stats'

    # Get list of skill table files
    files = get_skill_files(ofs, skill_stats_file_path, logger)

    # Check to see if files variable is empty
    try:
        files[0]
    except IndexError:
        logger.error('No skill stat tables were found! Please check directory '
                     '%s and try again!',skill_stats_file_path)
        sys.exit()

    # Start looping through files
    logger.info('Here we go! '
               + '(db_table ' + db_table + ')')
    db_file = os.path.join(db_path, (ofs +'.db'))
    logger.info('Sqlite database file is ' + db_file)
    for file in files:

        try:
            this_product = file.replace('.csv','').split('_')[2]
        except IndexError:
            logger.error('Skill stats file name is incorrect! Check file: %s ',
                          file)
        if this_product == 'water':
            if file.replace('.csv','').split('_')[3] == \
                'level':
                    this_product = this_product + '_' +\
                        'level'
            else:
                    this_product = this_product + '_' +\
                        'temperature'
        if 'nowcast' in file:
            this_type = 'nowcast'
        elif 'forecast' in file:
            this_type = 'forecast_b'

        # Set path for file
        filepath = os.path.join(skill_stats_file_path, file)

        logger.info(
            '---> Starting skill assessment statistics ingestion for ' + ofs\
            + ' (' + this_product + ', ' + this_type + ')')
        logger.info('Filename to ingest: [%s]', file)

        # Create DB if it does not exist
        # Create table if it doesn't exist. Columns:
        # product: TEXT
        # station_id: TEXT
        # type: TEXT (nowcast or forecast)
        # begin_date_time:  TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS")
        # end_date_time:  TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS")
        # node: INTEGER
        # rmse: REAL
        # r: REAL
        # bias: REAL
        # bias_perc: REAL
        # bias_dir: REAL
        create_sql = 'CREATE TABLE IF NOT EXISTS ' + db_table + '( \
            datetime_inserted TEXT, \
            product TEXT, \
            station_id TEXT, \
            type TEXT,\
            begin_date_time TEXT, \
            end_date_time TEXT, \
            node INTEGER, \
            rmse REAL, \
            r REAL, \
            bias REAL, \
            bias_perc REAL, \
            bias_dir REAL, \
            central_freq REAL, \
            central_freq_pass_fail TEXT, \
            pos_outlier_freq REAL, \
            pos_outlier_freq_pass_fail TEXT, \
            neg_outlier_freq REAL, \
            neg_outlier_freq_pass_fail TEXT, \
            bias_standard_dev REAL, \
            target_error_range REAL, \
            PRIMARY KEY (product, station_id, begin_date_time, type) \
        );'
        conn = None
        cur = None
        # Make sqlite table if it does not exist
        try:
            conn = sqlite3.connect (db_file)
            cur = conn.cursor ()
            cur.execute (create_sql)
        except sqlite3.Error as er:
            logger.error (
                str (er) + '. Error code ' + str (
                    er.sqlite_errorcode) + ' ' + er.sqlite_errorname)
            return
        except Exception as er:
            logger.error ('Error creating table: ' + str (er))
            return
        finally:
            if (cur):
                try:
                    cur.close ()
                except Exception as er:
                    logger.error ('Error closing Sqlite DB cursor ' + str(er))
            if (conn):
                try:
                    conn.close ()
                except Exception as er:
                    logger.error (
                        'Error closing Sqlite DB connection ' + str(er))

        # Parse data file and store in list
        lines = []
        fh = None
        try:
            fh = open(filepath)
            lines = fh.readlines()
        except Exception as er:
            logger.error(
                'Error reading file ' + filepath + ': ' + str (er))
        finally:
            if(fh):
                try:
                    fh.close()
                except Exception as er:
                    logger.error(
                        'Error closing file ' + filepath + ': ' + er)
        # Get begin and end date indices
        date_idx = lines[0].replace('\n','').split(',')
        start_ind = date_idx.index('start_date')
        end_ind = date_idx.index('end_date')
        # Get begin and end dates using indices
        begin_date_time = lines[1].strip().split(',')[start_ind]
        end_date_time = lines[1].strip().split(',')[end_ind]
        # Reformat begin and end dates
        if 'T' in end_date_time and 'Z' in end_date_time:
            begin_date_time = begin_date_time.replace('T',' ').replace('Z','')
            end_date_time = end_date_time.replace('T',' ').replace('Z','')
        else:
            begin_date_time = begin_date_time[0:4] + '-' + \
                begin_date_time[4:6] + '-' + begin_date_time[6:8] + ' ' +\
                    begin_date_time[9:]
            end_date_time = end_date_time[0:4] + '-' + \
                end_date_time[4:6] + '-' + end_date_time[6:8] + ' ' +\
                    end_date_time[9:]
        try:
            is_datetime(begin_date_time)
            is_datetime(end_date_time)
        except Exception as err:
            print (
                'Fatal error:  Invalid --begin-datetime specified: ' + \
                    str (err))
            sys.exit()
        # Check time period and see if it's correct
        dt = datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S') - \
            datetime.strptime(begin_date_time, '%Y-%m-%d %H:%M:%S')
        if period == 'daily' or period == 'monthly':
            if dt.days == 1 and period != 'daily':
                logger.error('Input period is %s and the day range found '
                             'in skill stats table is %s! If daily period, '
                             'day range must be 1 day; if monthly period, day range '
                             'must be 28-31 days. Moving to next file...',
                             period, dt.days)
                continue
            elif (dt.days >= 28 and dt.days <= 31
                      and period != 'monthly'):
                logger.error('Input period is %s and the day range found '
                             'in skill stats table is %s! If daily period, '
                             'day range must be 1 day; if monthly period, day range '
                             'must be 28-31 days. Moving to next file...',
                             period, dt.days)
                continue

        # Insert into Sqllite database
        insert_sql = ''' INSERT OR IGNORE INTO ''' + db_table + '''(datetime_inserted, product, station_id, type, \
        begin_date_time, end_date_time, node, rmse, r, bias, bias_perc, bias_dir, \
        central_freq, central_freq_pass_fail, pos_outlier_freq, pos_outlier_freq_pass_fail, \
        neg_outlier_freq, neg_outlier_freq_pass_fail,  bias_standard_dev, target_error_range) \
        VALUES(strftime('%Y-%m-%d %H:%M:%S', datetime('now')),?,?,?,?,?,?,?,?,?,?,?, \
        ?,?,?,?,?,?,?,?) '''

        insert_sql_no_biasdir = ''' INSERT OR IGNORE INTO ''' + db_table + '''(datetime_inserted, product, station_id, type, \
        begin_date_time, end_date_time, node, rmse, r, bias, bias_perc, \
        central_freq, central_freq_pass_fail, pos_outlier_freq, pos_outlier_freq_pass_fail, \
        neg_outlier_freq, neg_outlier_freq_pass_fail,  bias_standard_dev, target_error_range) \
        VALUES(strftime('%Y-%m-%d %H:%M:%S', datetime('now')),?,?,?,?,?,?,?,?,?,?, \
        ?,?,?,?,?,?,?,?) '''

        try:
            conn = sqlite3.connect(db_file)
            # Creating a cursor object using the
            # cursor() method
            cur = conn.cursor()
            # Loop through lines and insert into DB
            idx = 0

            # Get headers and lower them
            headers = lines[0].strip().split(',')
            headers = [item.lower() for item in headers]
            # Get indices from skill table for each sqlite column that we want
            # Doing it this way will find the correct column indices even when
            # the skill table format is updated or changed
            sqlite_all = ['this_id',
                        'this_node',
                        'this_rmse',
                        'this_r',
                        'this_bias',
                        'this_bias_perc',
                        'this_bias_dir',
                        'this_central_freq',
                        'this_central_freq_pass_fail',
                        'this_pos_outlier_freq',
                        'this_pos_outlier_freq_pass_fail',
                        'this_neg_outlier_freq',
                        'this_neg_outlier_freq_pass_fail',
                        'this_bias_standard_dev',
                        'this_target_error_range']
            indices = []
            for item in sqlite_all:
                try:
                    indices.append(headers.index(item.replace('this_','')))
                except ValueError:
                    print('uh oh devs need to figure out how to handle this '
                          'error')
            # Loop through lines of skill table
            for this_line in lines:
                this_line = this_line.strip() #removes \n from end of line
                # Check if line starts with integer node info (skips header)
                if (this_line[0].isdigit()):
                    s = this_line.split (',')
                    sqlite_values = [s[i] for i in indices]
                    sqlite_dict = dict(zip(sqlite_all, sqlite_values))

                    if sqlite_dict['this_bias_dir'] != '':
                        vals = (\
                            this_product,
                            sqlite_dict['this_id'],
                            this_type,
                            begin_date_time,
                            end_date_time,
                            sqlite_dict['this_node'],
                            sqlite_dict['this_rmse'],
                            sqlite_dict['this_r'],
                            sqlite_dict['this_bias'],
                            sqlite_dict['this_bias_perc'],
                            sqlite_dict['this_bias_dir'],
                            sqlite_dict['this_central_freq'],
                            sqlite_dict['this_central_freq_pass_fail'],
                            sqlite_dict['this_pos_outlier_freq'],
                            sqlite_dict['this_pos_outlier_freq_pass_fail'],
                            sqlite_dict['this_neg_outlier_freq'],
                            sqlite_dict['this_neg_outlier_freq_pass_fail'],
                            sqlite_dict['this_bias_standard_dev'],
                            sqlite_dict['this_target_error_range']
                            )
                        cur.execute (insert_sql,vals)
                    # No bias direction
                    else:
                        vals = (\
                            this_product,
                            sqlite_dict['this_id'],
                            this_type,
                            begin_date_time,
                            end_date_time,
                            sqlite_dict['this_node'],
                            sqlite_dict['this_rmse'],
                            sqlite_dict['this_r'],
                            sqlite_dict['this_bias'],
                            sqlite_dict['this_bias_perc'],
                            sqlite_dict['this_central_freq'],
                            sqlite_dict['this_central_freq_pass_fail'],
                            sqlite_dict['this_pos_outlier_freq'],
                            sqlite_dict['this_pos_outlier_freq_pass_fail'],
                            sqlite_dict['this_neg_outlier_freq'],
                            sqlite_dict['this_neg_outlier_freq_pass_fail'],
                            sqlite_dict['this_bias_standard_dev'],
                            sqlite_dict['this_target_error_range']
                            )
                        cur.execute(insert_sql_no_biasdir,vals)
                    idx = idx + 1

            conn.commit ()
            logger.info ('Inserted or processed ' + str (idx) + \
                         ' values (ignores if values exist)')

        except sqlite3.Error as er:
            logger.error (str (er) + '. Error code ' + str (
                    er.sqlite_errorcode) + ' ' + er.sqlite_errorname)
        except Exception as er:
            logger.error ('Error insert data into Sqlite DB file ' + db_file + \
                          ': ' + str (er))

        finally:
            if (cur):
                try:
                    cur.close ()
                except Exception as er:
                    logger.error('Error closing Sqlite DB cursor ' + er)
            if (conn):
                try:
                    conn.close ()
                except Exception as er:
                    logger.error('Error closing Sqlite DB connection ' + er)

    logger.info (
        '<--- Finished skill assessment statistics ingestion for ' + ofs)

# Check if string is a valid date/time in YYYY-mm-dd HH:MM:SS format
def is_datetime(string):
    if not re.match (
            '^[1-2]{1}[0-9]{3}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1} [0-9]{2}:[0-9]{2}:[0-9]{2}',
            string):
        msg = 'Not a valid datetime [YYYY-MM-DD HH:mm:ss]'
        raise argparse.ArgumentTypeError (msg)
    return string


# Execute main
if __name__ == '__main__':
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(
        prog = 'insert_skill_stats.py',
        description = 'Insert OFS skill assessment statistics into Sqlite '
        'database file',
        epilog = 'python insert_skill_stats.py --ofs=[] '
        '--file=[] --period=[] --database-path=[]')
    parser.add_argument(
        '-f','--file',
        required = False,
        default = './data/skill/stats/',
        help = 'OFS SA statistics file to ingest (w/ absolute path)')
    parser.add_argument(
        '-o','--ofs',
        required = True,
        help = 'OFS to get skill tables for, e.g. cbofs, dbofs, etc.')
    parser.add_argument(
        '-dp','--database-path',
        required = False,
        default = './db/',
        help = 'Sqlite database path to write to, i.e. /opt/ofs_dps/db/')
    parser.add_argument(
        '-p','--period',
        required = True,
        help = 'SA period. Valid values: daily or monthly',)
    args = parser.parse_args()

    # Parse input arguments
    errors = False

    # Skill table file path
    if args.file:
        skill_stats_file_path = args.file
        skill_stats_file_path = Path(skill_stats_file_path)

    # Path to database file
    if args.database_path:
        db_path = os.path.abspath(args.database_path)
        if not os.path.exists (db_path):
            print(
                'Error: Sqlite database file path not found: ' + db_path)
            errors = True
    else:
        errors = True
        print('Error: No --database specified')
    # Period
    if args.period:
        period = args.period.lower()
        periods = ['daily','weekly','monthly','yearly']
        if period not in periods:
            print('Custom period specified! Continuing...')
    else:
        print('Default daily period set')
    # OFS
    if args.ofs:
        ofs = args.ofs
    # Leave, if errors
    if errors:
        sys.exit (-1)
    # Enter main
    main(
         skill_stats_file_path,
         db_path,
         period,
         ofs,
         None
         )
