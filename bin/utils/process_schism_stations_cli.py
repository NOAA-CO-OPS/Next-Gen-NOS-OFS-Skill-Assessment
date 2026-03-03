"""
This script gathers up raw SCHISM station output and processes it to
OFS-standard NetCDF files that contain all variables needed for the skill
assessment package, including water level, salinity, water temp, and current
velocity.

@author: PWL
Created on Fri Jan 23 15:01:50 2026
"""

import argparse
import logging
import logging.config
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from netCDF4 import Dataset, date2num
from pyproj import Transformer

from ofs_skill.model_processing import model_properties
from ofs_skill.model_processing.model_source import get_model_source
from ofs_skill.obs_retrieval import utils


def parameter_validation(prop, dir_params, logger):
    """Parameter validation"""
    if prop.path is None:
        prop.path = dir_params['home']
    # Path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = f'ofs_extents/ folder is not found. ' \
                        f'Please check Path - ' \
                        f"'{prop.path}'. Abort!"
        logger.error(error_message)
        sys.exit(-1)
    # OFS validation
    shapefile = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shapefile):
        error_message = f"Shapefile '{prop.ofs}' " \
                        f'is not found at the folder' \
                        f' {ofs_extents_path}. Abort!'
        logger.error(error_message)
        sys.exit(-1)


def make_dir_list(prop, logger):
    '''
    Makes a list of directories to check for SCHISM output. Right now this
    function only applies to the LOOFS2 SCHISM output file structure, but
    can be modified to other dir structures.

    Parameters
    ----------
    prop : contains all run-related parameters.
    logger : logger!

    Returns
    -------
    dir_list: a list of paths to all SCHISM output directories.

    '''

    loofshr = ['00', '06', '12', '18']
    # Check hours to make they correspond to output directories
    if prop.start_date_full[-2:] not in loofshr:
        prop.start_date_full = prop.start_date_full[:-2] + '00'
    if prop.end_date_full[-2:] not in loofshr:
        prop.end_date_full = prop.end_date_full[:-2] + '18'
    # First get list of dates at x-hourly interval
    hr_interval = 6
    date_list = make_datetime_list(datetime.strptime(prop.start_date_full,
                                                     '%Y%m%d-%H'),
                                   datetime.strptime(prop.end_date_full,
                                                     '%Y%m%d-%H'),
                                   hr_interval)
    # Now with date list, loop and make dir list
    dir_list = []
    for date in date_list:
        year = date.year
        month = date.month
        day = date.day
        # Do LOOFS2 hindcast directory structure
        model_dir = [Path(f'{prop.filepath}/{year}/{month:02}{day:02}{hr}/outputs/').as_posix() \
                     for hr in loofshr]
        for mdir in model_dir:
            if mdir not in dir_list:
                dir_list.append(mdir)
            if not os.path.exists(mdir):
                logger.error('Did not find model output dir!')

    return dir_list

def make_datetime_list(start_date, end_date, interval_hours):
    '''

    Generates a list of datetimes every `interval_hours` between start and
    end dates (inclusive of start & end).

    Parameters
    ----------
    start_date : start date, datetime object
    end_date : end date, datetime object
    interval_hours : the interval in hours between each entry in the final
    datetime list.

    Returns
    -------
    date_list : a list of datetime objects where the delta between each date
    corresponds to interval_hours.

    '''

    date_list = []
    delta = timedelta(hours=interval_hours)

    while start_date <= end_date:
        date_list.append(start_date)
        start_date += delta

    return date_list


def copy_field_files(prop, filepath, destination, logger):
    '''
    Copies raw SCHISM field output frm the filepath to the destination path,
    and renames it in OFS-standard format.

    Parameters
    ----------
    prop : global input arguments/parameters/properties
    filepath : path where the raw SCHISM output field files are located
    destination : copy file destination, including new filename
    logger : logger

    Returns
    -------
    Nothing.
    '''

    cyc = filepath.split('/')[-3][-2:]
    filename = filepath.split('/')[-1].split('.')[-2].split('_')[0]
    date = filepath.split('/')[-4] + filepath.split('/')[-3][0:4]
    destination_name = f'{prop.ofs}.t{cyc}z.{date}.{filename}.{prop.whichcast}.nc'
    destination_path = Path(os.path.join(destination,destination_name)).as_posix()
    shutil.copyfile(filepath,destination_path)


def load_2d_station_files(filepath, filename, logger):
    '''
    Loading function for 2D SCHISM output variables, including temp, salt, and
    currents.

    Parameters
    ----------
    filepath : path to SCHISM output file
    filename : SCHISM output filename
    logger : logger!

    Returns
    ------
    Returns all 2D-related numpy arrays:
        prof_var_data: 2D data values
        prof_z_data: 2D depth values
        surf_data: surface values
    '''

    def generate_specific_rows(file_path, row_indices=[]):
        '''
        A helper that yields text file lines at specified indices.
        '''
        with open(file_path) as f:
            for i, line in enumerate(f):
                if i in row_indices:
                    yield line

    # Number of rows (times)
    dt_folder = 6*60 #minutes
    dt_data = 6 #minutes
    nrows = (dt_folder/dt_data)*2 #*2 because there are two rows for each time step
    surf_rows = [int(num) for num in np.linspace(
        0,nrows-2,int(nrows/2))] #rows to extract for surface
    prof_rows = [int(num) for num in np.linspace(
        1,nrows-1,int(nrows/2))] #rows to extract for profiles
    # Extract prof and surf rows from staout file
    gen = generate_specific_rows(filepath+'/'+filename,
                                 surf_rows)
    surf_data = np.loadtxt(gen)
    gen = generate_specific_rows(filepath+'/'+filename,
                                 prof_rows)
    prof_data = np.loadtxt(gen)
    # Slice off time
    surf_data = np.delete(surf_data, 0, axis=1)
    prof_data = np.delete(prof_data, 0, axis=1)
    # Replace no data values with nans
    surf_data[surf_data < -100000] = np.nan
    prof_data[prof_data < -100000] = np.nan
    # Now parse prof_data rows to get var values and z values
    nsta = int(surf_data.shape[1]) # number of stations
    nvrt = int(prof_data.shape[1]/2/nsta) # number of depth vertices
    nt = int(surf_data.shape[0]) # number of time steps
    # prof_var_data column indices -->   0:(nvrt*nsta)
    # prof_depth_data column indices --> (nvrt*nsta):
    prof_var_data = prof_data[:,0:(nvrt*nsta)]
    prof_z_data = prof_data[:,(nvrt*nsta):]

    # OK GOOD! Now we need to reshape these to be OFS-compatible:
        # time x nsta x nvrt (60 x 14 x 32)
    prof_var_data = prof_var_data.reshape(nt, nsta, nvrt)
    prof_z_data = prof_z_data.reshape(nt, nsta, nvrt)

    # HOORAY, that was difficult <party popper>
    # What to return? Options: prof_var_data, surf_data, prof_z_data
    return prof_var_data, prof_z_data, surf_data

def load_1d_station_files(filepath, filename, logger):
    '''
    Parameters
    ----------
    filepath : path to SCHISM output file
    filename : SCHISM output filename
    logger : logger!

    Returns:
    ------
    np_arr: water level data values
    t: time array

    '''

    # First get basedate then add time steps on top of that. Basedate is 6 hours
    # before the directory date
    dt_folder = 6 # This is important! Time difference between successive output dirs -- consider moving to somewhere more visible
    dirdate_str = filepath.split('/')[-3] + filepath.split('/')[-2]
    basedate = datetime.strptime(dirdate_str, '%Y%m%d%H') - \
        timedelta(hours=dt_folder)
    # Retrieve `filename` from the `filepath`, heigh-ho heigh-ho
    # Load as numpy array
    np_arr = np.loadtxt(filepath+'/'+filename)
    # Make time array using dt + basedate
    t = [basedate+timedelta(seconds=int(dt)) for dt in np_arr[:,0]]
    # Cut dt from numpy array, no longer needed
    np_arr = np.delete(np_arr, 0, axis=1)

    return np_arr, t

def get_station_info(prop, dir_list, logger):
    '''
    Parameters
    ----------
    prop : holds all CLI input paramaters
    logger : logger!

    Returns
    -------
    a pandas dataframe with all station info in it, including lat, lon, ID.

    '''

    file_path = Path(os.path.join(dir_list,'station.in')).as_posix()
    # Define the list of column names
    column_names = ['ID_num', 'X', 'Y', 'WHAT IS THIS']
    # Read the file and skip the first 2 rows
    df = pd.read_csv(file_path,
                     delimiter=' ',
                     skiprows=2,
                     header=None,
                     names=column_names
                     )
    # Define the transformation from EPSG:3174 (Great Lakes Albers)
    # to EPSG:4326 (WGS84 Lat/Lon)!
    transformer = Transformer.from_crs('EPSG:3174', 'EPSG:4326',
                                       always_xy=True)
    # Apply transformation across rows
    df['lon'], df['lat'] = zip(*df.apply(
        lambda row: transformer.transform(row['X'], row['Y']), axis=1
        ))
    # Remove Albers coords!
    df = df.drop(['X', 'Y'], axis=1)

    # All set!
    return df

def make_ofs_dir_list(prop, basepath, logger):
    """
    This function creates a list of directories where model output is
    stored, using the standard OFS/skill assessment dir tree format.
    Returns a list of directory paths to save files to.
    """

    dir_list_ofs = []
    hr_interval = 24
    date_list = make_datetime_list(datetime.strptime(prop.start_date_full,
                                                     '%Y%m%d-%H'),
                                   datetime.strptime(prop.end_date_full,
                                                     '%Y%m%d-%H'),
                                   hr_interval)

    # After 12/31/24, directory structure changes! Now we need to sort
    # a dir list that might have two different formats.
    datethreshold = datetime.strptime('12/31/24', '%m/%d/%y')
    logger.info(f'Starting list of directories for {basepath}')
    ####
    for date in date_list:
        year = date.year
        month = date.month
        # Add stofs directory structure
        if prop.ofs in ['stofs_3d_atl', 'stofs_2d_global', 'stofs_3d_pac']:
            day = date.day
            model_dir = f'{basepath}{prop.model_path}/{prop.ofs}.{year}' +\
                        f'{month:02}{day:02}'
        else:
            # Do old directory structure
            if (
                date <= datethreshold
            ):
                model_dir = f'{basepath}/{year}{month:02}'
            # Do new directory structure
            elif (
                date > datethreshold
            ):
                day = date.day
                model_dir = f'{basepath}/{year}/{month:02}/{day:02}'
            # Whoops! I'm out
            else:
                logger.error("Check the date -- can't find model output dir!")
                sys.exit()
        model_dir = Path(model_dir).as_posix()
        # if model_dir not in dir_list:
        dir_list_ofs.append(model_dir)
        logger.info('Found model output dir: %s', model_dir)
    return dir_list_ofs

def create_directories(dir_list, logger):
    """Creates directory tree from a list of directory names."""
    for dir_name in dir_list:
        try:
            os.makedirs(dir_name)
            logger.info(f"Directory '{dir_name}' created successfully.")
        except FileExistsError:
            logger.info(f"Directory '{dir_name}' already exists.")
        except Exception as e_x:
            logger.error(f"Error creating directory '{dir_name}': {e_x}")
            sys.exit(-1)

def process_schism_stations(prop, logger):
    '''
    MAIN FUNCTION! Calls all other functions, and ultimately writes NetCDFs
    of all SCHISM station output.

    Parameters
    ----------
    prop : all command line inputs are stored here.
    logger : logger!

    Returns
    -------
    None.

    '''
    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            print('No log file! Cannot continue.')
            sys.exit()

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Start loading SCHISM station output text files ---')
    # Directory parameters
    dir_params = utils.Utils().read_config_section('directories', logger)
    # Parameter validation
    parameter_validation(prop, dir_params, logger)

    # Path for saving netcdfs
    prop.model_path = os.path.join(
        dir_params['model_historical_dir'], prop.ofs, dir_params['netcdf_dir']
    )
    prop.model_path = Path(prop.model_path).as_posix()

    # -----> Done with set-up
    logger.info('Done with set-up, start searching directories!')

    # First make list of expected directories, and return dir list
    dir_list = make_dir_list(prop, logger)
    # Now set up dir tree in standard OFS format to save output netcdf files
    dir_list_ofs = make_ofs_dir_list(prop, prop.model_path, logger)
    create_directories(dir_list_ofs, logger)

    # Station out file names to load
    staout_names = {'staout_1': 'wl', # elev/wl
                    'staout_3': 'u_wind', # wind u-vel
                    'staout_4': 'v_wind', # wind v-vel
                    'staout_5': 'temp', # temp
                    'staout_6': 'salt', # salt
                    'staout_7': 'u', # current u-vel
                    'staout_8': 'v', # current v-vel
                    }
    # placeholder for station ID list
    station_df = None
    # Field/out2d file names -- add more files here to complete fields
    # processing. Right now it is set up only for ice & water level
    field_names = ['out2d_1.nc',
                   'temperature_1.nc',
                   'zCoordinates_1.nc',
                   'horizontalVelX_1.nc',
                   'horizontalVelY_1.nc',
                   ]

    # Loop through dir list and retrieve station output files, and
    # station info from the `station.in` file (only need to read one)
    if len(dir_list) > 0:
        #load_schism_stations(prop, [dir_list,dir_list_ofs], logger)
        # Loop through dir_list, collect station output files, and save them
        # in the OFS dir tree
        station_info_flag = None # Raise flag after finding a `station.in` file
        # First get list of days
        hr_interval = 24
        date_list = make_datetime_list(datetime.strptime(prop.start_date_full,
                                                    '%Y%m%d-%H'),
                                  datetime.strptime(prop.end_date_full,
                                                    '%Y%m%d-%H'),
                                  hr_interval
                                  )
        # Loop through days
        for i,date in enumerate(date_list):
            datestr = f'{date.month:02}{date.day:02}'
            # Now find dirs that correpsond to that datestr
            dir_list_filt = [entry for entry in dir_list if datestr in \
                             entry.split('/')[-2][0:4]]
            # Loop through directories
            for dir_path in dir_list_filt:
                #
                # First check for station.in file, and raise flag when found
                if (os.path.isfile(os.path.dirname(dir_path) + '/station.in') and
                    station_info_flag is None):
                    # Found station info. Load it one time!
                    try:
                        station_df = get_station_info(prop,
                                                      os.path.dirname(dir_path),
                                                      logger)
                        station_info_flag = 'Eureka!'
                        logger.info('Eureka! We found the station.in file!')
                    except Exception as ex:
                        logger.error('Exception caught while getting station '
                                     'info from station.in! Error: %s', ex)
                # Set up dictionaries & empties for all vars
                # 2D vars
                twod_vars = {}
                for key in ['temp','salt','u','v']:
                    twod_vars[key] = []
                # 2D z-coords
                twod_z = {}
                for key in ['temp','salt','u','v']:
                    twod_z[key] = []
                # Surface vars
                surf_vars = {}
                for key in ['wl','u_wind','v_wind','temp','salt','u','v']:
                    surf_vars[key] = []
                # Loop through staout/station files
                for name in staout_names:
                    # Load each dir's station out files
                    try:
                        if int(name[-1]) < 5: # do surface water level, no z-coords
                            np_staout, t = load_1d_station_files(dir_path,
                                           name, logger)
                            surf_vars[staout_names[name]] = np_staout
                        else: # do 2D profiles (temp, salt, u, and v)
                            np_staout, np_staout_z, np_staout_surf = \
                                load_2d_station_files(dir_path, name, logger)
                            surf_vars[staout_names[name]] = np_staout_surf
                            twod_vars[staout_names[name]] = np_staout
                            twod_z[staout_names[name]] = np_staout_z
                    except Exception as ex:
                        logger.error('Error caught loading station files!'
                                     'Error: %s', ex)
                # Now loop through field 2D/3D output and copy it
                for name in field_names:
                    filepath = Path(os.path.join(dir_path, name)).as_posix()
                    try:
                        logger.info('Copying field files for %s and %s-%s...',
                                    name, datestr,filepath.split('/')[-3][-2:])
                        copy_field_files(prop, filepath, dir_list_ofs[i], logger)
                    except Exception as ex:
                        logger.error('Error when copying SCHISM field files! '
                                     'Error: %s', ex)
                # Now save stations to 6-hourly netcdfs
                logger.info('Saving all station files for %s...', datestr)

                '''
                Contents of station netcdf:
                    1) all vars, [time x stations x z-coords]
                    2) lat coords [stations]
                    3) lon coords [stations]
                    4) time [time]
                    5) water depth [stations]
                    6) all surf vars
                    7) all var z-coords
                '''
                if prop.whichcast == 'hindcast':
                    cyc = t[-1].hour
                    date = datetime.strftime(t[-1],'%Y%m%d')
                else:
                    cyc = t[0].hour
                    date = datetime.strftime(t[0], '%Y%m%d')
                ### Filename & filepath
                filename=f'{prop.ofs}.t{cyc:02}z.{date}.stations.{prop.whichcast}.nc'
                filepath = Path(os.path.join(dir_list_ofs[i],filename)).as_posix()
                ### Set up station netcdf
                if not os.path.isfile(filepath):
                    ncfile = Dataset(filepath, mode='w', format='NETCDF4')
                    name_length = 20
                    ### Set up dimensions
                    try:
                        ncfile.createDimension('station', int(station_df['ID_num'].max()))
                        ncfile.createDimension('clen', name_length)
                        ncfile.createDimension('time', len(t))
                        ncfile.createDimension('siglay', twod_vars['temp'].shape[2])
                        num_strings_dim_name = 'num_entries'
                        num_entries = twod_vars['temp'].shape[2]
                        ncfile.createDimension(num_strings_dim_name, num_entries)
                        ### Create variables
                        # Deal with time
                        time = ncfile.createVariable('time', np.float32, ('time'))
                        time.units = (f'seconds since {prop.start_date_full[0:4]}-'
                                      f'{prop.start_date_full[4:6]}-'
                                      f'{prop.start_date_full[6:8]} '
                                      f'{prop.start_date_full[-2:]}:00:00')
                        # Do rest of vars
                        lon = ncfile.createVariable('lon', np.float32, ('station'))
                        lat = ncfile.createVariable('lat', np.float32, ('station'))
                        name_station_var = ncfile.createVariable('name_station', 'S1', ('station'))
                        zeta = ncfile.createVariable('zeta', np.float32, ('time','station'))
                        uwind = ncfile.createVariable('uwind_speed', np.float32, ('time','station'))
                        vwind = ncfile.createVariable('vwind_speed', np.float32, ('time','station'))
                        temp = ncfile.createVariable('temp', np.float32, ('time','station','siglay',))
                        salinity = ncfile.createVariable('salinity', np.float32, ('time','station','siglay'))
                        u = ncfile.createVariable('u', np.float32, ('time','station','siglay'))
                        v = ncfile.createVariable('v', np.float32, ('time','station','siglay'))
                        zcoord = ncfile.createVariable('zcoords', np.float32, ('station','siglay'))
                        # Assign vars to netcdf
                        numeric_time = date2num(t, time.units)
                        station_names = [f'station_{prop.ofs}_{i+1:02d}' \
                                         for i in range(int(station_df['ID_num'].max()))]
                        # names_char_array = nc.stringtochar(np.array(station_names,
                        #                                             dtype=f'S{name_length}'))
                        name_station_var[:] = np.array(station_names,
                                                       dtype=f'S{name_length}')
                        time[:] = numeric_time[:]
                        lon[:] = station_df['lon']
                        lat[:] = station_df['lat']
                        zeta[:,:] = surf_vars['wl']
                        uwind[:,:] = surf_vars['u_wind']
                        vwind[:,:] = surf_vars['v_wind']
                        temp[:,:,:] = twod_vars['temp']
                        salinity[:,:,:] = twod_vars['salt']
                        u[:,:,:] = twod_vars['u']
                        v[:,:,:] = twod_vars['v']
                        zcoord[:,:] = twod_z['u'][0,:,:]
                        ncfile.close()
                    except TypeError as te:
                        logger.error('Station info was not found in SCHISM '
                                     'output! Cannot process staout files '
                                     'to netcdf! Error: %s', te)
                    except Exception as ex:
                        logger.error('Cannot process staout files '
                                     'to netcdf! Error: %s', ex)
    else:
        logger.error('No output directories found. Please check the file '
                      'path: %s', prop.filepath)
        sys.exit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='python process_schism_stations.py',
        usage='%(prog)s',
        description='Process SCHISM station text file outputs to netcdf files',
    )
    parser.add_argument(
        '-o', '--OFS',
        required=True,
        help="""Choose from the list on the ofs_extents/folder,
        you can also create your own shapefile, add it at the
        ofs_extents/folder and call it here""", )
    parser.add_argument(
        '-s', '--StartDate',
        required=True,
        help='Assessment start date: YYYYMMDD-HH '
        "e.g. '20241201-00'")
    parser.add_argument(
        '-e', '--EndDate',
        required=True,
        help='Assessment end date: YYYYMMDD-HH '
        "e.g. '20250202-18'")
    parser.add_argument(
        '-p', '--Path',
        required=True,
        help='Path to your skill assessment working directory', )
    parser.add_argument(
        '-fp', '--FilePath',
        required=True,
        help='Path to the SCHISM output', )
    parser.add_argument(
        '-ws', '--Whichcasts',
        required=True,
        #default='nowcast,forecast_b,hindcast',
        help="Choose one 'cast': 'nowcast', 'forecast', 'hindcast'", )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    args = parser.parse_args()
    prop1.start_date_full = args.StartDate
    prop1.end_date_full = args.EndDate
    prop1.path = args.Path
    prop1.filepath = args.FilePath
    prop1.ofs = args.OFS.lower()
    prop1.model_source = get_model_source(args.OFS)
    prop1.whichcast = args.Whichcasts.lower()

    process_schism_stations(prop1, None)
