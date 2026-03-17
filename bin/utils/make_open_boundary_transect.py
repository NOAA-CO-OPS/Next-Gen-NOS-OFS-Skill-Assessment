"""
Created on Tue Aug 19 13:53:41 2025

@author: PWL
"""

import argparse
import logging
import logging.config
import os
import sys
from datetime import datetime, timedelta
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import plotly.express as px
import xarray as xr
from plotly.subplots import make_subplots

from ofs_skill.model_processing import model_properties, model_source
from ofs_skill.obs_retrieval import utils


def parameter_validation(prop,logger):

    dir_params = utils.Utils().read_config_section('directories', logger)
    # Start Date and End Date validation
    try:
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (
            f'Please check Start Date format! '
            f'{prop.start_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = Path(dir_params['home'])

    # prop.path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    if not os.path.exists(ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. '
            f'Please check prop.path - {prop.path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'
    if not os.path.isfile(shape_file):
        error_message = (
            f'Shapefile {prop.ofs} is not found at '
            f'the folder {ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # model cycle validation -- wait to do this until forecast horizon skill
    # is merged to main and borrow functions from get_forecast_hours

    # Set up directory tree
    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir']
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['observations_dir'],
        dir_params['1d_station_dir'], )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'], )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir'], )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'], )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'], )
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    # Assign path to OBC files
    prop.model_obc_path = os.path.join(
        dir_params['model_obc_dir'], prop.ofs, 'input'
    )
    prop.model_obc_path = Path(prop.model_obc_path).as_posix()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth
    (specified in decimal degrees) using the Haversine formula.

    Args:
        lat1 (float): Latitude of the first point in degrees.
        lon1 (float): Longitude of the first point in degrees.
        lat2 (float): Latitude of the second point in degrees.
        lon2 (float): Longitude of the second point in degrees.

    Returns:
        float: The distance between the two points in kilometers.
    """
    # Radius of Earth in kilometers (mean radius)
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Differences in coordinates
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

def load_obc_file(prop,logger):
    '''
    '''
    # path /opt/archive/prod/{prop.ofs}/input/{yyyymm}
    # filename {prop.ofs}.t{cycle}z.{yyyymmdd}.obc.nc

    # Get next directory name from input date and then set full file path
    date_dt = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
    dir_name = datetime.strftime(date_dt,'%Y%m')
    file_date = datetime.strftime(date_dt,'%Y%m%d')
    file_name = f'{prop.ofs}.t{prop.model_cycle}z.{file_date}.obc.nc'
    file_path = os.path.join(prop.model_obc_path, dir_name, file_name)
    file_path = Path(file_path).as_posix() # Done with file path

    # Finally try to load the file
    try:
        decode = True
        if (model_source.model_source(prop.ofs) == 'fvcom'):
            decode = False
        ds = xr.open_dataset(file_path,decode_times=decode)
    except FileNotFoundError:
    # Catch the FileNotFoundError if the file is not found
        logger.error('Error: The OBC file was not found. Quitting...')
        sys.exit(-1)
    except Exception as e_x:
    # Catch any other potential exceptions during file operations
        logger.error(f'An unexpected error occurred: {e_x}')
        sys.exit(-1)

    return ds

def parse_roms_obc(prop,ds,logger):
    '''
    '''
    for variable in ['zeta','temp','salt']:
        boundaries = []
        # Search for variable boundaries in dataset
        for var_name in ds.data_vars:
            if variable in var_name:
                boundaries.append(var_name)

def mask_distance_gaps(x_orig,x_interp,z,logger):
    '''
    '''
    gap_length = np.nanpercentile(np.diff(x_orig), 99) # Set maximum gap length in km
    dx = np.argwhere(np.diff(x_orig) > gap_length) # Locate gap indices
    gaps = np.array([x_orig[dx],x_orig[dx+1]]) # Assign distance ranges to gaps

    # now loop and fill gaps with NaNs
    for i in range(gaps.shape[1]):
        to_fill = np.argwhere(
            (x_interp > gaps[0,i]) & (x_interp < gaps[1,i]))
        z[:,:,to_fill] = np.nan

    return z

def transform_to_z(ds,var,x_labels,logger):
    '''
    '''

    # Set new siglay length from max depth & min dz
    siglay_len = int(np.ceil(np.nanmax(np.array(ds['h']))/\
                             np.nanmin(np.array(ds['h'])/len(ds['siglay']))))
    # Need to reduce spatial and temporal resolution for plotting
    # with a time slider!
    max_rows = 500
    max_cols = 500
    if siglay_len > max_rows:
        siglay_len = int(siglay_len/(np.ceil(siglay_len/max_rows)))
    time_iterator = 1
    if len(ds['time']) > 55:
        time_iterator = int(np.ceil(len(ds['time'])/55))

    # Make new empty arrays for the new sigma layer length
    z_depth_single = np.array(np.full((siglay_len, len(ds['lon'])), np.nan))
    ref_depth = np.linspace(0,np.nanmax(np.array(ds['h'])),siglay_len,
                            endpoint=True)
    ref_index = np.linspace(0,siglay_len-1,siglay_len)
    siglay_index = np.linspace(0,len(ds['siglay'])-1,len(ds['siglay']))

    # Loop through each time and each column and assign z values
    z_depth_all = []
    for i in range(0,len(ds['time']),time_iterator):
        logger.info('Interpolating depths, time %s of %s', str(i),
                    str(len(ds['time'])))
        for j in range(len(ds['lon'])):
            # Find nearest row of each column's max depth
            col_depth = np.array(ds['h'])[j]
            depth_row = int(np.argmin(np.abs(ref_depth-col_depth)))
            # Interpolate variable downwards to the depth_row
            xint = np.linspace(siglay_index[0],siglay_index[-1],
                               len(ref_index[0:depth_row+1]))
            z_depth_single[0:depth_row+1,j] = np.interp(xint,siglay_index,
                                             np.array(ds[var])[i,:,j])
        z_depth_all.append(z_depth_single)
        z_depth_single = np.array(np.full((siglay_len, len(ds['lon'])), np.nan))

    z_depth_all = np.stack(z_depth_all)

    # Now loop through each time and each row and assign z values
    # across each depth
    x_spacing = 0.25 #km
    dist_len = int(np.ceil(np.nanmax(x_labels)/x_spacing))
    if dist_len > max_cols:
        dist_len = int(dist_len/(np.ceil(dist_len/max_cols)))
    x_grid = np.linspace(0,x_labels[-1],dist_len)
    # Make new z array for the interpolated x-axis length
    z_dist_single = np.array(np.full((z_depth_all.shape[1], len(x_grid)),
                         np.nan))
    z_dist_all = []
    for i in range(z_depth_all.shape[0]):
        for j in range(z_depth_all.shape[1]):
            z_dist_single[j,:] = np.interp(x_grid,x_labels,z_depth_all[i,j,:])
        z_dist_all.append(z_dist_single)
        z_dist_single = np.array(np.full((z_depth_all.shape[1], len(x_grid)),
                             np.nan))
    z_dist_all = np.stack(z_dist_all)

    # Finally we need to mask distance gaps with NaNs
    z_mask = mask_distance_gaps(x_labels, x_grid, z_dist_all, logger)

    # New ref_depth
    ref_depth = np.linspace(0,np.nanmax(np.array(ds['h'])),z_dist_all.shape[1],
                            endpoint=True)

    return z_mask, ref_depth, x_grid

def make_x_labels(ds,logger):
    '''
    '''
    x_labels = []
    for i in range(len(ds['lat'])-1):
        x_labels.append(haversine(np.array(ds['lat'])[i],
                                  np.array(ds['lon'])[i],
                                  np.array(ds['lat'])[i+1],
                                  np.array(ds['lon'])[i+1]))
    x_labels = np.cumsum(np.concatenate(([0], x_labels)))
    return x_labels

def plot_fvcom_obc(prop,ds,logger):
    '''
    '''
    # Convert time to datetime object. Units of time are:
    # days since 1858-11-17 00:00:00
    time_dt = []
    time_0 = datetime.strptime('1858-11-17','%Y-%m-%d')
    for t in np.array(ds['time']):
        delta = timedelta(days=float(t))
        time_dt.append(time_0 + delta)

    # First do temp & salt
    nrows = 1
    ncols = 1
    for name_var in ['temp','salinity']:
        if name_var == 'temp':
            #name_var = 'temperature'
            cbar_title = 'Water Temperature<br>(\u00b0C)'
        else:
            cbar_title = 'Salinity (<i>PSU<i>)'

        # Figure out dataset variable name
        try:
            var=str([item for item in list(ds.keys()) if name_var in item][0])
        except Exception as e_x:
            logger.error('Cannot find variable in xarray dataset. Error: %s',
                         e_x)
            continue

        # Make x-axis labels
        x_labels = make_x_labels(ds,logger)
        # Transform sigma layers to z-coordinates & interpolate transect
        z, y_labels, x_labels = transform_to_z(ds,var,x_labels,logger)

        # Some OFS repeat themselves -- set boundaries
        # if prop.ofs == 'ngofs2':
        #     z = z[:,:,:171]
        #     x_labels = x_labels[:171]

        # Figures
        fig = make_subplots(rows=nrows, cols=ncols)
        # Make df from z for plotly express animations
        plot_title = prop.ofs.upper() + ' ' + name_var + ' OBC transect, ' +\
            datetime.strftime(time_dt[0],'%m/%d/%Y %H:%M:%S') + ' - ' +\
                datetime.strftime(time_dt[-1],'%m/%d/%Y %H:%M:%S')
        fig = px.imshow(z,
                animation_frame=0,
                aspect=0.33,
                x=x_labels,
                y=y_labels,
                labels=dict(x='Distance along transect (km)', y='Depth (m)',
                            color=cbar_title),
                range_color=[np.nanpercentile(z, 2),
                             np.nanpercentile(z, 98)],
                color_continuous_scale='Turbo',
                title=plot_title
                )
        for i,step in enumerate(fig.layout.sliders[0].steps):
            step.label = datetime.strftime(time_dt[i],'%m/%d/%Y %H:%M:%S')
        fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 50
        filename = prop.ofs + '_' + name_var + '_OBC.html'
        savepath = os.path.join(prop.visuals_1d_station_path, filename)
        fig.write_html(savepath, auto_play=False)

def make_open_boundary_transects(prop,logger):
    '''coming soon'''

    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting open boundary transect ---')

    # Do parameter validation
    parameter_validation(prop,logger)

    # Load file(s)
    ds = load_obc_file(prop,logger)

    # Parse OBC netcdf file
    if model_source.model_source(prop.ofs) == 'roms':
        parse_roms_obc(prop,ds,logger)
    elif model_source.model_source(prop.ofs) == 'fvcom':
        #parse_fvcom_obc(prop,ds,logger)
        plot_fvcom_obc(prop,ds,logger)
    else:
        logger.error('Model source %s not found!',
                     model_source.model_source(prop.ofs))
        sys.exit(-1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python make_open_boundary_transect.py', usage='%(prog)s',
        description='Run open boundary condition transect maker'
    )
    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help='Choose from the list on the prop.ofs_Extents folder, '
        'you can also create your own shapefile, add it to the '
        'prop.ofs_Extents folder and call it here',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=False,
        help='Use /home as the default. User can specify path',
    )
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-hr',
        '--Cycle_Hr',
        required=False,
        help="Model cycle padded hour, e.g. '02', '06', '12', '24' ... ",
    )
    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.model_cycle = args.Cycle_Hr

    make_open_boundary_transects(prop1, None)
