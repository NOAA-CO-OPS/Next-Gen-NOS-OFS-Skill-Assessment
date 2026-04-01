"""
Created on Wed Apr  1 11:02:36 2026

@author: PWL
"""

import os
import sys
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import xarray as xr

from ofs_skill.model_processing import model_source
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
        logger.info('Interpolating depths for %s, time %s of %s', var, str(i),
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
