"""
Created on Wed Apr  1 11:02:36 2026

@author: PWL
"""

import os
import sys
from datetime import datetime, timedelta
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from ofs_skill.model_processing import model_source
from ofs_skill.obs_retrieval import utils

_ZETA_MISSING_WARNED = False
_TIME_FALLBACK_WARNED = False
_MJD_EPOCH = '1858-11-17'
_MAX_SIGLAY_ROWS = 300
_MAX_X_COLS = 300
_TIME_SLIDER_CAP = 55
_X_SPACING_KM = 0.25


def _decode_time(ds, logger):
    """
    Decode the OBC dataset's time coordinate into a list of ``datetime`` objects.

    Resolution order:
    1. If ``ds['time']`` has a CF ``units`` attr, use
       ``xr.coding.times.decode_cf_datetime``.
    2. Else if FVCOM's ``Itime`` (integer days since MJD epoch) and ``Itime2``
       (milliseconds past midnight) are present, compose them.
    3. Else fall back to the legacy MJD assumption
       (``_MJD_EPOCH`` + days), logging a one-shot WARNING.

    Args:
        ds (xarray.Dataset): OBC dataset with a ``time`` variable.
        logger (logging.Logger): Logger for fallback warnings.

    Returns:
        list[datetime.datetime]: One entry per time step.
    """
    global _TIME_FALLBACK_WARNED
    units = ds['time'].attrs.get('units') if 'time' in ds else None
    calendar = ds['time'].attrs.get('calendar', 'standard') if 'time' in ds else 'standard'

    if units:
        try:
            decoded = xr.coding.times.decode_cf_datetime(
                np.asarray(ds['time'].values), units=units, calendar=calendar)
            return [pd.Timestamp(t).to_pydatetime() for t in np.asarray(decoded)]
        except (ValueError, TypeError) as e_x:
            logger.warning(
                'CF time decode failed (%s); trying Itime/Itime2 or MJD fallback.',
                e_x)

    if 'Itime' in ds.variables and 'Itime2' in ds.variables:
        epoch = datetime.strptime(_MJD_EPOCH, '%Y-%m-%d')
        itime = np.asarray(ds['Itime']).astype('int64')
        itime2 = np.asarray(ds['Itime2']).astype('int64')
        return [epoch + timedelta(days=int(d), milliseconds=int(ms))
                for d, ms in zip(itime, itime2)]

    if not _TIME_FALLBACK_WARNED:
        logger.warning(
            "OBC time has no CF 'units' and no Itime/Itime2; "
            'falling back to MJD epoch %s with days unit.', _MJD_EPOCH)
        _TIME_FALLBACK_WARNED = True
    epoch = datetime.strptime(_MJD_EPOCH, '%Y-%m-%d')
    return [epoch + timedelta(days=float(t)) for t in np.asarray(ds['time'])]


def parameter_validation(prop, logger):
    """
    Validates input parameters and initializes the directory structure for processing.

    Performs the following checks:
    - Validates that the 'StartDate_full' matches the ISO format.
    - Verifies the existence of the OFS extents directory and the specific
      shapefile for the model.
    - Automatically creates necessary subdirectories for control files,
      observations, model data, skill stats, and visuals.
    - Sets the full path to the OBC input files.

    Args:
        prop (ModelProperties): Configuration object to be validated and updated.
        logger (logging.Logger): Logger for error reporting.

    Raises:
        SystemExit: If date format is invalid or required directories/files
            are missing.
    """
    dir_params = utils.Utils().read_config_section('directories', logger)

    # Model source validation
    if 'roms' in prop.model_source:
        logger.error('ROMS OBC plotting is not currently supported! '
                     'Try an FVCOM OFS.')
        sys.exit(-1)

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

    # Set up directory tree. Only the visuals dir is consumed by the OBC
    # pipeline (plot_fvcom_obc writes HTML there); the 1D skill-assessment
    # dirs are unused here.
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

def load_obc_file(prop, logger):
    """
    Constructs the file path and loads the OBC netCDF file into an xarray Dataset.

    The path is built using the model's 'ofs' name, cycle, and start date.
    Time decoding is disabled specifically for FVCOM models to handle
    custom time formats.

    Args:
        prop (ModelProperties): Object containing model name, cycle, and path info.
        logger (logging.Logger): Logger for error reporting.

    Returns:
        xarray.Dataset: The loaded netCDF data.
    """
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
        ds = xr.open_dataset(file_path, decode_times=decode)
    except FileNotFoundError:
    # Catch the FileNotFoundError if the file is not found
        logger.error('Error: The OBC file was not found. Quitting...')
        sys.exit(-1)
    except (OSError, ValueError, KeyError, RuntimeError) as e_x:
    # Narrowed: netCDF / xarray open failures; re-raise other programming errors
        logger.exception('Failed to open OBC dataset: %s', e_x)
        sys.exit(-1)

    return ds

def mask_distance_gaps(x_orig, x_interp, z, logger):
    """
    Identifies large spatial gaps in the original boundary coordinates and
    masks the interpolated data with NaNs in those regions.

    This prevents the plotting of artificial "interpolated" data across
    discontinuous boundary segments.

    Args:
        x_orig (array-like): Original cumulative distance labels.
        x_interp (array-like): The new interpolated x-grid.
        z (numpy.ndarray): The 3D data array (time, depth, distance) to mask.
        logger (logging.Logger): Logger for status updates.

    Returns:
        numpy.ndarray: The masked data array.
    """
    # Robust threshold: median + 3*IQR is stable when one real gap dominates
    # the distribution (percentile-based thresholds degenerate there) and still
    # flags multiple similarly-sized gaps on unstructured FVCOM boundaries.
    diffs = np.diff(x_orig)
    q75 = np.nanpercentile(diffs, 75)
    q25 = np.nanpercentile(diffs, 25)
    gap_length = np.nanmedian(diffs) + 3.0 * (q75 - q25)
    dx = np.flatnonzero(diffs > gap_length)  # 1-D indices
    gaps = np.array([x_orig[dx], x_orig[dx + 1]])  # shape (2, N)

    # now loop and fill gaps with NaNs
    for i in range(gaps.shape[1]):
        to_fill = np.flatnonzero(
            (x_interp > gaps[0, i]) & (x_interp < gaps[1, i]))
        z[:, :, to_fill] = np.nan

    return z

def transform_to_z(ds, var, x_labels, logger):
    """
    Transforms model data from sigma layers to vertical z-coordinates (depth)
    and interpolates onto a regular spatial grid.

    The process involves:
    1. Calculating a new vertical z-grid based on maximum depth.
    2. Interpolating the variable from sigma layers to fixed depth levels.
    3. Interpolating the spatial dimension to a regular horizontal spacing (0.25km).
    4. Reducing temporal and spatial resolution if necessary to optimize for
       web-based plotting.

    Args:
        ds (xarray.Dataset): The source dataset.
        var (str): The name of the variable to transform (e.g., 'temp').
        x_labels (array-like): Cumulative distances along the boundary.
        logger (logging.Logger): Logger for status updates.

    Returns:
        tuple: (z_mask, ref_depth, x_grid) containing the 3D data array,
            the depth axis, and the horizontal distance axis.
    """
    # Pull bathymetry, sigma layers, and free-surface elevation up front.
    # FVCOM convention: h positive-down; siglay in [-1, 0] with -1 at bottom,
    # 0 at surface. siglay may be stored as (siglay,) or (siglay, node).
    h = np.asarray(ds['h'])
    siglay_arr = np.asarray(ds['siglay'])
    n_sig = siglay_arr.shape[0]
    n_node = len(ds['lon'])
    n_time = len(ds['time'])

    zeta_name = next(
        (v for v in ds.data_vars if v in ('zeta', 'elevation')), None)
    if zeta_name is None:
        global _ZETA_MISSING_WARNED
        if not _ZETA_MISSING_WARNED:
            logger.warning(
                "No 'zeta'/'elevation' variable in OBC file; "
                'assuming free surface = 0 for sigma->z transform.')
            _ZETA_MISSING_WARNED = True
        zeta_arr = np.zeros((n_time, n_node))
    else:
        zeta_arr = np.asarray(ds[zeta_name])

    # Set new siglay length from max depth & min dz
    siglay_len = int(np.ceil(np.nanmax(h) / np.nanmin(h / n_sig)))
    # Need to reduce spatial and temporal resolution for plotting
    # with a time slider!
    if siglay_len > _MAX_SIGLAY_ROWS:
        siglay_len = int(siglay_len / (np.ceil(siglay_len / _MAX_SIGLAY_ROWS)))
    time_iterator = 1
    if n_time > _TIME_SLIDER_CAP:
        time_iterator = int(np.ceil(n_time / _TIME_SLIDER_CAP))

    ref_depth = np.linspace(0, np.nanmax(h), siglay_len, endpoint=True)

    # Sigma->z transform (per node j, per time t):
    #   z_local = siglay[k,j] * (h[j] + zeta[t,j]) + zeta[t,j]
    # z_local is negative-up (0 at free surface, -(h+zeta) at bottom). We flip
    # sign to positive-down so it aligns with ref_depth. This preserves
    # non-uniform sigma stretching (generalized/tanh/s-coord) and tide-driven
    # surface elevation, unlike the prior layer-index proxy.
    var_arr = np.asarray(ds[var])
    z_depth_all = []
    for i in range(0, n_time, time_iterator):
        logger.info('Interpolating depths for %s, time %s of %s', var, str(i),
                    str(n_time))
        z_depth_single = np.full((siglay_len, n_node), np.nan)
        for j in range(n_node):
            sig_j = siglay_arr[:, j] if siglay_arr.ndim == 2 else siglay_arr
            total_depth = h[j] + zeta_arr[i, j]
            depth_pd = -(sig_j * total_depth + zeta_arr[i, j])
            order = np.argsort(depth_pd)
            depth_sorted = depth_pd[order]
            vals_sorted = var_arr[i, :, j][order]
            interp = np.interp(ref_depth, depth_sorted, vals_sorted,
                               left=np.nan, right=np.nan)
            # Mask above free surface and below seafloor
            interp[ref_depth < -zeta_arr[i, j]] = np.nan
            interp[ref_depth > total_depth] = np.nan
            z_depth_single[:, j] = interp
        z_depth_all.append(z_depth_single)

    z_depth_all = np.stack(z_depth_all)

    # Now loop through each time and each row and assign z values
    # across each depth
    dist_len = int(np.ceil(np.nanmax(x_labels) / _X_SPACING_KM))
    if dist_len > _MAX_X_COLS:
        dist_len = int(dist_len / (np.ceil(dist_len / _MAX_X_COLS)))
    x_grid = np.linspace(0, x_labels[-1], dist_len)
    # Make new z array for the interpolated x-axis length
    z_dist_single = np.array(np.full((z_depth_all.shape[1], len(x_grid)),
                         np.nan))
    z_dist_all = []
    for i in range(z_depth_all.shape[0]):
        for j in range(z_depth_all.shape[1]):
            z_dist_single[j, :] = np.interp(
                x_grid, x_labels, z_depth_all[i, j, :])
        z_dist_all.append(z_dist_single)
        z_dist_single = np.array(np.full((z_depth_all.shape[1], len(x_grid)),
                             np.nan))
    z_dist_all = np.stack(z_dist_all)

    # Finally we need to mask distance gaps with NaNs
    z_mask = mask_distance_gaps(x_labels, x_grid, z_dist_all, logger)

    # New ref_depth
    ref_depth = np.linspace(
        0, np.nanmax(np.array(ds['h'])), z_dist_all.shape[1], endpoint=True)

    return z_mask, ref_depth, x_grid

def make_x_labels(ds, logger):
    """
    Calculates the cumulative distance along the open boundary nodes.

    Iterates through the latitude and longitude of the boundary nodes and
    uses the Haversine formula to determine the distance between
    consecutive points.

    Args:
        ds (xarray.Dataset): Dataset containing 'lat' and 'lon' coordinates.
        logger (logging.Logger): Logger for status updates.

    Returns:
        numpy.ndarray: An array of cumulative distances in kilometers,
            starting from 0.
    """
    x_labels = []
    for i in range(len(ds['lat'])-1):
        x_labels.append(haversine(np.array(ds['lat'])[i],
                                  np.array(ds['lon'])[i],
                                  np.array(ds['lat'])[i+1],
                                  np.array(ds['lon'])[i+1]))
    x_labels = np.cumsum(np.concatenate(([0], x_labels)))
    return x_labels
