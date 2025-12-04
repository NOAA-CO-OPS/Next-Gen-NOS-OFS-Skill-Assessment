"""
-*- coding: utf-8 -*-

Documentation for file processing_2d.py

Directory Location:   /path/to/ofs_dps/bin/visualization

Technical Contact(s): Name:  AJK

Abstract:

    This file contains functions needed to output leaflet contour plot CSV files for visualization
    on the web interface.

Language:  Python 3.11

Estimated Execution Time:

Author Name:  AJK       Creation Date:  09/2024

Revisions:
    Date          Author             Description
    12/2024       AJK                Updated interpolation routine using pyinterp
    03/2025       AJK                Renamed 'processing_2d' from 'leaflet_contour'
    03/2025       AJK                Updates for intake
"""
from __future__ import annotations

import json
import logging.config
import math
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.path as mplPath
import numpy as np
import pandas as pd
import pyinterp
import shapefile  # Standard library for .shp files (pip install pyshp)
from global_land_mask import globe
from netCDF4 import Dataset
from obs_retrieval import utils
from scipy.spatial import cKDTree

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))


def param_val(netcdf_file_sat):
    '''
    This function validates the inputs and creates
    the missing directories
    '''
    # Check logger
    logger = None
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent / log_config_file
        ).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Parsing to leaflet JSON file ---')

    outdir = []
    # outdir.append(os.path.join(os.path.dirname(os.path.dirname(netcdf_file_model)),'2d'))
    outdir.append(
        os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(netcdf_file_sat)),
            ), 'model', '2d',
        ),
    )
    logger.info('--- Checking 2d model output dir: %s ---', outdir[0])
    outdir.append(
        os.path.join(
            os.path.dirname(
                os.path.dirname(netcdf_file_sat),
            ), '2d',
        ),
    )
    logger.info('--- Checking 2d satellite output dir: %s ---', outdir[1])
    os.makedirs(outdir[0], exist_ok=True)
    os.makedirs(outdir[1], exist_ok=True)
    return (logger, outdir)


def parse_leaflet_json(model, netcdf_file_sat, prop1):
    '''
    This takes the lazily loaded model file and path to concatonated satellite
    netcdf  and outputs leaflet csv files for web UI visualization.
    '''
    [logger, outdir] = param_val(netcdf_file_sat)

    # Read and process netcdf
    # logger.info("--- Reading NETCDF files ---")

    # model = Dataset(netcdf_file_model, 'r')
    ocean_dtime = None
    lon_grid = None
    lat_grid = None
    sst_in_model = None
    if prop1.model_source == 'roms':
        logger.info('--- '+'ROMS: Calculating regular grid'+' ---')
        mask = model.variables['mask_rho'][:][0, :, :].astype(bool).compute()
        lons = np.asarray(model.variables['lon_rho'][:])
        lats = np.asarray(model.variables['lat_rho'][:])
        [lat_grid, lon_grid] = resample_latlon(lats[mask], lons[mask], prop1)
        ocean_dtime = np.array(model['ocean_time'], dtype='datetime64[ns]')
        sst_in_model = np.squeeze(
            np.asarray(
                model.variables['temp'][:][:, -1, :, :],
            ),
        )
        ssh_in_model = np.squeeze(np.asarray(model.variables['zeta'][:]))
        # 0 index for surface level
        logger.info('--- '+'ROMS: finished calculating regular grid'+' ---')

    elif prop1.model_source == 'fvcom':
        logger.info('--- '+'FVCOM: Calculating regular grid'+' ---')
        lons = np.asarray(model.variables['lon'][:])
        lats = np.asarray(model.variables['lat'][:])
        [lat_grid, lon_grid] = resample_latlon(lats, lons, prop1)
        ocean_dtime = np.array(model['time'], dtype='datetime64[ns]')
        # 0 index for surface level
        sst_in_model = np.asarray(model.variables['temp'][:][:, 0, :])
        # 0 index for surface level
        ssh_in_model = np.asarray(model.variables['zeta'][:])
        logger.info('--- '+'FVCOM: finished calculating regular grid'+' ---')

    ocean_dtime = [
        dt.astype('datetime64[s]').astype(datetime)
        for dt in ocean_dtime
    ]

    # Check if lons in -180 to 180 or 0 to 360
    lon_grid = normalize_longitudes(lon_grid)
    lons = normalize_longitudes(lons)

    try:
        if 'sport' in str(netcdf_file_sat):
            logger.info(
                '--- '+'Satellite: Processing SPoRT observations'+' ---',
            )
            nc_sat = Dataset(netcdf_file_sat, 'r')
            lons_sport = np.asarray(nc_sat.variables['lon'][:])
            lats_sport = np.asarray(nc_sat.variables['lat'][:])
            lons_sport, lats_sport = np.meshgrid(lons_sport, lats_sport)
            dtime_sport = pd.to_datetime(
                nc_sat['time'][:], unit='s', origin='1981-01-01',
            )
            sst_in_sport = nc_sat['analysed_sst'][:]-273.15
            for i, t in enumerate(dtime_sport):
                out_file_sport = os.path.join(
                    outdir[1], str(
                        prop1.ofs+'_' +
                        t.strftime('%Y%m%d-%Hz')+'_sst_SPoRT.json',
                    ),
                )
                out_file_sportL = os.path.join(
                    outdir[1], str(
                        prop1.ofs+'_' +
                        t.strftime('%Y%m%d-%Hz')+'_lnc_SPoRT.json',
                    ),
                )
                sst_sat_sport = interp_grid(
                    lons_sport.ravel(),
                    lats_sport.ravel(),
                    sst_in_sport[i, :, :].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )

                latency_sat_sport = interp_grid(
                    lons_sport.ravel(),
                    lats_sport.ravel(),
                    nc_sat['latency'][:][i, :, :].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )

                logger.info(
                    '--- Writing 2D leaflet JSON file to: %s ---', out_file_sport,
                )
                write_2d_arrays_to_json(
                    lat_grid, lon_grid,
                    np.round(sst_sat_sport, decimals=2),
                    out_file_sport,
                )

                write_2d_arrays_to_json(
                    lat_grid, lon_grid,
                    np.round(latency_sat_sport, decimals=2),
                    out_file_sportL,
                )
            return 'Finished SPoRT processing'
    except UnboundLocalError:
        logger.error('Problem processing SPoRT satellite file.')
        sys.exit(1)
    except FileNotFoundError:
        logger.error('SPoRT satellite data not found! Run '
                     'get_satellite_observations.py to '
                     'download it.')
        sys.exit(1)

    # process satellite netcdf
    try:
        logger.info('--- '+'Satellite: Processing l3c observations'+' ---')
        nc_sat = Dataset(netcdf_file_sat, 'r')
        lons_sat = np.asarray(nc_sat.variables['lon'][:])
        lats_sat = np.asarray(nc_sat.variables['lat'][:])
        lons_sat, lats_sat = np.meshgrid(lons_sat, lats_sat)
        [ref_year, ref_month, ref_day] = [
            int(i) for i in
            nc_sat.variables['time'].comment.split()[-2].split('-')
        ]
        [ref_hour, ref_min, ref_sec] = [
            int(i) for i in
            nc_sat.variables['time'].comment.split()[-1].split(':')
        ]
        ref_dtime = datetime(
            ref_year, ref_month, ref_day,
            ref_hour, ref_min, ref_sec,
        )
        sat_dtime = [
            ref_dtime+timedelta(seconds=int(isec))
            for isec in nc_sat.variables['time'][:]
        ]
        sst_in_sat = np.asarray(nc_sat.variables['sea_surface_temperature'][:])\
            - 273.15  # convert from kelvin to celcius
        nc_sat.close()
    except FileNotFoundError:
        logger.error('L3C Satellite data not found! Run '
                     'get_satellite_observations.py to '
                     'download it.')
        sys.exit(1)

    ### This section does the daily averages ###
    # Compute and write daily avg for model
    dtime = datetime.fromisoformat(
        prop1.start_date_full.replace('Z', '+00:00'),
    )
    dtime_end = datetime.fromisoformat(
        prop1.end_date_full.replace('Z', '+00:00'),
    )
    if abs(dtime-dtime_end) == timedelta(days=1):
        logger.info('Computing daily SST average for model ...')
        out_file_model_sst = os.path.join(
            outdir[0], str(
                prop1.ofs+'_' +
                dtime.strftime('%Y%m%d')+'-daily_sst_model.' +
                prop1.whichcast+'.json',
            ),
        )
        out_file_model_ssh = os.path.join(
            outdir[0], str(
                prop1.ofs+'_' +
                dtime.strftime('%Y%m%d')+'-daily_ssh_model.' +
                prop1.whichcast+'.json',
            ),
        )
        sst_model_avg = np.nanmean(sst_in_model, axis=0)
        logger.info('Computing daily SSH average for model ...')
        ssh_model_avg = np.nanmean(ssh_in_model, axis=0)
        try:
            if prop1.model_source == 'fvcom':
                logger.info('--- Resampling FVCOM grid ---')
                sst_model_ai = interp_grid(
                    lons, lats, sst_model_avg, lon_grid, lat_grid, logger, prop1,
                )
                ssh_model_ai = interp_grid(
                    lons, lats, ssh_model_avg, lon_grid, lat_grid, logger, prop1,
                )
            else:  # roms
                logger.info('--- Resampling ROMS grid ---')
                sst_model_ai = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    sst_model_avg[:, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssh_model_ai = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    ssh_model_avg[:, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_sst,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(sst_model_ai, decimals=2),
                out_file_model_sst,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssh,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssh_model_ai, decimals=2),
                out_file_model_ssh,
            )
        except UnboundLocalError:
            logger.error('Problem writing daily averaged model JSON file.')

    # Compute and write daily avg for l3c
    if abs(sat_dtime[0]-sat_dtime[-1]) == timedelta(days=1):
        logger.info('Computing daily SST average for satellite ...')
        out_file_sat = os.path.join(
            outdir[1], str(
                prop1.ofs+'_' +
                sat_dtime[0].strftime('%Y%m%d')+'-daily_sst_l3c.json',
            ),
        )

        sst_sat_avg = np.nanmean(sst_in_sat, axis=0)
        try:
            sst_sat_ai = interp_grid(
                lons_sat.ravel(),
                lats_sat.ravel(),
                sst_sat_avg.ravel(),
                lon_grid, lat_grid, logger, prop1,
            )

            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_sat,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid,
                np.round(sst_sat_ai, decimals=2),
                out_file_sat,
            )
        except UnboundLocalError:
            logger.error('Problem writing daily averaged satellite JSON file.')

    # Loop over times and write out leaflet csv files
    while dtime <= datetime.fromisoformat(prop1.end_date_full.replace('Z', '+00:00')):
        # for i, dtime in enumerate(ocean_dtime):
        # Find the index of the nearest datetime
        i = next(
            (
                i for i, dt in enumerate(ocean_dtime)
                if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)
            ), -1,
        )
        isat = next(
            (
                i for i, dt in enumerate(sat_dtime)
                if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)
            ), -1,
        )
        if i == -1 or isat == -1:  # make sure expected times match
            ni = min(
                enumerate(ocean_dtime), key=lambda x:
                abs(x[1] - dtime.replace(tzinfo=None)),
            )[0]
            ni_sat = min(
                enumerate(sat_dtime), key=lambda x:
                abs(x[1] - dtime.replace(tzinfo=None)),
            )[0]
            logger.info(
                f'--- Unexpected time error: requested time = %s {dtime},\
                    model_time = %s  ---', dtime, ocean_dtime[ni],
            )
            logger.info(
                '--- Unexpected time error: requested time = %s,\
                    satellite_time = %s  ---', dtime, sat_dtime[ni_sat],
            )
        else:
            out_file_model_sst = os.path.join(
                outdir[0], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_sst_model.' +
                    prop1.whichcast+'.json',
                ),
            )
            out_file_model_ssh = os.path.join(
                outdir[0], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_ssh_model.' +
                    prop1.whichcast+'.json',
                ),
            )

            if prop1.model_source == 'fvcom':
                logger.info('--- Resampling FVCOM grid ---')
                sst_model = interp_grid(
                    lons, lats, sst_in_model[i,
                                             :], lon_grid, lat_grid, logger, prop1,
                )
                ssh_model = interp_grid(
                    lons, lats, ssh_in_model[i,
                                             :], lon_grid, lat_grid, logger, prop1,
                )

            else:  # roms
                logger.info('--- Resampling ROMS grid ---')
                sst_model = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    sst_in_model[i, :, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssh_model = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    ssh_in_model[i, :, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )

            out_file_sat = os.path.join(
                outdir[1], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_sst_l3c.json',
                ),
            )

            i = next(
                (
                    i for i, dt in enumerate(sat_dtime)
                    if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)
                ), -1,
            )

            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_sst,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(sst_model, decimals=2),
                out_file_model_sst,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssh,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssh_model, decimals=2),
                out_file_model_ssh,
            )

            try:
                sst_sat = interp_grid(
                    lons_sat.ravel(),
                    lats_sat.ravel(),
                    sst_in_sat[i, :, :].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )

                logger.info(
                    '--- Writing 2D leaflet JSON file to: %s ---', out_file_sat,
                )
                write_2d_arrays_to_json(
                    lat_grid, lon_grid,
                    np.round(sst_sat, decimals=2),
                    out_file_sat,
                )
            except UnboundLocalError:
                logger.error('Problem writing satellite JSON file.')

            # logger.info("--- Finished writing JSON file %s/%s ---", str(i+1),str(len(ocean_dtime)))
        dtime += timedelta(hours=1)
        # assuming hourly data. If we update to run
        # 2d SA on higher temporal resolution, we'll need
        # to change this.

    # Close the NetCDF file

    logger.info('--- Finished leaflet JSON processing ---')


def interp_grid(lons, lats, sst, lon_grid, lat_grid, logger, prop1):
    '''
    Interpolation routine. Uses pyinterp package inverse distance
    weighting (IDW) interpolation.
    Based on:
    https://cnes.github.io/pangeo-pyinterp/auto_examples/pangeo_unstructured_grid.html
    inputs:
     - lons: original grid longitude, must be 1d
     - lats: original grid latitude, must be 1d
     - sst: original sst to be interpolated
     - lon_grid: longitude grid to be interpolated too
     - lat_grid: latitude grid to be interpolated too
     - logger: logger for output messages
     outputs:
     - sst_out: interpolated sst
    '''
    logger.info('--- Resampling grid ---')
    mesh = pyinterp.RTree()
    mesh.packing(np.vstack((lons, lats)).T, sst)

    # --- Estimate source spacing (in meters) ---
    lon_factor = np.cos(np.radians(np.median(lats))) * 111_320
    lat_factor = 110_540
    xy = np.column_stack((lons * lon_factor, lats * lat_factor))
    tree = cKDTree(xy)
    # Sample only 10k points max for speed
    sample_idx = np.random.choice(
        len(xy), size=min(10_000, len(xy)), replace=False,
    )
    dists, _ = tree.query(xy[sample_idx], k=6)
    median_spacing = np.median(dists[:, 1:])
    base_radius = 3.0 * median_spacing

    logger.info(
        f'Estimated base spacing {median_spacing:.1f} m, radius={base_radius:.1f} m',
    )

    # --- First pass (strict) ---
    targets = np.vstack((lon_grid.ravel(), lat_grid.ravel())).T
    sst_out, _ = mesh.inverse_distance_weighting(
        targets,
        within=True,
        radius=base_radius,
        k=8,
        num_threads=0,
    )
    sst_out = sst_out.reshape(lon_grid.shape)

    # --- Second pass (fill remaining gaps) ---
    mask = np.isnan(sst_out)
    if np.any(mask):
        logger.info(f'Filling {mask.sum()} NaN values with larger radius...')
        sst_fill, _ = mesh.inverse_distance_weighting(
            np.vstack((lon_grid[mask], lat_grid[mask])).T,
            #targets[mask],
            within=False,
            #radius=100 * base_radius,
            radius=None,
            k=16,
            num_threads=0,
        )
        sst_out[mask] = sst_fill

    # ==========================================
    # --- Apply Land Mask ---
    # ==========================================
    logger.info('--- Applying Land Mask ---')

    # globe.is_land returns True for land, False for water (and oceans)
    # Pass the grid coordinates to check every point
    is_land = globe.is_land(lat_grid, lon_grid)

    # Set all land points to NaN (making them transparent in plotting)
    sst_out[is_land] = np.nan

    # ==========================================
    # --- Apply Shapefile Mask ---
    # ==========================================
    logger.info('--- Applying OFS Shapefile Mask ---')
    grid_shapefile=Path(__file__).parents[2] / 'ofs_extents' / f'{prop1.ofs}.shp'
    logger.info('Shapefile path: %s',grid_shapefile)

    # Define cache file path
    mask_cache_file = str(grid_shapefile).replace('.shp', '_mask.npy')
    mask_to_apply = None

    # Try to load from cache
    if os.path.exists(mask_cache_file):
        logger.info(f'Loading cached domain mask from {mask_cache_file}')
        mask_to_apply = np.load(mask_cache_file)

    # If not cached, calculate it
    if mask_to_apply is None:
        # Note: We pass flattened coordinates to the generator
        # targets[:,0] is likely the longitudes
        mask_to_apply = generate_domain_mask_bool(
            targets[:, 0],
            targets[:, 1],
            str(grid_shapefile),
            logger
        )
        # Save for future use
        np.save(mask_cache_file, mask_to_apply)

    # SAFETY RESHAPE (Fixes the axis error)
    # The generator returns a 1D mask. Your sst_out is 2D.
    # This forces them to align.
    if mask_to_apply.shape != sst_out.shape:
        mask_to_apply = mask_to_apply.reshape(sst_out.shape)

    logger.info(f'Applying domain mask from {mask_cache_file}')
    # Apply the mask
    sst_out[~mask_to_apply] = np.nan

    return sst_out


def write_2d_arrays_to_json(x, y, z, filename):
    """
    Write 2D arrays to a JSON file with keys 'x', 'y', and 'z'.

    Parameters:
    x (2D array): 2D array of X values
    y (2D array): 2D array of Y values
    z (2D array): 2D array of Z values (with None or NaN for missing data)
    filename (str): Name of the JSON file to write (default is 'output.json')
    """

# Convert numpy arrays to lists if they are not lists already
    x_list = x.tolist() if isinstance(x, np.ndarray) else x
    y_list = y.tolist() if isinstance(y, np.ndarray) else y
    z_list = z.tolist() if isinstance(z, np.ndarray) else z

# Ensure perimeter of 'z' is set to null
    rows, cols = len(z_list), len(z_list[0])
    for i in range(rows):
        for j in range(cols):
            if i == 0 or i == rows - 1 or j == 0 or j == cols - 1:
                # Replace any non-null perimeter value with None
                z_list[i][j] = np.nan

    # Replace None or np.nan values in z with `null` for JSON compatibility
    z_list = [
        [None if np.isnan(val) else val for val in row]
        for row in z_list
    ]
    z_list = [
        [
            None if val is not None and int(val) > 101 and int(val) < -101
            else val for val in row
        ] for row in z_list
    ]

    # Flatten z_list to compute val_min and val_max, ignoring None
    flattened_z = [val for row in z_list for val in row if val is not None]
    if flattened_z:
        val_min = min(flattened_z)
        val_max = max(flattened_z)
    else:
        val_min = None
        val_max = None

    # Create dictionary structure
    json_data = {
        'lats': x_list,
        'lons': y_list,
        'sst': z_list,
        'val_min': val_min,
        'val_max': val_max,
    }

    # Write the data to a JSON file with manual formatting for arrays
    with open(filename, 'w', encoding='utf-8') as json_file:
        json_file.write('{\n')
        keys = list(json_data.keys())
        for idx, key in enumerate(keys):
            value = json_data[key]
            if isinstance(value, list):
                json_file.write(f'  "{key}": [\n')
                for i, row in enumerate(value):
                    row_str = json.dumps(row).replace(
                        'null', 'null',
                    ).replace('None', 'null')
                    if i == len(value) - 1:
                        json_file.write(f'    {row_str}\n')
                    else:
                        json_file.write(f'    {row_str},\n')
                json_file.write('  ]')
            else:
                value_str = json.dumps(value)
                json_file.write(f'  "{key}": {value_str}')
            if idx < len(keys) - 1:
                json_file.write(',\n')
            else:
                json_file.write('\n')
        json_file.write('}\n')


def resample_latlon(lats, lons, prop1):
    '''
    This function resamples lat and lon to a regular grid
    '''
    if prop1.ofs == 'wcofs':
        deg_res_resample = 0.04  # degree resolution for resample regular grid
    elif prop1.ofs == 'ngofs2':
        deg_res_resample = 0.04  # degree resolution for resample regular grid
    else:
        deg_res_resample = 0.01  # degree resolution for resample regular grid

    lat_min = np.round(np.min(lats), decimals=2)-.25
    lat_max = np.round(np.max(lats), decimals=2)+.25
    lon_min = np.round(np.min(lons), decimals=2)-.25
    lon_max = np.round(np.max(lons), decimals=2)+.25

    lon_grid, lat_grid = np.meshgrid(
        np.arange(lon_min, lon_max, deg_res_resample),
        np.arange(lat_min, lat_max, deg_res_resample),
    )
    return np.round(lat_grid, decimals=2), np.round(lon_grid, decimals=2)

def normalize_longitudes(longitudes):
    """
    Detects longitudes > 180 and converts them to the -180/180 format.
    Args:
        longitudes (list, tuple, or np.array): Input longitude values.
    Returns:
        np.array: Array of normalized longitudes.
    """
    # Create a copy to avoid modifying the original data in place
    lons = np.array(longitudes, dtype=float).copy()
    # CONDITION: Find indices where longitude is > 180
    mask = lons > 180
    # ACTION: Subtract 360 only from those specific indices
    lons[mask] -= 360
    return lons

def generate_domain_mask_bool(grid_lon, grid_lat, shapefile_path, logger):
    """
    Generates a boolean mask (True=Valid, False=Masked) based on a Shapefile.

    Optimizations:
    1. Global Bounding Box: Instantly drops points far outside the domain.
    2. Vertex Decimation: Uses every Nth vertex for the polygon check (speeds up math 5x).
    3. Chunking: Processes points in batches to prevent UI hangs and show progress.

    Args:
        grid_lon (np.array): Longitude array (1D or 2D).
        grid_lat (np.array): Latitude array (1D or 2D).
        shapefile_path (str): Path to the .shp file.

    Returns:
        np.array: Boolean mask with same shape as input grid (True = Inside Domain).
    """
    t0 = time.time()
    logger.info(f'Generating new domain mask from: {shapefile_path}')

    # 1. Load Shapefile
    sf = shapefile.Reader(shapefile_path)
    shapes = sf.shapes()

    # 2. Calculate Global Bounding Box (Union of all shapes)
    # This allows us to quickly discard points that are nowhere near the coast
    if not shapes:
        logger.error('Error: Shapefile is empty.')
        return np.ones(grid_lon.shape, dtype=bool)

    total_bbox = list(shapes[0].bbox)
    for shape in shapes[1:]:
        bbox = shape.bbox
        total_bbox[0] = min(total_bbox[0], bbox[0]) # Min Lon
        total_bbox[1] = min(total_bbox[1], bbox[1]) # Min Lat
        total_bbox[2] = max(total_bbox[2], bbox[2]) # Max Lon
        total_bbox[3] = max(total_bbox[3], bbox[3]) # Max Lat

    # 3. Prepare Grid Points
    # Flatten inputs to (N, 2) for vectorization
    points = np.column_stack((grid_lon.flatten(), grid_lat.flatten()))
    n_total_points = points.shape[0]

    # Initialize mask as FALSE (assume everything is masked/invalid initially)
    final_mask_flat = np.zeros(n_total_points, dtype=bool)

    # 4. Global Box Filter (Fast Pre-check)
    # Only select points strictly inside the rectangular extents of the shapefile
    in_global_box = (
        (points[:, 0] >= total_bbox[0]) &
        (points[:, 0] <= total_bbox[2]) &
        (points[:, 1] >= total_bbox[1]) &
        (points[:, 1] <= total_bbox[3])
    )

    # Get indices of points that passed the first test
    indices_to_check = np.where(in_global_box)[0]
    n_check = len(indices_to_check)
    logger.info(f'Global BBox reduced points to check from {n_total_points} to {n_check}')

    # 5. Prepare Polygon Paths with Decimation
    # DECIMATION=5 means we skip 4 vertices and take the 5th.
    # High-res coastlines have too many vertices; this speeds it up ~5x with minimal accuracy loss.
    DECIMATION = 5
    paths = []

    for s in shapes:
        # s.parts contains the starting index of each "part" (island/ring)
        # e.g., [0, 1050, 2400]
        # We must append the total length to loop correctly
        parts_indices = list(s.parts) + [len(s.points)]

        # Loop over each distinct polygon part (island/ring)
        for i in range(len(parts_indices) - 1):
            start = parts_indices[i]
            end = parts_indices[i+1]

            # Extract the points for JUST this island
            part_points = s.points[start:end]

            # Create Path (Apply Decimation ONLY to this part)
            # We only decimate if the part is large enough to matter
            if len(part_points) > 100:
                # Standard Python slicing for decimation
                decimated_points = part_points[::DECIMATION]
                # Ensure the polygon is closed (append start point if needed)
                # (Matplotlib usually handles unclosed paths, but explicit is better)
                if decimated_points[0] != decimated_points[-1]:
                    decimated_points.append(decimated_points[0])
                paths.append(mplPath.Path(decimated_points))
            else:
                paths.append(mplPath.Path(part_points))

    # 6. Chunked Processing
    # Process in batches to avoid freezing and enable progress printing
    chunk_size = 50000
    n_chunks = math.ceil(n_check / chunk_size)

    if n_chunks > 0:
        logger.info(f'Processing {n_check} points in {n_chunks} chunks (Decimation={DECIMATION})...')

        for i in range(n_chunks):
            # Define chunk range
            start = i * chunk_size
            end = min((i + 1) * chunk_size, n_check)

            # Get the specific points to check
            chunk_indices = indices_to_check[start:end]
            chunk_points = points[chunk_indices]

            # Check this chunk against ALL polygons
            chunk_mask = np.zeros(len(chunk_indices), dtype=bool)

            for poly_path in paths:
                # The expensive math operation
                is_inside = poly_path.contains_points(chunk_points)
                # Logical OR: if inside ANY polygon, it is valid
                chunk_mask = np.logical_or(chunk_mask, is_inside)

            # Map results back to the master flat mask
            final_mask_flat[chunk_indices] = chunk_mask

            # Progress Print (Every 5 chunks or at end)
            if (i+1) % 5 == 0 or (i+1) == n_chunks:
                percent = ((i + 1) / n_chunks) * 100
                print(f'\r  - Masking progress: {percent:.0f}%', end='')
                sys.stdout.flush()

    # 7. Reshape to match original input dimensions
    # This ensures 2D inputs return a 2D mask
    mask_reshaped = final_mask_flat.reshape(grid_lon.shape)

    logger.info(f'Mask generation complete in {time.time() - t0:.2f}s')
    return mask_reshaped
