
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

import logging
import logging.config
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from netCDF4 import Dataset
import pyinterp
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils

def param_val(netcdf_file_sat):
    '''
    This function validates the inputs and creates
    the missing directories
    '''
    # Check logger
    logger = None
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s",config_file)
        logger.info("Using log config %s",log_config_file)

    logger.info("--- Parsing to leaflet JSON file ---")

    outdir = []
    #outdir.append(os.path.join(os.path.dirname(os.path.dirname(netcdf_file_model)),'2d'))
    outdir.append(os.path.join(os.path.dirname(
        os.path.dirname(os.path.dirname(netcdf_file_sat))),'model','2d'))
    logger.info("--- Checking 2d model output dir: %s ---", outdir[0])
    outdir.append(os.path.join(os.path.dirname(os.path.dirname(netcdf_file_sat)),'2d'))
    logger.info("--- Checking 2d satellite output dir: %s ---", outdir[1])
    os.makedirs(outdir[0],exist_ok = True)
    os.makedirs(outdir[1],exist_ok = True)
    return(logger, outdir)

def parse_leaflet_json(model, netcdf_file_sat, prop1):
    '''
    This takes the lazily loaded model file and path to concatonated satellite
    netcdf  and outputs leaflet csv files for web UI visualization.
    '''
    [logger, outdir] = param_val(netcdf_file_sat)

    # Read and process netcdf
    #logger.info("--- Reading NETCDF files ---")

    #model = Dataset(netcdf_file_model, 'r')
    ocean_dtime = None
    lon_grid = None
    lat_grid = None
    sst_in_model = None
    if prop1.model_source == 'roms':
        mask = model.variables['mask_rho'][:][0,:,:].astype(bool).compute()
        lons = np.asarray(model.variables['lon_rho'][:])
        lats = np.asarray(model.variables['lat_rho'][:])
        [lat_grid, lon_grid] = resample_latlon(lats[mask], lons[mask], prop1)
        ocean_dtime = np.array(model['ocean_time'], dtype='datetime64[ns]')
        sst_in_model = np.squeeze(np.asarray(model.variables['temp'][:][:,-1,:,:]))
        #0 index for surface level

    elif prop1.model_source == 'fvcom':
        logger.info("--- "+"FVCOM: Calculating regular grid"+" ---")
        lons = np.asarray(model.variables['lon'][:])
        lats = np.asarray(model.variables['lat'][:])
        [lat_grid, lon_grid] = resample_latlon(lats, lons, prop1)
        ocean_dtime = np.array(model['time'], dtype='datetime64[ns]')
        sst_in_model = np.asarray(model.variables['temp'][:][:,0,:]) #0 index for surface level

    ocean_dtime = [dt.astype('datetime64[s]').astype(datetime) for dt in ocean_dtime]

    try:
        if 'sport' in str(netcdf_file_sat):
            nc_sat = Dataset(netcdf_file_sat, 'r')
            lons_sport = np.asarray(nc_sat.variables['lon'][:])
            lats_sport = np.asarray(nc_sat.variables['lat'][:])
            lons_sport, lats_sport = np.meshgrid(lons_sport, lats_sport)
            dtime_sport = pd.to_datetime(nc_sat['time'][:], unit='s', origin='1981-01-01') 
            sst_in_sport = nc_sat['analysed_sst'][:]-273.15
            for i, t in enumerate(dtime_sport):
                out_file_sport = os.path.join(outdir[1], str(prop1.ofs+'_'+\
                        t.strftime('%Y%m%d-%Hz')+'_sst_SPoRT.json'))
                sst_sat_sport =interp_grid(lons_sport.ravel(),
                                    lats_sport.ravel(),
                                    sst_in_sport[i,:,:].ravel(),
                                    lon_grid, lat_grid, logger)

                logger.info("--- Writing 2D leaflet JSON file to: %s ---", out_file_sport)
                write_2d_arrays_to_json(lat_grid, lon_grid,
                                        np.round(sst_sat_sport, decimals=2),
                                       out_file_sport)
            return
    except UnboundLocalError:
        logger.error('Problem processing SPoRT satellite file.')
        sys.exit(1)

    #process satellite netcdf
    try:
        nc_sat = Dataset(netcdf_file_sat, 'r')
        lons_sat = np.asarray(nc_sat.variables['lon'][:])
        lats_sat = np.asarray(nc_sat.variables['lat'][:])
        lons_sat, lats_sat = np.meshgrid(lons_sat, lats_sat)
        [ref_year, ref_month, ref_day] = [int(i) for i in \
            nc_sat.variables['time'].comment.split()[-2].split('-')]
        [ref_hour, ref_min, ref_sec] = [int(i) for i in \
            nc_sat.variables['time'].comment.split()[-1].split(':')]
        ref_dtime = datetime(ref_year, ref_month, ref_day, \
                             ref_hour, ref_min, ref_sec)
        sat_dtime = [ref_dtime+timedelta(seconds=int(isec)) \
            for isec in nc_sat.variables['time'][:]]
        sst_in_sat = np.asarray(nc_sat.variables['sea_surface_temperature'][:])\
            -273.15 #convert from kelvin to celcius
        nc_sat.close()
    except FileNotFoundError:
        logger.error('PROBLEM READING SATELLITE DATA IN LEAFLET_CONTOUR.')
        sat_dtime = ocean_dtime


    ### This section does the daily averages ###
    ### Compute and write daily avg for model
    dtime = datetime.fromisoformat(prop1.start_date_full.replace('Z', '+00:00'))
    dtime_end = datetime.fromisoformat(prop1.end_date_full.replace('Z', '+00:00'))
    if abs(dtime-dtime_end) == timedelta(days=1):
        logger.info("Computing daily SST average for model ...")
        out_file_model = os.path.join(outdir[0], str(prop1.ofs+'_'+\
                dtime.strftime('%Y%m%d')+'-daily_sst_model.'+prop1.whichcast+'.json'))
        sst_model_avg = np.nanmean(sst_in_model, axis=0)
        try:
            if prop1.model_source == 'fvcom':
                logger.info("--- Resampling FVCOM grid ---")
                sst_model_ai =interp_grid(lons, lats, sst_model_avg, lon_grid, lat_grid, logger)
            else: #roms
                logger.info("--- Resampling ROMS grid ---")
                sst_model_ai = interp_grid(lons[mask].ravel(),
                                       lats[mask].ravel(),
                                       sst_model_avg[:,:,][mask].ravel(),
                                       lon_grid, lat_grid, logger)
            logger.info("--- Writing 2D leaflet JSON file to: %s ---", out_file_model)
            write_2d_arrays_to_json(lat_grid, lon_grid, np.round(sst_model_ai, decimals=2),\
                            out_file_model)
        except UnboundLocalError:
            logger.error('Problem writing daily averaged model JSON file.')

    ### Compute and write daily avg for l3c
    if abs(sat_dtime[0]-sat_dtime[-1]) == timedelta(days=1):
        logger.info("Computing daily SST average for satellite ...")
        out_file_sat = os.path.join(outdir[1], str(prop1.ofs+'_'+\
                sat_dtime[0].strftime('%Y%m%d')+'-daily_sst_l3c.json'))

        sst_sat_avg = np.nanmean(sst_in_sat, axis=0)
        try:
            sst_sat_ai = interp_grid(lons_sat.ravel(),
                                lats_sat.ravel(),
                                sst_sat_avg.ravel(),
                                lon_grid, lat_grid, logger)

            logger.info("--- Writing 2D leaflet JSON file to: %s ---", out_file_sat)
            write_2d_arrays_to_json(lat_grid, lon_grid,
                                    np.round(sst_sat_ai, decimals=2),
                                   out_file_sat)
        except UnboundLocalError:
            logger.error('Problem writing daily averaged satellite JSON file.')


    # Loop over times and write out leaflet csv files
    while dtime <= datetime.fromisoformat(prop1.end_date_full.replace('Z', '+00:00')):
    #for i, dtime in enumerate(ocean_dtime):
        # Find the index of the nearest datetime
        i = next((i for i, dt in enumerate(ocean_dtime) \
                if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)), -1)
        isat = next((i for i, dt in enumerate(sat_dtime) \
                if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)), -1)
        if i == -1 or isat == -1:  #make sure expected times match
            ni = min(enumerate(ocean_dtime), key=lambda x: \
                    abs(x[1] - dtime.replace(tzinfo=None)))[0]
            ni_sat = min(enumerate(sat_dtime), key=lambda x: \
                    abs(x[1] - dtime.replace(tzinfo=None)))[0]
            logger.info(f"--- Unexpected time error: requested time = %s {dtime},\
                    model_time = %s  ---", dtime, ocean_dtime[ni])
            logger.info("--- Unexpected time error: requested time = %s,\
                    satellite_time = %s  ---", dtime, sat_dtime[ni_sat])
        else:
            out_file_model = os.path.join(outdir[0], str(prop1.ofs+'_'+\
                    dtime.strftime('%Y%m%d-%Hz')+'_sst_model.'+prop1.whichcast+'.json'))

            if prop1.model_source == 'fvcom':
                logger.info("--- Resampling FVCOM grid ---")
                sst_model =interp_grid(lons, lats, sst_in_model[i,:], lon_grid, lat_grid, logger)

            else: #roms
                logger.info("--- Resampling ROMS grid ---")
                sst_model =interp_grid(lons[mask].ravel(),
                                       lats[mask].ravel(),
                                       sst_in_model[i,:,:,][mask].ravel(),
                                       lon_grid, lat_grid, logger)

            out_file_sat = os.path.join(outdir[1], str(prop1.ofs+'_'+\
                    dtime.strftime('%Y%m%d-%Hz')+'_sst_l3c.json'))

            i = next((i for i, dt in enumerate(sat_dtime) \
                    if dt.replace(tzinfo=None) == dtime.replace(tzinfo=None)), -1)

            logger.info("--- Writing 2D leaflet JSON file to: %s ---", out_file_model)
            write_2d_arrays_to_json(lat_grid, lon_grid, np.round(sst_model, decimals=2),\
                            out_file_model)

            try:
                sst_sat =interp_grid(lons_sat.ravel(),
                                    lats_sat.ravel(),
                                    sst_in_sat[i,:,:].ravel(),
                                    lon_grid, lat_grid, logger)

                logger.info("--- Writing 2D leaflet JSON file to: %s ---", out_file_sat)
                write_2d_arrays_to_json(lat_grid, lon_grid,
                                        np.round(sst_sat, decimals=2),
                                       out_file_sat)
            except UnboundLocalError:
                logger.error('Problem writing satellite JSON file.')

            #logger.info("--- Finished writing JSON file %s/%s ---", str(i+1),str(len(ocean_dtime)))
        dtime += timedelta(hours=1)
        #assuming hourly data. If we update to run
        #2d SA on higher temporal resolution, we'll need
        #to change this.

    # Close the NetCDF file

    logger.info("--- Finished leaflet JSON processing ---")


def interp_grid(lons, lats, sst, lon_grid, lat_grid, logger):
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
    logger.info("--- Resampling grid ---")
    mesh = pyinterp.RTree()
    mesh.packing(np.vstack((lons, lats)).T,sst)
    sst_out, _ = mesh.inverse_distance_weighting(
        np.vstack((lon_grid.ravel(), lat_grid.ravel())).T,
        within=True,
        # No exploration outside of original grid
        radius=5000,
        # Radius to search
        k=8,
        # Maximum number of neighbors to look for
        num_threads=0)
    sst_out = sst_out.reshape(lon_grid.shape)
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
                z_list[i][j] = np.nan  # Replace any non-null perimeter value with None

    #Replace None or np.nan values in z with `null` for JSON compatibility
    z_list = [[None if np.isnan(val) else val for val in row] for row in z_list]
    z_list = [[None if val is not None and int(val)>101 and int(val)<-101 \
                            else val for val in row] for row in z_list]

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
        "lats": x_list,
        "lons": y_list,
        "sst": z_list,
        "val_min": val_min,
        "val_max": val_max
    }

    # Write the data to a JSON file with manual formatting for arrays
    with open(filename, 'w', encoding="utf-8") as json_file:
        json_file.write("{\n")
        keys = list(json_data.keys())
        for idx, key in enumerate(keys):
            value = json_data[key]
            if isinstance(value, list):
                json_file.write(f'  "{key}": [\n')
                for i, row in enumerate(value):
                    row_str = json.dumps(row).replace("null", "null").replace("None", "null")
                    if i == len(value) - 1:
                        json_file.write(f'    {row_str}\n')
                    else:
                        json_file.write(f'    {row_str},\n')
                json_file.write("  ]")
            else:
                value_str = json.dumps(value)
                json_file.write(f'  "{key}": {value_str}')
            if idx < len(keys) - 1:
                json_file.write(",\n")
            else:
                json_file.write("\n")
        json_file.write("}\n")


def resample_latlon(lats, lons, prop1):
    '''
    This function resamples lat and lon to a regular grid
    '''
    if prop1.ofs == 'wcofs':
        deg_res_resample = 0.04 #degree resolution for resample regular grid
    elif prop1.ofs == 'ngofs2':
        deg_res_resample = 0.04 #degree resolution for resample regular grid
    else:
        deg_res_resample = 0.01 #degree resolution for resample regular grid

    lat_min = np.round(np.min(lats), decimals=2)-.25
    lat_max = np.round(np.max(lats), decimals=2)+.25
    lon_min = np.round(np.min(lons), decimals=2)-.25
    lon_max = np.round(np.max(lons), decimals=2)+.25

    lon_grid, lat_grid = np.meshgrid(np.arange(lon_min, lon_max, deg_res_resample),
                                     np.arange(lat_min, lat_max, deg_res_resample))
    return np.round(lat_grid, decimals=2), np.round(lon_grid, decimals=2)
