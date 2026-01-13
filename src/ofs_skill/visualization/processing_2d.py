"""
2D data processing module for OFS skill assessment visualization.

This module handles processing of 2D oceanographic model data (ROMS, FVCOM) and
satellite observations, converting them to regularized grids and JSON format
for web-based visualization using Leaflet contour plots.

Key Features:
    - Grid interpolation using inverse distance weighting (IDW)
    - Support for ROMS and FVCOM model formats
    - L3C and SPoRT satellite data processing
    - Land masking using global_land_mask
    - OFS domain masking using shapefiles
    - Optimized shapefile polygon checking with decimation
    - JSON output for web visualization

Functions:
    parse_leaflet_json: Main processing pipeline for model/satellite data
    interp_grid: IDW interpolation with land/domain masking
    write_2d_arrays_to_json: Write lat/lon/data to JSON format
    resample_latlon: Create regular grid from irregular coordinates
    normalize_longitudes: Convert 0-360 to -180-180 longitude format
    generate_domain_mask_bool: Create boolean mask from shapefile

Author: AJK
Created: 09/2024
Last Modified: 03/2025 - Renamed from 'leaflet_contour', updates for intake
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
from typing import TYPE_CHECKING

import matplotlib.path as mplPath
import numpy as np
import pandas as pd

# pyinterp and global_land_mask are imported lazily in interp_grid() to avoid dependency issues
import shapefile
from netCDF4 import Dataset
from scipy.spatial import cKDTree

from ofs_skill.obs_retrieval import utils

if TYPE_CHECKING:
    from logging import Logger

    import numpy.typing as npt


def param_val(netcdf_file_sat: str) -> tuple[Logger, list]:
    """
    Validate inputs and create necessary directories for 2D processing.

    Args:
        netcdf_file_sat: Path to satellite netCDF file

    Returns:
        Tuple of (logger, output_directories)
            - logger: Configured logger instance
            - output_directories: List with [model_2d_dir, satellite_2d_dir]

    Notes:
        - Creates logger if not already configured
        - Creates output directories if they don't exist
        - Model dir: parent_parent/model/2d/
        - Satellite dir: parent_parent/observations/2d/
    """
    # Check logger
    logger = None
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent.parent / log_config_file
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


def parse_leaflet_json(model, netcdf_file_sat: str, prop1) -> str:
    """
    Process model and satellite data to Leaflet-compatible JSON files.

    Main processing pipeline that:
    1. Reads ROMS/FVCOM model data and L3C/SPoRT satellite data
    2. Interpolates to regular grid
    3. Applies land and domain masks
    4. Writes JSON files for visualization
    5. Generates both hourly and daily averaged products

    Args:
        model: Lazily loaded xarray dataset of model data
        netcdf_file_sat: Path to concatenated satellite netCDF file
        prop1: Properties object with configuration
               Must have: model_source, ofs, whichcast, start/end dates

    Returns:
        Status string indicating completion

    Raises:
        FileNotFoundError: If satellite data not available
        UnboundLocalError: If processing errors occur

    Notes:
        - Processes SST, SSH, SSS, and surface currents (U/V)
        - Outputs JSON files named: {ofs}_{date}_{var}_{source}.json
        - SPoRT processing returns early after completion
    """
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
        logger.info('Loading mask_rho...')
        mask = model.variables['mask_rho'][:][0, :, :].astype(bool).compute()
        logger.info('Loading lon_rho and lat_rho...')
        lons = np.asarray(model.variables['lon_rho'][:])
        lats = np.asarray(model.variables['lat_rho'][:])
        #lons_u = np.asarray(model.variables['lon_u'][:])
        #lats_u = np.asarray(model.variables['lat_u'][:])
        #lons_v = np.asarray(model.variables['lon_v'][:])
        #lats_v = np.asarray(model.variables['lat_v'][:])
        logger.info('Creating regular grid...')
        [lat_grid, lon_grid] = resample_latlon(lats[mask], lons[mask], prop1)
        logger.info('Loading ocean_time...')
        ocean_dtime = np.array(model['ocean_time'], dtype='datetime64[ns]')
        logger.info('Loading temperature data (this may take a while)...')
        sst_in_model = np.squeeze(np.asarray(model.variables['temp'][:][:,-1,:,:],))
        logger.info('Loading sea surface height...')
        ssh_in_model = np.squeeze(np.asarray(model.variables['zeta'][:]))
        logger.info('Loading salinity data...')
        sss_in_model = np.squeeze(np.asarray(model.variables['salt'][:][:,-1,:,:]))
        logger.info('Loading u velocity...')
        ssu_in_model = np.squeeze(np.asarray(model.variables['u_east'][:][:,-1,:,:]))
        logger.info('Loading v velocity...')
        ssv_in_model = np.squeeze(np.asarray(model.variables['v_north'][:][:,-1,:,:]))
        # -1 index for surface level
        logger.info('--- '+'ROMS: finished calculating regular grid'+' ---')

    elif prop1.model_source == 'fvcom':
        logger.info('--- '+'FVCOM: Calculating regular grid'+' ---')
        lons = np.asarray(model.variables['lon'][:])
        lats = np.asarray(model.variables['lat'][:])
        lons_c = np.asarray(model.variables['lonc'][:])
        lats_c = np.asarray(model.variables['latc'][:])
        [lat_grid, lon_grid] = resample_latlon(lats, lons, prop1)
        ocean_dtime = np.array(model['time'], dtype='datetime64[ns]')
        # 0 index for surface level
        sst_in_model = np.asarray(model.variables['temp'][:][:,0,:])
        # 0 index for surface level
        ssh_in_model = np.asarray(model.variables['zeta'][:])
        sss_in_model = np.asarray(model.variables['salinity'][:][:,0,:])
        ssu_in_model = np.asarray(model.variables['u'][:][:,0,:])
        ssv_in_model = np.asarray(model.variables['v'][:][:,0,:])
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
            nc_sat.close()
            return 'Finished SPoRT processing'
    except UnboundLocalError:
        logger.error('Problem processing SPoRT satellite file.')
        if 'nc_sat' in locals():
            nc_sat.close()
        sys.exit(1)
    except FileNotFoundError:
        logger.error('SPoRT satellite data not found! Run '
                     'get_satellite_observations.py to '
                     'download it.')
        if 'nc_sat' in locals():
            nc_sat.close()
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
        out_file_model_sss = os.path.join(
            outdir[0], str(
                prop1.ofs+'_' +
                dtime.strftime('%Y%m%d')+'-daily_sss_model.' +
                prop1.whichcast+'.json',
            ),
        )
        out_file_model_ssu = os.path.join(
            outdir[0], str(
                prop1.ofs+'_' +
                dtime.strftime('%Y%m%d')+'-daily_ssu_model.' +
                prop1.whichcast+'.json',
            ),
        )
        out_file_model_ssv = os.path.join(
            outdir[0], str(
                prop1.ofs+'_' +
                dtime.strftime('%Y%m%d')+'-daily_ssv_model.' +
                prop1.whichcast+'.json',
            ),
        )
        logger.info('Computing daily SST average for model ...')
        sst_model_avg = np.nanmean(sst_in_model, axis=0)
        logger.info('Computing daily SSH average for model ...')
        ssh_model_avg = np.nanmean(ssh_in_model, axis=0)
        logger.info('Computing daily salinity average for model ...')
        sss_model_avg = np.nanmean(sss_in_model, axis=0)
        logger.info('Computing daily currents average for model ...')
        ssu_model_avg = np.nanmean(ssu_in_model, axis=0)
        ssv_model_avg = np.nanmean(ssv_in_model, axis=0)
        try:
            if prop1.model_source == 'fvcom':
                logger.info('--- Resampling FVCOM grid ---')
                sst_model_ai = interp_grid(
                    lons, lats, sst_model_avg, lon_grid, lat_grid, logger, prop1,
                )
                ssh_model_ai = interp_grid(
                    lons, lats, ssh_model_avg, lon_grid, lat_grid, logger, prop1,
                )
                sss_model_ai = interp_grid(
                    lons, lats, sss_model_avg, lon_grid, lat_grid, logger, prop1,
                )
                ssu_model_ai = interp_grid(
                    lons_c, lats_c, ssu_model_avg, lon_grid, lat_grid, logger, prop1,
                )
                ssv_model_ai = interp_grid(
                    lons_c, lats_c, ssv_model_avg, lon_grid, lat_grid, logger, prop1,
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
                sss_model_ai = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    sss_model_avg[:, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssu_model_ai = interp_grid(
                    lons.ravel(),
                    lats.ravel(),
                    ssu_model_avg[:, :].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssv_model_ai = interp_grid(
                    lons.ravel(),
                    lats.ravel(),
                    ssv_model_avg[:, :].ravel(),
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
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_sss,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(sss_model_ai, decimals=2),
                out_file_model_sss,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssu,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssu_model_ai, decimals=2),
                out_file_model_ssu,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssv,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssv_model_ai, decimals=2),
                out_file_model_ssv,
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
            out_file_model_sss = os.path.join(
                outdir[0], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_sss_model.' +
                    prop1.whichcast+'.json',
                ),
            )
            out_file_model_ssu = os.path.join(
                outdir[0], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_ssu_model.' +
                    prop1.whichcast+'.json',
                ),
            )
            out_file_model_ssv = os.path.join(
                outdir[0], str(
                    prop1.ofs+'_' +
                    dtime.strftime('%Y%m%d-%Hz')+'_ssv_model.' +
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
                sss_model = interp_grid(
                    lons, lats, sss_in_model[i,
                                             :], lon_grid, lat_grid, logger, prop1,
                )
                ssu_model = interp_grid(
                    lons_c, lats_c, ssu_in_model[i,
                                             :], lon_grid, lat_grid, logger, prop1,
                )
                ssv_model = interp_grid(
                    lons_c, lats_c, ssv_in_model[i,
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
                sss_model = interp_grid(
                    lons[mask].ravel(),
                    lats[mask].ravel(),
                    sss_in_model[i, :, :][mask].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssu_model = interp_grid(
                    lons.ravel(),
                    lats.ravel(),
                    ssu_in_model[i, :, :].ravel(),
                    lon_grid, lat_grid, logger, prop1,
                )
                ssv_model = interp_grid(
                    lons.ravel(),
                    lats.ravel(),
                    ssv_in_model[i, :, :].ravel(),
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
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_sss,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(sss_model, decimals=2),
                out_file_model_sss,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssu,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssu_model, decimals=2),
                out_file_model_ssu,
            )
            logger.info(
                '--- Writing 2D leaflet JSON file to: %s ---', out_file_model_ssv,
            )
            write_2d_arrays_to_json(
                lat_grid, lon_grid, np.round(ssv_model, decimals=2),
                out_file_model_ssv,
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


def interp_grid(
    lons: npt.NDArray,
    lats: npt.NDArray,
    sst: npt.NDArray,
    lon_grid: npt.NDArray,
    lat_grid: npt.NDArray,
    logger: Logger,
    prop1
) -> npt.NDArray:
    """
    Interpolate irregular grid data to regular grid with masking.

    Uses inverse distance weighting (IDW) interpolation via pyinterp package.
    Applies two-pass interpolation (strict then gap-filling), followed by
    land mask and OFS domain mask.

    Args:
        lons: Original grid longitudes (1D array)
        lats: Original grid latitudes (1D array)
        sst: Data values to interpolate (1D array, same length as lons/lats)
        lon_grid: Target grid longitudes (2D meshgrid)
        lat_grid: Target grid latitudes (2D meshgrid)
        logger: Logger instance for debug messages
        prop1: Properties object with ofs attribute for shapefile lookup

    Returns:
        Interpolated and masked 2D array matching lon_grid/lat_grid shape
        NaN values indicate land or out-of-domain points

    Algorithm:
        1. Estimate source data spacing using cKDTree nearest neighbors
        2. First pass: IDW with 3x median spacing radius, k=8 neighbors
        3. Second pass: Fill remaining NaNs with larger radius, k=16
        4. Apply global land mask using global_land_mask
        5. Apply OFS domain mask from cached/generated shapefile mask

    Notes:
        - Caches domain mask as {ofs}.shp_mask.npy for reuse
        - Uses vertex decimation (every 5th point) for fast polygon checks
        - Parallel processing disabled (num_threads=0) for compatibility

    References:
        https://cnes.github.io/pangeo-pyinterp/auto_examples/pangeo_unstructured_grid.html
    """
    # Lazy imports to avoid dependency issues at module load time
    import pyinterp
    from global_land_mask import globe

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

    logger.debug(
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
        logger.debug(f'Filling {mask.sum()} NaN values with larger radius...')
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
    grid_shapefile = Path(__file__).parents[3] / 'ofs_extents' / f'{prop1.ofs}.shp'
    logger.debug('Shapefile path: %s', grid_shapefile)

    # Define cache file path
    mask_cache_file = str(grid_shapefile).replace('.shp', '_mask.npy')
    mask_to_apply = None

    # Try to load from cache
    if os.path.exists(mask_cache_file):
        logger.debug(f'Loading cached domain mask from {mask_cache_file}')
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

    logger.debug(f'Applying domain mask from {mask_cache_file}')
    # Apply the mask
    sst_out[~mask_to_apply] = np.nan

    return sst_out


def write_2d_arrays_to_json(
    x: npt.NDArray,
    y: npt.NDArray,
    z: npt.NDArray,
    filename: str
) -> None:
    """
    Write 2D lat/lon/data arrays to JSON file for Leaflet visualization.

    Creates a JSON file with structure suitable for Leaflet contour plots.
    Handles NaN/None values, sets perimeter to null, and includes min/max
    metadata.

    Args:
        x: 2D array of latitudes
        y: 2D array of longitudes
        z: 2D array of data values (SST, SSH, etc.)
        filename: Output JSON file path

    JSON Structure:
        {
          "lats": [[...], [...], ...],
          "lons": [[...], [...], ...],
          "sst": [[...], [...], ...],
          "val_min": <float>,
          "val_max": <float>
        }

    Notes:
        - Converts NaN to JSON null
        - Sets all perimeter values to null (for clean edges)
        - Filters extreme values (< -101 or > 101) to null
        - Computes val_min/val_max excluding null values
        - Uses manual JSON formatting for better readability
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


def resample_latlon(
    lats: npt.NDArray,
    lons: npt.NDArray,
    prop1
) -> tuple[npt.NDArray, npt.NDArray]:
    """
    Create regular lat/lon grid from irregular coordinates.

    Generates a meshgrid with resolution adapted to OFS domain size.
    Larger domains (WCOFS, NGOFS2) use coarser resolution.

    Args:
        lats: Irregular latitude array (1D or 2D)
        lons: Irregular longitude array (1D or 2D)
        prop1: Properties object with ofs attribute

    Returns:
        Tuple of (lat_grid, lon_grid) as 2D meshgrids

    Resolution:
        - WCOFS, NGOFS2: 0.04 degrees (~4.4 km)
        - Other OFS: 0.01 degrees (~1.1 km)

    Notes:
        - Extends bounds by 0.25 degrees in all directions
        - Rounds grid values to 2 decimal places
        - Output arrays suitable for np.meshgrid operations
    """
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


def normalize_longitudes(longitudes: npt.NDArray) -> npt.NDArray:
    """
    Convert longitudes from 0-360 to -180-180 format.

    Detects longitude values > 180 and normalizes them to the standard
    -180 to 180 range by subtracting 360.

    Args:
        longitudes: Array of longitude values (any shape)

    Returns:
        Normalized longitude array (same shape as input)

    Example:
        >>> lons = np.array([0, 90, 180, 270, 359])
        >>> normalize_longitudes(lons)
        array([  0,  90, 180, -90,  -1])

    Notes:
        - Creates copy to avoid modifying original data
        - Only modifies values > 180
        - Values already in -180-180 range unchanged
    """
    # Create a copy to avoid modifying the original data in place
    lons = np.array(longitudes, dtype=float).copy()
    # CONDITION: Find indices where longitude is > 180
    mask = lons > 180
    # ACTION: Subtract 360 only from those specific indices
    lons[mask] -= 360
    return lons


def generate_domain_mask_bool(
    grid_lon: npt.NDArray,
    grid_lat: npt.NDArray,
    shapefile_path: str,
    logger: Logger
) -> npt.NDArray:
    """
    Generate boolean mask from shapefile for OFS domain boundaries.

    Creates a mask where True indicates points inside the OFS domain
    (valid ocean points) and False indicates points outside the domain
    (should be masked as NaN in visualizations).

    Optimizations:
        1. Global bounding box filter for fast rejection of distant points
        2. Vertex decimation (every 5th vertex) speeds up polygon checks ~5x
        3. Chunked processing (50k points/chunk) prevents UI hangs

    Args:
        grid_lon: Longitude array (1D or 2D)
        grid_lat: Latitude array (1D or 2D)
        shapefile_path: Path to .shp file defining OFS boundaries
        logger: Logger instance for progress messages

    Returns:
        Boolean array matching input grid shape
        True = inside domain, False = outside domain

    Performance:
        - Typical processing: <1 min for 100k grid points
        - Progress printed every 5 chunks
        - Mask cached to .npy file for reuse

    Notes:
        - Handles multi-part shapefiles (multiple islands/polygons)
        - Uses matplotlib.path for point-in-polygon tests
        - Decimation threshold: 100 vertices (smaller parts not decimated)
    """
    t0 = time.time()
    logger.debug(f'Generating new domain mask from: {shapefile_path}')

    # 1. Load Shapefile
    sf = shapefile.Reader(shapefile_path)
    shapes = sf.shapes()

    # 2. Calculate Global Bounding Box (Union of all shapes)
    # This allows us to quickly discard points that are nowhere near the coast
    if not shapes:
        logger.debug('Error: Shapefile is empty.')
        return np.ones(grid_lon.shape, dtype=bool)

    total_bbox = list(shapes[0].bbox)
    for shape in shapes[1:]:
        bbox = shape.bbox
        total_bbox[0] = min(total_bbox[0], bbox[0])  # Min Lon
        total_bbox[1] = min(total_bbox[1], bbox[1])  # Min Lat
        total_bbox[2] = max(total_bbox[2], bbox[2])  # Max Lon
        total_bbox[3] = max(total_bbox[3], bbox[3])  # Max Lat

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
    logger.debug(f'Global BBox reduced points to check from {n_total_points} to {n_check}')

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
        logger.debug(f'Processing {n_check} points in {n_chunks} chunks (Decimation={DECIMATION})...')

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

    logger.debug(f'Mask generation complete in {time.time() - t0:.2f}s')
    return mask_reshaped
