"""
Model Data Intake and Lazy Loading Module

This module creates catalogs and lazily loads model netCDF files using Intake and Dask.
It supports both local file access and remote S3 streaming from NOAA NODD buckets.

The module handles:
- Catalog creation for multiple model files
- Lazy loading with Dask for efficient memory usage
- Model-specific adjustments (FVCOM, ROMS, SCHISM)
- Current velocity rotations for ROMS models
- Sigma coordinate calculations for FVCOM models
- Station dimension compatibility checking

Functions
---------
intake_model : Main function to create catalog and lazily load model data
fix_roms_uv : Adjust ROMS currents from grid-relative to true north
fix_fvcom : Apply FVCOM-specific coordinate adjustments
calc_sigma : Calculate sigma coordinates for FVCOM models
get_station_dim : Check station dimension compatibility
remove_extra_stations : Handle inconsistent station dimensions

Notes
-----
The file_list can contain local file paths or remote S3 URLs. Remote URLs
are automatically detected and handled via fsspec/h5netcdf.

Lazy loading strategy:
- Uses Intake to create a catalog of netCDF files
- Uses Dask to lazily load data (doesn't read into memory until needed)
- Enables processing of large datasets that don't fit in memory

Author: AJK
Created: 12/2024

Revisions:
    Date          Author             Description
    05/01/2025    AJK                Fix CIOFS issues and optimize fix_roms_uv
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any

import intake
import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours


def _extract_filename_from_encoding(ds):
    """Best-effort extraction of filename from dataset encoding."""
    # Try common encoding keys
    for key in ('source', 'original_source'):
        source = ds.encoding.get(key, '')
        if source:
            if '::' in source:
                source = source.split('::')[-1]
            return os.path.basename(source)

    # Scan all encoding values for a .nc path
    for val in ds.encoding.values():
        if isinstance(val, str) and '.nc' in val:
            if '::' in val:
                val = val.split('::')[-1]
            return os.path.basename(val)

    # Try variable-level encoding
    for var_name in ds.data_vars:
        var_source = ds[var_name].encoding.get('source', '')
        if var_source:
            if '::' in var_source:
                var_source = var_source.split('::')[-1]
            return os.path.basename(var_source)

    return ''


def make_preprocess_with_filename(urlpaths):
    """Create a preprocess function that maps datasets to filenames.

    When files are opened through simplecache or other fsspec wrappers,
    ds.encoding may not contain the original file path. This factory
    creates a closure that tracks call order and falls back to extracting
    the filename from the known urlpaths list.
    """
    call_count = [0]  # mutable counter for closure

    def preprocess_with_filename(ds):
        filename = _extract_filename_from_encoding(ds)
        if not filename or filename == 'unknown':
            # Fall back to the original urlpath by call order
            idx = call_count[0]
            if idx < len(urlpaths):
                path = urlpaths[idx]
                if isinstance(path, str):
                    # Strip protocol prefixes
                    if '::' in path:
                        path = path.split('::')[-1]
                    filename = os.path.basename(path)
            if not filename:
                filename = 'unknown'
        call_count[0] += 1
        return ds.assign_coords(filename=filename)

    return preprocess_with_filename


def preprocess_with_filename(ds):
    """Standalone preprocess — used when urlpaths aren't available."""
    filename = _extract_filename_from_encoding(ds)
    if not filename:
        filename = 'unknown'
    return ds.assign_coords(filename=filename)

def intake_model(file_list: list[str], prop: Any, logger: Logger) -> xr.Dataset:
    """
    Create a catalog and lazily load model files using Intake and Dask.

    This function uses Intake and dask to create a catalog of model files
    (passed from list_of_files) and lazily load the catalog using Dask.
    This function also calls fix_roms_uv, which makes current adjustments for
    ROMS based models (fields and stations).

    The file_list can contain local file paths or remote S3 URLs. Remote URLs
    are automatically detected and handled via fsspec/h5netcdf.

    Parameters
    ----------
    file_list : list of str
        List of model netCDF file paths or S3 URLs
    prop : ModelProperties
        ModelProperties object containing:
        - model_source : str
            Model type ('fvcom', 'roms', 'schism')
        - ofs : str
            OFS model name
        - ofsfiletype : str
            'fields' or 'stations'
        - whichcast : str
            'nowcast', 'forecast_a', or 'forecast_b'
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset
        Lazily loaded model dataset with all adjustments applied

    Notes
    -----
    Model-specific variable dropping:
    - ROMS: Drops many auxiliary variables to reduce memory
    - FVCOM: Minimal dropping (siglay/siglev handled separately)
    - SCHISM: Drops surface/bottom variables
    - ADCIRC: Drops some mesh/connectivity variables

    Time handling:
    - Rounds all times to nearest minute
    - Removes duplicate times (keeps 'last' for nowcast, 'first' for forecast_a)

    Station files:
    - Checks dimension compatibility across all files
    - If dimensions don't match, slices to common set of stations

    Examples
    --------
    >>> ds = intake_model(file_list, prop, logger)
    INFO:root:Starting catalog ...
    INFO:root:Lazy loading complete applying adjustments ...
    >>> print(ds)
    <xarray.Dataset>
    """
    logger.info('Starting catalog ...')

    # Check if we have any remote URLs in the file list
    has_remote = any(isinstance(f, str) and f.startswith(('http://', 'https://'))
                     for f in file_list)
    if has_remote:
        remote_count = sum(1 for f in file_list
                          if isinstance(f, str) and f.startswith(('http://', 'https://')))
        logger.info(f'File list contains {remote_count} remote URLs (will stream from S3)')

    drop_variables = None
    time_name = None
    if prop.model_source == 'roms':
        time_name = 'ocean_time'
        drop_variables = [
            'Akk_bak', 'Akp_bak', 'Akt_bak', 'Akv_bak', 'Cs_r', 'Cs_w',
            'dtfast', 'el', 'f', 'Falpha', 'Fbeta', 'Fgamma', 'FSobc_in',
            'FSobc_out', 'gamma2', 'grid', 'hc', 'lat_psi', 'lon_psi',
            'Lm2CLM', 'Lm3CLM', 'LnudgeM2CLM', 'LnudgeM3CLM', 'LnudgeTCLM',
            'LsshCLM', 'LtracerCLM', 'LtracerSrc', 'LuvSrc', 'LwSrc', 'M2nudg',
            'M2obc_in', 'M2obc_out', 'M3nudg', 'M3obc_in', 'M3obc_out',
            'mask_psi', 'mask_u', 'mask_v', 'ndefHIS', 'ndtfast', 'nHIS',
            'nRST', 'nSTA', 'ntimes', 'Pair', 'pm', 'pn', 'rdrg', 'rdrg2',
            'rho0', 's_w', 'spherical', 'Tcline', 'theta_b', 'theta_s',
            'Tnudg', 'Tobc_in', 'Tobc_out', 'Uwind', 'Vwind', 'Vstretching',
            'Vtransform', 'w', 'wetdry_mask_psi', 'wetdry_mask_rho',
            'wetdry_mask_u', 'wetdry_mask_v', 'xl', 'Znudg', 'Zob', 'Zos',
        ]
    elif prop.model_source == 'fvcom':
        time_name = 'time'
    elif prop.model_source == 'schism':
        drop_variables = [
            'temp_surface', 'temp_bottom', 'salt_surface', 'salt_bottom',
            'uvel_surface', 'vvel_surface', 'uvel_bottom', 'vvel_bottom',
            'uvel4.5','vvel4.5','crs', 'SCHISM_hgrid_edge_x','SCHISM_hgrid_edge_y',
            'SCHISM_hgrid_face_y','SCHISM_hgrid_face_x',
        ]
        time_name = 'time'
    elif prop.model_source == 'adcirc':
        drop_variables = [
            'nvel', 'element', 'adcirc_mesh', 'nvell', 'max_nvell',
            'ibtype', 'nbvv'
        ]
        time_name = 'time'

    if prop.ofs in ['necofs', 'loofs2','secofs']:
        engine = 'netcdf4'
    elif prop.ofs in ['stofs_2d_glo']:
        engine = 'scipy'
    else:
        engine = 'h5netcdf'

    urlpaths = file_list
    if len(urlpaths) == 0:
        return None
    if len(urlpaths) == 1:
        urlpaths = urlpaths + urlpaths


    if has_remote:
        logger.info('Creating catalog with mix of local and remote (S3) files...')
        logger.info('Remote files will be streamed directly from NODD S3 bucket')
    else:
        logger.info('Creating catalog with local files...')

    # First check stations dimensions to see if all are compatible --
    # only for stations files!
    dim_compat = True

    if prop.ofsfiletype == 'stations':
        try:
            dim_compat, dim_ref = get_station_dim(engine, urlpaths,
                                                  drop_variables, logger)
        except Exception as ex:
            logger.warning('Could not check number of stations before '
                           'combining netcdfs in intake! Error: %s. '
                           'Continuing...',
                           ex)
    # Build storage_options for S3 streaming when remote files are present.
    # Only use S3-specific options (anon, block_size) when URLs use s3://
    # protocol. For https:// URLs, use simpler HTTP-compatible options.
    s3_storage_opts = {}
    if has_remote:
        has_s3_proto = any(
            isinstance(f, str) and f.startswith('s3://')
            for f in urlpaths
        )
        if has_s3_proto:
            s3_storage_opts = {
                'storage_options': {
                    'anon': True,
                    'default_block_size': 64 * 1024 * 1024,
                    'default_fill_cache': False,
                }
            }
        # For https:// URLs, no special storage_options needed

        # Apply fsspec caching for remote URLs to avoid re-downloading
        if has_remote and not has_s3_proto:
            # For https:// URLs (NODD S3 via HTTPS)
            cache_dir = os.path.join(os.path.expanduser('~'), '.ofs_cache', 's3')
            os.makedirs(cache_dir, exist_ok=True)

            # Check if all files are remote — simplecache cannot handle
            # mixed local + remote file lists (fsspec protocol mismatch)
            all_remote = all(
                isinstance(f, str) and f.startswith('http')
                for f in urlpaths
            )

            if prop.ofsfiletype == 'stations' and all_remote:
                # simplecache: cache whole files (stations files are small,
                # typically 1-10 MB each)
                try:
                    cached_urlpaths = [
                        f'simplecache::{url}' for url in urlpaths
                    ]
                    urlpaths = cached_urlpaths
                    s3_storage_opts = {
                        'storage_options': {
                            'simplecache': {
                                'cache_storage': cache_dir,
                                'same_names': True,
                            },
                        }
                    }
                    logger.info(
                        'Using simplecache for %d remote station files '
                        '(cache: %s)', remote_count, cache_dir,
                    )
                except Exception as cache_err:
                    logger.warning(
                        'Failed to set up simplecache, falling back to '
                        'direct access: %s', cache_err,
                    )
                    # Restore original urlpaths on failure
                    urlpaths = file_list
                    if prop.ofsfiletype == 'stations' \
                            and prop.whichcast == 'forecast_a':
                        urlpaths = urlpaths + urlpaths
            elif prop.ofsfiletype == 'stations' and not all_remote:
                # Mixed local + remote: download remote files to cache
                # so all paths are local (fsspec requires uniform protocol)
                import urllib.request
                remote_n = sum(1 for f in urlpaths
                               if isinstance(f, str) and f.startswith('http'))
                local_n = len(urlpaths) - remote_n
                logger.info(
                    'Mixed file list: downloading %d remote files to '
                    'local cache (%d already local)', remote_n, local_n,
                )
                resolved = []
                for f in urlpaths:
                    if isinstance(f, str) and f.startswith('http'):
                        local_path = os.path.join(
                            cache_dir, os.path.basename(f))
                        if not os.path.isfile(local_path):
                            try:
                                urllib.request.urlretrieve(f, local_path)
                            except Exception as dl_err:
                                logger.warning(
                                    'Failed to cache %s: %s. '
                                    'Using direct URL.', f, dl_err)
                                resolved.append(f)
                                continue
                        resolved.append(local_path)
                    else:
                        resolved.append(f)
                urlpaths = resolved
            else:
                # For fields files, caching is skipped (files are 100-500 MB
                # each and would quickly exhaust local disk)
                logger.info(
                    'Skipping cache for %d remote fields files '
                    '(too large for local cache)', remote_count,
                )

    # Build a preprocess function that knows the original urlpaths,
    # so it can recover filenames even when simplecache hides them.
    # Strip simplecache:: prefixes to get the real filenames.
    raw_paths = [
        p.split('::')[-1] if isinstance(p, str) and '::' in p else p
        for p in urlpaths
    ]
    preprocess_fn = make_preprocess_with_filename(raw_paths)

    if dim_compat:  # This will only be FALSE for stations files when
        # station dimensions do not match! Always True for fields
        # files
        # If station dimensions are all the same/compatible, send in all file
        # names (urlpaths) at one time and let xarray/intake automagically
        # combine datasets
        if prop.model_source == 'schism':
            if  prop.ofsfiletype == 'fields':
                source = intake.open_netcdf(
                    urlpath=urlpaths,
                    xarray_kwargs={
                        'combine': 'by_coords',  # <-- align files by coordinates
                        'engine': engine,
                        'preprocess': preprocess_fn,
                        'drop_variables': drop_variables,
                        'chunks': {'time': 1},
                    },
                    **s3_storage_opts,
                )
            else:
                source = intake.open_netcdf(
                    urlpath=urlpaths,
                    xarray_kwargs={
                        'combine': 'nested',
                        'engine': engine,
                        'preprocess': preprocess_fn,
                        'concat_dim': time_name,
                        'decode_times': True,
                        'chunks': 'auto',  # Enables lazy loading with Dask
                    },
                    **s3_storage_opts,
                )

        else:
            # For fields files, chunk by single time step to bound memory
            # for monthly/yearly runs with hundreds of files. Stations
            # files have small spatial dims, so auto-chunking is safe.
            if prop.ofsfiletype == 'fields':
                chunk_spec = {time_name: 1}
            else:
                chunk_spec = 'auto'
            # Note it might be possible to add
            #     'data_vars': 'minimal'
            # to the xarray_kwargs to avoid expanding spatial/mesh variables
            # in the time dimension, which can result in very large arrays.
            # But this messes up the indexing.py process, so we will leave
            # it out for now.
            source = intake.open_netcdf(
                urlpath=urlpaths,
                xarray_kwargs={
                    'combine': 'nested',
                    'engine': engine,
                    'preprocess': preprocess_fn,
                    'concat_dim': time_name,
                    'decode_times': True,
                    'drop_variables': drop_variables,
                    'chunks': chunk_spec,
                },
                **s3_storage_opts,
            )
        # Read the dataset lazily
        logger.info('No dimension changes needed, lazy loading catalog ...')
        ds = source.to_dask()
    else:
        logger.info('Station dimensions are inconsistent! Slicing stations...')
        ds = remove_extra_stations(engine,
            urlpaths, dim_ref, drop_variables,
            time_name, logger,
        )

    # If ADCIRC, we need to subset times to the appropriate whichcast.
    # Note that this needs to be done before removal of duplicate times.
    if prop.model_source == 'adcirc':
        ds = fix_adcirc_dataset(prop, ds, urlpaths, logger)

    # Round all times to nearest minute
    ds[time_name] = ds[time_name].dt.round('1min')
    if prop.ofsfiletype == 'stations' and prop.whichcast != 'forecast_a':
        ds = ds.drop_duplicates(dim=time_name, keep='last')
    elif prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
        ds = ds.drop_duplicates(dim=time_name, keep='first')

    logger.info('Lazy loading complete applying adjustments ...')

    if prop.model_source == 'roms':
        ds = fix_roms_uv(prop, ds, logger)
    elif prop.model_source == 'fvcom':
        ds = fix_fvcom(prop, ds, logger)
    return ds


def fix_roms_uv(prop: Any, data_set: xr.Dataset, logger: Logger) -> xr.Dataset:
    """
    Adjust ROMS current velocities for proper coordinate system.

    This function adjusts currents (u and v) for ROMS models:
    1. Adjusts from phi grid to rho grid (fields files only)
    2. Adjusts from grid-relative direction to true north-relative

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - ofsfiletype : str
            'fields' or 'stations'
    data_set : xr.Dataset
        ROMS model dataset containing u, v, and angle variables
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset
        Dataset with u_east and v_north variables added

    Notes
    -----
    For fields files:
    - U and V are on staggered grids (u-points and v-points)
    - Must average to center points (rho-points)
    - Then rotate using grid angle

    For stations files:
    - U and V are already at station locations
    - Only need rotation using grid angle

    Rotation formula:
    - u_east + i*v_north = (u + i*v) * e^(i*angle)

    Examples
    --------
    >>> ds = fix_roms_uv(prop, data_set, logger)
    INFO:root:Applying adjustments for ROMS currents ...
    INFO:root:Finished adjusting ROMS currents.
    """
    logger.info('Applying adjustments for ROMS currents ...')

    if prop.ofsfiletype == 'fields':
        mask_rho = None
        if len(data_set['ocean_time']) > 1:
            mask_rho = np.array(data_set.variables['mask_rho'][:][0])

        elif len(data_set['ocean_time']) == 1:
            mask_rho = np.array(data_set.variables['mask_rho'][:])

        # Compute slices for interior (exclude boundaries)
        eta_slice = slice(1, mask_rho.shape[-2] - 1)
        xi_slice = slice(1, mask_rho.shape[-1] - 1)

        # Average u to rho-points (middle cells), using xarray/dask ops
        u1 = data_set['u'].isel(eta_u=eta_slice, xi_u=xi_slice)
        u2 = data_set['u'].isel(eta_u=eta_slice, xi_u=slice(
            0, mask_rho.shape[-1] - 2))  # shifted left
        avg_u = xr.concat([u1, u2], dim='avg').mean(
            dim='avg', skipna=True).fillna(0)

        v1 = data_set['v'].isel(eta_v=eta_slice, xi_v=xi_slice)
        v2 = data_set['v'].isel(eta_v=slice(
            0, mask_rho.shape[-2] - 2), xi_v=xi_slice)  # shifted up
        avg_v = xr.concat([v1, v2], dim='avg').mean(
            dim='avg', skipna=True).fillna(0)

        # Pad with zeros to match rho grid shape
        pad_width = {
            'ocean_time': (0, 0),
            's_rho': (0, 0),
            'eta_rho': (1, 1),
            'xi_rho': (1, 1),
        }
        # Ensure correct dims before padding (rename axes to match rho grid)
        avg_u = avg_u.rename({'eta_u': 'eta_rho', 'xi_u': 'xi_rho'})
        avg_v = avg_v.rename({'eta_v': 'eta_rho', 'xi_v': 'xi_rho'})

        avg_u = avg_u.pad(
            eta_rho=pad_width['eta_rho'],
            xi_rho=pad_width['xi_rho'],
            constant_values=0,
        )
        avg_v = avg_v.pad(
            eta_rho=pad_width['eta_rho'],
            xi_rho=pad_width['xi_rho'],
            constant_values=0,
        )

        # Broadcast angle to have time/layer dims
        angle = data_set['angle']
        # Broadcast angle to have ocean_time and s_rho dims (if not already)
        angle_broadcasted, _ = xr.broadcast(angle, avg_u)

        # Complex rotation (using dask/xarray, lazy)
        uveitheta = (avg_u + 1j * avg_v) * np.exp(1j * angle_broadcasted)
        u_east = uveitheta.real
        v_north = uveitheta.imag

        # Add to dataset (still lazy)
        data_set = data_set.assign(u_east=u_east, v_north=v_north)

    elif prop.ofsfiletype == 'stations':
        # Stations files don't need the adjustment from corner points to center
        # but they still need the conversion from grid dir to true north.

        # Broadcast angle to match (ocean_time, station, s_rho)
        # angle: (ocean_time, station)
        # s_rho: (s_rho,)
        # We want angle_broadcasted: (ocean_time, station, s_rho)
        angle_broadcasted, _, _ = xr.broadcast(
            data_set['angle'],                # (ocean_time, station)
            data_set['u'],                    # (ocean_time, station, s_rho)
            data_set['s_rho'],                 # (s_rho,)
        )

        # Now compute the complex rotation lazily
        uveitheta = (data_set['u'] + 1j * data_set['v']
                     ) * np.exp(1j * angle_broadcasted)
        u_east = uveitheta.real
        v_north = uveitheta.imag

        # Assign back to the dataset using DataArray assignment for metadata
        # preservation
        data_set = data_set.assign(u_east=u_east, v_north=v_north)

    logger.info('Finished adjusting ROMS currents.')
    return data_set


def fix_fvcom(prop: Any, data_set: xr.Dataset, logger: Logger) -> xr.Dataset:
    """
    Apply FVCOM-specific coordinate adjustments.

    The FVCOM model netCDF files require special handling of sigma coordinates.
    This function recreates depth coordinates (z) from sigma layers and bathymetry.

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - ofsfiletype : str
            'fields' or 'stations'
    data_set : xr.Dataset
        FVCOM model dataset
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset
        Dataset with z or zc coordinates added

    Notes
    -----
    FVCOM uses sigma coordinates:
    - siglay: sigma coordinate at layer centers
    - siglev: sigma coordinate at layer interfaces
    - h: bathymetric depth

    Depth calculation:
    - z = siglay * h (for nodes)
    - zc = average of z at element vertices (for elements)

    For stations files:
    - Adds 'z' coordinate (depth at nodes)

    For fields files:
    - Adds 'z' coordinate (depth at nodes)
    - Adds 'zc' coordinate (depth at element centers)

    Examples
    --------
    >>> ds = fix_fvcom(prop, data_set, logger)
    INFO:root:Applying adjustments for FVCOM ...
    """
    logger.info('Applying adjustments for FVCOM ...')
    if prop.ofsfiletype == 'stations':

        [_, _, deplay, _] = calc_sigma(data_set.h[0, :], data_set.siglev)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay * data_set.h
        z_cdt.attrs = {'long_name': 'nodal z-coordinate', 'units': 'meters'}
        data_set = data_set.assign_coords(z=z_cdt)

    elif prop.ofsfiletype == 'fields':

        [_, _, deplay, _] = calc_sigma(data_set.h[0, :], data_set.siglev)

        # We now can assign the z coordinate for the data.
        data_set['z'] = (['node', 'depth'], deplay)
        data_set['z'].attrs = {
            'long_name': 'nodal z-coordinate', 'units': 'meters'}
        # We now can assign the zc coordinate for the data.
        nvs = np.array(data_set.nv)[0, :, :].T - 1
        zc = []
        for tri in nvs:
            zc.append(np.mean(deplay.T[:, tri], axis=1))
        zc = np.asarray(zc).T

        # We now can assign the zc coordinate for the data.
        data_set['zc'] = (['siglay', 'nele'], zc)
        data_set['zc'].attrs = {
            'long_name': 'nele z-coordinate', 'units': 'meters'}
        data_set = data_set.assign_coords(zc=data_set['zc'])

    return data_set


def calc_sigma(h: np.ndarray, sigma: xr.DataArray) -> tuple[np.ndarray, np.ndarray,
                                                             np.ndarray, np.ndarray]:
    """
    Calculate sigma coordinates for FVCOM models.

    Converts sigma coordinates to actual depth values based on bathymetry.

    Parameters
    ----------
    h : np.ndarray
        Bathymetric depth at nodes
    sigma : xr.DataArray
        Sigma coordinate levels

    Returns
    -------
    tuple of (np.ndarray, np.ndarray, np.ndarray, np.ndarray)
        - siglay: Sigma coordinates at layer centers
        - siglev: Sigma coordinates at layer interfaces
        - deplay: Depth at layer centers (m)
        - deplev: Depth at layer interfaces (m)

    Notes
    -----
    Sigma coordinate system:
    - sigma = 0 at surface
    - sigma = -1 at bottom
    - Linearly distributed in between

    Depth calculation:
    - depth = -sigma * h

    Taken from: https://github.com/SiqiLiOcean/matFVCOM/blob/main/calc_sigma.m

    Examples
    --------
    >>> siglay, siglev, deplay, deplev = calc_sigma(h, sigma)
    """
    h = np.array(h, dtype=float).flatten()
    kb = np.shape(sigma)[0]
    kbm1 = kb - 1
    siglev = np.zeros((len(h), kb))

    for iz in range(kb):
        siglev[:, iz] = -(iz / (kb - 1))

    siglay = (siglev[:, :kbm1] + siglev[:, 1:kb]) / 2
    deplay = -siglay * h[:, np.newaxis]
    deplev = -siglev * h[:, np.newaxis]

    return siglay, siglev, deplay, deplev


def fix_adcirc_dataset(
    prop: Any,
    data_set: xr.Dataset,
    urlpaths: Any,
    logger: Logger
) -> xr.Dataset:
    """
    Apply ADCIRC-specific coordinate adjustments.

    The ADCIRC model netCDF files require special handling of time coordinate.
    This function subsets to take just the timesteps that are needed for
    the specific whichcast (nowcast, forecast_a, forecast_b).

    Parameters
    ----------
    prop : ModelProperties
        ModelProperties object containing:
        - ofsfiletype : str
            'fields' or 'stations'
        - whichcast: str
            'nowcast', 'forecast_a', 'forecast_b'
    data_set : xr.Dataset
        ADCIRC model dataset
    urlpaths: List
        List of urls/paths that were joined to make data_set.
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset
        Dataset with timesteps only for the appropriate whichcast.

    Notes
    -----
    ADCIRC outputs a single file containing all the nowcast and forecast
    data, contatenated together. So when multiple files are loaded with
    intake, the time coordinate goes something like
        t_start_0 ... t_end_0, t_start_1 ... t_end_1, ...
    where t_start_1 can be earlier than t_end_0.
    I.e., this can be a non-monotonic series.

    We need to drop the timesteps that are not part of the requested whichcast.
    We illustrate how to do this with a single STOFS-2D-Global run initialized
    at 12:00 UTC on March 1st.
    Nowcast: 06:00 UTC -> 12:00 UTC, both on March 1st
    Forecast_b: 12:00 UTC -> 18:: UTC, both on March 1st
    Forecast_a: 12:00 UTC -> 00:00 UTC on March 9th.

    Technical note: we cannot use time for subsetting, because we have a
    non-monotonic coordinate with repeated labels. Therefore we have to
    calculate everything with positional indexing (numpy style). This
    requires some strong assumptions about structure, and we raise an
    exception if these assumptions are off.
    """
    if prop.model_source != 'adcirc':
        raise ValueError('Function fix_adcirc_dataset should only be used with ADCIRC data!')

    # We use the run length and number of cycles per day for timestep
    # indexing, so get them from the appropriate function.
    fcst_a_hours, fcstcycles = get_fcst_hours(prop.ofs)

    # Get the number of timesteps in various pieces of the dataset:
    fcst_b_hours = 24 / len(fcstcycles)
    nowcast_hours = 24 / len(fcstcycles)
    if int(fcst_a_hours) != fcst_a_hours:
        logger.warning('ADCIRC temporal subsetting: '
                       f'Forecast_a hours is not an integer: {fcst_a_hours}. '
                       'This may cause issues with subsetting. Continuing...')
    if int(fcst_b_hours) != fcst_b_hours:
        logger.warning('ADCIRC temporal subsetting: '
                       f'Forecast_b hours is not an integer: {fcst_b_hours}. '
                       'This may cause issues with subsetting. Continuing...')
    if int(nowcast_hours) != nowcast_hours:
        logger.warning('ADCIRC temporal subsetting: '
                       f'Nowcast hours is not an integer: {nowcast_hours}. '
                       'This may cause issues with subsetting. Continuing...')
    if prop.ofsfiletype == 'fields':
        timesteps_per_hour = 1
    elif prop.ofsfiletype == 'stations':
        timesteps_per_hour = 10
    else:
        raise ValueError(f'ofsfiletype {prop.ofsfiletype} not recognized.')
    n_t_fcst_a = int(fcst_a_hours) * timesteps_per_hour
    n_t_fcst_b = int(fcst_b_hours) * timesteps_per_hour
    n_t_nowcast = int(nowcast_hours) * timesteps_per_hour
    n_t_total = n_t_nowcast + n_t_fcst_a

    # Calculate the number of runs in the dataset, and check
    # that it matches the number of items in urlpaths.
    N_runs = len(data_set.time) / n_t_total
    if int(N_runs) != N_runs:
        logger.warning('ADCIRC temporal subsetting: '
                       f'Number of runs calculated from dataset time coordinate ({N_runs}) is not an integer. '
                       'This may cause issues with subsetting. Converting to integer with int() function.')
    if N_runs != len(urlpaths):
        logger.warning('ADCIRC temporal subsetting: '
                       f'Number of constituent files in ADCIRC dataset ({len(urlpaths)})'
                       'not equal to the number calculated from the number of timesteps '
                       f'({len(data_set.time)} / {n_t_total}). Continuing with the calculated number.')

    # Set up a variable that will be equal to 1 for the timesteps
    # we want to keep.
    keep_t_s = np.zeros(len(data_set.time))

    # Loop over each run and convert values from 0 to 1 for the
    # timesteps we want to keep.
    if prop.whichcast == 'nowcast':
        for i_run in range(int(N_runs)):
            keep_t_s[(i_run*n_t_total) + 0:
                     (i_run*n_t_total) + n_t_nowcast] = 1
    elif prop.whichcast == 'forecast_b':
        for i_run in range(int(N_runs)):
            keep_t_s[(i_run*n_t_total) + n_t_nowcast:
                     (i_run*n_t_total) + n_t_nowcast + n_t_fcst_b] = 1
    elif prop.whichcast == 'forecast_a':
        for i_run in range(int(N_runs)):
            keep_t_s[(i_run*n_t_total) + n_t_nowcast:
                     (i_run*n_t_total) + n_t_nowcast + n_t_fcst_a] = 1
    else:
        raise ValueError(f'ofsfiletype {prop.ofsfiletype} not recognized.')

    # Subset the dataset.
    data_set = data_set.loc[dict(time=(keep_t_s == 1))]

    # Check timesteps.
    if prop.whichcast == 'forecast_a':
        # forecast_a repeats the same time series, so we just consider half.
        delta_t = np.diff(data_set.time.data[0:int(len(data_set.time)/2)])
    else:
        delta_t = np.diff(data_set.time.data)
    if np.any(delta_t <= np.timedelta64(0, 's')):
        logger.warning('ADCIRC temporal subsetting: '
                       'Time coordinate is not strictly increasing after subsetting. '
                       'This may cause issues with downstream processing. Continuing...')
    if delta_t.max() != delta_t.min():
        logger.warning('ADCIRC temporal subsetting: '
                       'Time coordinate has non-uniform spacing after subsetting '
                       f'(max = {delta_t.max()}, min = {delta_t.min()}). '
                       'This may cause issues with downstream processing. Continuing...')
    expected_delta_t = np.timedelta64(int(3600 / timesteps_per_hour), 's')
    if delta_t.max() != expected_delta_t:
        logger.warning('ADCIRC temporal subsetting: '
                       f'Time coordinate spacing (max = {delta_t.max()}) does not match expected spacing ({expected_delta_t}). '
                       'This may cause issues with downstream processing. Continuing...')
    #
    return data_set


def get_station_dim(engine: str, urlpaths: list[str],
                    drop_variables: list[str], logger: Logger) -> tuple[bool, int]:
    """
    Check dimension compatibility of all stations files.

    Verifies that all stations files have the same number of stations.
    If dimensions are inconsistent, identifies the reference file with
    the minimum number of stations.

    Parameters
    ----------
    engine : str
        NetCDF engine to use ('netcdf4' or 'h5netcdf')
    urlpaths : list of str
        List of file paths or URLs
    drop_variables : list of str
        Variables to drop when opening files
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    tuple of (bool, int)
        - dim_compat: True if all files have same station dimension
        - dim_ref: Index of file with minimum stations (for slicing reference)

    Notes
    -----
    Station files may have different numbers of stations if:
    - Model grid changed during the time period
    - Some stations were added/removed
    - Files come from different model configurations

    If incompatible, subsequent processing will slice all files
    to match the minimum station dimension.

    Examples
    --------
    >>> dim_compat, dim_ref = get_station_dim(engine, urlpaths, drop_variables, logger)
    >>> if not dim_compat:
    ...     print(f"Will use file {dim_ref} as reference for slicing")
    """


    def _read_dim(file):
        source = intake.open_netcdf(
            urlpath=file,
            xarray_kwargs={
                'engine': engine,
                'drop_variables': drop_variables,
                'chunks': {},
            },
        )
        ds = source.read()
        dim = ds.dims['station']
        ds.close()
        return dim

    num_files = len(urlpaths)
    max_workers = min(num_files, 8)
    logger.info(
        'Checking station dimensions for %d files in parallel '
        '(max_workers=%d)', num_files, max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        station_dim = list(executor.map(_read_dim, urlpaths))

    dim_compat = True
    dim_ref = []
    if np.nanmax(np.diff(station_dim)) != 0:
        dim_compat = False
        # Get reference dataset index
        dim_ref = np.argmin(station_dim)
    return dim_compat, dim_ref


def remove_extra_stations(engine: str,
    urlpaths: list[str], dim_ref: int, drop_variables: list[str], time_name: str,
    logger: Logger,
) -> xr.Dataset:
    """
    Remove extra stations from files to ensure dimension compatibility.

    If station dimensions are NOT all the same/compatible, this function:
    1. Reads the reference file (with minimum stations)
    2. For each file, removes stations not in the reference set
    3. Combines all files with consistent dimensions

    Parameters
    ----------
    engine : str
        NetCDF engine to use ('netcdf4' or 'h5netcdf')
    urlpaths : list of str
        List of file paths or URLs
    dim_ref : int
        Index of reference file with minimum stations
    drop_variables : list of str
        Variables to drop when opening files
    time_name : str
        Name of time dimension for concatenation
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    xr.Dataset
        Combined dataset with consistent station dimensions

    Raises
    ------
    SystemExit
        If station dimensions remain inconsistent after processing

    Notes
    -----
    Station matching is done by latitude comparison:
    - Reference latitudes are extracted from dim_ref file
    - Each file is checked for stations not in reference
    - Extra stations are dropped before concatenation

    Examples
    --------
    >>> ds = remove_extra_stations(engine, urlpaths, dim_ref,
    ...                            drop_variables, time_name, logger)
    INFO:root:Looping through each stations file, applying corrections...
    INFO:root:Done with corrections loop! Files are combined.
    """
    refsource = intake.open_netcdf(
        urlpath=urlpaths[dim_ref],
        xarray_kwargs={
            'engine': engine,
            'drop_variables': drop_variables,
        },
    )
    refds = refsource.read()
    reflat = np.array(refds['lat_rho'])
    # Now loop through datasets. Check for and remove extra stations.
    logger.info('Looping through each stations file, applying corrections...')
    for i, file in enumerate(urlpaths):
        tempsource = intake.open_netcdf(
            urlpath=file,
            xarray_kwargs={
                'engine': 'h5netcdf',
                'drop_variables': drop_variables,
                'decode_times': True,
                'chunks': 'auto',
            },
        )
        tempds = tempsource.read()
        latcheck = np.isin(np.array(tempds['lat_rho']), reflat, invert=True)
        latcheck = np.where(latcheck)[0]
        tempds = tempds.drop_isel(station=latcheck)
        # If compatible, then combine datasets
        if file == urlpaths[0]:
            ds = tempds
        elif file != urlpaths[0]:
            try:
                ds = xr.combine_nested(
                    [ds, tempds],
                    concat_dim=time_name,
                )
            except ValueError as e_x:
                logger.error(f'Station dims are inconsistent! {e_x}')
                logger.info('Check intake_scisa.py.')
                raise SystemExit(-1)
    logger.info('Done with corrections loop! Files are combined.')
    return ds
