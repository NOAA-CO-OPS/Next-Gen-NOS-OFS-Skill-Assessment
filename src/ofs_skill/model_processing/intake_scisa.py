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

from logging import Logger
from typing import Any

import intake
import numpy as np
import xarray as xr


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
    elif prop.model_source == 'schism' and prop.ofs != 'loofs-nextgen':
        drop_variables = [
            'temp_surface', 'temp_bottom', 'salt_surface', 'salt_bottom',
            'uvel_surface', 'vvel_surface', 'uvel_bottom', 'vvel_bottom',
            'uvel4.5', 'vvel4.5',
        ]
        time_name = 'time'
    elif prop.model_source == 'schism' and prop.ofs == 'loofs-nextgen':
        time_name = 'time'

    if prop.ofs == 'necofs':
        engine = 'netcdf4'
    else:
        engine = 'h5netcdf'

    urlpaths = file_list
    if prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
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
        dim_compat, dim_ref = get_station_dim(engine, urlpaths, drop_variables, logger)

    if dim_compat:  # This will only be FALSE for stations files when
        # station dimensions do not match! Always True for fields
        # files
        # If station dimensions are all the same/compatible, send in all file
        # names (urlpaths) at one time and let xarray/intake automagically
        # combine datasets
        if prop.model_source == 'schism':
            source = intake.open_netcdf(
                urlpath=urlpaths,
                xarray_kwargs={
                    'combine': 'by_coords',  # <-- align files by coordinates
                    'engine': engine,
                    'drop_variables': drop_variables,
                    'chunks': {'time': 1},
                },
            )

        else:
            source = intake.open_netcdf(
                urlpath=urlpaths,
                xarray_kwargs={
                    'combine': 'nested',
                    'engine': engine,
                    'concat_dim': time_name,
                    'decode_times': 'False',
                    'drop_variables': drop_variables,
                    'chunks': 'auto',  # Enables lazy loading with Dask
                },
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
        ocean_time = data_set['ocean_time']
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
    station_dim = []
    dim_compat = True
    dim_ref = []
    for file in urlpaths:
        source = intake.open_netcdf(
            urlpath=file,
            xarray_kwargs={
                'engine': engine,
                'drop_variables': drop_variables,
                'chunks': {},
            },
        )
        ds = source.read()
        station_dim.append(ds.dims['station'])
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
                'decode_times': 'False',
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
