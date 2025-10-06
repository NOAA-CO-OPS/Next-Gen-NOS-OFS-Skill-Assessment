"""
-*- coding: utf-8 -*-

Documentation for Script intake_sci-sa.py

Directory Location:   /path/to/ofs_dps/server/bin/model_processing

Technical Contact(s): Name:  AJK

This script creates a catalog and lazily loads model netcdf files.

Language:  Python 3.11

Estimated Execution Time: 1 min

usage: Called by write_ofs_ctlfile

Arguments:
    - file_list: List of model NETCDF4 files to include inthe catalog.
                 Output from list_of_files.
    - prop: Includes parameters of the Skill Assesment run.
    - logger: Handles Skill Assesment run logging messages.
Returns:
    - Lazily loaded model dataset

Author Name:  AJK       Creation Date:  12/2024

Revisions:
    Date          Author             Description
    05/01/2025    AJK                Fix CIOFS issues and optimze fix_roms_uv function

"""

#from datetime import timedelta
import sys
import intake
import numpy as np
import xarray as xr

#from dateutil import parser

def intake_model(file_list, prop, logger):
    """
    This function uses Intake and dask to create a catalog of model files (passed
    from list_of_files) and lazily load the catalog using Dask.
    This function also calls fix_roms_uv, which makes current adjustments for
    ROMS based models (fields and stations).
    """

    logger.info("Starting catalog ...")
    #start_time = time.time()
    drop_variables = None
    time_name = None
    if prop.model_source == 'roms':
        time_name = "ocean_time"
        drop_variables = ["Akk_bak","Akp_bak","Akt_bak","Akv_bak","Cs_r","Cs_w","dtfast",
        "el","f","Falpha","Fbeta","Fgamma","FSobc_in","FSobc_out","gamma2","grid","hc",
        "lat_psi","lon_psi","Lm2CLM","Lm3CLM","LnudgeM2CLM","LnudgeM3CLM","LnudgeTCLM",
        "LsshCLM","LtracerCLM","LtracerSrc","LuvSrc","LwSrc","M2nudg","M2obc_in","M2obc_out",
        "M3nudg","M3obc_in","M3obc_out","mask_psi","mask_u","mask_v","ndefHIS",
        "ndtfast","nHIS","nRST","nSTA","ntimes","Pair","pm","pn","rdrg","rdrg2","rho0",
        "s_w","spherical","Tcline","theta_b","theta_s","Tnudg","Tobc_in","Tobc_out",
        "Uwind","Vwind","Vstretching","Vtransform","w","wetdry_mask_psi","wetdry_mask_rho",
        "wetdry_mask_u","wetdry_mask_v","xl","Znudg","Zob","Zos"]
    elif prop.model_source == 'fvcom':
        time_name = "time"
        #drop_variables = ["siglay", "siglev"]
    elif prop.model_source == 'schism':
        drop_variables = ["temp_surface","temp_bottom", "salt_surface","salt_bottom",
                         "uvel_surface","vvel_surface","uvel_bottom","vvel_bottom",
                         "uvel4.5","vvel4.5"]
        time_name = "time"
    urlpaths = file_list
    if prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
        urlpaths = urlpaths+urlpaths
    logger.info("Creating catalog ...")

    # First check stations dimensions to see if all are compatible --
    # only for stations files!
    dim_compat = True
    if prop.ofsfiletype == 'stations':
        dim_compat, dim_ref = get_station_dim(urlpaths, drop_variables, logger)
    if dim_compat: # This will only be FALSE for stations files when
                   # station dimensions do not match! Always True for fields
                   # files
        # If station dimensions are all the same/compatible, send in all file
        # names (urlpaths) at one time and let xarray/intake automagically
        # combine datasets
        if prop.model_source == 'schism':
           source = intake.open_netcdf(
           urlpath=urlpaths,
           xarray_kwargs={
                "combine": "by_coords",  # <-- align files by coordinates 
                #"engine": "netcdf4",
                "engine": "h5netcdf",
                "drop_variables": drop_variables,
                #"chunks": "auto" #might need to set to auto 
                "chunks": {"time": 1} #might need to set to auto 
                #"decode_cf": False
                }
           )
 
        else:     
           source = intake.open_netcdf(
           urlpath=urlpaths,
           xarray_kwargs={
                "combine": "nested",
                #"engine": "netcdf4",
                "engine": "h5netcdf",
                #"coords": "minimal",
                #"compat": "override",
                "concat_dim": time_name,
                "decode_times": "False",
                "drop_variables": drop_variables,
                "chunks": "auto"  # Enables lazy loading with Dask
                }    
           )

        # Read the dataset lazily
        logger.info("No dimension changes needed, lazy loading catalog ...")
        ds = source.to_dask()
    else:
        logger.info("Station dimensions are inconsistent! Slicing stations...")
        ds = remove_extra_stations(urlpaths, dim_ref, drop_variables,
                                   time_name, logger)
    #end_time = time.time()
    #elapsed_time = end_time - start_time
    #print(f"Time to create intake catalog: {elapsed_time} seconds")

    #start_time = time.time()

    # Round all times to nearest minute
    ds[time_name] = ds[time_name].dt.round('1min')
    if prop.ofsfiletype == 'stations' and prop.whichcast != 'forecast_a':
        ds = ds.drop_duplicates(dim=time_name,keep='last')
    elif prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
        ds = ds.drop_duplicates(dim=time_name,keep='first')

    logger.info("Lazy loading complete applying adjustments ...")
    #end_time = time.time()
    #elapsed_time = end_time - start_time
    #print(f"Time to lazily load catalog: {elapsed_time} seconds")

    if prop.model_source == 'roms':
        ds = fix_roms_uv(prop, ds, logger)
    elif prop.model_source == 'fvcom':
        ds = fix_fvcom(prop, ds, logger)
    return ds


def fix_roms_uv(prop, data_set, logger):
    """
    This function adjusts currents (u and v) for ROMS models, including:
    (1) Adjusts from phi to rho grid (fields files only)
    (2) Adjusts from grid relative direction to true north relative
    Documentation here
    """

    logger.info("Applying adjustments for ROMS currents ...")

    if prop.ofsfiletype == 'fields':
        ocean_time = data_set["ocean_time"]
        mask_rho = None
        if len(data_set["ocean_time"]) > 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:][0])

        elif len(data_set["ocean_time"]) == 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:])

        # Compute slices for interior (exclude boundaries)
        eta_slice = slice(1, mask_rho.shape[-2]-1)
        xi_slice = slice(1, mask_rho.shape[-1]-1)

        # Average u to rho-points (middle cells), using xarray/dask ops
        u1 = data_set['u'].isel(eta_u=eta_slice, xi_u=xi_slice)
        u2 = data_set['u'].isel(eta_u=eta_slice, xi_u=slice(0, mask_rho.shape[-1]-2))  # shifted left
        avg_u = xr.concat([u1, u2], dim='avg').mean(dim='avg', skipna=True).fillna(0)

        v1 = data_set['v'].isel(eta_v=eta_slice, xi_v=xi_slice)
        v2 = data_set['v'].isel(eta_v=slice(0, mask_rho.shape[-2]-2), xi_v=xi_slice)  # shifted up
        avg_v = xr.concat([v1, v2], dim='avg').mean(dim='avg', skipna=True).fillna(0)

        # Pad with zeros to match rho grid shape
        pad_width = {
            'ocean_time': (0, 0),
            's_rho': (0, 0),
            'eta_rho': (1, 1),
            'xi_rho': (1, 1)
        }
        # Ensure correct dims before padding (rename axes to match rho grid)
        avg_u = avg_u.rename({'eta_u': 'eta_rho', 'xi_u': 'xi_rho'})
        avg_v = avg_v.rename({'eta_v': 'eta_rho', 'xi_v': 'xi_rho'})

        avg_u = avg_u.pad(
            eta_rho=pad_width['eta_rho'],
            xi_rho=pad_width['xi_rho'],
            constant_values=0
        )
        avg_v = avg_v.pad(
            eta_rho=pad_width['eta_rho'],
            xi_rho=pad_width['xi_rho'],
            constant_values=0
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
        # Stations files done need the adjustment from corner points to center
        # but they still need the conversion from grid dir to true north.
        # Since the code below applys that conversion to the averaged U & V
        # for stations, we rename U and V to avg_u and avg_v for ease of
        # apply the angle conversion.

        # Broadcast angle to match (ocean_time, station, s_rho)
        # angle: (ocean_time, station)
        # s_rho: (s_rho,)
        # We want angle_broadcasted: (ocean_time, station, s_rho)
        angle_broadcasted, _, _ = xr.broadcast(
            data_set['angle'],                # (ocean_time, station)
            data_set['u'],                    # (ocean_time, station, s_rho)
            data_set['s_rho']                 # (s_rho,)
        )

        # Now compute the complex rotation lazily
        uveitheta = (data_set['u'] + 1j * data_set['v']) * np.exp(1j * angle_broadcasted)
        u_east = uveitheta.real
        v_north = uveitheta.imag

        # Assign back to the dataset using DataArray assignment for metadata preservation
        data_set = data_set.assign(u_east=u_east, v_north=v_north)

    logger.info("Finished adjusting ROMS currents.")
    return data_set

def fix_fvcom(prop, data_set, logger):
    """
    The FVCOM model netcdf files cannot be opened in python due to the
    'siglay','siglev' variables.
    This is a "workaround" to recreate the model netcdf files in a
    format that is readable.
    """
    logger.info("Applying adjustments for FVCOM ...")
    if prop.ofsfiletype == "stations":

        [_, _, deplay, _] = calc_sigma(data_set.h[0,:], data_set.siglev)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay * data_set.h
        z_cdt.attrs = {"long_name": 'nodal z-coordinate', "units": 'meters'}
        data_set = data_set.assign_coords(z=z_cdt)

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        #if "nos." in file_list:
        #    dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))
        #    # parse dates summing days to the origin
        #    data_set = data_set.assign(
        #        time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
        #    )
        #elif "nos." not in file_list:
        #    dt0 = parser.parse(data_set.time.attrs["units"].replace("seconds since ", ""))
        #    data_set = data_set.assign(
        #        time=[dt0+timedelta(seconds=secs) for secs in data_set.time.values])


    elif prop.ofsfiletype == "fields":

        [_, _, deplay, _] = calc_sigma(data_set.h[0,:], data_set.siglev)

        # We now can assign the z coordinate for the data.
        data_set["z"] = (["node","depth"],deplay)
        data_set["z"].attrs = {"long_name": 'nodal z-coordinate', "units": 'meters'}
        #We now can assign the zc coordinate for the data.
        nvs  = np.array(data_set.nv)[0,:,:].T-1
        zc = []
        for tri in nvs:
            zc.append(np.mean(deplay.T[:,tri],axis=1))
        zc = np.asarray(zc).T

        #We now can assign the zc coordinate for the data.
        data_set["zc"] = (["siglay","nele"],zc)
        data_set["zc"].attrs = {"long_name": 'nele z-coordinate', "units": 'meters'}
        data_set = data_set.assign_coords(zc=data_set["zc"])

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        #if "nos." in file_list:
        #    print('!!! nos. in file list')
        #    dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))
        #    # parse dates summing days to the origin
        #    data_set = data_set.assign(
        #        time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
        #    )
        #if "nos." not in file_list:
        #    print('!!! nos. not in file list')
        #    # changed "units" to "format"
        #    dt0 = parser.parse(data_set.time.attrs["format"].replace("seconds since ", ""))
        #        data_set = data_set.assign(
        #        time=[dt0+timedelta(seconds=secs) for secs in data_set.time.values])

    # # Round all times to nearest minute
    # data_set['time'] = data_set['time'].dt.round('1min')

    return data_set


def calc_sigma(h, sigma):
    """
    Taken from https://github.com/SiqiLiOcean/matFVCOM/blob/main/calc_sigma.m
    """
    h = np.array(h, dtype=float).flatten()
    #kb = sigma['nz'] #original line
    kb = np.shape(sigma)[0] # new line
    kbm1 = kb - 1
    siglev = np.zeros((len(h), kb))

    for iz in range(kb):
        siglev[:, iz] = -(iz / (kb - 1))

    siglay = (siglev[:, :kbm1] + siglev[:, 1:kb]) / 2
    deplay = -siglay * h[:, np.newaxis]
    deplev = -siglev * h[:, np.newaxis]

    return siglay, siglev, deplay, deplev

def get_station_dim(urlpaths,drop_variables,logger):
    """
    Check dimensions of all stations files needed for model time series
    extraction. If dims are consistent, then dim_compat=True; otherwise,
    dim_compat=False and we'll need to remove extra stations in a subsequent
    function. dim_ref returns the reference index of the stations file
    with the fewest number of stations.
    """
    station_dim = []
    dim_compat = True
    dim_ref = []
    for file in urlpaths:
        source = intake.open_netcdf(
            urlpath=file,
            xarray_kwargs={
                #"engine": "netcdf4",
                "engine": "h5netcdf",
                "drop_variables": drop_variables,
                "chunks": {}})
        ds = source.read()
        station_dim.append(ds.dims['station'])
    if np.nanmax(np.diff(station_dim)) != 0:
        dim_compat = False
        # Get reference dataset index
        dim_ref = np.argmin(station_dim)
    return dim_compat, dim_ref

def remove_extra_stations(urlpaths, dim_ref, drop_variables, time_name,
                          logger):
    '''
    If station dimensions are NOT all the same/compatible, check one file
    at a time. Then use the minimum station dimension (station_dim) to
    slice off extra stations in other files before combining.
    First, get reference coordinates to compare other datasets to. Finally,
    return combined dataset where all files have the same number of stations.
    '''
    refsource = intake.open_netcdf(
        urlpath=urlpaths[dim_ref],
        xarray_kwargs={
            #"engine": "netcdf4",
            "engine": "h5netcdf",
            "drop_variables": drop_variables,
            #"chunks": {}
            })
    refds = refsource.read()
    reflat = np.array(refds['lat_rho'])
    # Now loop through datasets. Check for and remove extra stations.
    logger.info("Looping through each stations file, applying corrections...")
    for i,file in enumerate(urlpaths):
        tempsource = intake.open_netcdf(
            urlpath=file,
            xarray_kwargs={
                #"engine": "netcdf4",
                "engine": "h5netcdf",
                "drop_variables": drop_variables,
                "decode_times": "False",
                "chunks": "auto"
                })
        tempds = tempsource.read()
        latcheck = np.isin(np.array(tempds['lat_rho']),reflat,invert=True)
        latcheck = np.where(latcheck)[0]
        tempds = tempds.drop_isel(station=latcheck)
        # If compatible, then combine datasets
        if file == urlpaths[0]:
            ds = tempds
        elif file != urlpaths[0]:
            try:
                ds = xr.combine_nested(
                    [ds,tempds],
                    concat_dim=time_name,
                    #chunks='auto'
                    #decode_times=False,
                    #engine='netcdf4',
                    )
            except ValueError as e_x:
                logger.error(f"Station dims are inconsistent! {e_x}")
                logger.info("Check intake_scisa.py.")
                sys.exit(-1)
    logger.info("Done with corrections loop! Files are combined.")
    return ds
