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

"""

#from datetime import timedelta
import intake
import numpy as np
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

    urlpaths = file_list
    if prop.ofsfiletype == 'stations' and prop.whichcast == 'forecast_a':
        urlpaths = urlpaths+urlpaths
    logger.info("Creating catalog ...")
    source = intake.open_netcdf(
        urlpath=urlpaths,
        xarray_kwargs={
            "combine": "nested",
            "engine": "netcdf4",
            #"coords": "minimal",
            #"compat": "override",
            "concat_dim": time_name,
            #"decode_times": "True",
            "drop_variables": drop_variables,
            "chunks": {}  # Enables lazy loading with Dask
        }
    )

    #end_time = time.time()
    #elapsed_time = end_time - start_time
    #print(f"Time to create intake catalog: {elapsed_time} seconds")

    #start_time = time.time()
    # Read the dataset lazily
    logger.info("Lazy loading catalog ...")
    ds = source.to_dask()
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
        mask_rho = None
        if len(data_set["ocean_time"]) > 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:][0])

        elif len(data_set["ocean_time"]) == 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:])


        # here we just calculate the average for the middle cells (ignore the boundaries)
        avg_u = np.mean([np.nan_to_num(np.array(
            data_set['u'][:, :, range(1, len(mask_rho) - 1),
            range(1, len(mask_rho[0]) - 1)]), nan=0),
            np.nan_to_num(np.array(
            data_set['u'][:, :, range(1, len(mask_rho) - 1),
            [j - 1 for j in range(1, len(mask_rho[0]) - 1)]]), nan=0)
            ], axis=0).tolist()

        avg_v = np.mean([np.nan_to_num(np.array(
            data_set['v'][:, :, range(1, len(mask_rho) - 1),
            range(1, len(mask_rho[0]) - 1)]), nan=0),
            np.nan_to_num(np.array(
            data_set['v'][:, :, [i - 1 for i in range(1, len(mask_rho) - 1)],
            range(1, len(mask_rho[0]) - 1)]), nan=0)
            ], axis=0).tolist()
        # this method used compreshensions which were unnecessary - replaced w/
        # above for pep8 complience
        #avg_u = np.mean([np.nan_to_num(np.array(
        #    data_set['u'][:,:, [i for i in range(1, len(mask_rho)-1)],
        #    [i for i in range(1, len(mask_rho[0])-1)]]), nan=0),
        #    np.nan_to_num(np.array(
        #    data_set['u'][:,:, [i for i in range(1, len(mask_rho)-1)],
        #    [i-1 for i in range(1, len(mask_rho[0])-1)]]), nan=0)],
        #    axis=0).tolist()

        #avg_v = np.mean([np.nan_to_num(np.array(
        #    data_set['v'][:,:, [i for i in range(1, len(mask_rho)-1)],
        #    [i for i in range(1, len(mask_rho[0])-1)]]), nan=0),
        #    np.nan_to_num(np.array(
        #    data_set['v'][:,:, [i-1 for i in range(1, len(mask_rho)-1)]
        #    ,[i for i in range(1, len(mask_rho[0])-1)]]), nan=0)],
        #    axis=0).tolist()

        # here we create a "ring" of zeros around the cells to represent the boundary
        # This way the index of avg_u and avg_v will match that of the rho variables
        npad = ((0, 0), (0,0), (1,1), (1,1))
        avg_u = np.pad(np.array(avg_u),
            pad_width=npad,
            mode='constant',
            constant_values=0)
        avg_v = np.pad(np.array(avg_v),
            pad_width=npad,
            mode='constant',
            constant_values=0)
        # Here we create a time dimension for "angle"
        # "angle" does not change over time and layers
        angle_dim = np.array(data_set['angle'])[:,np.newaxis,:,:]
        angle_dim = np.tile(angle_dim,(1,np.shape(data_set['s_rho'])[0],1,1))

        ## FROM JOHN WILKIN (https://www.myroms.org/forum/viewtopic.php?t=295):
        # % rotate coordinates from roms xi,eta grid directions to geographic east,north
        # % (but only after averaging/interpolating u,v to the rho points)
        # uveitheta = (u_roms+1i*v_roms).*exp(1i*angle); % 1i = sqrt(-1) in Matlab
        # u_east = real(uveitheta);
        # v_north = imag(uveitheta);
        uveitheta = (np.array(avg_u)+np.sqrt(-1+0j)*np.array(avg_v))*\
            np.exp(np.sqrt(-1+0j)*angle_dim)

        # If the JOHN WILKIN equation is correct (and if I copied correctly)
        # these are the u and v at rho:
        u_east = uveitheta.real
        v_north = uveitheta.imag

        # If the JOHN WILKIN equation is correct (and if I copied correctly)
        # This is just adding the 'u_east' and 'v_north' as Data Variables in the .nc file
        data_set['u_east']  = (('ocean_time', 's_rho','eta_rho','xi_rho'), np.array(u_east))
        data_set['v_north'] = (('ocean_time', 's_rho','eta_rho','xi_rho'), np.array(v_north))

    elif prop.ofsfiletype == 'stations':
        # Stations files done need the adjustment from corner points to center
        # but they still need the conversion from grid dir to true north.
        # Since the code below applys that conversion to the averaged U & V
        # for stations, we rename U and V to avg_u and avg_v for ease of
        # apply the angle conversion.

        # Here we create a time dimension for "angle"
        # "angle" does not change over time and layers
        angle_dim = np.array(data_set['angle'])[:,:,np.newaxis]
        angle_dim = np.tile(angle_dim,(1,1,np.shape(data_set['s_rho'])[0]))

        uveitheta = (np.array(data_set['u'])+np.sqrt(-1+0j)*np.array(data_set['v']))*\
            np.exp(np.sqrt(-1+0j)*angle_dim)

        u_east = uveitheta.real
        v_north = uveitheta.imag

        data_set['u_east']  = (('ocean_time', 'station', 's_rho'), np.array(u_east))
        data_set['v_north'] = (('ocean_time', 'station','s_rho',), np.array(v_north))

    # The time coordinate still needs some fixing. We will parse it and
    # reassign to the dataset.
    # the first day
    #dt0 = parser.parse(data_set.ocean_time.attrs['units'].replace('seconds since ',''))
    # parse dates summing days to the origin
    #data_set = data_set.assign(
    #    ocean_time=[dt0+timedelta(seconds=secs) for secs in data_set.ocean_time.values])

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
