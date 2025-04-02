"""
 The FVCOM model netcdf files cannot be opened in python due to the
 'siglay','siglev' variables.
 This is a "workaround" to recreate the model netcdf files in a format
 that is readable. Likewise, the ROMS model netcdf files require some 
 retooling for use in the package
"""
from datetime import timedelta
import xarray as xr
from netCDF4 import Dataset
from dateutil import parser
import numpy as np

def fvcom_netcdf(prop,dataset):
    """
    The FVCOM model netcdf files cannot be opened in python due to the
    'siglay','siglev' variables.
    This is a "workaround" to recreate the model netcdf files in a
    format that is readable.
    """
    if prop.ofsfiletype == "stations":
        # Dropping the problematic variables
        drop_variables = ["siglay", "siglev"]
        data_set = xr.open_dataset(
            dataset, drop_variables=drop_variables, decode_times=False,
            chunks='auto'
        )

        # Solving the problem with siglay and siglev. We need to workaround
        # using netCDF4 and renaming the coordinates.
        # load data with netCDF4
        #cdataset = 
        netcdf = Dataset(dataset)
        # load the problematic coordinates
        coords = {name: netcdf[name] for name in drop_variables}

        # function to extract ncattrs from `Dataset()`
        get_attrs = lambda name: {
            attr: coords[name].getncattr(attr) for attr in coords[name].ncattrs()
        }
        # function to convert from `Dataset()` to `xr.DataArray()`
        nc2xr = lambda name: xr.DataArray(
            coords[name],
            attrs=get_attrs(name),
            name=f"{name}_coord",
            dims=(f"{name}", "node"),
        )

        # apply `nc2xr()` and merge `xr.DataArray()` objects
        coords = xr.merge([nc2xr(name) for name in coords.keys()])

        # reassign to the main `xr.Dataset()`
        data_set = data_set.assign_coords(coords)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay_coord * data_set.h
        z_cdt.attrs = dict(long_name="nodal z-coordinate", units="meters")
        data_set = data_set.assign_coords(z=z_cdt)

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        if "nos." in dataset:
            dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))
            # parse dates summing days to the origin
            data_set = data_set.assign(
                time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
            )
        elif "nos." not in dataset:
            dt0 = parser.parse(data_set.time.attrs["units"].replace("seconds since ", ""))
            data_set = data_set.assign(
                time=[dt0+timedelta(seconds=secs) for secs in data_set.time.values])
            
        data_set = data_set.drop_vars([
        #"atmos_press",
        "uwind_speed","vwind_speed",
        "x","y",
        ])

    elif prop.ofsfiletype == "fields":
        drop_variables = ["siglay", "siglev"]
        data_set = xr.open_dataset(
            dataset, drop_variables=drop_variables, decode_times=False
        )

        # convert lon/c, lat/c to coordinates
        data_set = data_set.assign_coords(
            {var: data_set[var] for var in ["lon", "lat", "lonc", "latc"]}
        )

        # Solving the problem with siglay and siglev. We need to workaround
        # using netCDF4 and renaming the coordinates.
        # load data with netCDF4
        netcdf = Dataset(dataset)
        # load the problematic coordinates
        coords = {name: netcdf[name] for name in drop_variables}

        # function to extract ncattrs from `Dataset()`
        get_attrs = lambda name: {
            attr: coords[name].getncattr(attr) for attr in coords[name].ncattrs()
        }
        # function to convert from `Dataset()` to `xr.DataArray()`
        nc2xr = lambda name: xr.DataArray(
            coords[name],
            attrs=get_attrs(name),
            name=f"{name}_coord",
            dims=(f"{name}", "node"),
        )

        # apply `nc2xr()` and merge `xr.DataArray()` objects
        coords = xr.merge([nc2xr(name) for name in coords.keys()])

        # reassign to the main `xr.Dataset()`
        data_set = data_set.assign_coords(coords)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay_coord * data_set.h
        z_cdt.attrs = dict(long_name="nodal z-coordinate", units="meters")
        data_set = data_set.assign_coords(z=z_cdt)

        #We now can assign the zc coordinate for the data.
        nvs = np.array(data_set.nv).T-1
        z = np.array(z_cdt)
        zc = np.array([np.mean(z[:,tri],axis=1) for tri in nvs]).T
        data_set["zc"] = (["siglay","nele"],zc)
        data_set["zc"].attrs = dict(long_name='nele z-coordinate',units='meters')
        data_set = data_set.assign_coords(zc=data_set["zc"])

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        if "nos." in dataset:
            dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))
            # parse dates summing days to the origin
            data_set = data_set.assign(
                time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
            )
        if "nos." not in dataset:
            dt0 = parser.parse(data_set.time.attrs["units"].replace("seconds since ", ""))
            data_set = data_set.assign(
                time=[dt0+timedelta(seconds=secs) for secs in data_set.time.values])
    
    
        data_set = data_set.drop_vars(["nbsn", "partition",'nbve',
        "a1u","a2u","art1","art2",#"atmos_press",
        "aw0","awx","awy","net_heat_flux",
        "short_wave","tauc","uwind_speed","vwind_speed",
        "x","xc","y","yc", "nprocs"
        ])

    return data_set

def roms_netcdf(prop,dataset):
    """
    This function fixes two things in the roms file:
    First, it converts the vector data (e.g. currents), from the u and v coordinates
    to rho coordinates by averaging the current vectors at u and v (faces of the cell),
    and attributing the averaged value to the center of the cell (rho)
    Second, it fixes the ocean_time variable, converting it from seconds from set date,
    to actual datetime values
    """

    if prop.ofsfiletype == 'fields':
        drop_variables = ["Akk_bak","Akp_bak","Akt_bak","Akv_bak","Cs_r","Cs_w","dtfast",
        "el","f","Falpha","Fbeta","Fgamma","FSobc_in","FSobc_out","gamma2","grid","hc",
        "lat_psi","lon_psi","Lm2CLM","Lm3CLM","LnudgeM2CLM","LnudgeM3CLM","LnudgeTCLM",
        "LsshCLM","LtracerCLM","LtracerSrc","LuvSrc","LwSrc","M2nudg","M2obc_in","M2obc_out",
        "M3nudg","M3obc_in","M3obc_out","mask_psi","mask_u","mask_v","ndefHIS",
        "ndtfast","nHIS","nRST","nSTA","ntimes","Pair","pm","pn","rdrg","rdrg2","rho0",
        "s_w","spherical","Tcline","theta_b","theta_s","Tnudg","Tobc_in","Tobc_out",
        "Uwind","Vwind","Vstretching","Vtransform","w","wetdry_mask_psi","wetdry_mask_rho",
        "wetdry_mask_u","wetdry_mask_v","xl","Znudg","Zob","Zos"]
    
        data_set = xr.open_dataset(dataset, drop_variables=drop_variables,decode_times=False)
    
        #loading the center of the cells, mask indicates if it is wet or dry
        #This IF statement takes care of case where
        #we pass a .nc with 1 or more timesteps ['ocean_time']
        # lats = np.array(data_set.variables["lat_rho"][:])
        # lons = np.array(data_set.variables["lon_rho"][:])
        if len(data_set["ocean_time"]) > 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:][0])
    
        elif len(data_set["ocean_time"]) == 1:
            mask_rho =  np.array(data_set.variables["mask_rho"][:])
    
        # here we just calculate the average for the middle cells (ignore the boundaries)
        avg_u = np.mean([np.nan_to_num(np.array(
            data_set['u'][:,:, [i for i in range(1, len(mask_rho)-1)],
                                [i for i in range(1, len(mask_rho[0])-1)]]), nan=0),
                         np.nan_to_num(np.array(
                            data_set['u'][:,:, [i for i in range(1, len(mask_rho)-1)],
                                [i-1 for i in range(1, len(mask_rho[0])-1)]]), nan=0)], axis=0).tolist()
    
        avg_v = np.mean([np.nan_to_num(np.array(
            data_set['v'][:,:, [i for i in range(1, len(mask_rho)-1)],
                                [i for i in range(1, len(mask_rho[0])-1)]]), nan=0),
                         np.nan_to_num(np.array(
                            data_set['v'][:,:, [i-1 for i in range(1, len(mask_rho)-1)],
                                [i for i in range(1, len(mask_rho[0])-1)]]), nan=0)], axis=0).tolist()
    
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
        angle_dim=[]
        for i in range(len(np.array(avg_u))):
            angle_dim.append(np.array(data_set.variables["angle"]))
    
        angle_dim = np.array(angle_dim)
    
        ## FROM JOHN WILKIN (https://www.myroms.org/forum/viewtopic.php?t=295):
        # % rotate coordinates from roms xi,eta grid directions to geographic east,north
        # % (but only after averaging/interpolating u,v to the rho points)
        # uveitheta = (u_roms+1i*v_roms).*exp(1i*angle); % 1i = sqrt(-1) in Matlab
        # u_east = real(uveitheta);
        # v_north = imag(uveitheta);
    
        uveitheta = (np.array(avg_u)+np.sqrt(-1+0j)*np.array(avg_v))*np.exp(np.sqrt(-1+0j)*angle_dim)
    
        # If the JOHN WILKIN equation is correct (and if I copied correctly)
        # these are the u and v at rho:
        u_east = uveitheta.real
        v_north = uveitheta.imag
    
        # This is just adding the 'u_east' and 'v_north' as Data Variables in the .nc file
        data_set['u_east'] = (('ocean_time', 's_rho','eta_rho','xi_rho'), np.array(u_east))
        data_set['v_north'] = (('ocean_time', 's_rho','eta_rho','xi_rho'), np.array(v_north))
    
        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        dt0 = parser.parse(data_set.ocean_time.attrs['units'].replace('seconds since ',''))
        # parse dates summing days to the origin
        data_set = data_set.assign(
            ocean_time=[dt0+timedelta(seconds=secs) for secs in data_set.ocean_time.values])

    elif prop.ofsfiletype == 'stations':
        drop_variables = ["Akk_bak","Akp_bak","Akt_bak","Akv_bak","Cs_r","Cs_w","dtfast",
        "el","f","Falpha","Fbeta","Fgamma","FSobc_in","FSobc_out","gamma2","grid","hc",
        "lat_psi","lon_psi","Lm2CLM","Lm3CLM","LnudgeM2CLM","LnudgeM3CLM","LnudgeTCLM",
        "LsshCLM","LtracerCLM","LtracerSrc","LuvSrc","LwSrc","M2nudg","M2obc_in","M2obc_out",
        "M3nudg","M3obc_in","M3obc_out","mask_psi","mask_u","mask_v","ndefHIS",
        "ndtfast","nHIS","nRST","nSTA","ntimes","Pair","pm","pn","rdrg","rdrg2","rho0",
        "s_w","spherical","Tcline","theta_b","theta_s","Tnudg","Tobc_in","Tobc_out",
        "Uwind","Vwind","Vstretching","Vtransform","w","wetdry_mask_psi","wetdry_mask_rho",
        "wetdry_mask_u","wetdry_mask_v","xl","Znudg","Zob","Zos"]
    
        data_set = xr.open_dataset(dataset, drop_variables=drop_variables,decode_times=False)
        
        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        dt0 = parser.parse(data_set.ocean_time.attrs['units'].replace('seconds since ',''))
        # parse dates summing days to the origin
        data_set = data_set.assign(
            ocean_time=[dt0+timedelta(seconds=secs) for secs in data_set.ocean_time.values])
        
    return data_set
