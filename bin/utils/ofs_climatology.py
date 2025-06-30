# """
# -*- coding: utf-8 -*-

# Documentation for Scripts ofs_climatology.py

# Directory Location:   tbd

# Technical Contact(s): Name:  FC, AJK

# Abstract:

#    This script is used to calculate the average ssh,
#    sst, salinity, u and v for the ofs station and fields files.

# Language:  Python 3.8

# Estimated Execution Time:

# Scripts/Programs Called:

# Usage (windows): python ofs_climatology.py -s 2023-11-15T02:02:02Z -e 2024-01-15T15:15:15Z -p \path\to\dbofs_stations -d \path\to\outdir -o dbofs -m roms -t fields -g 1,2
# Usage (linux): python ofs_climatology.py  -s 2023-01-01T00:00:00Z -e 2024-12-31T23:59:59Z -p /path/to/dbofs/netcdf/ -d /path/to/outdir -o dbofs -m roms -t fields -g all

# Arguments:
#   -h, --help            show this help message and exit
#   -o OFS, --ofs OFS     Name of the OFS
#   -p PathToOFS, --PathToOFS
#                         Path to the directory where the .station/.fields files are saved
#   -d DirOut, --DirOut
#                         Path to the directory where the output climatology file will be saved
#   -s StartDate, --StartDate
#                         Start Date YYYY-MM-DDThh:mm:ssZ
#                         e.g. '2023-01-01T12:34:00Z'
#   -e EndDate, --EndDate
#                         End Date YYYY-MM-DDThh:mm:ssZ
#                         e.g. '2023-01-01T12:34:00Z'
#   -m Model, --Model
#                         roms or fvcom
#   -t DataType, --DataType
#                         stations or fields
#   -g DataGrouping, --DataGrouping
#                         How the data will be grouped, e.g.: 01,02,03 (Jan, Feb, Mar),
#                                                             all (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec),
#                                                             none (no monthly means, i.e. it will average everything between start and end dates)

# Output:
# 1)  climatology file
#     /{DirOut}/{OFS}_Clim_{DataType}_{StartDate}_{EndDate}_{Month_Name or none}.nc

# Author Name:  FC       Creation Date:  02/13/2024

# Revisions:
#     Date          Author             Description
#     06/13/2024    AJK    Adapting for merge into SCI_SA code base
#     02/26/2024    FC     Added the capability of grouping by month
#     xx/xx/xxxx    FC     Added the capability of retrieving observations
#     yy/yy/yyyy    FC     Added the capability of creating plots

# """


import os
import sys
from datetime import datetime, timedelta
from os import listdir

import argparse
import xarray as xr
import numpy as np
from netCDF4 import Dataset
from dateutil import parser

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
# from pylab import *
from matplotlib.cm import ScalarMappable


def dates_range(start_date, end_date, month_list):
    """
    This function takes the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates=[]
    for i in range(
        int(
            (
                datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                - datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
            ).days
        )
        + 1
    ):
        date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=i)

        if int(date.month) in month_list:
            dates.append(date.strftime("%m/%d/%y"))

    return dates

def list_of_dir(start_date, end_date, path_files, month_list):
    """
    This function takes the output of dates_range, which is a
    list of dates, and creates a list of directories based on model_path
    model_path is the path to where the model data is stored,
    that will change from server to server.
    """

    dir_list = []
    dates = dates_range(start_date, end_date, month_list)
    dates_len = len(dates)
    for date_index in range(0, dates_len):
        year = datetime.strptime(dates[date_index], "%m/%d/%y").year
        month = datetime.strptime(dates[date_index], "%m/%d/%y").month
        model_dir = f"{path_files}/{year}{month:02}"
        # if not os.path.exists(model_dir):
        #     print("Model data path " + model_dir + " not found. ")

        #     sys.exit(-1)
        dir_list.append(model_dir)

    dir_list = list(set(dir_list))
    dir_list.sort(key=lambda x: x.split("//")[-1][-2:])
    dir_list.sort(key=lambda x: x.split("//")[-1][-6:-2])
    # dir_list.sort(key = lambda x: x.split('\\')[-1][-2:])

    return dir_list

def list_of_files(start_date, end_date, dir_list, type_data, datagroup):
    """
    This function takes the output of list_of_dir and lists all
    files inside each directory.
    These files are then sorted according to their model temporal order.
    This is important for ensuring that the data is concatenated
    correctly (in the correct temporal order)
    Sorting is different if we are concatenating nowcast or
    Forecast model files (see the if statements below)
    """
    if type_data == "stations":
        list_files = []
        for i_index in range(0, len(dir_list)):

            all_files = listdir(dir_list[i_index])
            files = []
            for af_name in all_files:
                if "stations.n" in af_name:
                    files.append(af_name)

            files.sort(key=lambda x: x.split(".")[-4][-3:])
            files.sort(key=lambda x: x.split(".")[-2][1:3])
            files.sort(key=lambda x: x.split(".")[-3][-2:])
            files = [i for i in files if i.find("000") == -1]
            files = [dir_list[i_index] + "//" + i for i in files]

            list_files.append(files)

        list_files = [[i for i in item if "nos." in i] for item in list_files]

        if datagroup == "none":
            list_files[0] = [
                i
                for i in list_files[0]
                if int(i.split(".")[-3][-2:])
                >= datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").day
            ]
            list_files[-1] = [
                i for i in list_files[-1] if
                int(i.split(".")[-3][-2:]) <= datetime.strptime(
                    end_date , "%Y-%m-%dT%H:%M:%SZ").day
            ]

        list_files = sum(list_files, [])

    elif type_data == "fields":
        list_files = []
        dir_list_len = len(dir_list)
        for i_index in range(0, dir_list_len):
            all_files = listdir(dir_list[i_index])
            files = []
            for af_name in all_files:
                if "fields.n" in af_name:
                    files.append(af_name)

            files.sort(key=lambda x: x.split(".")[-4][-3:])
            files.sort(key=lambda x: x.split(".")[-2][1:3])
            files.sort(key=lambda x: x.split(".")[-3][-2:])
            files = [i for i in files if i.find("000") == -1]
            files = [dir_list[i_index] + "//" + i for i in files]

            list_files.append(files)

        #this line is temporary, it excludes the files with the new OFS naming convention.
        list_files = [[i for i in item if "nos." in i] for item in list_files]

        if datagroup == "none":
            list_files[0] = [
                i
                for i in list_files[0]
                if int(i.split(".")[-3][-2:])
                >= datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").day
            ]
            list_files[-1] = [
                i for i in list_files[-1] if
                int(i.split(".")[-3][-2:]) <= datetime.strptime(
                    end_date , "%Y-%m-%dT%H:%M:%SZ").day]

        list_files = sum(list_files, [])

    return list_files

def fvcom_netcdf(dataset, type_data):
    """
    The FVCOM model netcdf files cannot be opened in python due to the
    'siglay','siglev' variables.
    This is a "workaround" to recreate the model netcdf files in a
    format that is readable.
    """

    if type_data == "stations":
        # Dropping the problematic variables
        drop_variables = ["siglay", "siglev"]
        data_set = xr.open_dataset(
            dataset, drop_variables=drop_variables, decode_times=False
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

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))

        # parse dates summing days to the origin
        data_set = data_set.assign(
            time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
        )

    elif type_data == "fields":
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
        dt0 = parser.parse(data_set.time.attrs["units"].replace("days since ", ""))

        # parse dates summing days to the origin
        data_set = data_set.assign(
            time=[dt0 + timedelta(seconds=day * 86400) for day in data_set.time.values]
        )

        data_set = data_set.drop_vars(["nbsn", "partition",'nbve',
        "a1u","a2u","art1","art2",#"atmos_press",
        "aw0","awx","awy","net_heat_flux",
        "short_wave","tauc","uwind_speed","vwind_speed",
        "x","xc","y","yc"
        ])


    return data_set

def stations_climatology(model, list_of_files_outp):
    '''
    note: adding name_station won't work because not all ofs have "name_station"
    '''
    if model == "roms":
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [],[],[],[],[]
        for i in range(len(list_of_files_outp)):
            ds = xr.open_dataset(f"{list_of_files_outp[i]}")
            print(f"{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}")

            zeta = np.array([np.array(ds['zeta'][:,i]).mean() for i in range(len(np.array(ds['zeta'][0])))])
            temp = np.array([np.array(ds['temp'][:,i,-1]).mean() for i in range(len(np.array(ds['temp'][0])))])
            salt = np.array([np.array(ds['salt'][:,i,-1]).mean() for i in range(len(np.array(ds['salt'][0])))])
            u = np.array([np.array(ds['u'][:,i,-1]).mean() for i in range(len(np.array(ds['u'][0])))])
            v = np.array([np.array(ds['v'][:,i,-1]).mean() for i in range(len(np.array(ds['v'][0])))])

            zeta[zeta==float('inf')] =np.nan
            temp[temp==float('inf')] =np.nan
            salt[salt==float('inf')] =np.nan
            u[u==float('inf')] =np.nan
            v[v==float('inf')] =np.nan

            ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']  = zeta,temp,salt,u,v

            cycle_zeta.append(ds['zeta_avg'])
            cycle_temp.append(ds['temp_avg'])
            cycle_salt.append(ds['salt_avg'])
            cycle_u.append(ds['u_avg'])
            cycle_v.append(ds['v_avg'])

        climatology_zeta = np.mean(cycle_zeta, axis=0)
        climatology_temp = np.mean(cycle_temp, axis=0)
        climatology_salt = np.mean(cycle_salt, axis=0)
        climatology_u = np.mean(cycle_u, axis=0)
        climatology_v = np.mean(cycle_v, axis=0)

        ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']= climatology_zeta, climatology_temp, climatology_salt,climatology_u,climatology_v
        ds2 = ds[['zeta_avg','temp_avg','salt_avg','u_avg','v_avg','lon_rho','lat_rho']]

    elif model == "fvcom":
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [],[],[],[],[]
        for i in range(len(list_of_files_outp)):
            ds = fvcom_netcdf(f"{list_of_files_outp[i]}", "stations")
            print(f"{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}")

            zeta = np.array([np.array(ds['zeta'][:,i]).mean() for i in range(len(np.array(ds['zeta'][0])))])
            temp = np.array([np.array(ds['temp'][:,0,i]).mean() for i in range(len(np.array(ds['temp'][0,0])))])
            salinity = np.array([np.array(ds['salinity'][:,0,i]).mean() for i in range(len(np.array(ds['salinity'][0,0])))])
            u = np.array([np.array(ds['u'][:,0,i]).mean() for i in range(len(np.array(ds['u'][0,0])))])
            v = np.array([np.array(ds['v'][:,0,i]).mean() for i in range(len(np.array(ds['v'][0,0])))])

            zeta[zeta==float('inf')] =np.nan
            temp[temp==float('inf')] =np.nan
            salinity[salinity==float('inf')] =np.nan
            u[u==float('inf')] =np.nan
            v[v==float('inf')] =np.nan

            ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']  = zeta,temp,salinity,u,v

            cycle_zeta.append(ds['zeta_avg'])
            cycle_temp.append(ds['temp_avg'])
            cycle_salt.append(ds['salt_avg'])
            cycle_u.append(ds['u_avg'])
            cycle_v.append(ds['v_avg'])

        climatology_zeta = np.mean(cycle_zeta, axis=0)
        climatology_temp = np.mean(cycle_temp, axis=0)
        climatology_salt = np.mean(cycle_salt, axis=0)
        climatology_u = np.mean(cycle_u, axis=0)
        climatology_v = np.mean(cycle_v, axis=0)

        ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']= climatology_zeta, climatology_temp, climatology_salt,climatology_u,climatology_v
        ds2 = ds[['zeta_avg','temp_avg','salt_avg','u_avg','v_avg','lon','lat']]

    return ds2

def fields_climatology(model, list_of_files_outp):
    if model == "roms":
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [],[],[],[],[]
        for i in range(len(list_of_files_outp)):
            ds = xr.open_dataset(f"{list_of_files_outp[i]}")
            print(f"{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}")

            zeta = np.array([np.array(ds['zeta'][0,:,:])]).ravel()
            temp = np.array([np.array(ds['temp'][0,-1,:,:])]).ravel()
            salt = np.array([np.array(ds['salt'][0,-1,:,:])]).ravel()
            u = np.array([np.array(ds['u'][0,-1,:,:])]).ravel()
            v = np.array([np.array(ds['v'][0,-1,:,:])]).ravel()

            zeta[zeta==float('inf')] =np.nan
            temp[temp==float('inf')] =np.nan
            salt[salt==float('inf')] =np.nan
            u[u==float('inf')] =np.nan
            v[v==float('inf')] =np.nan

            ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg'] = zeta,temp,salt,u,v

            cycle_zeta.append(ds['zeta_avg'])
            cycle_temp.append(ds['temp_avg'])
            cycle_salt.append(ds['salt_avg'])
            cycle_u.append(ds['u_avg'])
            cycle_v.append(ds['v_avg'])

        climatology_zeta = np.mean(cycle_zeta, axis=0).reshape(len(np.array(ds['zeta'])[0]),len(np.array(ds['zeta'])[0][0]))
        climatology_temp = np.mean(cycle_temp, axis=0).reshape(len(np.array(ds['temp'])[0][0]),len(np.array(ds['temp'])[0][0][0]))
        climatology_salt = np.mean(cycle_salt, axis=0).reshape(len(np.array(ds['salt'])[0][0]),len(np.array(ds['salt'])[0][0][0]))
        climatology_u = np.mean(cycle_u, axis=0).reshape(len(np.array(ds['u'])[0][0]),len(np.array(ds['u'])[0][0][0]))
        climatology_v = np.mean(cycle_v, axis=0).reshape(len(np.array(ds['v'])[0][0]),len(np.array(ds['v'])[0][0][0]))

        ds['zeta_avg'] = xr.DataArray(climatology_zeta,
                           coords={
                                   "lon_rho": ds['lon_rho'],
                                   "lat_rho": ds['lat_rho'],
                                  },
                           dims=['eta_rho', 'xi_rho']
                          )
        ds['temp_avg'] = xr.DataArray(climatology_temp,
                           coords={
                                   "lon_rho": ds['lon_rho'],
                                   "lat_rho": ds['lat_rho'],
                                  },
                           dims=['eta_rho', 'xi_rho']
                          )
        ds['salt_avg'] = xr.DataArray(climatology_salt,
                           coords={
                                   "lon_rho": ds['lon_rho'],
                                   "lat_rho": ds['lat_rho'],
                                  },
                           dims=['eta_rho', 'xi_rho']
                          )
        ds['u_avg'] = xr.DataArray(climatology_u,
                           coords={
                                   "lon_u": ds['lon_u'],
                                   "lat_u": ds['lat_u'],
                                  },
                           dims=['eta_u', 'xi_u']
                          )
        ds['v_avg'] = xr.DataArray(climatology_v,
                           coords={
                                   "lon_v": ds['lon_v'],
                                   "lat_v": ds['lat_v'],
                                  },
                           dims=['eta_v', 'xi_v']
                          )
        ds2 = ds[['zeta_avg','temp_avg','salt_avg','u_avg','v_avg']]

    elif model == "fvcom":
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [],[],[],[],[]
        for i in range(len(list_of_files_outp)):
            ds = fvcom_netcdf(f"{list_of_files_outp[i]}", "fields")
            print(f"{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}")

            zeta = np.array([np.array(ds['zeta'][0])]).ravel()
            temp = np.array([np.array(ds['temp'][0][0])]).ravel()
            salinity = np.array([np.array(ds['salinity'][0][0])]).ravel()
            u = np.array([np.array(ds['u'][0][0])]).ravel()
            v = np.array([np.array(ds['v'][0][0])]).ravel()

            zeta[zeta==float('inf')] =np.nan
            temp[temp==float('inf')] =np.nan
            salinity[salinity==float('inf')] =np.nan
            u[u==float('inf')] =np.nan
            v[v==float('inf')] =np.nan

            ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']  = zeta,temp,salinity,u,v

            cycle_zeta.append(ds['zeta_avg'])
            cycle_temp.append(ds['temp_avg'])
            cycle_salt.append(ds['salt_avg'])
            cycle_u.append(ds['u_avg'])
            cycle_v.append(ds['v_avg'])

        climatology_zeta = np.mean(cycle_zeta, axis=0)
        climatology_temp = np.mean(cycle_temp, axis=0)
        climatology_salt = np.mean(cycle_salt, axis=0)
        climatology_u = np.mean(cycle_u, axis=0)
        climatology_v = np.mean(cycle_v, axis=0)

        ds['zeta_avg'],ds['temp_avg'],ds['salt_avg'],ds['u_avg'],ds['v_avg']= climatology_zeta, climatology_temp, climatology_salt,climatology_u,climatology_v
        ds2 = ds[['zeta_avg','temp_avg','salt_avg','u_avg','v_avg','lon','lat','lonc','latc','nv']]

    return ds2

def basemap(avg_file,model):
    mapping_buffer = .1

    if model == "fvcom":
        lon_min,lon_max = np.array(avg_file['lon']).min(),np.array(avg_file['lon']).max()
        lat_min, lat_max = np.array(avg_file['lat']).min(),np.array(avg_file['lat']).max()

    elif model == "roms":
        lon_rho,lat_rho = np.array(avg_file["lon_rho"])[~np.isnan(avg_file["zeta_avg"])], np.array(avg_file['lat_rho'])[~np.isnan(avg_file["zeta_avg"])]
        lon_min,lon_max = lon_rho.min(),lon_rho.max()
        lat_min, lat_max = lat_rho.min(),lat_rho.max()

    extents = np.array((lon_min,lon_max,
                        lat_min, lat_max))

    m = Basemap(llcrnrlon=extents[0]-mapping_buffer,
                llcrnrlat=extents[2]-mapping_buffer,
                urcrnrlon=extents[1]+mapping_buffer,
                urcrnrlat=extents[3]+mapping_buffer,
                rsphere=(6378137.00, 6356752.3142),
                resolution='h',
                projection='cyl',
                lat_0=extents[-2:].mean(),
                lon_0=extents[:2].mean(),
                lat_ts=extents[2:].mean())

    parallels = np.arange(np.floor(extents[2]), np.ceil(extents[3]), abs((abs(np.ceil(extents[3]))-abs(np.floor(extents[2])))/10))
    meridians = np.arange(np.floor(extents[0]), np.ceil(extents[1]), abs((abs(np.ceil(extents[1]))-abs(np.floor(extents[0])))/10))

    return m,parallels,meridians

def fields_plot(file,ofs,month_name,model,path_save):

    avg_file = xr.open_dataset(file,decode_times=False)
    base=basemap(avg_file,model)
    variables = ["zeta_avg", "temp_avg", "salt_avg", "u_avg", "v_avg"]

    print(f"Creating Field Plots...")

    if model == "fvcom":
        fvcom_lon = np.array(avg_file['lon'])
        fvcom_lat = np.array(avg_file['lat'])
        triangles = np.array(avg_file['nv']).T-1

        for var in variables:
            fig = plt.figure(figsize=(10, 10))  # units are inches for the size

            # if var == "zeta_avg":
            #     zmax = .5
            #     zmin = -.5
            # elif var == "temp_avg":
            #     zmax = 30
            #     zmin = 0
            # elif var == "salt_avg":
            #     zmax = 50
            #     zmin = 0
            # elif var == "u_avg":
            #     zmax = 1
            #     zmin = -1
            # elif var == "v_avg":
            #     zmax = 1
            #     zmin = -1

            zmax = np.array(avg_file[var].max()).max()
            zmin = np.array(avg_file[var].min()).min()

            # FVCOM plot
            #ax11 = fig.add_subplot(221)
            ax11 = fig.add_axes([0.1,0.1,0.8,0.8])
            ax11.title.set_text(f"{ofs}: {var} - {month_name}")
            tp2 = ax11.tripcolor(fvcom_lon, fvcom_lat, triangles, np.array(avg_file[var]))#, cmap=cm.viridis)
            tp2.set_clim(zmin,zmax)

            base[0].drawcoastlines(linewidth=.5)
            base[0].drawparallels(base[1], labels=[1,0,0,0], linewidth=.5)
            base[0].drawmeridians(base[2], labels=[0,0,0,1], linewidth=.5)
            base[0].arcgisimage(service='World_Imagery', xpixels = 900, verbose= False)


            # Plot Attributes

            cmap = plt.get_cmap('viridis')
            norm = plt.Normalize(zmin, zmax)
            sm =  ScalarMappable(norm=norm, cmap=cmap)
            cbar = fig.colorbar(sm, ax=[ax11],orientation="horizontal", pad=0.05)
            #cbar.set_label("{} ({})".format(fvcom[var].attrs["long_name"], fvcom[var].attrs["units"]))

            # Save
            plt.savefig(r'{}\{}_{}_{}.jpeg'.format(path_save,var,ofs,month_name),bbox_inches='tight',dpi=300)


    elif model == "roms":

        for var in variables:
            if var == "u_avg":
                lats = avg_file.variables["lat_u"]
                lons = avg_file.variables["lon_u"]
            elif var == "v_avg":
                lats = avg_file.variables["lat_v"]
                lons = avg_file.variables["lon_v"]
            else:
                lats = avg_file.variables["lat_rho"]
                lons = avg_file.variables["lon_rho"]

            fig = plt.figure(figsize=(10, 10))  # units are inches for the size

            zmax = np.array(avg_file[var].max()).max()
            zmin = np.array(avg_file[var].min()).min()

            ax11 = fig.add_axes([0.1,0.1,0.8,0.8])
            ax11.title.set_text(f"{ofs}: {var} - {month_name}")
            tp2=ax11.pcolormesh(lons,lats,avg_file[var],cmap='viridis')
            tp2.set_clim(zmin,zmax)

            base[0].drawcoastlines(linewidth=.5)
            base[0].drawparallels(base[1], labels=[1,0,0,0], linewidth=.5)
            base[0].drawmeridians(base[2], labels=[0,0,0,1], linewidth=.5)
            base[0].arcgisimage(service='World_Imagery', xpixels = 900, verbose= False)


            ##Plot Attributes
            cmap = plt.get_cmap('viridis')
            norm = plt.Normalize(zmin, zmax)
            sm =  ScalarMappable(norm=norm, cmap=cmap)
            cbar = fig.colorbar(sm, ax=[ax11],orientation="horizontal", pad=0.05)

            # Save
            plt.savefig(r'{}\{}_{}_{}.jpeg'.format(path_save,var,ofs,month_name),bbox_inches='tight',dpi=300)

    print(f"... Plot Creating Complete!")

def stations_plot(files_to_plot,ofs,path_save):

    print(f"Creating Station Plots...")

    variables = ["zeta_avg", "temp_avg", "salt_avg", "u_avg", "v_avg"]

    z,t,s,u,v,m=[],[],[],[],[],[]
    for d in enumerate(files_to_plot):
        ds = xr.open_dataset(d[-1])
        z.append(np.array(ds["zeta_avg"]))
        t.append(np.array(ds["temp_avg"]))
        s.append(np.array(ds["salt_avg"]))
        u.append(np.array(ds["u_avg"]))
        v.append(np.array(ds["v_avg"]))
        m.append(datetime.strptime(str(int(d[0])+1), "%m").strftime("%b"))

    for site in range(len(z[0])):
        # for month in enumerate(m):
        zz = np.array(z)[:,site]
        tt = np.array(t)[:,site]
        ss = np.array(s)[:,site]
        uu = np.array(u)[:,site]
        vv = np.array(v)[:,site]

        fig, axs = plt.subplots(5,1, figsize=(6, 10), layout='constrained')

        print(f"Creating Plot: {site+1} of {len(z[0])}")
        for ax, var, c, l in zip(axs.flat, variables, ["b","r","g","k","k"], ["meters","Celsius","ppm","meters per second","meters per second"]):
            ax.set_title(f'OFS: {ofs}, Station: {site}, Variable: {var}')
            if var == "zeta_avg":
                y = zz
            elif var == "temp_avg":
                y = tt
            elif var == "salt_avg":
                y = ss
            elif var == "u_avg":
                y = uu
            elif var == "v_avg":
                y = vv

            ax.grid(ls='--')
            ax.set_ylabel(l)
            ax.plot(m, y, 'o', ls='-', ms=4, color = c)

        plt.savefig(r'{}\{}_{}.jpeg'.format(path_save,site,ofs),bbox_inches='tight',dpi=300)

    print(f"... Plot Creating Complete!")

def ofs_climatology(start_date, end_date, path_files, path_save, ofs, model, type_data, datagroup):

    dates = []
    if str(datagroup)=="all" or str(datagroup)=="none":
        month_list=[1,2,3,4,5,6,7,8,9,10,11,12]
    else:
        month_list=datagroup.split(",")
        month_list=[int(i) for i in month_list]

    if datagroup == "none":
        print("Starting run...")
        list_of_directories = list_of_dir(start_date, end_date, path_files, month_list)

        list_of_files_outp = list_of_files(start_date, end_date,list_of_directories, type_data, datagroup)
        print(f".{type_data} files found!")

        print("Start creating Climatology")
        if f"{type_data}" == "stations":
            clim_nc = stations_climatology(model, list_of_files_outp)
        elif f"{type_data}" == "fields":
            clim_nc = fields_climatology(model, list_of_files_outp)


        start_date_str, end_date_str = start_date, end_date
        file_name=f'{ofs}_Clim_{type_data}_{start_date.split("T")[0]}_{end_date.split("T")[0]}_{datagroup}.nc'

        clim_nc.to_netcdf(f"{path_save}/{file_name}")
        print("... Run complete!")

        files_to_plot = [f"{path_save}/{file_name}"]

    else:
        files_to_plot = []
        for m in month_list:
            print(f"Starting run {m} of {len(month_list)}...")
            list_of_directories = list_of_dir(start_date, end_date, path_files, [m])

            list_of_files_outp = list_of_files(start_date, end_date,list_of_directories, type_data, datagroup)
            print(f".{type_data} files found!")

            print("Start creating Climatology")
            if f"{type_data}" == "stations":
                clim_nc = stations_climatology(model, list_of_files_outp)
            elif f"{type_data}" == "fields":
                clim_nc = fields_climatology(model, list_of_files_outp)


            start_date_str, end_date_str = start_date, end_date
            month_name = datetime.strptime(str(m), "%m").strftime("%b")
            file_name=f'{ofs}_Clim_{type_data}_{start_date.split("T")[0]}_{end_date.split("T")[0]}_{month_name}.nc'

            clim_nc["time"] = month_name
            clim_nc.to_netcdf(f"{path_save}/{file_name}")

            files_to_plot.append(f"{path_save}/{file_name}")

            print(f"... Run complete {m} of {len(month_list)}!")


    if f"{type_data}" == "fields":
        [fields_plot(files_to_plot[i], ofs, datetime.strptime(str(month_list[i]), "%m").strftime("%b"), model,path_save) for i in range(len(files_to_plot))]

    elif f"{type_data}" == "stations":
        stations_plot(files_to_plot,ofs,path_save)



if __name__ == "__main__":
    parser_arg = argparse.ArgumentParser(
        prog="python ofs_climatology.py",
        usage="%(prog)s",
        description="Lists all .station files between start and end dates, and calculates the average per station",
    )

    parser_arg.add_argument(
        "-s",
        "--StartDate",
        required=True,
        help="Start Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser_arg.add_argument(
        "-e",
        "--EndDate",
        required=True,
        help="End Date YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser_arg.add_argument(
        "-p",
        "--PathToOFS",
        required=True,
        help="Path to the directory where the monthly station files are saved",
    )
    parser_arg.add_argument(
        "-d",
        "--DirOut",
        required=True,
        help="Directory where the output climatology will be saved",
    )
    parser_arg.add_argument(
        "-o",
        "--OFSName",
        required=True,
        help="Name of the OFS (e.g. cbofs)",
    )
    parser_arg.add_argument(
        "-m",
        "--Model",
        required=True,
        help="Name of the model used in the OFS (roms or fvcom)",
    )
    parser_arg.add_argument(
        "-t",
        "--DataType",
        required=True,
        help="stations or fields",
    )
    parser_arg.add_argument(
        "-g",
        "--DataGrouping",
        required=True,
        help="How the data will be grouped, e.g.: 01,02,03 (Jan, Feb, Mar), all (Jan, Feb,...Nov, Dec), none (no monthly means)",
    )
    args = parser_arg.parse_args()


    ofs_climatology(args.StartDate, args.EndDate, args.PathToOFS, args.DirOut, args.OFSName, args.Model, args.DataType, args.DataGrouping)
