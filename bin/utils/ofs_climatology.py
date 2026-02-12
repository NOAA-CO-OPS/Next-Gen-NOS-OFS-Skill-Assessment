"""
-*- coding: utf-8 -*-

Documentation for Scripts ofs_climatology.py

Directory Location:   /bin/utils/

Technical Contact(s): Name:  FC, AJK

Abstract:

   This script is used to calculate the average ssh,
   sst, salinity, u and v for the ofs station and fields files.

Language:  Python 3.8

Estimated Execution Time:

Scripts/Programs Called:

Usage (windows): python ofs_climatology.py -s 2023-11-15T02:02:02Z -e 2024-01-15T15:15:15Z -d \\path\to\\outdir -o dbofs -t fields -g 1,2
Usage (linux): python ofs_climatology.py  -s 2023-01-01T00:00:00Z -e 2024-12-31T23:59:59Z -d /path/to/outdir -o dbofs -t fields -g all

Arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Name of the OFS
  -d DirOut, --DirOut
                        Path to the directory where the output climatology file will be saved
  -s StartDate, --StartDate
                        Start Date YYYY-MM-DDThh:mm:ssZ
                        e.g. '2023-01-01T12:34:00Z'
  -e EndDate, --EndDate
                        End Date YYYY-MM-DDThh:mm:ssZ
                        e.g. '2023-01-01T12:34:00Z'
  -t DataType, --DataType
                        stations or fields
  -g DataGrouping, --DataGrouping
                        How the data will be grouped, e.g.: 01,02,03 (Jan, Feb, Mar),
                                                            all (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec),
                                                            none (no monthly means, i.e. it will average everything between start and end dates)

Output:
1) Climatology file
    /{DirOut}/{OFS}_Clim_{DataType}_{StartDate}_{EndDate}_{Month_Name or none}.nc
2) Map plot
    /{DirOut}/{var}_avg_{ofs}_{month}.jpeg

Author Name:  FC       Creation Date:  02/13/2024

Revisions:
    Date          Author             Description
    11/14/2025    AJK    Updating from basemap to cartopy and for modern filename convention
    06/13/2024    AJK    Adapting for merge into SCI_SA code base
    02/26/2024    FC     Added the capability of grouping by month
    xx/xx/xxxx    FC     Added the capability of retrieving observations
    yy/yy/yyyy    FC     Added the capability of creating plots

"""
from __future__ import annotations

import argparse
import logging.config
import os
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.img_tiles as cimgt
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from dateutil import parser
from netCDF4 import Dataset

warnings.filterwarnings('ignore', category=DeprecationWarning)

# Import from ofs_skill package
from ofs_skill.model_processing import list_of_files, model_properties, model_source
from ofs_skill.obs_retrieval import utils

# print(f"--- CONTENTS OF cimgt: {dir(cimgt)} ---") # <-- ADD THIS LINE
# from pylab import *
# Define the URL for the ArcGIS World Imagery service
url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}.jpg'


def fvcom_netcdf(dataset, type_data):
    """
    The FVCOM model netcdf files cannot be opened in python due to the
    'siglay','siglev' variables.
    This is a "workaround" to recreate the model netcdf files in a
    format that is readable.
    """

    if type_data == 'stations':
        # Dropping the problematic variables
        drop_variables = ['siglay', 'siglev']
        data_set = xr.open_dataset(
            dataset, drop_variables=drop_variables, decode_times=False,
        )

        # Solving the problem with siglay and siglev. We need to workaround
        # using netCDF4 and renaming the coordinates.
        # load data with netCDF4
        netcdf = Dataset(dataset)
        # load the problematic coordinates
        coords = {name: netcdf[name] for name in drop_variables}

        # function to extract ncattrs from `Dataset()`
        def get_attrs(name): return {
            attr: coords[name].getncattr(attr) for attr in coords[name].ncattrs()
        }
        # function to convert from `Dataset()` to `xr.DataArray()`

        def nc2xr(name): return xr.DataArray(
            coords[name],
            attrs=get_attrs(name),
            name=f'{name}_coord',
            dims=(f'{name}', 'node'),
        )

        # apply `nc2xr()` and merge `xr.DataArray()` objects
        coords = xr.merge([nc2xr(name) for name in coords.keys()])

        # reassign to the main `xr.Dataset()`
        data_set = data_set.assign_coords(coords)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay_coord * data_set.h
        z_cdt.attrs = dict(long_name='nodal z-coordinate', units='meters')
        data_set = data_set.assign_coords(z=z_cdt)

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        dt0 = parser.parse(
            data_set.time.attrs['units'].replace('days since ', ''),
        )

        # parse dates summing days to the origin
        data_set = data_set.assign(
            time=[
                dt0 + timedelta(seconds=day * 86400)
                for day in data_set.time.values
            ],
        )

    elif type_data == 'fields':
        drop_variables = ['siglay', 'siglev']
        data_set = xr.open_dataset(
            dataset, drop_variables=drop_variables, decode_times=False,
        )

        # convert lon/c, lat/c to coordinates
        data_set = data_set.assign_coords(
            {var: data_set[var] for var in ['lon', 'lat', 'lonc', 'latc']},
        )

        # Solving the problem with siglay and siglev. We need to workaround
        # using netCDF4 and renaming the coordinates.
        # load data with netCDF4
        netcdf = Dataset(dataset)
        # load the problematic coordinates
        coords = {name: netcdf[name] for name in drop_variables}

        # function to extract ncattrs from `Dataset()`
        def get_attrs(name): return {
            attr: coords[name].getncattr(attr) for attr in coords[name].ncattrs()
        }
        # function to convert from `Dataset()` to `xr.DataArray()`

        def nc2xr(name): return xr.DataArray(
            coords[name],
            attrs=get_attrs(name),
            name=f'{name}_coord',
            dims=(f'{name}', 'node'),
        )

        # apply `nc2xr()` and merge `xr.DataArray()` objects
        coords = xr.merge([nc2xr(name) for name in coords.keys()])

        # reassign to the main `xr.Dataset()`
        data_set = data_set.assign_coords(coords)

        # We now can assign the z coordinate for the data.
        z_cdt = data_set.siglay_coord * data_set.h
        z_cdt.attrs = dict(long_name='nodal z-coordinate', units='meters')
        data_set = data_set.assign_coords(z=z_cdt)

        # We now can assign the zc coordinate for the data.
        nvs = np.array(data_set.nv).T-1
        z = np.array(z_cdt)
        zc = np.array([np.mean(z[:, tri], axis=1) for tri in nvs]).T
        data_set['zc'] = (['siglay', 'nele'], zc)
        data_set['zc'].attrs = dict(
            long_name='nele z-coordinate', units='meters',
        )
        data_set = data_set.assign_coords(zc=data_set['zc'])

        # The time coordinate still needs some fixing. We will parse it and
        # reassign to the dataset.
        # the first day
        dt0 = parser.parse(
            data_set.time.attrs['units'].replace('days since ', ''),
        )

        # parse dates summing days to the origin
        data_set = data_set.assign(
            time=[
                dt0 + timedelta(seconds=day * 86400)
                for day in data_set.time.values
            ],
        )

        data_set = data_set.drop_vars([
            'nbsn', 'partition', 'nbve',
            'a1u', 'a2u', 'art1', 'art2',  # "atmos_press",
            'aw0', 'awx', 'awy', 'net_heat_flux',
            'short_wave', 'tauc', 'uwind_speed', 'vwind_speed',
            'x', 'xc', 'y', 'yc',
        ])

    return data_set


def stations_climatology(logger, model, list_of_files_outp):
    '''
    note: adding name_station won't work because not all ofs have "name_station"
    '''
    if model == 'roms':
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [], [], [], [], []
        for i in range(len(list_of_files_outp)):
            ds = xr.open_dataset(f'{list_of_files_outp[i]}')
            logger.info(
                f'{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}',
            )

            zeta = np.array([
                np.array(ds['zeta'][:, i]).mean()
                for i in range(len(np.array(ds['zeta'][0])))
            ])
            temp = np.array([
                np.array(ds['temp'][:, i, -1]).mean()
                for i in range(len(np.array(ds['temp'][0])))
            ])
            salt = np.array([
                np.array(ds['salt'][:, i, -1]).mean()
                for i in range(len(np.array(ds['salt'][0])))
            ])
            u = np.array([
                np.array(ds['u'][:, i, -1]).mean()
                for i in range(len(np.array(ds['u'][0])))
            ])
            v = np.array([
                np.array(ds['v'][:, i, -1]).mean()
                for i in range(len(np.array(ds['v'][0])))
            ])

            zeta[zeta == float('inf')] = np.nan
            temp[temp == float('inf')] = np.nan
            salt[salt == float('inf')] = np.nan
            u[u == float('inf')] = np.nan
            v[v == float('inf')] = np.nan

            ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = zeta, temp, salt, u, v

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

        ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = climatology_zeta, climatology_temp, climatology_salt, climatology_u, climatology_v
        ds2 = ds[[
            'zeta_avg', 'temp_avg', 'salt_avg',
            'u_avg', 'v_avg', 'lon_rho', 'lat_rho',
        ]]

    elif model == 'fvcom':
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [], [], [], [], []
        for i in range(len(list_of_files_outp)):
            ds = fvcom_netcdf(f'{list_of_files_outp[i]}', 'stations')
            logger.info(
                f'{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}',
            )

            zeta = np.array([
                np.array(ds['zeta'][:, i]).mean()
                for i in range(len(np.array(ds['zeta'][0])))
            ])
            temp = np.array([
                np.array(ds['temp'][:, 0, i]).mean()
                for i in range(len(np.array(ds['temp'][0, 0])))
            ])
            salinity = np.array([
                np.array(ds['salinity'][:, 0, i]).mean()
                for i in range(len(np.array(ds['salinity'][0, 0])))
            ])
            u = np.array([
                np.array(ds['u'][:, 0, i]).mean()
                for i in range(len(np.array(ds['u'][0, 0])))
            ])
            v = np.array([
                np.array(ds['v'][:, 0, i]).mean()
                for i in range(len(np.array(ds['v'][0, 0])))
            ])

            zeta[zeta == float('inf')] = np.nan
            temp[temp == float('inf')] = np.nan
            salinity[salinity == float('inf')] = np.nan
            u[u == float('inf')] = np.nan
            v[v == float('inf')] = np.nan

            ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = zeta, temp, salinity, u, v

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

        ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = climatology_zeta, climatology_temp, climatology_salt, climatology_u, climatology_v
        ds2 = ds[[
            'zeta_avg', 'temp_avg', 'salt_avg',
            'u_avg', 'v_avg', 'lon', 'lat',
        ]]

    return ds2


def fields_climatology(logger, model, list_of_files_outp):
    if model == 'roms':
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [], [], [], [], []
        for i in range(len(list_of_files_outp)):
            ds = xr.open_dataset(f'{list_of_files_outp[i]}')
            logger.info(
                f'{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}',
            )

            zeta = ds['zeta'][0, :, :].values.ravel()
            # zeta = np.array([np.array(ds['zeta'][0, :, :])]).ravel()
            temp = ds['temp'][0, -1, :, :].values.ravel()
            # temp = np.array([np.array(ds['temp'][0, -1, :, :])]).ravel()
            salt = ds['salt'][0, -1, :, :].values.ravel()
            # salt = np.array([np.array(ds['salt'][0, -1, :, :])]).ravel()
            u = ds['u'][0, -1, :, :].values.ravel()
            v = ds['v'][0, -1, :, :].values.ravel()
            # u = np.array([np.array(ds['u'][0, -1, :, :])]).ravel()
            # v = np.array([np.array(ds['v'][0, -1, :, :])]).ravel()

            zeta[zeta == float('inf')] = np.nan
            temp[temp == float('inf')] = np.nan
            salt[salt == float('inf')] = np.nan
            u[u == float('inf')] = np.nan
            v[v == float('inf')] = np.nan

            ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = zeta, temp, salt, u, v

            cycle_zeta.append(ds['zeta_avg'])
            cycle_temp.append(ds['temp_avg'])
            cycle_salt.append(ds['salt_avg'])
            cycle_u.append(ds['u_avg'])
            cycle_v.append(ds['v_avg'])

        climatology_zeta = np.mean(cycle_zeta, axis=0).reshape(
            len(np.array(ds['zeta'])[0]), len(np.array(ds['zeta'])[0][0]),
        )
        climatology_temp = np.mean(cycle_temp, axis=0).reshape(
            len(np.array(ds['temp'])[0][0]), len(
                np.array(ds['temp'])[0][0][0],
            ),
        )
        climatology_salt = np.mean(cycle_salt, axis=0).reshape(
            len(np.array(ds['salt'])[0][0]), len(
                np.array(ds['salt'])[0][0][0],
            ),
        )
        climatology_u = np.mean(cycle_u, axis=0).reshape(
            len(np.array(ds['u'])[0][0]), len(np.array(ds['u'])[0][0][0]),
        )
        climatology_v = np.mean(cycle_v, axis=0).reshape(
            len(np.array(ds['v'])[0][0]), len(np.array(ds['v'])[0][0][0]),
        )

        ds['zeta_avg'] = xr.DataArray(
            climatology_zeta,
            coords={
                'lon_rho': ds['lon_rho'],
                'lat_rho': ds['lat_rho'],
            },
            dims=['eta_rho', 'xi_rho'],
        )
        ds['temp_avg'] = xr.DataArray(
            climatology_temp,
            coords={
                'lon_rho': ds['lon_rho'],
                'lat_rho': ds['lat_rho'],
            },
            dims=['eta_rho', 'xi_rho'],
        )
        ds['salt_avg'] = xr.DataArray(
            climatology_salt,
            coords={
                'lon_rho': ds['lon_rho'],
                'lat_rho': ds['lat_rho'],
            },
            dims=['eta_rho', 'xi_rho'],
        )
        ds['u_avg'] = xr.DataArray(
            climatology_u,
            coords={
                'lon_u': ds['lon_u'],
                'lat_u': ds['lat_u'],
            },
            dims=['eta_u', 'xi_u'],
        )
        ds['v_avg'] = xr.DataArray(
            climatology_v,
            coords={
                'lon_v': ds['lon_v'],
                'lat_v': ds['lat_v'],
            },
            dims=['eta_v', 'xi_v'],
        )
        ds2 = ds[['zeta_avg', 'temp_avg', 'salt_avg', 'u_avg', 'v_avg']]

    elif model == 'fvcom':
        cycle_zeta, cycle_temp, cycle_salt, cycle_u, cycle_v = [], [], [], [], []
        for i in range(len(list_of_files_outp)):
            ds = fvcom_netcdf(f'{list_of_files_outp[i]}', 'fields')
            logger.info(
                f'{list_of_files_outp[i]} -- found!... file {i+1} of {len(list_of_files_outp)}',
            )

            zeta = np.array([np.array(ds['zeta'][0])]).ravel()
            temp = np.array([np.array(ds['temp'][0][0])]).ravel()
            salinity = np.array([np.array(ds['salinity'][0][0])]).ravel()
            u = np.array([np.array(ds['u'][0][0])]).ravel()
            v = np.array([np.array(ds['v'][0][0])]).ravel()

            zeta[zeta == float('inf')] = np.nan
            temp[temp == float('inf')] = np.nan
            salinity[salinity == float('inf')] = np.nan
            u[u == float('inf')] = np.nan
            v[v == float('inf')] = np.nan

            ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = zeta, temp, salinity, u, v

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

        ds['zeta_avg'], ds['temp_avg'], ds['salt_avg'], ds['u_avg'], ds['v_avg'] = climatology_zeta, climatology_temp, climatology_salt, climatology_u, climatology_v
        ds2 = ds[[
            'zeta_avg', 'temp_avg', 'salt_avg', 'u_avg',
            'v_avg', 'lon', 'lat', 'lonc', 'latc', 'nv',
        ]]

    return ds2


def basemap(avg_file, model):
    mapping_buffer = .1

    if model == 'fvcom':
        lon_min, lon_max = np.array(
            avg_file['lon'],
        ).min(), np.array(avg_file['lon']).max()
        lat_min, lat_max = np.array(
            avg_file['lat'],
        ).min(), np.array(avg_file['lat']).max()

    elif model == 'roms':
        lon_rho, lat_rho = np.array(avg_file['lon_rho'])[
            ~np.isnan(
                avg_file['zeta_avg'],
            )
        ], np.array(avg_file['lat_rho'])[~np.isnan(avg_file['zeta_avg'])]
        lon_min, lon_max = lon_rho.min(), lon_rho.max()
        lat_min, lat_max = lat_rho.min(), lat_rho.max()

    extents = np.array((
        lon_min, lon_max,
        lat_min, lat_max,
    ))

    # Define the map projection
    # 'cyl' projection in basemap is PlateCarree in cartopy.
    # The lat_0, lon_0, lat_ts parameters are ignored by basemap for 'cyl'
    projection = ccrs.PlateCarree()

    # Create the figure and GeoAxes
    fig = plt.figure(figsize=(10, 8))
    m = fig.add_subplot(1, 1, 1, projection=projection)

    map_extents = [
        extents[0] - mapping_buffer,  # min longitude
        extents[1] + mapping_buffer,  # max longitude
        extents[2] - mapping_buffer,  # min latitude
        extents[3] + mapping_buffer,
    ]  # max latitude
    m.set_extent(map_extents, crs=ccrs.PlateCarree())

    # Add map features (replaces basemap's 'resolution=h')
    # We use '10m' scale features, which is high-resolution
    m.add_feature(
        cfeature.LAND.with_scale('10m'),
        facecolor='lightgray', zorder=0,
    )
    m.add_feature(
        cfeature.OCEAN.with_scale('10m'),
        facecolor='aliceblue', zorder=0,
    )
    m.coastlines(resolution='10m', color='black', linewidth=1, zorder=1)
    # Other features:
    # m.add_feature(cfeature.BORDERS, linestyle=':', zorder=1)
    # m.add_feature(cfeature.STATES, linestyle=':', zorder=1)

    parallels = np.arange(
        np.floor(extents[2]), np.ceil(extents[3]), abs(
            (abs(np.ceil(extents[3]))-abs(np.floor(extents[2])))/10,
        ),
    )
    meridians = np.arange(
        np.floor(extents[0]), np.ceil(extents[1]), abs(
            (abs(np.ceil(extents[1]))-abs(np.floor(extents[0])))/10,
        ),
    )

    return m, parallels, meridians


def fields_plot(logger, file, ofs, month_name, model, path_save):

    avg_file = xr.open_dataset(file, decode_times=False)
    # base = basemap(avg_file, model)
    variables = ['zeta_avg', 'temp_avg', 'salt_avg', 'u_avg', 'v_avg']

    logger.info('Creating Field Plots...')

    if model == 'fvcom':
        fvcom_lon = np.array(avg_file['lon'])
        fvcom_lat = np.array(avg_file['lat'])
        triangles = np.array(avg_file['nv']).T-1

        for var in variables:
            base = basemap(avg_file, model)
            m = base[0]

            zmax = np.array(avg_file[var].max()).max()
            zmin = np.array(avg_file[var].min()).min()

            # tiler = cimgt.ArcGISOnline(service='World_Imagery')
            tiler = cimgt.GoogleTiles(url=url)
            # May need to tune the zoom level (e.g., 8, 9, 10)
            m.add_image(tiler, 8, zorder=0)

            m.title.set_text(f'{ofs}: {var} - {month_name}')
            cmap = plt.get_cmap('viridis')
            norm = plt.Normalize(zmin, zmax)
            tp2 = m.tripcolor(
                fvcom_lon, fvcom_lat, triangles, np.array(
                    avg_file[var],
                ), transform=ccrs.PlateCarree(), zorder=2,  # zorder=2 plots on top of image
                cmap=cmap, norm=norm,
            )

            fig = m.get_figure()

            cbar = fig.colorbar(
                tp2, ax=m, orientation='horizontal', pad=0.05,
            )
            # cbar.set_label("{} ({})".format(avg_file[var].attrs["long_name"], avg_file[var].attrs["units"]))

            # Save
            plt.savefig(
                fr'{path_save}\{var}_{ofs}_{month_name}.jpeg',
                bbox_inches='tight', dpi=300,
            )

    elif model == 'roms':

        for var in variables:
            if var == 'u_avg':
                lats = avg_file.variables['lat_u']
                lons = avg_file.variables['lon_u']
            elif var == 'v_avg':
                lats = avg_file.variables['lat_v']
                lons = avg_file.variables['lon_v']
            else:
                lats = avg_file.variables['lat_rho']
                lons = avg_file.variables['lon_rho']

            # Assumes 'basemap' is my setup_cartopy_map function
            base = basemap(avg_file, model)
            m = base[0]  # m is the map/GeoAxes object

            zmax = np.array(avg_file[var].max()).max()
            zmin = np.array(avg_file[var].min()).min()

            # tiler = cimgt.ArcGISOnline(service='World_Imagery')
            tiler = cimgt.GoogleTiles(url=url)
            # zorder=0 puts it in the background
            m.add_image(tiler, 8, zorder=0)

            cmap = plt.get_cmap('viridis')
            norm = plt.Normalize(zmin, zmax)

            m.title.set_text(f'{ofs}: {var} - {month_name}')
            tp2 = m.pcolormesh(
                lons, lats, avg_file[var],
                cmap=cmap, norm=norm,
                transform=ccrs.PlateCarree(), zorder=2,
            )

            fig = m.get_figure()

            cbar = fig.colorbar(
                tp2, ax=m, orientation='horizontal', pad=0.05,
            )
            # cbar.set_label("{} ({})".format(avg_file[var].attrs["long_name"], avg_file[var].attrs["units"]))

            # Save
            plt.savefig(
                fr'{path_save}\{var}_{ofs}_{month_name}.jpeg',
                bbox_inches='tight', dpi=300,
            )

    logger.info('... Plot Creating Complete!')


def stations_plot(logger, files_to_plot, ofs, path_save):

    logger.info('Creating Station Plots...')

    variables = ['zeta_avg', 'temp_avg', 'salt_avg', 'u_avg', 'v_avg']

    z, t, s, u, v, m = [], [], [], [], [], []
    for d in enumerate(files_to_plot):
        ds = xr.open_dataset(d[-1])
        z.append(np.array(ds['zeta_avg']))
        t.append(np.array(ds['temp_avg']))
        s.append(np.array(ds['salt_avg']))
        u.append(np.array(ds['u_avg']))
        v.append(np.array(ds['v_avg']))
        m.append(datetime.strptime(str(int(d[0])+1), '%m').strftime('%b'))

    for site in range(len(z[0])):
        # for month in enumerate(m):
        zz = np.array(z)[:, site]
        tt = np.array(t)[:, site]
        ss = np.array(s)[:, site]
        uu = np.array(u)[:, site]
        vv = np.array(v)[:, site]

        fig, axs = plt.subplots(5, 1, figsize=(6, 10), layout='constrained')

        logger.info(f'Creating Plot: {site+1} of {len(z[0])}')
        for ax, var, c, l in zip(axs.flat, variables, ['b', 'r', 'g', 'k', 'k'], ['meters', 'Celsius', 'ppm', 'meters per second', 'meters per second']):
            ax.set_title(f'OFS: {ofs}, Station: {site}, Variable: {var}')
            if var == 'zeta_avg':
                y = zz
            elif var == 'temp_avg':
                y = tt
            elif var == 'salt_avg':
                y = ss
            elif var == 'u_avg':
                y = uu
            elif var == 'v_avg':
                y = vv

            ax.grid(ls='--')
            ax.set_ylabel(l)
            ax.plot(m, y, 'o', ls='-', ms=4, color=c)

        plt.savefig(
            fr'{path_save}\{site}_{ofs}.jpeg',
            bbox_inches='tight', dpi=300,
        )

    logger.info('... Plot Creating Complete!')


def ofs_climatology(prop1, logger, path_save, datagroup):

    dates = []
    if str(datagroup) == 'all' or str(datagroup) == 'none':
        month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    else:
        month_list = datagroup.split(',')
        month_list = [int(i) for i in month_list]

    if datagroup == 'none':
        logger.info('Starting run...')
        dir_list = list_of_files.list_of_dir(prop1, logger)
        list_of_files_outp = list_of_files.list_of_files(
            prop1, dir_list, logger)

        logger.info(f'.{prop1.ofsfiletype} files found!')

        logger.info('Start creating Climatology')
        if f'{prop1.ofsfiletype}' == 'stations':
            clim_nc = stations_climatology(logger, prop1.model_source, list_of_files_outp)
        elif f'{prop1.ofsfiletype}' == 'fields':
            clim_nc = fields_climatology(logger, prop1.model_source, list_of_files_outp)

        file_name = f'{prop1.ofs}_Clim_{prop1.ofsfiletype}_{prop1.start_date_full.split("T")[0]}_{prop1.end_date_full.split("T")[0]}_{datagroup}.nc'

        clim_nc.to_netcdf(f'{path_save}/{file_name}')
        logger.info('... Run complete!')

        files_to_plot = [f'{path_save}/{file_name}']

    else:
        files_to_plot = []
        for m in month_list:
            logger.info(f'Starting run {m} of {len(month_list)}...')
            dir_list = list_of_files.list_of_dir(prop1, logger)
            list_of_files_outp = list_of_files.list_of_files(
                prop1, dir_list, logger)
            logger.info(f'.{prop1.ofsfiletype} files found!')

            logger.info('Start creating Climatology')
            if f'{prop1.ofsfiletype}' == 'stations':
                clim_nc = stations_climatology(
                    logger, prop1.model_source, list_of_files_outp)
            elif f'{prop1.ofsfiletype}' == 'fields':
                clim_nc = fields_climatology(logger, prop1.model_source, list_of_files_outp)

            month_name = datetime.strptime(str(m), '%m').strftime('%b')
            file_name = f'{prop1.ofs}_Clim_{prop1.ofsfiletype}_{prop1.start_date_full.split("T")[0]}_{prop1.end_date_full.split("T")[0]}_{month_name}.nc'

            clim_nc['time'] = month_name

            clim_nc.to_netcdf(Path(path_save, file_name))

            files_to_plot.append(f'{path_save}/{file_name}')

            logger.info(f'... Run complete {m} of {len(month_list)}!')

    if f'{prop1.ofsfiletype}' == 'fields':
        [
            fields_plot(
                logger,
                files_to_plot[i], prop1.ofs, datetime.strptime(str(month_list[i]), '%m').strftime(
                    '%b',
                ), prop1.model_source, path_save,
            ) for i in range(len(files_to_plot))
        ]

    elif f'{prop1.ofsfiletype}' == 'stations':
        stations_plot(logger, files_to_plot, prop1.ofs, path_save)


if __name__ == '__main__':
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python ofs_climatology.py',
        usage='%(prog)s',
        description='Lists all .station files between start and end dates, and calculates the average per station',
    )
    parser.add_argument(
        '-d',
        '--DirOut',
        required=True,
        help='Directory where the output climatology will be saved',
    )
    parser.add_argument(
        '-o', '--OFS', required=True, help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""", )

    parser.add_argument(
        '-t', '--FileType', required=False,
        help="OFS output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        '-g',
        '--DataGrouping',
        required=True,
        help='How the data will be grouped, e.g.: 01,02,03 (Jan, Feb, Mar), all (Jan, Feb,...Nov, Dec), none (no monthly means)',
    )
    parser.add_argument(
        '-s', '--StartDate_full', required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e', '--EndDate_full', required=False,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-ws', '--Whichcasts', required=False,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )
    parser.add_argument(
        '-f',
        '--Forecast_Hr',
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ", )

    args = parser.parse_args()
    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts.lower()
    prop1.whichcast = args.Whichcasts.lower()
    prop1.model_source = model_source.model_source(prop1.ofs)
    prop1.ofsfiletype = args.FileType.lower()
    prop1.start_date_full = prop1.start_date_full.replace('-', '')
    prop1.end_date_full = prop1.end_date_full.replace('-', '')
    prop1.start_date_full = prop1.start_date_full.replace('Z', '')
    prop1.end_date_full = prop1.end_date_full.replace('Z', '')
    prop1.start_date_full = prop1.start_date_full.replace('T', '-')
    prop1.end_date_full = prop1.end_date_full.replace('T', '-')

    logger = None
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(os.getcwd(), log_config_file)
        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)
        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    try:
        prop1.startdate = (
            datetime.strptime(
                prop1.start_date_full.split('-')[0], '%Y%m%d',
            )
        ).strftime(
            '%Y%m%d',
        ) + '00'
        prop1.enddate = (
            datetime.strptime(
                prop1.end_date_full.split('-')[0], '%Y%m%d',
            )
        ).strftime(
            '%Y%m%d',
        ) + '23'
    except Exception as e:
        logger.error(f'Problem with date format in get_node_ofs: {e}')
        sys.exit(-1)

    dir_params = utils.Utils().read_config_section('directories', logger)

    prop1.model_path = os.path.join(
        dir_params['model_historical_dir'], prop1.ofs, dir_params['netcdf_dir'],
    )
    prop1.model_path = Path(prop1.model_path).as_posix()

    ofs_climatology(
        prop1, logger,
        args.DirOut,
        args.DataGrouping,
    )
