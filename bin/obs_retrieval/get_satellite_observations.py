"""
-*- coding: utf-8 -*-

Documentation for Scripts get_satellite_observations.py

Directory Location:   /path/to/ofs_dps/server/bin/obs_retrieval

Technical Contact(s): Name:  FC

Abstract:

   This is the main script of the 2d observations module.
   This function calls GOES 16 and 18 retrieval
   Then it extract the variable of interest (temp)
   and clips the concatenated satellite data for the OFS

Language:  Python 3.8

Estimated Execution Time: <5min

usage: python bin/obs_retrieval/get_satellite_observations.py
-s 2024-02-01T00:00:00Z -e 2024-02-01T01:00:00Z -p ./ -o wcofs

optional arguments:
  -h, --help            show this help message and exit
  -o OFS, --ofs OFS     Choose from the list on the ofs_Extents folder, you
                        can also create your own shapefile, add it top the
                        ofs_Extents folder and call it here
  -p PATH, --path PATH  Path to home
  -s STARTDATE_FULL, --StartDate_full STARTDATE_FULL
                        Start Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20220115-05:05:05'
  -e ENDDATE_FULL, --EndDate_full ENDDATE_FULL
                        End Date_full YYYYMMDD-hh:mm:ss e.g.
                        '20230808-05:05:05'

Output:
1) observation data
    /data/observations/2d_satellite
    .nc file that has the concatenated satellite data

Author Name:  FC       Creation Date:  03/12/2024

Revisions:
    Date          Author             Description
    3/12/2025     RA                 Collects data from a variety of other
                                        satellite sources
                                     (N20, N21, NPP, LEO-L3S (OSPO and STAR))
    4/24/2025     RA                 Added collection from NASA SPoRT
    5/6/2025      RA                 Concat for SPoRT

"""
from __future__ import annotations

import argparse
import logging.config
import os
import sys
import time
import urllib.request
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils
from model_processing import model_properties

import geopandas as gpd
import regionmask
import xarray as xr


def hours_range(start_date, end_date):
    """
    This function takes the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates = []
    for i in range(
        int(
            (
                datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%SZ')
                - datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')
            ).total_seconds()
            / 60
            / 60,
        )
        + 1,
    ):
        date = datetime.strptime(
            start_date, '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=i)
        dates.append(date.strftime('%Y-%m-%dT%H:%M:%SZ'))

    return dates


def list_of_urls_goes_east(hours_range1, url_params):
    """
    This function will list the API's for all the GOES 16 or 19 (GOES-East)
    files between the range of data (output from hour_range())
    """

    # GOES-16 transitioned to GOES-19 in April 2025. So before that date,
    # get GOES-16. After that date get GOES-19.
    changedate = datetime.strptime(
        '2025-04-07T21:00:00Z', '%Y-%m-%dT%H:%M:%SZ')

    url_root = url_params['nesdis_thredds']
    url_east_list = []
    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')
        if mydate > changedate:
            east_num = '19'
            name_num = 'NOAA'
            V_num = '3.00'
            v_num = '02.1'
            fv_num = '01.0'
        else:
            east_num = '16'
            name_num = 'STAR'
            V_num = '2.70'
            v_num = '02.0'
            fv_num = '01.0'
        url_east = (
            f'{url_root}'
            f'gridG{east_num}ABINRTL3CWW00/'
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%j')}/"
            f"{mydate.strftime('%Y')}{mydate.strftime('%m')}"
            f"{mydate.strftime('%d')}{mydate.strftime('%H')}0000"
            f'-{name_num}-L3C_GHRSST-SSTsubskin-'
            f'ABI_G{east_num}-ACSPO_V{V_num}-v{v_num}-fv{fv_num}.nc'
        )

        url_east_list.append(url_east)

    return url_east_list


def list_of_urls_g18(hours_range1, url_params):
    """
    This function will list the API's for all the GOES 18
    files between the range of data (output from hour_range())
    """
    url_root = url_params['nesdis_thredds']
    url_18_list = []
    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_18 = (
            f'{url_root}'
            f'gridG18ABINRTL3CWW00/'
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%j')}/"
            f"{mydate.strftime('%Y')}{mydate.strftime('%m')}"
            f"{mydate.strftime('%d')}{mydate.strftime('%H')}0000"
            f'-STAR-L3C_GHRSST-SSTsubskin-'
            f'ABI_G18-ACSPO_V2.90-v02.0-fv01.0.nc'
        )

        url_18_list.append(url_18)

    return url_18_list


def list_of_urls_n20(hours_range1, url_params):
    """
    This function will list the APIs for all the N20
    files between the range of data (output from hour_range())
    """

    def append_to_list(mydate, minutes):
        if minutes is not None:
            minutes_var = minutes
        else:
            minutes_var = '00'

        url_20 = (
            f'{url_root}/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'{mydate:%H}'
            f'{minutes_var:02}'
            f'{mydate:%S}'
            f'-STAR-L3U_GHRSST-SSTsubskin-'
            f'VIIRS_N20-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_20_list.append(url_20)

    url_root = url_params['nesdis_n20']

    url_20_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        if i != hours_range1[len(hours_range1) - 1]:
            minutes = 0

            while minutes < 60:
                append_to_list(mydate, minutes)

                minutes = minutes + 10

        else:
            append_to_list(mydate, None)

    return url_20_list


def list_of_urls_n21(hours_range1, url_params):
    """
    This function will list the APIs for all the N21
    files between the range of data (output from hour_range())
    """

    def append_to_list(mydate, minutes):
        if minutes is not None:
            minutes_var = minutes
        else:
            minutes_var = '00'

        url_21 = (
            f'{url_root}/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'{mydate:%H}'
            f'{minutes_var:02}'
            f'{mydate:%S}'
            f'-STAR-L3U_GHRSST-SSTsubskin-'
            f'VIIRS_N21-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_21_list.append(url_21)

    url_root = url_params['nesdis_n21']

    url_21_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        if i != hours_range1[len(hours_range1) - 1]:
            minutes = 0

            while minutes < 60:
                append_to_list(mydate, minutes)

                minutes = minutes + 10

        else:
            append_to_list(mydate, None)

    return url_21_list


def list_of_urls_npp(hours_range1, url_params):
    """
    This function will list the APIs for all the NPP
    files between the range of data (output from hour_range())
    """

    def append_to_list(mydate, minutes):
        if minutes is not None:
            minutes_var = minutes
        else:
            minutes_var = '00'

        url_npp = (
            f'{url_root}/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'{mydate:%H}'
            f'{minutes_var:02}'
            f'{mydate:%S}'
            f'-STAR-L3U_GHRSST-SSTsubskin-'
            f'VIIRS_NPP-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_npp_list.append(url_npp)

    url_root = url_params['nesdis_npp']

    url_npp_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        if i != hours_range1[len(hours_range1) - 1]:
            minutes = 0

            while minutes < 60:
                append_to_list(mydate, minutes)

                minutes = minutes + 10

        else:
            append_to_list(mydate, None)

    return url_npp_list


def list_of_urls_l3s_amd_ospo(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S OSPO AM D
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_coastwatch']

    url_l3s_amd_ospo_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_amd_ospo = (
            f'{url_root}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate.day:02}'
            f'120000-OSPO-L3S_GHRSST-SSTsubskin-'
            f'LEO_AM_D-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_amd_ospo_list.append(url_l3s_amd_ospo)

    return url_l3s_amd_ospo_list


def list_of_urls_l3s_amn_ospo(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S OSPO AM N
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_coastwatch']

    url_l3s_amn_ospo_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_amn_ospo = (
            f'{url_root}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate.day:02}'
            f'120000-OSPO-L3S_GHRSST-SSTsubskin-'
            f'LEO_AM_N-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_amn_ospo_list.append(url_l3s_amn_ospo)

    return url_l3s_amn_ospo_list


def list_of_urls_l3s_pmd_ospo(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S OSPO PM D
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_coastwatch']

    url_l3s_pmd_ospo_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_pmd_ospo = (
            f'{url_root}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate.day:02}'
            f'120000-OSPO-L3S_GHRSST-SSTsubskin-'
            f'LEO_PM_D-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_pmd_ospo_list.append(url_l3s_pmd_ospo)

    return url_l3s_pmd_ospo_list


def list_of_urls_l3s_pmn_ospo(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S OSPO PM N
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_coastwatch']

    url_l3s_pmn_ospo_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_pmn_ospo = (
            f'{url_root}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate.day:02}'
            f'120000-OSPO-L3S_GHRSST-SSTsubskin-'
            f'LEO_PM_N-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_pmn_ospo_list.append(url_l3s_pmn_ospo)

    return url_l3s_pmn_ospo_list


def list_of_urls_l3s_amd_star(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S STAR AM D
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_dodsC']

    url_l3s_amd_star_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_amd_star = (
            f'{url_root}/'
            f'gridLEOAMNRTL3SWW00/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'120000-STAR-L3S_GHRSST-SSTsubskin-'
            f'LEO_AM_D-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_amd_star_list.append(url_l3s_amd_star)

    return url_l3s_amd_star_list


def list_of_urls_l3s_amn_star(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S STAR AM N
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_dodsC']

    url_l3s_amn_star_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_amn_star = (
            f'{url_root}/'
            f'gridLEOAMNRTL3SWW00/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'120000-STAR-L3S_GHRSST-SSTsubskin-'
            f'LEO_AM_N-ACSPO_V2.80-v02.0-fv01.0.nc'
        )

        url_l3s_amn_star_list.append(url_l3s_amn_star)

    return url_l3s_amn_star_list


def list_of_urls_l3s_pmd_star(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S STAR PM D
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_dodsC']

    url_l3s_pmd_star_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_pmd_star = (
            f'{url_root}/'
            f'gridLEOPMNRTL3SWW00/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'120000-STAR-L3S_GHRSST-SSTsubskin-'
            f'LEO_PM_D-ACSPO_V2.81-v02.0-fv01.0.nc'
        )

        url_l3s_pmd_star_list.append(url_l3s_pmd_star)

    return url_l3s_pmd_star_list


def list_of_urls_l3s_pmn_star(hours_range1, url_params):
    """
    This function will list the APIs for all the L3S STAR PM N
    files between the range of data (output from hour_range())
    """

    url_root = url_params['nesdis_dodsC']

    url_l3s_pmn_star_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        url_l3s_pmn_star = (
            f'{url_root}/'
            f'gridLEOPMNRTL3SWW00/'
            f'{mydate.year}/'
            f'{mydate.timetuple().tm_yday:03}/'
            f'{mydate:%Y}'
            f'{mydate:%m}'
            f'{mydate:%d}'
            f'120000-STAR-L3S_GHRSST-SSTsubskin-'
            f'LEO_PM_N-ACSPO_V2.81-v02.0-fv01.0.nc'
        )

        url_l3s_pmn_star_list.append(url_l3s_pmn_star)

    return url_l3s_pmn_star_list


def list_of_urls_sport(hours_range1, url_params):
    """
    This function will list the APIs for NASA SPoRT.

    The SPoRT SST Composite is blend of VIIRS, MODIS, NESDIS1, and OSTIA SST
    products (except over the inland lakes, where the NESDIS is unavailable).
    """

    url_root = url_params['nssr_sport']

    url_sport_list = []

    for i in hours_range1:
        mydate = datetime.strptime(i, '%Y-%m-%dT%H:%M:%SZ')

        if int(f'{mydate:%H}') < 6:
            sport_time = '180000'
            sport_day = mydate - timedelta(days=1)

        elif int(f'{mydate:%H}') >= 6 & int(f'{mydate:%H}') < 18:
            sport_time = '060000'
            sport_day = mydate

        else:
            sport_time = '180000'
            sport_day = mydate

        url_sport = (
            f'{url_root}/'
            f'{sport_day:%Y}'
            f'{sport_day:%m}'
            f'{sport_day:%d}'
            f'{sport_time}'
            f'-NASA-L4_GHRSST-SSTfnd-'
            f'SPoRT-GLOB-v02.0-fv02.0.nc'
        )

        if url_sport not in url_sport_list:
            url_sport_list.append(url_sport)

    return url_sport_list


def get_sat(list_of_urls, obs2d_dir, logger, sat_type):
    """
    This function gets the satellite data from API,
    drops all unecessary variables,
    deletes the priginal file and saves the new .nc file
    lastly it appends the path to the files saved

    """

    list_of_files = []
    for sat_dat in list_of_urls:
        sat_fname = os.path.join(Path.cwd(), Path(obs2d_dir))

        if 'G16' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'G16')

        elif 'G18' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'G18')

        elif 'G19' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'G19')

        elif 'n20' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'N20')

        elif 'n21' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'N21')

        elif 'npp' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'NPP')

        elif 'L3S' in sat_dat:
            if 'OSPO' in sat_dat:
                sat_fname = os.path.join(sat_fname, 'L3S-OSPO')

            elif 'STAR' in sat_dat:
                sat_fname = os.path.join(sat_fname, 'L3S-STAR')

            else:
                pass
        # Temporarily commented out 6/11/25 to avoid filling up server disk
        # space
        elif 'SPoRT' in sat_dat:
            sat_fname = os.path.join(sat_fname, 'SPoRT')

        else:
            pass

        if not os.path.exists(sat_fname):
            os.makedirs(sat_fname)

        sat_fname = os.path.join(
            sat_fname, str(
                f'{sat_dat}'.split('/')[-1].split('.')[0]
                + '_sst.nc',
            ),
        )

        logger.info(f'Checking for {sat_fname}')

        if os.path.exists(sat_fname):
            logger.info(f'{sat_fname} exists')

            if sat_type == 'GOES':
                if (sat_fname.find('G16') > -1 or sat_fname.find('G18') > -1
                    or sat_fname.find('G19') > -1):
                    list_of_files.append(sat_fname)

            else:
                if sat_fname.find('SPoRT') > -1:
                    list_of_files.append(sat_fname)
        else:
            try:
                download_var = False

                if sat_type == 'GOES':
                    if (sat_fname.find('G16') > -1 or sat_fname.find('G18')
                        > -1 or sat_fname.find('G19') > -1):
                        download_var = True

                else:
                    if sat_fname.find('SPoRT') > -1:
                        download_var = True

                if download_var:
                    logger.info(f'Downloading satellite data: {sat_dat}')

                    urllib.request.urlretrieve(
                        sat_dat, obs2d_dir + r'/' +
                        f'{sat_dat}'.split('/')[-1],
                    )

                    drop_variables = [
                        'quality_level',
                        'l2p_flags',
                        'or_number_of_pixels',
                        'dt_analysis',
                        'satellite_zenith_angle',
                        'sses_bias',
                        'sses_standard_deviation',
                        'wind_speed',
                        'sst_dtime',
                        'sst_gradient_magnitude',
                        'sst_front_position',
                    ]
                    data_set = xr.open_dataset(
                        obs2d_dir + r'/' + f'{sat_dat}'.split('/')[-1],
                        drop_variables=drop_variables,
                        engine='netcdf4',
                        decode_times=False,
                    )

                    data_set.to_netcdf(
                        sat_fname,
                        # obs2d_dir
                        # + r"/"
                        # + f"{sat_dat}".split("/")[-1].split(".")[0]
                        # + "_sst.nc",
                        mode='w',
                    )
                    data_set.close()
                    os.remove(obs2d_dir + r'/' + f'{sat_dat}'.split('/')[-1])

                    list_of_files.append(sat_fname)

                    # list_of_files.append(
                    #     obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1].
                    #     split(".")[0] + "_sst.nc"
                    # )

            except (ValueError, HTTPError, Exception) as ex:
                if 'G16' in sat_dat:
                    g16date = datetime.strptime(
                        sat_fname.split('-')[0][-14:-1],
                        '%Y%m%d%H%M%S',
                    )
                    g16end = datetime.strptime(
                        '20250407210000', '%Y%m%d%H%M%S')
                    if g16date > g16end:
                        error_message = f"""Error: {str (ex)}.
                        Oops! GOES-16 does not exist for
                        {sat_fname.split('-')[0][-14:-1]}. It is replaced by
                        GOES-19."""
                        logger.error(error_message)
                    else:
                        error_message = f"""Error: {str (ex)}.
                        Failed downloading files {sat_dat}!!"""
                        logger.error(error_message)
                else:
                    error_message = f"""Error: {str (ex)}.
                    Failed downloading files {sat_dat}!!"""
                    logger.error(error_message)

    return list_of_files


def concat_sat(list_of_files, obs2d_dir, logger, prop1):
    """
    Concatenates the satellite files on
    list_of_files into once single file,
    deletes the files in list_of_files
    """
    try:
        save_path = (
            Path(__file__).parent.parent.parent /
            Path(obs2d_dir) /
            str(
                f'{list_of_files[0]}'.split(
                    '/')[-1].split('00-')[-1].split('.')[0]
                + '_concat_'
                + datetime.strptime(prop1.start_date_full,
                                    '%Y-%m-%dT%H:%M:%SZ').\
                    strftime('%Y%m%d%H')+'_'
                + datetime.strptime(prop1.end_date_full,
                                    '%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H')
                + '.nc',
            )
        ).resolve()

    except IndexError as ex:
        error_message = (
            f"""Error: {str(ex)}. No satellite files to concat in concat_sat.
            Exiting! \n"""
        )
        logger.error(error_message)
        sys.exit(-1)

    logger.info(f'Checking for concatenated file: {save_path}')
    if os.path.exists(save_path) and os.path.getsize(save_path)/1024 > 1000:
        logger.info('Valid concatenated file found - skipping concatenation')
        # if os.path.getsize(save_path)/1024 > 1000:
        #   logger.info(f"{save_path} is valid - skipping concatenation ")
    else:
        try:
            logger.info('No global concatenated file found ')
            logger.info('Begining concatenating of the satellite data ... ')
            nc_list = []

            for file in list_of_files:
                if os.path.exists(file):
                    nc_list.append(
                        xr.open_dataset(
                            file,
                            chunks='auto',
                            decode_times=False,
                            lock=False,
                        ),
                    )
            nc_item = xr.concat(
                nc_list,
                dim='time',
                data_vars='minimal',
            )

            logger.info('Concatenation complete!')
        except Exception as ex:
            logger.error(f'Error happened at Concatenation: {str(ex)}')
            sys.exit(-1)

        # nc_item = xr.concat([xr.open_dataset(i) for i in list_of_files],
        #                     dim="time",
        #                     data_vars="minimal"
        #                    )

        try:
            logger.info(f'Writing concatenated file to {save_path} ... ')
            nc_item.to_netcdf(
                save_path,
                mode='w',
                format='NETCDF4',
                engine='netcdf4',
                # encoding={"chunksizes": 1000},
                # computer=False
            )

            logger.info('Finished writing the concatenated satellite file ')
        except MemoryError as ex:
            logger.error(
                f'Error happened at saving file {save_path} -- {str(ex)}')
            sys.exit(-1)
        except KeyboardInterrupt:
            logger.error('Keyboard interupt by user, abandoning save ... ')
    return str(save_path)


def masksat_by_ofs(sat_path, shape_file):
    """
    Clips out the part of the GOES product that
    falls within the OFS shapefile.
    Saves the clipped concatenated file.
    Does not delete the file for the entire
    GOES coverage as it can be used for other OFS
    """

    shp_mask = gpd.read_file(f'{shape_file}')
    bounds = shp_mask.geometry.apply(lambda x: x.bounds).tolist()
    minx, miny, maxx, maxy = (
        min(bounds)[0],
        min(bounds)[1],
        max(bounds)[2],
        max(bounds)[3],
    )
    poly = regionmask.Regions(list(shp_mask.geometry))

    sat_nc = xr.open_dataset(
        sat_path,
        engine='netcdf4',
        decode_times=False,
    )

    if sat_path.find('SPoRT') > -1:
        sat_nc_slice = sat_nc.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))

    else:
        sat_nc_slice = sat_nc.sel(lon=slice(minx, maxx), lat=slice(maxy, miny))

    mask_sat = poly.mask(sat_nc_slice.isel(time=0))
    masked_sat = sat_nc.where(mask_sat == 0)

    return masked_sat


def parameter_dir_validation(prop, dir_params, logger):
    '''
    parameter_validation
    '''
    # Start Date and End Date validation
    try:
        datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        error_message = (
            f'Please check Start Date - '
            f'{prop.start_date_full}, End Date - '
            f'{prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
        prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ',
    ) > datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ'):
        error_message = (
            f'End Date {prop.end_date_full} '
            f'is before Start Date {prop.end_date_full}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params['home']

    # prop.path validation
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])

    if not os.path.exists(ofs_extents_path):
        error_message = (
            f'ofs_extents/ folder is not found. '
            f'Please check prop.path - {prop.path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f'{ofs_extents_path}/{prop.ofs}.shp'

    if not os.path.isfile(shape_file):
        error_message = (
            f'Shapefile {prop.ofs} is not found at '
            f'the folder {ofs_extents_path}. Abort!'
        )
        logger.error(error_message)
        sys.exit(-1)
    prop.data_observations_2d_satellite_path = os.path.join(
        prop.path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['2d_satellite_dir'],
    )

    os.makedirs(prop.data_observations_2d_satellite_path, exist_ok=True)


def get_satellite(prop, logger):
    """
    get_satellite
    """

    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent / log_config_file).resolve()

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

    # logger.info("--- Starting Visulization Process ---")

    dir_params = utils.Utils().read_config_section('directories', logger)
    url_params = utils.Utils().read_config_section('urls', logger)

    parameter_dir_validation(prop, dir_params, logger)

    logger.info('--- Starting Satellite Observation Process ---')

    hours = hours_range(prop.start_date_full, prop.end_date_full)

    list_of_urls = []

    if prop.ofs in [
        'ciofs',
        'sscofs',
        'wcofs',
        'sfbofs',
        'creofs',
        'wcofs2',
    ]:
        list_of_urls = list_of_urls_g18(hours, url_params)

    elif prop.ofs in [
        'loofs',
        'lmhofs',
        'lsofs',
        'leofs',
        'gomofs',
        'cbofs',
        'dbofs',
        'sjrofs',
        'tbofs',
        'ngofs',
        'ngofs2',
        'necofs',
        'nyofs',
        'secofs',
    ]:
        list_of_urls = list_of_urls_goes_east(hours, url_params)

    # Temporarily commented out 6/11/25 to avoid filling up server disk space
    list_of_urls.extend(list_of_urls_sport(hours, url_params))

    '''
    Commented out 4/7/25 to reduce processing time
    list_of_urls.extend(list_of_urls_n20(hours, url_params))
    list_of_urls.extend(list_of_urls_n21(hours, url_params))
    list_of_urls.extend(list_of_urls_npp(hours, url_params))

    list_of_urls_l3s = list_of_urls_l3s_amd_ospo(hours, url_params)
    list_of_urls_l3s.extend(list_of_urls_l3s_amn_ospo(hours, url_params))
    list_of_urls_l3s.extend(list_of_urls_l3s_pmd_ospo(hours, url_params))
    list_of_urls_l3s.extend(list_of_urls_l3s_pmn_ospo(hours, url_params))
    '''

    # Commenting these out for now because I haven't had much success with the
    # server being up
    """
    list_of_urls_l3s.extend(list_of_urls_l3s_amd_star(hours, url_params))
    list_of_urls_l3s.extend(list_of_urls_l3s_amn_star(hours, url_params))
    list_of_urls_l3s.extend(list_of_urls_l3s_pmd_star(hours, url_params))
    list_of_urls_l3s.extend(list_of_urls_l3s_pmn_star(hours, url_params))
    """

    logger.info(
        'Begin retriving the following files:%s',
        [i.split('/')[-1] for i in list_of_urls],
    )

    try:
        list_of_files_goes = get_sat(
            list_of_urls,
            prop.data_observations_2d_satellite_path,
            logger,
            'GOES',
        )

        # Temporarily commented out 6/11/25 to avoid filling up server disk
        # space
        list_of_files_sport = get_sat(
            list_of_urls,
            prop.data_observations_2d_satellite_path,
            logger,
            'SPoRT',
        )

        logger.info('Satellite data downloaded')
    except ValueError as ex:
        error_message = f'Error: {str(ex)}. Failed downloading files. Abort!'
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    # Now concat sat files
    try:
        concated_sat_goes = ''
        # Temporarily commented out 6/11/25 to avoid filling up server disk
        # space
        concated_sat_sport = ''

        if len(list_of_files_goes) > 0:
            concated_sat_goes = concat_sat(
                list_of_files_goes,
                prop.data_observations_2d_satellite_path,
                logger,
                prop1,
            )

        # Temporarily commented out 6/11/25 to avoid filling up server disk
        # space
        if len(list_of_files_sport) > 0:
            concated_sat_sport = concat_sat(
                list_of_files_sport,
                prop.data_observations_2d_satellite_path,
                logger,
                prop1,
            )
        try:
            shape_file = f'{prop.ofs_extents_path}/{prop.ofs}.shp'

            masked_sat_path = (
                Path(__file__).parent.parent.parent /
                Path(prop.data_observations_2d_satellite_path) /
                str(prop.ofs + '.nc')
            ).resolve()

            if os.path.exists(masked_sat_path):
                file_age = time.time() - os.path.getmtime(masked_sat_path)
                if (file_age < 3600 and
                    os.path.getsize(masked_sat_path)/1024 > 50):
                    logger.info(
                        'Recent valid masked file found - skipping clipping')

            else:
                logger.info('No recent masked file found ')
                logger.info('Begin clipping satellite data for %s', prop.ofs)

                if len(concated_sat_goes) > 0:
                    masked_sat_goes = masksat_by_ofs(
                        concated_sat_goes, shape_file)

                    masked_sat_goes.to_netcdf(
                        f'{prop.data_observations_2d_satellite_path}/'
                        f'{args.ofs}.nc', mode='w',
                    )

                # Temporarily commented out 6/11/25 to avoid filling up server
                # disk space
                if len(concated_sat_sport) > 0:
                    masked_sat_sport = masksat_by_ofs(
                        concated_sat_sport,
                        shape_file,
                    )

                    masked_sat_sport.to_netcdf(
                        f'{prop.data_observations_2d_satellite_path}/'
                        f'{args.ofs}_sport.nc', mode='w',
                    )

                logger.info(
                    'Finished clipping satellite data for %s', prop.ofs)

        except ValueError as ex:
            error_message = f'Error: {str(ex)}. ' + \
                'Failed clipping satellite data. Abort!'
            logger.error(error_message)
            print(error_message)
            sys.exit(-1)

    except ValueError as ex:
        error_message = (
            f'Error: {str(ex)}. ' + \
                'Failed concatenation of satellite data. Abort!'
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)


if __name__ == '__main__':
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python write_obs_ctlfile.py',
        usage='%(prog)s',
        description='ofs write Station Control File',
    )
    parser.add_argument(
        '-o',
        '--ofs',
        required=True,
        help='Choose from the list on the ofs_Extents folder, you can also '
        'create your own shapefile, add it top the ofs_Extents folder and '
        'call it here',
    )
    parser.add_argument('-p', '--path', required=True,
                        help='/PATH/TO/SA_HOMEIDR/')
    parser.add_argument(
        '-s',
        '--StartDate_full',
        required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        '-e',
        '--EndDate_full',
        required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'",
    )
    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    prop1.ofs_extents_path = r'' + prop1.path + 'ofs_extents' + '/'
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full

    get_satellite(prop1, None)
