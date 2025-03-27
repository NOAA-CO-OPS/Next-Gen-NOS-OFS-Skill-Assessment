"""
-*- coding: utf-8 -*-

Documentation for Scripts get_satellite_observations.py

Directory Location:   /path/to/ofs_dps/server/bin/obs_retrieval

Technical Contact(s): Name:  FC

Abstract:

   This is the main script of the 2d observations module.
   This function calls GOES 16 and 18 retrieval
   Then it extracts the variable of interest (temp)
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


"""

import argparse
import logging
import logging.config
import os
import sys
from pathlib import Path

from datetime import datetime, timedelta
import time
import urllib.request
from urllib.error import HTTPError

import geopandas as gpd
import regionmask

import xarray as xr
import numpy as np

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from model_processing import model_properties
from obs_retrieval import utils


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
                datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                - datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
            ).total_seconds()
            / 60
            / 60
        )
        + 1
    ):
        date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=i)
        dates.append(date.strftime("%Y-%m-%dT%H:%M:%SZ"))

    return dates


def list_of_urls_g16(hours_range1):
    """
    This function will list the API's for all the GOES 16
    files between the range of data (output from hour_range())
    """

    url_root = "https://www.star.nesdis.noaa.gov/thredds/fileServer/"
    url_16_list = []
    for i in hours_range1:
        mydate = datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ")

        url_16 = (
            f"{url_root}"
            f"gridG16ABINRTL3CWW00/"
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%j')}/"
            f"{mydate.strftime('%Y')}{mydate.strftime('%m')}"
            f"{mydate.strftime('%d')}{mydate.strftime('%H')}0000"
            f"-STAR-L3C_GHRSST-SSTsubskin-"
            f"ABI_G16-ACSPO_V2.70-v02.0-fv01.0.nc"
        )

        url_16_list.append(url_16)

    return url_16_list


def list_of_urls_g18(hours_range1):
    """
    This function will list the API's for all the GOES 18
    files between the range of data (output from hour_range())
    """
    url_root = "https://www.star.nesdis.noaa.gov/thredds/fileServer/"
    url_18_list = []
    for i in hours_range1:
        mydate = datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ")

        url_18 = (
            f"{url_root}"
            f"gridG18ABINRTL3CWW00/"
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%j')}/"
            f"{mydate.strftime('%Y')}{mydate.strftime('%m')}"
            f"{mydate.strftime('%d')}{mydate.strftime('%H')}0000"
            f"-STAR-L3C_GHRSST-SSTsubskin-"
            f"ABI_G18-ACSPO_V2.90-v02.0-fv01.0.nc"
        )

        url_18_list.append(url_18)

    return url_18_list


def get_sat(list_of_urls, obs2d_dir, logger):
    """
    This function gets the satellite data from API,
    drops all unecessary variables,
    deletes the original file and saves the new .nc file.
    Lastly, it appends the path to the files saved

    """

    list_of_files = []
    for sat_dat in list_of_urls:
        sat_fname=os.path.join(Path.cwd(), \
                Path(obs2d_dir),str(f"{sat_dat}".split("/")[-1].split(".")[0]\
                + "_sst.nc"))
        list_of_files.append(sat_fname)
        logger.info(f"Checking for {sat_fname}")
        if os.path.exists(sat_fname):
            logger.info(f"{sat_fname} exists")
        else:
            try:
                logger.info(f"Downloading satellite data: {sat_dat}")

                urllib.request.urlretrieve(
                    sat_dat, obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1]
                )

                drop_variables = [
                    "quality_level",
                    "l2p_flags",
                    "or_number_of_pixels",
                    "dt_analysis",
                    "satellite_zenith_angle",
                    "sses_bias",
                    "sses_standard_deviation",
                    "wind_speed",
                    "sst_dtime",
                    "sst_gradient_magnitude",
                    "sst_front_position",
                ]
                data_set = xr.open_dataset(
                    obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1],
                    drop_variables=drop_variables,
                    engine="netcdf4",
                )

                data_set.to_netcdf(sat_fname,
                    #obs2d_dir
                    #+ r"/"
                    #+ f"{sat_dat}".split("/")[-1].split(".")[0]
                    #+ "_sst.nc",
                    mode="w",
                )
                data_set.close()
                os.remove(obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1])

                #list_of_files.append(
                #    obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1].split(".")[0] + "_sst.nc"
                #)

            except (ValueError, HTTPError, Exception) as ex:
                error_message = f"""Error: {str (ex)}. Failed downloading files {sat_dat}!!"""
                print (error_message)
                logger.error (error_message)

    return list_of_files


def concat_sat(list_of_files, obs2d_dir, logger, prop1):
    """
    Concatenates the satellite files on
    list_of_files into one single file,
    deletes the files in list_of_files
    """

    save_path = (os.path.join(
        os.getcwd(),
        Path(obs2d_dir),
        str(f"{list_of_files[0]}".split("/")[-1].split("00-")[-1].split(".")[0]
        + "_concat_"
        + datetime.strptime(prop1.start_date_full, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H")+"_"
        + datetime.strptime(prop1.end_date_full, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H")
        + ".nc")
        )
    )

    logger.info(f"Checking for concatenated file: {save_path}")
    if os.path.exists(save_path) and os.path.getsize(save_path)/1024 > 1000:
        logger.info(f"Valid concatenated file found - skipping concatenation")
        #if os.path.getsize(save_path)/1024 > 1000:
         #   logger.info(f"{save_path} is valid - skipping concatenation ")
    else:
        try:
            logger.info("No global concatenated file found ")
            logger.info("Begining concatenating of the satellite data ... ")
            nc_list = []
            newarr = np.array_split(list_of_files, 4)
            arrlength = len(newarr)
            for i_index in range(0, arrlength):
                if newarr[i_index] is not None and newarr[i_index].size > 0:
                    nc_item = xr.concat(
                        [xr.open_dataset(i, chunks='auto') for i in newarr[i_index]],
                        dim="time",
                        data_vars="minimal",
                    )
                    nc_list.append(nc_item)
                    filelist = ""
                    for j_index in range(0, len(newarr[i_index])):
                        filelist = filelist + str(newarr[i_index][j_index]) + "\n"
                    logger.info(f"Reading file: {filelist}")
            nc_item = xr.concat(
                nc_list,
                dim="time",
                data_vars="minimal",
            )
            logger.info("Concatenation complete!")
        except Exception as ex:
            logger.error(f"Error happened at Concatenation: {str(ex)}")
            sys.exit(-1)

        # nc_item = xr.concat([xr.open_dataset(i) for i in list_of_files],
        #                     dim="time",
        #                     data_vars="minimal"
        #                    )

        try:
            logger.info(f"Writing concatenated file to {save_path} ... ")
            nc_item.to_netcdf (
                save_path,
                mode = "w",
                format = "NETCDF4",
                engine = "netcdf4",
                # encoding={"chunksizes": 1000},
                # computer=False
                )
            logger.info("Finished writing the concatenated satellite file ")
        except MemoryError as ex:
            logger.error(f"Error happened at saving file {save_path} -- {str(ex)}")
            sys.exit(-1)
        except KeyboardInterrupt:
            logger.error("Keyboard interupt by user, abandoning save ... ")
    return str(save_path)


def masksat_by_ofs(sat_path, shape_file):
    """
    Clips out the part of the GOES product that
    falls within the OFS shapefile.
    Saves the clipped concatenated file.
    Does not delete the file for the entire
    GOES coverage as it can be used for other OFS
    """

    shp_mask = gpd.read_file(f"{shape_file}")
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
        engine='netcdf4'
    )

    sat_nc_slice = sat_nc.sel(lon=slice(minx, maxx), lat=slice(maxy, miny))
    mask_sat = poly.mask(sat_nc_slice.isel(time=0))

    masked_sat = sat_nc.where(mask_sat == 0)

    return masked_sat

def parameter_dir_validation (prop,dir_params, logger):
    '''
    parameter_validation
    '''
    # Start Date and End Date validation
    try:
        datetime.strptime(prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = (
            f"Please check Start Date - "
            f"{prop.start_date_full}, End Date - "
            f"{prop.end_date_full}. Abort!"
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    if datetime.strptime(
        prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ"
    ) > datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ"):
        error_message = (
            f"End Date {prop.end_date_full} "
            f"is before Start Date {prop.end_date_full}. Abort!"
        )
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # prop.path validation
    ofs_extents_path = os.path.join(prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = (
            f"ofs_extents/ folder is not found. "
            f"Please check prop.path - {prop.path}. Abort!"
        )
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f"{ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shape_file):
        error_message = (
            f"Shapefile {prop.ofs} is not found at "
            f"the folder {ofs_extents_path}. Abort!"
        )
        logger.error(error_message)
        sys.exit(-1)
    prop.data_observations_2d_satellite_path = os.path.join (
        prop.path,
        dir_params["data_dir"],
        dir_params["observations_dir"],
        dir_params["2d_satellite_dir"],
        )
    os.makedirs (prop.data_observations_2d_satellite_path,exist_ok = True)

def get_satellite(prop, logger):
    """
    get_satellite
    """
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s", config_file)
        logger.info("Using log config %s", log_config_file)

    # logger.info("--- Starting Visulization Process ---")

    dir_params = utils.Utils().read_config_section("directories", logger)

    parameter_dir_validation (prop, dir_params, logger)

    logger.info("--- Starting Satellite Observation Process ---")

    hours = hours_range(prop.start_date_full, prop.end_date_full)

    if prop.ofs in ["ciofs", "sscofs", "wcofs", "sfbofs", "creofs", "wcofs2"]:
        list_of_urls = list_of_urls_g18(hours)

    elif prop.ofs in [
        "loofs",
        "lmhofs",
        "lsofs",
        "leofs",
        "gomofs",
        "cbofs",
        "dbofs",
        "sjrofs",
        "tbofs",
        "ngofs",
        "ngofs2",
        "necofs",
        "nyofs",
        "secofs",
    ]:
        list_of_urls = list_of_urls_g16(hours)

    logger.info(
        "Begin retriving the following files:%s",
        [i.split ('/')[-1] for i in list_of_urls]
    )

    try:
        list_of_files = get_sat(
            list_of_urls, prop.data_observations_2d_satellite_path, logger
        )
        logger.info("Satellite data downloaded")
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed downloading files'. Abort!"""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    try:
        concated_sat = concat_sat(
            list_of_files, prop.data_observations_2d_satellite_path, logger, prop1
        )
    except ValueError as ex:
        error_message = (
            f"""Error: {str(ex)}. Failed concatenation of satellite data. Abort!"""
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    try:
        shape_file = f"{prop.ofs_extents_path}/{prop.ofs}.shp"
        masked_sat_path = os.path.join(os.getcwd(), \
                Path(prop.data_observations_2d_satellite_path),str(args.ofs + '.nc'))
        if os.path.exists(masked_sat_path):
            file_age = time.time() - os.path.getmtime(masked_sat_path)
            if file_age < 3600 and os.path.getsize(masked_sat_path)/1024>50:
                logger.info("Recent valid masked file found - skipping clipping")
        else:
            logger.info("No recent masked file found ")
            logger.info("Begin clipping satellite data for %s", prop.ofs)
            masked_sat = masksat_by_ofs(concated_sat, shape_file)
            masked_sat.to_netcdf(
                    f"{prop.data_observations_2d_satellite_path}/{args.ofs}.nc", mode="w"
            )
            logger.info("Finished clipping satellite data for %s", prop.ofs)
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed clipping satellite data'. Abort!"""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)



if __name__ == "__main__":

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python write_obs_ctlfile.py",
        usage="%(prog)s",
        description="ofs write Station Control File",
    )
    parser.add_argument(
        "-o",
        "--ofs",
        required=True,
        help="Choose from the list on the ofs_Extents folder, you can also "
        "create your own shapefile, add it top the ofs_Extents folder and "
        "call it here",
    )
    parser.add_argument("-p", "--path", required=True, help="/PATH/TO/SA_HOMEIDR/")
    parser.add_argument(
        "-s",
        "--StartDate_full",
        required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    parser.add_argument(
        "-e",
        "--EndDate_full",
        required=True,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'",
    )
    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.ofs.lower()
    prop1.path = args.path
    # prop.data_observations_2d_satellite_path = r"" + prop.path + "data/observations/2d_satellite"
    prop1.ofs_extents_path = r"" + prop1.path + "ofs_extents" + "/"
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full

    get_satellite(prop1, None)
