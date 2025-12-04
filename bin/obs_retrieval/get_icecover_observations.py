"""
-*- coding: utf-8 -*-

Author: PL



This script downloads GLSEA netcdfs from GLERL. The netcdfs contain the daily
National Ice Center ice cover analysis that is used as observations in the
ice skill package. After the GLSEA files are downloaded for each day in the
skill assessment run, they are all concatenated, and the area is clipped to the
OFS of interest, and saved as a separate concatenated netcdf.

    1) Download GLSEA netcdfs from Thredds using a list of files from
    list_of_files.
    2) Concatenate GLSEA netcdfs into one big netcdf and save it.
    3) Clip GLSEA extent from Great Lakes-wide to OFS extent.
    4) Save clipped concatenated netcdf file.

"""

import argparse
import logging
import logging.config
import os
import sys
from datetime import datetime, timedelta
import urllib.request
from urllib.error import HTTPError
import geopandas as gpd
import regionmask
import xarray as xr
import numpy as np

from pathlib import Path
# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils
from model_processing import model_properties



def hours_range(start_date, end_date):
    """
    This function takes the start and end date and returns
    all the dates between start and end.
    This is useful when we need to list all the folders (one per date)
    where the data to be contatenated is stored
    """
    dates = []
    ndays = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")-\
        datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    ndays = int(ndays.days)+1
    for i in range(0,ndays):
        date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") +\
            timedelta(days=i)
        dates.append(date.strftime("%Y-%m-%dT%H:%M:%SZ"))

    return dates


def list_of_urls_glsea(hours_range1,logger):
    """
    This function will list the API's for all the GLSEA
    files between the range of data (output from hour_range())
    """
    # Retrieve urls from config file
    url_params = utils.Utils().read_config_section("urls", logger)
    url_root = url_params["glsea_thredds"]
    url_root_backup = url_params["glsea_erddap"]

    url_list = []
    url_list_backup = []
    for i in hours_range1:
        mydate = datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"{url_root}"
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%m')}/"
            f"{mydate.strftime('%Y')}_{mydate.strftime('%j')}"
            f"_glsea_ice.nc"
        )
        url_backup = (
            f"{url_root_backup}"
            f"{mydate.strftime('%Y')}/"
            f"{mydate.strftime('%m')}/"
            f"{mydate.strftime('%Y')}_{mydate.strftime('%j')}"
            f"_glsea_ice.nc"
            )
        url_list.append(url)
        url_list_backup.append(url_backup)

    return url_list, url_list_backup


def get_sat(list_of_urls1, list_of_urls2, obs2d_dir, logger):
    """
    This function gets the GLSEA analysiss from API and appends
    the path to the files saved

    """

    # First try the main Thredds data source
    try:
        logger.info("Try GLSEA Thredds download...")
        urllib.request.urlretrieve(
            list_of_urls1[0], obs2d_dir + r"/" +\
                f"{list_of_urls1[0]}".split("/")[-1]
            )
        logger.info("Thredds is responding!")
        list_of_urls = list_of_urls1
    except (ValueError, HTTPError, Exception):
        logger.info("Thredds is not responding. Switching to ERDDAP!")
        list_of_urls = list_of_urls2

    list_of_files = []
    for idx, sat_dat in enumerate(list_of_urls):
        try:
            logger.info(f"Downloading GLSEA satellite data: {sat_dat}")
            if not os.path.isfile(obs2d_dir+r"/"+f"{sat_dat}".split("/")[-1]):
                urllib.request.urlretrieve(
                    sat_dat, obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1]
                    )
        except (ValueError, HTTPError, Exception) as ex:
            error_message = f"""Error: {str (ex)}. GLSEA failed {sat_dat}!!"""
            logger.error(error_message)
            sys.exit(-1)

        list_of_files.append(
            obs2d_dir + r"/" + f"{sat_dat}".split("/")[-1]
        )

    return list_of_files


def concat_sat(list_of_files, obs2d_dir, logger):
    """
    Concatenates the satellite files on
    list_of_files into once single file,
    deletes the files in list_of_files
    """

    try:
        nc_item = xr.open_mfdataset(list_of_files,
                                    combine = 'nested', concat_dim="time",
                                    lock=False
                                    )
        logger.info("Concatenation complete!")
    except Exception as ex:
        logger.error(f"Error happened at Concatenation: {str(ex)}")
        sys.exit(-1)

    save_path = (
        obs2d_dir
        + r"/"
        + f"{list_of_files[0]}".split("/")[-1].split("00-")[-1].split(".")[0]
        + "_concat"
        + ".nc"
    )
    logger.info("Saving concatenated GLSEA file to netcdf...")
    try:
        nc_item.to_netcdf (
            save_path,
            mode = "w",
            format = "NETCDF4",
            engine = "netcdf4",
            )
    except MemoryError as ex:
        logger.error(f"Error happened at saving file {save_path} -- {str(ex)}")
        sys.exit(-1)

    return nc_item, save_path


def masksat_by_ofs(sat_path, shape_file, prop):
    """
    Clips out the part of the GLSEA product that
    falls within the OFS shapefile.
    Saves the clipped concatenated file.
    Does not delete the file for the entire
    GLSEA coverage as it can be used for other OFS.
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
        # engine='netcdf4'
    )

    sat_nc_slice = sat_nc.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))
    mask_sat = poly.mask(sat_nc_slice.isel(time=0))

    masked_sat = sat_nc.where(mask_sat == 0)

    # Now mask 2D climatology dataset
    filename = os.path.join(prop.path,'conf','gl_2d_clim.npy')
    gl_clim = np.load(filename,allow_pickle=True)
    lat_all = np.array(sat_nc.variables['lat'][:])
    lon_all = np.array(sat_nc.variables['lon'][:])
    latindex = np.where(np.logical_and(lat_all>=miny, lat_all<=maxy))
    latindex = np.array(latindex)
    lonindex = np.where(np.logical_and(lon_all>=minx, lon_all<=maxx))
    lonindex = np.array(lonindex)
    latindex = np.transpose(latindex)
    masked_clim = gl_clim[:,latindex,lonindex]

    return masked_sat, masked_clim

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


def get_icecover_observations(prop, logger):
    """
    1) Download GLSEA netcdfs from Thredds using a list of files from
    list_of_files.
    2) Concatenate GLSEA netcdfs into one big netcdf and save it.
    3) Clip GLSEA extent from Great Lakes-wide to match OFS extent.
    4) Save clipped concatenated netcdf file.
    """
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = (Path(__file__).parent.parent.parent / log_config_file).resolve()

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


    dir_params = utils.Utils().read_config_section("directories", logger)
    parameter_dir_validation (prop, dir_params, logger)
    logger.info("--- Starting Ice Cover Satellite Observation Process ---")

    hours = hours_range(prop.start_date_full, prop.end_date_full)

    #if prop.ofs in ["loofs", "lmhofs", "lsofs", "leofs"]:
    list_of_urls, list_of_urls_backup = list_of_urls_glsea(hours,logger)

    logger.info(
        "Begin retrieving the following files:%s",
        [i.split ('/')[-1] for i in list_of_urls]
    )

    try:
        list_of_files = get_sat(
            list_of_urls, list_of_urls_backup,
            prop.data_observations_2d_satellite_path, logger
        )
        logger.info("Satellite data downloaded")
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed downloading files."""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    try:
        logger.info("Begin concatenating the satellite data")
        concated_sat = concat_sat(
            list_of_files, prop.data_observations_2d_satellite_path, logger
        )
        logger.info("Finished concatenating the satellite data")
    except ValueError as ex:
        error_message = (
            f"""Error: {str(ex)}. Failed concatenation of satellite data."""
        )
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    try:
        shape_file = f"{prop.ofs_extents_path}/{prop.ofs}.shp"
        logger.info("Begin clipping satellite data for %s", prop.ofs)
        masked_sat,ice_clim = masksat_by_ofs(concated_sat[-1], shape_file, prop)
        masked_sat.to_netcdf(
            f"{prop.data_observations_2d_satellite_path}/{prop.ofs}_ice.nc",
            mode="w"
        )
        logger.info("Finished clipping satellite data for %s", prop.ofs)
    except ValueError as ex:
        error_message = f"""Error: {str(ex)}. Failed clipping sat data'."""
        logger.error(error_message)
        print(error_message)
        sys.exit(-1)

    return masked_sat,ice_clim

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
    parser.add_argument("-p", "--path", required=True,
                        help="Path to /opt/ofs_dps")
    parser.add_argument(
        "-s",
        "--StartDate_full",
        required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g.'2023-01-01T12:34:00Z'",
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
    prop1.ofs_extents_path = r"" + prop1.path + "ofs_extents" + "/"
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full

    get_icecover_observations(prop1, None)
