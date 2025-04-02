"""
-*- coding: utf-8 -*-

Documentation for Scripts inventory_T_C_station_station.py

Script Name: inventory_T_C_station.py

Technical Contact(s): Name:  FC

Abstract:

This script is used to create a dataframe with all NOAA Tides
and Currents Stations. This dataframe contains all stations
within lat_1,lat_2,lon_1,lon_2


Language:  Python 3.8

Estimated Execution Time: < 5sec

Author Name:  FC       Creation Date:  06/23/2023

Revisions:

Date          Author     Description
07-20-2023    MK   Modified the scripts to add config,
                         logging, try/except and argparse features
08-10-2023    MK   Modified the scripts to match the PEP-8
                         standard and code best practices


Remarks:
      The inputs for this function can either be entered manually,
      or by the ouputs of ofs_geometry
      The output from this script is used by ofs_inventory.py to
      create the final data inventory
"""

import json
import urllib.request
import urllib.error
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def get_inventory(station_type, url_params, variable, logger):
    """Get Inventory"""

    station_url = (
        url_params["co_ops_mdapi_base_url"]
        + "/webapi/stations.json?type="
        + station_type
        + "&units=english"
    )

    logger.info("Calling MDAPI: " + station_url)
    try:
        with urllib.request.urlopen(station_url) as url:
            inventory = json.load(url)
    except urllib.error.HTTPError as ex:
        logger.error(
            "T_C_station %s data download failed at %s -- %s.",
            variable,
            station_url,
            str(ex),
        )
        return None
    return inventory


def inventory_t_c_station(lat_1, lat_2, lon_1, lon_2, logger):
    """
    This function will search for all stations in the metadata and
    output the station id and lat and lon
    """

    url_params = utils.Utils().read_config_section("urls", logger)

    # Retrieve url from config file
    # base_url = url_params["co_ops_mdapi_base_url"]
    # station_url = base_url
    lat_1, lat_2, lon_1, lon_2 = (
        float(lat_1),
        float(lat_2),
        float(lon_1),
        float(lon_2),
    )

    id_list, lon_list, lat_list, name_list = [], [], [], []
    for variable in [
        "water_level",
        "water_temperature",
        "currents",
        "salinity",
    ]:

        if variable == "water_level":
            station_type = "waterlevels"
        elif variable == "water_temperature":
            station_type = "watertemp"
        elif variable in ("wind", "air_pressure"):
            station_type = "met"
        elif variable == "currents":
            station_type = "currents"
        elif variable == "salinity":
            station_type = "physocean"

        inventory = get_inventory(station_type, url_params, variable, logger)

        if inventory is not None:
            for i in range(0, len(inventory["stations"])):
                if (lon_1 < inventory["stations"][i]["lng"] < lon_2) & (
                    lat_1 < inventory["stations"][i]["lat"] < lat_2
                ):
                    id_list.append(inventory["stations"][i]["id"])
                    lon_list.append(inventory["stations"][i]["lng"])
                    lat_list.append(inventory["stations"][i]["lat"])
                    name_list.append(inventory["stations"][i]["name"])

    inventory_t_c_final = pd.DataFrame(
        {
            "ID": id_list,
            "X": pd.to_numeric(lon_list),
            "Y": pd.to_numeric(lat_list),
            "Source": "CO-OPS",
            "Name": name_list,
        }
    )

    inventory_t_c_final = inventory_t_c_final.drop_duplicates(
        subset=["ID"], keep="first"
    )

    logger.info("inventory_t_c_station.py run successfully")

    return inventory_t_c_final
