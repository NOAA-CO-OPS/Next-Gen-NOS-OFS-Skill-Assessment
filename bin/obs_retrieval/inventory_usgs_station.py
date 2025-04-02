"""
-*- coding: utf-8 -*-

 Documentation for Scripts inventory_USGS_station.py

 Script Name: inventory_USGS_station.py

 Technical Contact(s): Name:  FC

 Abstract:

   This script is used to create a dataframe with all USGS Stations
   This dataframe contains all stations within lat1,lat2,lon1,lon2
   Another function is also defined here for helping parse the USGS data


 Language:  Python 3.8

 Estimated Execution Time: < 3min

 Author Name:  FC       Creation Date:  06/23/2023

 Revisions:
 Date          Author     Description
 07-20-2023    MK   Modified the scripts to add config,
                          logging, try/except and argparse features

 08-10-2023    MK   Modified the scripts to match the PEP-8
                          standard and code best practices


 Remarks:
       The inputs for this functions can either be entered manually, or
       by the ouputs of ofs_geometry
       The output from this script is used by ofs_inventory.py to create
       the final data inventory
       The function find_between is also defined here. This funciton finds
       a string between two strings. This is used when parsing USGS data

"""

import urllib.request
import urllib.error
from datetime import datetime
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def get_inventory(data, start_dt):
    """Get Inventory"""
    id_list, lon_list, lat_list, name_list = [], [], [], []

    for i in range(len(data) - 1):
        try:
            line = data[i + 1].split("\n")

            value_line = []
            for i in line:
                value = find_between(i, ">", "<")
                value_line.append(value)

            if (
                int(value_line[9]) > 0
                or int(value_line[10]) > 3
                or int(value_line[11][:4]) > start_dt.year
                or int(value_line[12][:4]) > start_dt.year
            ):
                if (
                    value_line[12] == "--"
                    or int(value_line[12][:4]) > start_dt.year
                ):
                    id_list.append(value_line[2])
                    name_list.append(value_line[3])
                    lat_list.append(value_line[5])
                    lon_list.append(value_line[6])

        except ValueError:
            pass

    inventory_usgs_final = pd.DataFrame(
        {
            "ID": id_list,
            "X": pd.to_numeric(lon_list),
            "Y": pd.to_numeric(lat_list),
            "Source": "USGS",
            "Name": name_list,
        }
    )
    return inventory_usgs_final


def find_between(data_str, first, last):
    """
    This is a simple function to help parse the USGS data by selecting a
    string between two strings.
    """
    try:
        start = data_str.index(first) + len(first)
        end = data_str.index(last, start)
        return data_str[start:end]
    except ValueError:
        return ""


def inventory_usgs_station(argu_list, start_date, end_date, logger):
    """
    This function creates an inventory of all atmospheric and water
    related usgs stations
    To avoid selecting an unecessary number of stations, only those that are
    either realtime or have been visited more than 3 times are selected.
    This is necessary because there are several locations that are marked as
    usgs station but actually have no data
    """

    lat_1, lat_2, lon_1, lon_2, start_date, end_date = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(start_date),
        str(end_date),
    )

    start_dt = datetime.strptime(start_date, "%Y%m%d")
    # end_dt = datetime.strptime(end_date, "%Y%m%d")

    url_params = utils.Utils().read_config_section("urls", logger)

    url = (
        url_params["usgs_nwls_inventory_url"]
        + "/inventory?\
site_tp_cd=OC&site_tp_cd=OC-CO&site_tp_cd=ES&site_tp_cd=LK&site_tp_cd=ST&\
site_tp_cd=ST-CA&site_tp_cd=ST-DCH&site_tp_cd=ST-TS&site_tp_cd=WE&\
nw_longitude_va="
        + lon_1
        + "&nw_latitude_va="
        + lat_2
        + "&se_longitude_va="
        + lon_2
        + "&se_latitude_va="
        + lat_1
        + "&coordinate_format=decimal_degrees&site_md=1&group_key=NONE&\
format=sitefile_output&sitefile_output_format=xml&\
column_name=agency_cd&column_name=site_no&column_name=station_nm&\
column_name=site_tp_cd&column_name=dec_lat_va&column_name=dec_long_va&\
column_name=rt_bol&column_name=sv_count_nu&\
list_of_search_criteria=lat_long_bounding_box%2Csite_tp_cd%2Csite_md&\
column_name=sv_begin_date&column_name=sv_end_date"
    )

    try:
        logger.info("Calling USGS service: " + url)
        # data = urllib.request.urlopen(url).read()
        with urllib.request.urlopen(url) as response:
            data = response.read()
        data = data.decode("utf-8")
        data = data.split(" <site>")
    except urllib.error.HTTPError as ex:
        logger.error("USGS data download failed at %s -- %s.", url, str(ex))
        return None

    logger.info("inventory_USGS_station.py run successfully")

    return get_inventory(data, start_dt)
