"""
 -*- coding: utf-8 -*-

 Documentation for Scripts retrieve_usgs_station.py

---------------------------------------------------------------------------------------

 Script Name: retrieve_usgs_station.py

 Technical Contact(s): Name:  FC

 Abstract:

 USGS data is complicated!!!
 The url used to retrieve the data requires the parameter code.
 The problem is that there are multiple codes for the same variable -
 we have to try all of them to make sure we retrieve the data.


 Language:  Python 3.8

 Estimated Execution Time: < 3sec

 Author Name:  FC       Creation Date:  06/29/2023

 Revisions:
 Date          Author           Description
 07-20-2023    MK         Modified the scripts to add config,
                                logging, try/except and argparse features
 08-01-2023    FC Standardized all column names and units
 09-01-2023    MK         Modified the code to match PEP-8
                                standard.
"""

import json
from urllib.error import HTTPError
from datetime import datetime
import urllib.request
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import usgs_properties
from obs_retrieval import utils


def retrieve_usgs_station(retrieve_input, logger):
    """retrieve_usgs_station"""
    usgs = usgs_properties.USGSProperties()

    # Retrieve url from config file
    usgs.base_url = utils.Utils().read_config_section("urls",
                                                      logger)["usgs_nwis_url"]

    usgs.start = datetime.strptime(retrieve_input.start_date, "%Y%m%d")
    usgs.end = datetime.strptime(retrieve_input.end_date, "%Y%m%d")

    usgs.start_year = usgs.start.strftime("%Y")
    usgs.start_month = usgs.start.strftime("%m")
    usgs.start_day = usgs.start.strftime("%d")

    usgs.end_year = usgs.end.strftime("%Y")
    usgs.end_month = usgs.end.strftime("%m")
    usgs.end_day = usgs.end.strftime("%d")

    usgs.start_str = f"{usgs.start_year}-{usgs.start_month}-{usgs.start_day}"
    usgs.end_str = f"{usgs.end_year}-{usgs.end_month}-{usgs.end_day}"

    usgs.obs_final = None

    if retrieve_input.variable == "water_level":
        list_of_codes = [
            "62622",
            "62620",
            "72279",
            "63161",
            "63160",
            "62617",
            "62615",
            "62616",
            "62614",
            "72214",
            "72215",
            "00065",
        ]
        for code in list_of_codes:

            usgs.url = (
                f"{usgs.base_url}?sites="
                f"{retrieve_input.station}&parameterCd="
                f"{code}&st"
                f"artDT="
                f"{usgs.start_str}"
                f"T00:00:00.000-04:00&endDT="
                f"{usgs.end_str}"
                f"T23:59:59.999-04:00&siteStatus=all&format=json"
            )

            # obs = json.loads(requests.get(url).text)
            logger.info(f"Calling USGS Station service: {usgs.url}")
            try:
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    f"Retrieve USGS Station service failed at {usgs.url} "
                    f"-- {str(ex)}"
                )
                continue
            if (
                len(obs["value"]["timeSeries"]) > 0
                and len(obs["value"]["timeSeries"][0]["values"]) > 0
            ):
                data = obs["value"]["timeSeries"][0]["values"][0]["value"]
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    # fixed_date = data[i]['dateTime'].split('-')[:-1]
                    # fixed_date = fixed_date[0] + fixed_date[1] +
                    # fixed_date[2]
                    fixed_date = pd.to_datetime(
                        data[i]["dateTime"]).tz_convert(None)
                    date.append(fixed_date)
                    var.append(float(data[i]["value"]))  # converting to meters

                obs = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(date),
                        "DEP01": pd.to_numeric(0.0),
                        "OBS": pd.to_numeric(var),
                    }
                )

                if code in {
                    "00065",
                    "62620",
                    "72279",
                    "63160",
                    "062615",
                    "62614",
                    "72214",
                }:
                    obs["OBS"] = obs["OBS"] * 0.3048  # from feet to meters
                    obs["Datum"] = "NAVD88"

                if code in {"62616", "62614"}:
                    obs["Datum"] = "NGVD"

                if code in {"72214", "72215"}:
                    obs["Datum"] = "IGLD"
                usgs.obs_final = obs
                break

    elif retrieve_input.variable == "water_temperature":
        list_of_codes = ["00010", "00011", "99976", "99980", "99984"]
        for code in list_of_codes:

            usgs.url = (
                f"{usgs.base_url}?"
                f"sites={retrieve_input.station}&parameterCd="
                f"{code}&st"
                f"artDT="
                f"{usgs.start_str}"
                f"T00:00:00.000-00:00&endDT="
                f"{usgs.end_str}"
                f"T23:59:59.999-00:00&siteStatus=all&format=json"
            )

            logger.info(f"Calling USGS Station service: {usgs.url}")
            try:
                # obs = json.loads(requests.get(url).text)
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    f"Retrieve USGS Station service failed at {usgs.url} "
                    f"-- {str(ex)}"
                )
                continue

            if (
                len(obs["value"]["timeSeries"]) > 0
                and len(obs["value"]["timeSeries"][0]["values"]) > 0
            ):
                data = obs["value"]["timeSeries"][0]["values"][0]["value"]
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]["dateTime"].split("-")[:-1]
                    fixed_date = fixed_date[0] + fixed_date[1] + fixed_date[2]
                    date.append(fixed_date)
                    var.append(float(data[i]["value"]))  # converting to meters

                obs = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(date),
                        "DEP01": pd.to_numeric(0.0),
                        "OBS": pd.to_numeric(var),
                    }
                )

                if code == "00011":
                    obs["OBS"] = (obs["OBS"] - 32) * (
                        5 / 9
                    )  # converting temp from F to C
                usgs.obs_final = obs
                break

    elif retrieve_input.variable == "salinity":
        list_of_codes = [
            "00480",
            "00096",
            "70305",
            "72401",
            "90860",
            "90862",
        ]  # [ppth, mg/ml, g/l, PSU, PSU,
        # PSS] #These are all basically the same, no need for conversion
        for code in list_of_codes:

            usgs.url = (
                f"{usgs.base_url}?"
                f"sites={retrieve_input.station}&parameterCd="
                f"{code}&st"
                f"artDT="
                f"{usgs.start_str}"
                f"T00:00:00.000-00:00&endDT="
                f"{usgs.end_str}"
                f"T23:59:59.999-00:00&siteStatus=all&format=json"
            )

            logger.info(f"Calling USGS Station service: {usgs.url}")
            try:
                # obs = json.loads(requests.get(url).text)
                with urllib.request.urlopen(usgs.url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    f"Retrieve USGS Station service failed at {usgs.url} "
                    f"-- {str(ex)}"
                )
                continue

            if (
                len(obs["value"]["timeSeries"]) > 0
                and len(obs["value"]["timeSeries"][0]["values"]) > 0
            ):
                data = obs["value"]["timeSeries"][0]["values"][0]["value"]
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]["dateTime"].split("-")[:-1]
                    fixed_date = fixed_date[0] + fixed_date[1] + fixed_date[2]
                    date.append(fixed_date)
                    var.append(float(data[i]["value"]))  # converting to meters

                obs = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(date),
                        "DEP01": pd.to_numeric(0.0),
                        "OBS": pd.to_numeric(var),
                    }
                )
                usgs.obs_final = obs
                break

    elif retrieve_input.variable == "currents":
        list_of_codes = ["72255", "00055"]
        for code in list_of_codes:

            url = (
                f"{usgs.base_url}?"
                f"sites={retrieve_input.station}&parameterCd="
                f"{code}&st"
                f"artDT="
                f"{usgs.start_str}"
                f"T00:00:00.000-04:00&endDT="
                f"{usgs.end_str}"
                f"T23:59:59.999-04:00&siteStatus=all&format=json"
            )

            logger.info(f"Calling USGS Station service: {url}")
            try:
                # obs = json.loads(requests.get(url).text)
                with urllib.request.urlopen(url) as url_usgs:
                    obs = json.load(url_usgs)
            except HTTPError as ex:
                logger.error(
                    f"Retrieve USGS Station service failed at {url} "
                    f"-- {str(ex)}"
                )
                continue

            if (
                len(obs["value"]["timeSeries"]) > 0
                and len(obs["value"]["timeSeries"][0]["values"]) > 0
            ):

                data = obs["value"]["timeSeries"][0]["values"][0]["value"]
                date, var = [], []
                data_length = len(data)
                for i in range(data_length):
                    fixed_date = data[i]["dateTime"].split("-")[:-1]
                    fixed_date = fixed_date[0] + fixed_date[1] + fixed_date[2]
                    date.append(fixed_date)
                    var.append(float(data[i]["value"]))  # converting to meters

                obs = pd.DataFrame(
                    {
                        "DateTime": pd.to_datetime(date),
                        "DEP01": pd.to_numeric(0.0),
                        "DIR": pd.to_numeric(0.0),
                        "OBS": pd.to_numeric(var),
                    }
                )

                if code in {
                    "72255",
                    "00055",
                    "72168",
                    "72254",
                    "72322",
                    "81904",
                }:  # ft/s to m/s
                    obs["OBS"] = obs["OBS"] * 0.3048

                elif code == "70232":  # knots to m/s
                    obs["OBS"] = obs["OBS"] * 0.5144

                elif code in {"72294", "72321"}:  # mph to m/s
                    obs["OBS"] = obs["OBS"] * 0.44704
                usgs.obs_final = obs
                break
    return usgs.obs_final
