"""
-*- coding: utf-8 -*-

Documentation for Scripts retrieve_t_and_c_station.py

Abstract:
This script is used to retrieve time series from NOAA Tides and Currents.
This function will loop between the start and end date, gathering data
in 30-day pieces.
If the last 30 day period does not end exactly at the end date (which is
very likely),
data will be masked between start_dt_0 and  end_dt_0.

Script Name: retrieve_t_and_c_station.py

Language:  Python 3.8

Estimated Execution Time: < 3sec

Author Name:  FC       Creation Date:  06/27/2023

Revisions:
Date          Author        Description
07-20-2023    MK      Modified the scripts to add config,
                            logging, try/except and argparse features
08-01-2023    FC   Standardized all column names and units
08-29-2023    MK           Modified the code to match PEP-8 standard.
"""
import json
from datetime import datetime, timedelta
import urllib.request
from urllib.error import HTTPError
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import t_and_c_properties
from obs_retrieval import utils


def retrieve_t_and_c_station(retrieve_input, logger):
    """
    This function will loop between the start and end date, gathering data
    in 30-day pieces.
    If the last 30day period does not end exactly at the end date (which is
     very likely),
    data will be masked between start_dt_0 and  end_dt_0.

    This function inputs are:
    station ID, start_date, end_date, variable ['water_level',
    'water_temperature', 'currents', 'salinity', 'Wind', 'air_pressure']
    """

    variable = retrieve_input.variable

    t_c = t_and_c_properties.TidesandCurrentsProperties()

    # Retrieve url from config file
    url_params = utils.Utils().read_config_section("urls", logger)
    t_c.mdapi_url = url_params["co_ops_mdapi_base_url"]
    t_c.api_url = url_params["co_ops_api_base_url"]

    t_c.start_dt_0 = datetime.strptime(retrieve_input.start_date, "%Y%m%d")
    t_c.end_dt_0 = datetime.strptime(retrieve_input.end_date, "%Y%m%d")

    t_c.start_dt = datetime.strptime(retrieve_input.start_date, "%Y%m%d")
    t_c.end_dt = datetime.strptime(retrieve_input.end_date, "%Y%m%d")

    t_c.delta = timedelta(days=30)
    t_c.total_date, t_c.total_var, t_c.total_dir = [], [], []


    while t_c.start_dt <= t_c.end_dt:
        date_i = (
            t_c.start_dt.strftime("%Y") +
            t_c.start_dt.strftime("%m") +
            t_c.start_dt.strftime("%d")
        )
        date_f = (
            (t_c.start_dt + t_c.delta).strftime("%Y")
            + (t_c.start_dt + t_c.delta).strftime("%m")
            + (t_c.start_dt + t_c.delta).strftime("%d")
        )

        if variable == "water_level":
            t_c.station_url = (
                f"{t_c.api_url}/datagetter?begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&product={variable}&datum="
                f"{retrieve_input.datum}&time_zone=gmt&units="
                f"metric&format=json"
            )

        elif variable == "water_temperature":
            t_c.station_url = (
                f"{t_c.api_url}/datagetter?begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&product="
                f"{variable}&time_zone="
                f"gmt&units=metric&format=json"
            )

            t_c.station_url_2 = (
                f"{t_c.api_url}/datagetter?product="
                f"{variable}&application="
                f"NOS.COOPS.TAC.PHYSOCEAN&begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&time_zone=GMT&units="
                f"metric&interval=6&format=json"
            )

        elif variable == "salinity":

            t_c.station_url = (
                f"{t_c.api_url}/datagetter?begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&product="
                f"{variable}&time_zone="
                f"gmt&units=metric&format=json"
            )

            t_c.station_url_2 = (
                f"{t_c.api_url}/datagetter?product="
                f"{variable}&application="
                f"NOS.COOPS.TAC.PHYSOCEAN&begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&time_zone=GMT&units="
                f"metric&interval=6&format=json"
            )

        elif variable == "currents":
            t_c.station_url = (
                f"{t_c.api_url}/datagetter?begin_date="
                f"{date_i}&end_date={date_f}&station="
                f"{retrieve_input.station}&product={variable}&time_zone="
                f"gmt&units=metric&format=json"
            )

        #logger.info("Calling Retrieve T and C station service: %s", t_c.station_url)

        if variable in {"water_temperature", "salinity"}:
            try:
                with urllib.request.urlopen(t_c.station_url) as url:
                    obs = json.load(url)
                logger.info(
                    "Retrieve T and C station service completed at %s ",
                    t_c.station_url)
            except HTTPError as ex_1:
                logger.error(
                    "Retrieve T and C station service failed at %s -- "
                    "%s. Calling url_2",
                    t_c.station_url, str(ex_1))
                try:
                    with urllib.request.urlopen(t_c.station_url_2) as url_2:
                        obs = json.load(url_2)
                    logger.info(
                        "Retrieve T and C station service completed at %s ",
                        t_c.station_url_2)
                except HTTPError as ex_2:
                    logger.error(
                        "Retrieve T and C station service failed at %s -- %s",
                        t_c.station_url_2,
                        str(ex_2),
                    )
                    t_c.start_dt += t_c.delta
                    continue
        else:
            try:
                with urllib.request.urlopen(t_c.station_url) as url:
                    obs = json.load(url)
                logger.info(
                    "Retrieve T and C station service completed at %s ",
                    t_c.station_url)
            except HTTPError as ex:
                logger.error(
                    "Retrieve T and C station service failed at %s -- %s",
                    t_c.station_url,
                    str(ex),
                )
                t_c.start_dt += t_c.delta
                continue

        t_c.date, t_c.var, t_c.drt = [], [], []
        if "data" in obs.keys():
            for i in range(len(obs["data"])):
                t_c.date.append(obs["data"][i]["t"])

                if variable in {"water_level",
                                "water_temperature",
                                "air_pressure"}:
                    t_c.var.append(obs["data"][i]["v"])

                elif variable == "salinity":
                    t_c.var.append(obs["data"][i]["s"])

                elif variable == "currents":
                    t_c.var.append(
                        float(obs["data"][i]["s"]) / 100
                    )  # This is to convert speed from cm/s to m/s
                    t_c.drt.append(obs["data"][i]["d"])

                elif variable == "wind":
                    t_c.var.append(obs["data"][i]["s"])
                    t_c.drt.append(obs["data"][i]["d"])

            t_c.total_date.append(t_c.date)
            t_c.total_var.append(t_c.var)
            if variable in {"wind", "currents"}:
                t_c.total_dir.append(t_c.drt)

        t_c.start_dt += t_c.delta

    # This "TRY" is for finding the depth in which the observation was taken
    logger.info("Calling Retrieve T and C station service: %s", t_c.mdapi_url)

    t_c.depth = 0.0
    t_c.depth_url = None
    try:
        t_c.station_url = (
            f"{t_c.mdapi_url}/webapi/stations/{str(retrieve_input.station)}"
            f"/bins.json"
            f"?units=metric"
        )
        with urllib.request.urlopen(t_c.station_url) as url:
            t_c.depth_url = json.load(url)
        logger.info(
            "Retrieve T and C station service completed at %s ",
            t_c.station_url)
    except HTTPError as ex:
        logger.error(
            "Retrieve T and C station service failed at %s -- %s",
            t_c.station_url, str(ex)
        )
    if (
        t_c.depth_url is not None
        and t_c.depth_url["bins"] is not None
        and t_c.depth_url["real_time_bin"] is not None and
            t_c.depth_url["bins"][t_c.depth_url["real_time_bin"]-1][
                "depth"] is not None
    ):
        t_c.depth = float(
            t_c.depth_url["bins"][t_c.depth_url["real_time_bin"]-1]["depth"])

    t_c.total_date = sum(t_c.total_date, [])
    t_c.total_var = sum(t_c.total_var, [])

    if variable in {"wind", "currents"}:
        t_c.total_dir = sum(t_c.total_dir, [])

        obs = pd.DataFrame(
            {
                "DateTime": pd.to_datetime(t_c.total_date),
                "DEP01": pd.to_numeric(t_c.depth),
                "DIR": pd.to_numeric(t_c.total_dir),
                "OBS": pd.to_numeric(t_c.total_var),
            }
        )

    else:
        obs = pd.DataFrame(
            {
                "DateTime": pd.to_datetime(t_c.total_date),
                "DEP01": pd.to_numeric(t_c.depth),
                "OBS": pd.to_numeric(t_c.total_var),
            }
        )

    mask = (obs["DateTime"] >= t_c.start_dt_0) & (
                obs["DateTime"] <= t_c.end_dt_0)
    obs = obs.loc[mask]

    if len(obs.DateTime) > 0:
        obs = obs.sort_values(by="DateTime").drop_duplicates()

        return obs
