"""
 -*- coding: utf-8 -*-

 Documentation for Scripts retrieve_NDBC_month_station.py

 Script Name: retrieve_NDBC_month_station.py

 Technical Contact(s): Name:  FC

 Abstract:

This function will retrieve all Standard meteorological and Oceanographic
yearly data given the station:
ID, Year, Variable ['water_level', 'water_temperature', 'currents',
'salinity', 'wind', 'air_pressure', Hs]

In the NDBC website current year data is split by month with a completely
different url structure.
Therefore if the final date falls within the current year we have to
iterate by month and not by year.
This script is used for the yearly data


 Error Conditions:

 Author Name:  FC       Creation Date:  06/28/2023

 Revisions:
 Date          Author     Description
 07-20-2023    MK   Modified the scripts to add config,
                          logging, try/except and argparse features
 08-01-2023    FC   Standardized all column names
                          and units, also included WTMP in case OTMP is
                          not found
 08-25-2023    MK   Modified the scripts to match the PEP-8
                           standard and code best practices
 09-07-2023    MK 1) Fixed obs2 problem and obs2 is called by
                        variable in {'water_temperature', 'salinity'} only
                        2) Organized the code and
                        replaced some try/except with condition checking

"""

from urllib.error import HTTPError

import numpy as np
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def retrieve_ndbc_month_station(retrieve_input, logger):
    """
    This function will retrieve all Standard meteorological and
    Oceanographic yearly data given the station:
    ID, Year, Variable ['water_level', 'water_temperature', 'currents',
    'salinity', 'wind', 'air_pressure', Hs]

    In the NDBC website current year data is split by month with a
    completely different url structure.
    Therefore if the final date falls within the current year we have to
    iterate by month and not by year.
    This script only deals with the monthly data.
    """
    variable = retrieve_input.variable

    url_params = utils.Utils().read_config_section("urls", logger)
    base_url = url_params["ndbc_noaa_url"]

    if variable in {"water_level", "wind", "air_pressure", "Hs"}:
        url = (
            f"{base_url}/view_text_file.php?filename="
            f"{str(retrieve_input.station).lower()}"
            f"{retrieve_input.month_num}"
            f"{str(retrieve_input.year)}.txt.gz&dir=data/stdmet/"
            f"{retrieve_input.month}/"
        )

    elif variable in {"water_temperature", "salinity"}:
        url = (
            f"{base_url}/view_text_file.php?filename="
            f"{str(retrieve_input.station).lower()}"
            f"{retrieve_input.month_num}"
            f"{str(retrieve_input.year)}.txt.gz&dir=data/ocean/"
            f"{retrieve_input.month}/"
        )
        url2 = (
            f"{base_url}/view_text_file.php?filename="
            f"{str(retrieve_input.station).lower()}"
            f"{retrieve_input.month_num}"
            f"{str(retrieve_input.year)}.txt.gz&dir=data/stdmet/"
            f"{retrieve_input.month}/"
        )

    elif variable == "currents":
        url = (
            f"{base_url}/view_text_file.php?filename="
            f"{str(retrieve_input.station).lower()}"
            f"{retrieve_input.month_num}"
            f"{str(retrieve_input.year)}.txt.gz&dir=data/adcp/"
            f"{retrieve_input.month}/"
        )

    # The NDBC date data comes in all sorts of datetime formats,
    # this section will try to fit it to one

    logger.info("Calling Retrieve NDBC Month station service for %s", variable)
    obs = None
    obs2 = None
    if variable == "currents":
        try:
            obs = (
                pd.read_csv(url, sep="\\s+", dtype=object, usecols=range(10))
                .drop(index=0)
                .reset_index()
            )
        except (ValueError, HTTPError):
            try:
                obs = (
                    pd.read_csv(url, sep="\\s+", dtype=object)
                    .drop(index=0)
                    .reset_index()
                )
            except Exception as ex:
                logger.error(
                    "Retrieve NDBC Month Station service failed at %s -- %s",
                    url,
                    str(ex),
                )
                raise Exception from ex
    elif variable in {"water_temperature", "salinity"}:
        try:
            obs = pd.read_csv(url,
                              sep="\\s+",
                              dtype=object).drop(index=0).reset_index()
        except (ValueError, HTTPError) as ex1:
            logger.error(
                "Retrieve NDBC Month Station service failed at %s -- "
                "%s, calling url2",
                url,
                str(ex1),
            )
            try:
                obs2 = (
                    pd.read_csv(url2, sep="\\s+", dtype=object)
                    .drop(index=0)
                    .reset_index()
                )
            except Exception as ex2:
                logger.error(
                    "Retrieve NDBC Month Station service failed at %s -- %s",
                    url2,
                    str(ex2),
                )
                raise Exception from ex2
    else:
        try:
            obs = pd.read_csv(url,
                              sep="\\s+",
                              dtype=object).drop(index=0).reset_index()
        except Exception as ex:
            logger.error(
                "Retrieve NDBC Month Station service failed at %s -- %s",
                url,
                str(ex),
            )
            raise Exception from ex

    if (
        (variable in {"water_temperature", "salinity"})
        and (obs is None)
        and (obs2 is not None)
    ):
        if "YY" in obs2.columns:
            obs2["DateTime"] = pd.to_datetime(
                "19"
                + obs2["YY"].astype(str)
                + "-"
                + obs2["MM"].astype(str)
                + "-"
                + obs2["DD"].astype(str)
                + "-"
                + obs2["hh"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H",
            )
        elif "mm" in obs2.columns:
            obs2["DateTime"] = pd.to_datetime(
                obs2["#YY"].astype(str)
                + "-"
                + obs2["MM"].astype(str)
                + "-"
                + obs2["DD"].astype(str)
                + "-"
                + obs2["hh"].astype(str)
                + "-"
                + obs2["mm"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H-%M",
            )
        else:
            obs2["DateTime"] = pd.to_datetime(
                obs2["YYYY"].astype(str)
                + "-"
                + obs2["MM"].astype(str)
                + "-"
                + obs2["DD"].astype(str)
                + "-"
                + obs2["hh"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H",
            )
    else:
        if "YY" in obs.columns:
            obs["DateTime"] = pd.to_datetime(
                "19"
                + obs["YY"].astype(str)
                + "-"
                + obs["MM"].astype(str)
                + "-"
                + obs["DD"].astype(str)
                + "-"
                + obs["hh"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H",
            )
        elif "mm" in obs.columns:
            obs["DateTime"] = pd.to_datetime(
                obs["#YY"].astype(str)
                + "-"
                + obs["MM"].astype(str)
                + "-"
                + obs["DD"].astype(str)
                + "-"
                + obs["hh"].astype(str)
                + "-"
                + obs["mm"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H-%M",
            )
        else:
            obs["DateTime"] = pd.to_datetime(
                obs["YYYY"].astype(str)
                + "-"
                + obs["MM"].astype(str)
                + "-"
                + obs["DD"].astype(str)
                + "-"
                + obs["hh"].astype(str),
                yearfirst=True,
                format="%Y-%m-%d-%H",
            )

    if variable == "water_level":
        obs.loc[
            (obs["TIDE"].astype(float) >= 98) |
            (obs["TIDE"].astype(float) < -50),
            "TIDE",
        ] = np.nan
        obs = obs[["DateTime", "TIDE"]]
        obs = obs.dropna()
        obs["TIDE"] = obs["TIDE"].astype(float) * 0.3048
        obs["Datum"] = "MLLW"
        obs.rename(columns={"TIDE": "OBS"}, inplace=True)

    elif variable == "currents":
        obs.loc[
            (obs["DEP01"].astype(float) > 5) |
            (obs["DEP01"].astype(float) < 0), "DEP01"
        ] = np.nan
        obs.loc[
            (obs["DIR01"].astype(float) > 360) |
            (obs["DIR01"].astype(float) < 0),
            "DIR01",
        ] = np.nan
        obs.loc[
            (obs["SPD01"].astype(float) >= 900) |
            (obs["SPD01"].astype(float) < 0),
            "SPD01",
        ] = np.nan
        obs = obs[["DateTime", "DEP01", "DIR01", "SPD01"]]
        obs = obs.dropna()
        obs["SPD01"] = (
            obs["SPD01"].astype(float) / 100
        )  # This is to convert speed from cm/s to m/s
        obs.rename(columns={"DIR01": "DIR", "SPD01": "OBS"}, inplace=True)

    elif variable == "water_temperature":
        if obs is not None:
            obs.loc[
                (obs["OTMP"].astype(float) >= 98) |
                (obs["OTMP"].astype(float) < -50),
                "OTMP",
            ] = np.nan
            obs.loc[
                (obs["DEPTH"].astype(float) >= 5000) |
                (obs["DEPTH"].astype(float) < 0),
                "DEPTH",
            ] = np.nan
            obs = obs[["DateTime", "DEPTH", "OTMP"]]
            obs = obs.dropna()
            obs.rename(columns={"DEPTH": "DEP01", "OTMP": "OBS"}, inplace=True)
        elif obs2 is not None:
            obs2.loc[
                (obs2["WTMP"].astype(float) >= 98) |
                (obs2["WTMP"].astype(float) < -50),
                "WTMP",
            ] = np.nan
            # obs.loc[(obs['DEPTH'].astype(float)>=5000) | (obs[
            # 'DEPTH'].astype(float)<0), 'DEPTH'] = np.nan
            obs2 = obs2[["DateTime", "WTMP"]]
            obs2 = obs2.dropna()
            obs2.rename(columns={"WTMP": "OBS"}, inplace=True)
            obs2.insert(1, "DEP01", pd.to_numeric(0.0))
            obs = obs2

    elif variable == "salinity":
        if obs is not None:
            obs.loc[
                (obs["SAL"].astype(float) >= 98) |
                (obs["SAL"].astype(float) < 0), "SAL"
            ] = np.nan
            obs = obs[["DateTime", "DEPTH", "SAL"]]
            obs = obs.dropna()
            obs.rename(columns={"DEPTH": "DEP01", "SAL": "OBS"}, inplace=True)
        elif obs2 is not None:
            obs2.loc[
                (obs2["SAL"].astype(float) >= 98) |
                (obs2["SAL"].astype(float) < 0),
                "SAL",
            ] = np.nan
            obs2 = obs2[["DateTime", "DEPTH", "SAL"]]
            obs2 = obs2.dropna()
            obs2.rename(columns={"DEPTH": "DEP01", "SAL": "OBS"}, inplace=True)
            obs = obs2

    elif variable == "Hs":
        obs.loc[
            (obs["WVHT"].astype(float) >= 50) |
            (obs["WVHT"].astype(float) < 0), "WVHT"
        ] = np.nan
        obs = obs[["DateTime", "WVHT"]]
        obs = obs.dropna()
        obs.rename(columns={"WVHT": "OBS"}, inplace=True)

    elif variable == "wind":
        if "WD" in obs.columns:
            obs.loc[
                (obs["WD"].astype(float) > 360) |
                (obs["WD"].astype(float) < 0), "WD"
            ] = np.nan
            obs.loc[
                (obs["WSPD"].astype(float) > 98) |
                (obs["WSPD"].astype(float) < 0),
                "WSPD",
            ] = np.nan
            obs = obs[["DateTime", "WD", "WSPD"]]
            obs.rename(columns={"WD": "DIR", "WSPD": "OBS"}, inplace=True)
            obs = obs.dropna()
        elif "WDIR" in obs.columns:
            obs.loc[
                (obs["WDIR"].astype(float) > 360) |
                (obs["WDIR"].astype(float) < 0),
                "WDIR",
            ] = np.nan
            obs.loc[
                (obs["WSPD"].astype(float) > 98) |
                (obs["WSPD"].astype(float) < 0),
                "WSPD",
            ] = np.nan
            obs = obs[["DateTime", "WDIR", "WSPD"]]
            obs.rename(columns={"WDIR": "DIR", "WSPD": "OBS"}, inplace=True)
            obs = obs.dropna()

    elif variable == "air_pressure":
        if "PRES" in obs.columns:
            obs.loc[
                (obs["PRES"].astype(float) > 1500) |
                (obs["PRES"].astype(float) < 700),
                "PRES",
            ] = np.nan
            obs = obs[["DateTime", "PRES"]]
            obs.rename(columns={"PRES": "OBS"}, inplace=True)
            obs = obs.dropna()
        elif "BAR" in obs.columns:
            obs.loc[
                (obs["BAR"].astype(float) > 1500) |
                (obs["BAR"].astype(float) < 700),
                "BAR",
            ] = np.nan
            obs = obs[["DateTime", "BAR"]]
            obs.rename(columns={"BAR": "OBS"}, inplace=True)
            obs = obs.dropna()

    return obs
