"""
-*- coding: utf-8 -*-

 Documentation for Scripts retrieve_NDBC_RT_station.py

 Script Name: retrieve_NDBC_RT_station.py

 Technical Contact(s):
 Name:  FC

 Abstract:

 This function will retrieve all Standard meteorological and
 Oceanographic yearly data given the station:
 ID, Year, variable ['water_level', 'water_temperature',
 'currents', 'salinity', 'wind', 'air_pressure', Hs]

 In the NDBC website current year data is split by month. The last
 available monthly data has a diffent url structure than the
 previous monthly data
 Therefore if the final date falls after the last monthly data
 available we have to grab the data for the last month available plus
 their realtime data (last 45 days), which also uses a different
 url structure. And then remove duplicate datetimes.
 This function should be called in the last monthly iteration,
 so it returns the last monthly data available plus the realtime data.

 Author Name:  FC       Creation Date:  06/28/2023

 Revisions:
       Date          Author             Description
       07-20-2023    MK           Modified the scripts to add
       config,
                                        logging, try/except and argparse
                                        features
       08-01-2023    FC   Standardized all column names and
                                        units, also included WTMP in case
                                        OTMP is not found
       08-29-2023    MK           Modified the code to match PEP-8
                                        standard.

"""
from urllib.error import HTTPError

import numpy as np
import pandas as pd
import sys
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from obs_retrieval import utils


def retrieve_ndbc_rt_station(retrieve_input, logger):
    """
    This function will retrieve all current/realtime Standard
    meteorological
    and Oceanographic data given the station:
    ID, Year, variable ['water_level', 'water_temperature', 'currents',
    'salinity', 'wind', 'air_pressure', Hs], month (numerical),
    month [e.g., 'Jan', 'Feb', 'Mar']

    In the NDBC website current year data is split by month. The last
    available monthly data has a diffent url structure than the
    previous monthly data
    Therefore if the final date falls after the last monthly data
    available we have to grab the data for the last month available
    plus their realtime data (last 45 days), which also uses a
    different
    url structure. And then remove duplicate datetimes.

    This function should be called in the last monthly iteration, so it
    returns the last monthly data available plus the realtime data.
    """

    variable = retrieve_input.variable

    # Retrieve url from config file
    base_url = utils.Utils().read_config_section("urls", logger)[
        "ndbc_noaa_url"]

    # This section creates the urls for either the standard
    # meteorological and oceanographic monthly data plus the respective
    # realtime data
    if variable in {"water_level", "wind", "air_pressure", "Hs"}:
        url = (f"{base_url}/data/stdmet/{retrieve_input.month}/"
               f"{str(retrieve_input.station).lower()}.txt")
        url_rt = f"{base_url}/data/realtime2/" f""\
                 f"{str(retrieve_input.station)}.txt"

    elif variable in {"water_temperature", "salinity"}:
        url = (f"{base_url}/data/ocean/{retrieve_input.month}/"
               f"{str(retrieve_input.station).lower()}.txt")
        url_rt = f"{base_url}/data/realtim"\
                 f"e2/" f"{str(retrieve_input.station)}.ocean"

        url2 = (f"{base_url}/data/stdmet/{retrieve_input.month}/"
                f"{str(retrieve_input.station).lower()}.txt")
        url_rt2 = f"{base_url}/data/realtim"\
                  f"e2/" f"{str(retrieve_input.station)}.txt"

    elif variable == "currents":
        url = (f"{base_url}/data/adcp/{retrieve_input.month}/"
               f"{str(retrieve_input.station).lower()}.txt")
        url_rt = f"{base_url}/data/realti"\
                 f"me2/" f"{str(retrieve_input.station)}.adcp"

    # This section will try to open the realtime data and then the
    # monthly data and create a datetime collumn
    logger.info("Calling Retrieve NDBC RT station service for %s", variable)
    obs_rt = None
    obs_rt2 = None
    if variable == "currents":
        try:
            obs_rt = (pd.read_csv(
                url_rt, sep="\\s+", dtype=object, usecols=range(10)).drop(
                index=0).reset_index())
        except (ValueError, HTTPError):
            try:
                obs_rt = (
                    pd.read_csv(url_rt, sep="\\s+", dtype=object).drop(
                        index=0).reset_index())
            except Exception as ex:
                logger.error(
                    "Retrieve NDBC RT Station service failed at "
                    "%s -- %s", url_rt, str(ex))
                raise Exception from ex
    elif variable in {"water_temperature", "salinity"}:
        try:
            obs_rt = (pd.read_csv(url_rt, sep="\\s+", dtype=object).drop(
                index=0).reset_index())
        except (ValueError, HTTPError) as ex1:
            logger.error(
                "Retrieve NDBC RT station service failed at %s -- "
                "%s, calling url_rt2", url_rt, str(ex1), )
            try:
                obs_rt2 = (pd.read_csv(
                    url_rt2, sep="\\s+", dtype=object).drop(
                    index=0).reset_index())
            except Exception as ex2:
                logger.error(
                    "Retrieve NDBC RT station service failed at %s -- "
                    "%s", url_rt2, str(ex2), )
                #raise Exception from ex2
    else:
        try:
            obs_rt = (pd.read_csv(url_rt, sep="\\s+", dtype=object).drop(
                index=0).reset_index())
        except Exception as ex:
            logger.error(
                "Retrieve NDBC RT Station service failed at %s -- %s",
                url_rt, str(ex), )
            raise Exception from ex

    logger.info("Calling Retrieve NDBC RT tation service for %s", variable)
    obs = None
    obs2 = None
    if variable == "currents":
        try:
            obs = (pd.read_csv(
                url, sep="\\s+", dtype=object, usecols=range(10)).drop(
                index=0).reset_index())
        except (ValueError, HTTPError):
            try:
                obs = (pd.read_csv(url, sep="\\s+", dtype=object).drop(
                    index=0).reset_index())
            except Exception as ex:
                logger.error(
                    "Retrieve NDBC RT Station service failed at"
                    " %s -- %s", url, str(ex))
                #raise Exception from ex
    elif variable in {"water_temperature", "salinity"}:
        try:
            obs = pd.read_csv(url, sep="\\s+", dtype=object).drop(
                index=0).reset_index()
        except (ValueError, HTTPError) as ex1:
            logger.error(
                "Retrieve NDBC RT station service failed at %s -- "
                "%s, "
                "calling url2", url, str(ex1), )
            try:
                obs2 = (pd.read_csv(url2, sep="\\s+", dtype=object).drop(
                    index=0).reset_index())
            except Exception as ex2:
                logger.error(
                    "Retrieve NDBC RT station service failed at %s -- "
                    "%s", url2, str(ex2), )
                #raise Exception from ex2
    else:
        try:
            obs = pd.read_csv(url, sep="\\s+", dtype=object).drop(
                index=0).reset_index()
        except Exception as ex:
            logger.error(
                "Retrieve NDBC RT Station service failed at %s -- %s", url,
                str(ex), )
            #raise Exception from ex

    if ((variable in {"water_temperature", "salinity"}) and (
            obs_rt is None) and (obs_rt2 is not None)):
        obs_rt2["DateTime"] = pd.to_datetime(
            obs_rt2["#YY"].astype(str) + "-" + obs_rt2["MM"].astype(
                str) + "-" + obs_rt2["DD"].astype(str) + "-" + obs_rt2[
                "hh"].astype(str) + "-" + obs_rt2["mm"].astype(str),
            yearfirst=True, format="%Y-%m-%d-%H-%M", )
    elif obs_rt is not None:
        obs_rt["DateTime"] = pd.to_datetime(
            obs_rt["#YY"].astype(str) + "-" + obs_rt["MM"].astype(
                str) + "-" + obs_rt["DD"].astype(str) + "-" + obs_rt[
                "hh"].astype(str) + "-" + obs_rt["mm"].astype(str),
            yearfirst=True, format="%Y-%m-%d-%H-%M", )

    if ((variable in {"water_temperature", "salinity"}) and (
            obs is None) and (obs2 is not None)):
        obs2["DateTime"] = pd.to_datetime(
            obs2["#YY"].astype(str) + "-" + obs2["MM"].astype(str) + "-" +
            obs2["DD"].astype(str) + "-" + obs2["hh"].astype(str) + "-" +
            obs2["mm"].astype(str), yearfirst=True,
            format="%Y-%m-%d-%H-%M", )
    elif obs is not None:
        obs["DateTime"] = pd.to_datetime(
            obs["#YY"].astype(str) + "-" + obs["MM"].astype(str) + "-" +
            obs["DD"].astype(str) + "-" + obs["hh"].astype(str) + "-" +
            obs["mm"].astype(str), yearfirst=True,
            format="%Y-%m-%d-%H-%M", )

    # Selecting the last monthly variable data, concatenating it
    # with the realtime, and removing duplicates
    if variable == "water_level":
        if (
                obs is not None and obs_rt is not None and "TIDE" in
                obs.columns and "TIDE" in obs_rt.columns):
            obs.loc[(obs["TIDE"] == "MM"), "TIDE"] = np.nan
            obs.loc[(obs["TIDE"].astype(float) >= 98) | (
                    obs["TIDE"].astype(float) < -50), "TIDE"] = np.nan
            obs = obs[["DateTime", "TIDE"]]
            obs = obs.dropna()
            obs["TIDE"] = obs["TIDE"].astype(float) * 0.3048
            obs["Datum"] = "MLLW"

            obs_rt.loc[(obs_rt["TIDE"] == "MM"), "TIDE"] = np.nan
            obs_rt.loc[(obs_rt["TIDE"].astype(float) >= 98) | (
                    obs_rt["TIDE"].astype(float) < -50), "TIDE"] = np.nan
            obs_rt = obs_rt[["DateTime", "TIDE"]]
            obs_rt = obs_rt.dropna()
            obs["TIDE"] = obs["TIDE"].astype(float) * 0.3048
            obs["Datum"] = "MLLW"
            obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
                by="DateTime").drop_duplicates())

            obs.rename(columns={"TIDE": "OBS"}, inplace=True)
        else:
            obs = None

    elif variable == "currents":
        if (
                obs is not None or obs_rt is not None or "DEP01" in
                obs.columns or "DEP01" in obs_rt.columns):
            try:
                obs.loc[(obs["DEP01"] == "MM"), "DEP01"] = np.nan
                obs.loc[(obs["DEP01"].astype(float) > 5) | (
                        obs["DEP01"].astype(float) < 0), "DEP01"] = np.nan
                obs.loc[(obs["DIR01"] == "MM"), "DIR01"] = np.nan
                obs.loc[(obs["DIR01"].astype(float) > 360) | (
                        obs["DIR01"].astype(float) < 0), "DIR01"] = np.nan
                obs.loc[(obs["SPD01"] == "MM"), "SPD01"] = np.nan
                obs.loc[(obs["SPD01"].astype(float) >= 900) | (
                        obs["SPD01"].astype(float) < 0), "SPD01"] = np.nan
                obs = obs[["DateTime", "DEP01", "DIR01", "SPD01"]]
                obs = obs.dropna()
                obs["SPD01"] = (obs["SPD01"].astype(
                    float) / 100)  # This is to convert speed from cm/s to m/s
            except Exception as ex:
                logger.error(
                    "Fail to process NDBC currents data... "
                    "trying realtime data... -- %s",
                    str(ex))

            try:
                obs_rt.loc[(obs_rt["DEP01"] == "MM"), "DEP01"] = np.nan
                obs_rt.loc[(obs_rt["DEP01"].astype(float) > 5) | (
                        obs_rt["DEP01"].astype(float) < 0), "DEP01"] = np.nan
                obs_rt.loc[(obs_rt["DIR01"] == "MM"), "DIR01"] = np.nan
                obs_rt.loc[(obs_rt["DIR01"].astype(float) > 360) | (
                        obs_rt["DIR01"].astype(float) < 0), "DIR01"] = np.nan
                obs_rt.loc[(obs_rt["SPD01"] == "MM"), "SPD01"] = np.nan
                obs_rt.loc[(obs_rt["SPD01"].astype(float) >= 900) | (
                        obs_rt["SPD01"].astype(float) < 0), "SPD01"] = np.nan
                obs_rt = obs_rt[["DateTime", "DEP01", "DIR01", "SPD01"]]
                obs_rt = obs_rt.dropna()
                obs_rt["SPD01"] = obs_rt["SPD01"].astype(float) / 100
            except Exception as ex:
                logger.error(
                    "Fail to process NDBC real time currents data -- %s",
                    str(ex))

            try:
                obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
                    by="DateTime").drop_duplicates())
            except:
                obs = obs_rt

            obs.rename(
                columns={"DIR01": "DIR", "SPD01": "OBS"}, inplace=True)

        else:
            obs = None

    elif variable == "water_temperature":
        try:
            if (
                    obs is not None or obs_rt is not None or "OTMP" in
                    obs.columns or "OTMP" in obs_rt.columns):
                try:
                    obs.loc[(obs["OTMP"] == "MM"), "OTMP"] = np.nan
                    obs.loc[(obs["OTMP"].astype(float) >= 98) | (
                            obs["OTMP"].astype(float) < -50), "OTMP"] = np.nan
                    obs.loc[(obs["DEPTH"].astype(float) >= 5000) | (
                            obs["DEPTH"].astype(float) < 0), "DEPTH"] = np.nan
                    obs = obs[["DateTime", "DEPTH", "OTMP"]]
                    obs = obs.dropna()
                except Exception as ex:
                    logger.error(
                        "Fail to process NDBC ocean temperature data... "
                        "trying realtime data... -- %s",
                        str(ex), )

                try:
                    obs_rt.loc[(obs_rt["OTMP"] == "MM"), "OTMP"] = np.nan
                    obs_rt.loc[(obs_rt["OTMP"].astype(float) >= 98) | (
                            obs_rt["OTMP"].astype(float) < -50), "OTMP"] =\
                        np.nan
                    obs_rt.loc[(obs_rt["DEPTH"].astype(float) >= 5000) | (
                            obs_rt["DEPTH"].astype(float) < 0), "DEPTH"] =\
                        np.nan
                    obs_rt = obs_rt[["DateTime", "DEPTH", "OTMP"]]
                    obs_rt = obs_rt.dropna()
                except Exception as ex:
                    logger.error(
                        "Fail to process NDBC real time ocean temperature "
                        "data -- %s",
                        str(ex), )

                try:
                    obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
                        by="DateTime").drop_duplicates())
                except:
                    obs = obs_rt

                obs.rename(
                    columns={"DEPTH": "DEP01", "OTMP": "OBS"}, inplace=True)

        except:
            if (
                    obs2 is not None or obs_rt2 is not None or "WTMP" in
                    obs2.columns or "WTMP" in obs_rt2.columns):
                try:
                    obs2.loc[(obs2["WTMP"] == "MM"), "WTMP"] = np.nan
                    obs2.loc[(obs2["WTMP"].astype(float) >= 98) | (
                            obs2["WTMP"].astype(float) < -50), "WTMP"] = np.nan
                    # obs2.loc[(obs2['DEPTH'].astype(float)>=5000) | (obs2[
                    # 'DEPTH'].astype(float)<0), 'DEPTH'] = np.nan
                    obs2 = obs2[["DateTime", "WTMP"]]
                    obs2 = obs2.dropna()
                except Exception as ex:
                    logger.error(
                        "Fail to process NDBC surface temperature data... "
                        "trying realtime data... -- %s",
                        str(ex), )

                try:
                    obs_rt2.loc[(obs_rt2["WTMP"] == "MM"), "WTMP"] = np.nan
                    obs_rt2.loc[(obs_rt2["WTMP"].astype(float) >= 98) | (
                            obs_rt2["WTMP"].astype(float) < -50), "WTMP"] = np.nan
                    # obs_rt2.loc[(obs_rt2['DEPTH'].astype(float)>=5000) | (
                    # obs_rt2['DEPTH'].astype(float)<0), 'DEPTH'] = np.nan
                    obs_rt2 = obs_rt2[["DateTime", "WTMP"]]
                    obs_rt2 = obs_rt2.dropna()
                except Exception as ex:
                    logger.error(
                        "Fail to process NDBC real time surface temperature "
                        "data -- %s",
                        str(ex), )

                try:
                    obs = (pd.concat([obs2, obs_rt2], axis=0).sort_values(
                        by="DateTime").drop_duplicates())
                except:
                    obs = obs_rt2

                obs.insert(1, "DEP01", pd.to_numeric(0.0))

                obs.rename(columns={"WTMP": "OBS"}, inplace=True)

    elif variable == "salinity":
        if obs is None and obs2 is not None:
            obs = obs2
        if obs_rt is None and obs_rt2 is not None:
            obs_rt = obs_rt2
        if (
                obs is not None or obs_rt is not None or "SAL" in
                obs.columns or "SAL" in obs_rt.columns):
            try:
                obs.loc[(obs["SAL"] == "MM"), "SAL"] = np.nan
                obs.loc[(obs["SAL"].astype(float) >= 98) | (
                        obs["SAL"].astype(float) < 0), "SAL"] = np.nan
                obs = obs[["DateTime", "DEPTH", "SAL"]]
                obs = obs.dropna()
            except Exception as ex:
                logger.error(
                    "Fail to process NDBC salinity data... trying realtime "
                    "data... -- %s",
                    str(ex), )
            try:
                obs_rt.loc[(obs_rt["SAL"] == "MM"), "SAL"] = np.nan
                obs_rt.loc[(obs_rt["SAL"].astype(float) >= 98) | (
                        obs_rt["SAL"].astype(float) < 0), "SAL"] = np.nan
                obs_rt = obs_rt[["DateTime", "DEPTH", "SAL"]]
                obs_rt = obs_rt.dropna()
            except Exception as ex:
                logger.error(
                    "Fail to process NDBC real time salinity data -- %s",
                    str(ex), )

            try:
                obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
                    by="DateTime").drop_duplicates())
            except:
                obs = obs_rt

            obs.rename(
                columns={"DEPTH": "DEP01", "SAL": "OBS"}, inplace=True)
        else:
            obs = None

    elif variable == "Hs":
        obs.loc[(obs["WVHT"] == "MM"), "WVHT"] = np.nan
        obs.loc[(obs["WVHT"].astype(float) >= 50) | (
                obs["WVHT"].astype(float) < 0), "WVHT"] = np.nan
        obs = obs[["DateTime", "WVHT"]]
        obs = obs.dropna()

        obs_rt.loc[(obs_rt["WVHT"] == "MM"), "WVHT"] = np.nan
        obs_rt.loc[(obs_rt["WVHT"].astype(float) >= 50) | (
                obs_rt["WVHT"].astype(float) < 0), "WVHT"] = np.nan
        obs_rt = obs_rt[["DateTime", "WVHT"]]
        obs_rt = obs_rt.dropna()
        obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
            by="DateTime").drop_duplicates())

        obs.rename(columns={"WVHT": "OBS"}, inplace=True)

    elif variable == "wind":

        obs.loc[(obs["WD"] == "MM"), "WD"] = np.nan
        obs.loc[(obs["WD"].astype(float) > 360) | (
                obs["WD"].astype(float) < 0), "WD"] = np.nan
        obs.loc[(obs["WSPD"].astype(float) > 98) | (
                obs["WSPD"].astype(float) < 0), "WSPD"] = np.nan
        obs = obs[["DateTime", "WD", "WSPD"]]
        obs.rename(columns={"WD": "WDIR"}, inplace=True)
        obs = obs.dropna()

        obs.loc[(obs["WDIR"] == "MM"), "WDIR"] = np.nan
        obs.loc[(obs["WDIR"].astype(float) > 360) | (
                obs["WDIR"].astype(float) < 0), "WDIR"] = np.nan
        obs.loc[(obs["WSPD"].astype(float) > 98) | (
                obs["WSPD"].astype(float) < 0), "WSPD"] = np.nan
        obs = obs[["DateTime", "WDIR", "WSPD"]]
        obs = obs.dropna()

        obs_rt.loc[(obs_rt["WD"] == "MM"), "WD"] = np.nan
        obs_rt.loc[(obs_rt["WD"].astype(float) > 360) | (
                obs_rt["WD"].astype(float) < 0), "WD"] = np.nan
        obs_rt.loc[(obs_rt["WSPD"] == "MM"), "WSPD"] = np.nan
        obs_rt.loc[(obs_rt["WSPD"].astype(float) > 98) | (
                obs_rt["WSPD"].astype(float) < 0), "WSPD"] = np.nan
        obs_rt = obs_rt[["DateTime", "WD", "WSPD"]]
        obs_rt.rename(columns={"WD": "WDIR"}, inplace=True)
        obs_rt = obs.dropna()
        obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
            by="DateTime").drop_duplicates())

        obs_rt.loc[(obs_rt["WDIR"] == "MM"), "WDIR"] = np.nan
        obs_rt.loc[(obs_rt["WDIR"].astype(float) > 360) | (
                obs_rt["WDIR"].astype(float) < 0), "WDIR"] = np.nan
        obs_rt.loc[(obs_rt["WSPD"] == "MM"), "WSPD"] = np.nan
        obs_rt.loc[(obs_rt["WSPD"].astype(float) > 98) | (
                obs_rt["WSPD"].astype(float) < 0), "WSPD"] = np.nan
        obs_rt = obs_rt[["DateTime", "WDIR", "WSPD"]]
        obs_rt = obs_rt.dropna()
        obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
            by="DateTime").drop_duplicates())

        obs.rename(columns={"WD": "DIR", "WSPD": "OBS"}, inplace=True)

        obs.rename(
            columns={"WDIR": "DIR", "WSPD": "OBS"}, inplace=True)

    elif variable == "air_pressure":

        obs.loc[(obs["PRES"] == "MM"), "PRES"] = np.nan
        obs.loc[(obs["PRES"].astype(float) > 1500) | (
                obs["PRES"].astype(float) < 700), "PRES"] = np.nan
        obs = obs[["DateTime", "PRES"]]
        obs = obs.dropna()

        obs.loc[(obs["BAR"] == "MM"), "BAR"] = np.nan
        obs.loc[(obs["BAR"].astype(float) > 1500) | (
                obs["BAR"].astype(float) < 700), "BAR"] = np.nan
        obs = obs[["DateTime", "BAR"]]
        obs.rename(columns={"BAR": "PRES"}, inplace=True)
        obs = obs.dropna()

        obs_rt.loc[(obs_rt["PRES"] == "MM"), "PRES"] = np.nan
        obs_rt.loc[(obs_rt["PRES"].astype(float) > 1500) | (
                obs_rt["PRES"].astype(float) < 700), "PRES"] = np.nan
        obs_rt = obs_rt[["DateTime", "PRES"]]
        obs_rt = obs_rt.dropna()
        obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
            by="DateTime").drop_duplicates())

        obs_rt.loc[(obs_rt["BAR"] == "MM"), "BAR"] = np.nan
        obs_rt.loc[(obs_rt["BAR"].astype(float) > 1500) | (
                obs_rt["BAR"].astype(float) < 700), "BAR"] = np.nan
        obs_rt = obs_rt[["DateTime", "BAR"]]
        obs_rt.rename(columns={"BAR": "PRES"}, inplace=True)
        obs_rt = obs_rt.dropna()
        obs = (pd.concat([obs, obs_rt], axis=0).sort_values(
            by="DateTime").drop_duplicates())

        obs.rename(columns={"PRES": "OBS"}, inplace=True)

        obs.rename(columns={"BAR": "OBS"}, inplace=True)
    else:
        logger.warning(
            "The variable entered:" + variable + " is not valid.")
        return None
    if obs is None:
        logger.error(
            "Retrieve NDBC RT station service failed. '%s' data is not "
            "in the Response", variable, )
    return obs
