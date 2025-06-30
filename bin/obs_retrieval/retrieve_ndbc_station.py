"""
-*- coding: utf-8 -*-

 Documentation for Scripts retrieve_NDBC_station.py

 Script Name: retrieve_NDBC_station.py

 Technical Contact(s):
 Name:  PL

 Abstract:

This script uses SEARVEY to retrieve all NDBC (real-time, historical)
standard meteorological and oceanographic data using the station id_number,
start_date, end_date, and variable ('water_level', 'water_temperature',
'currents', 'salinity').

This script replaces three different NDBC scripts:
    -retrieve_ndbc_rt_station.py
    -retrieve_ndbc_year_station.py
    -retrieve_ndbc_month_station.py

 Author Name:  PL       Creation Date:  05/22/2025


"""
import numpy as np
import pandas as pd
import sys
from pathlib import Path
from searvey._ndbc_api import fetch_ndbc_station

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))



def retrieve_ndbc_station(start_date,end_date,id_number,variable,logger):
    """
    This function uses SEARVEY to retrieve all NDBC (real-time, historical)
    standard meteorological and oceanographic data using the station id_number,
    start_date, end_date, and variable ('water_level', 'water_temperature',
    'currents', 'salinity').
    """

    data_station = []
    start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
    end_date = end_date[:4] + '-' + end_date[4:6] + '-' + end_date[6:]
    # Figure out what mode to search for at each station depending on variable
    if variable == 'water_level':
        datamodes = ['stdmet']
    elif variable in ['water_temperature', 'salinity']:
        datamodes = ['ocean','stdmet']
    elif variable == 'currents':
        datamodes = ['adcp']
    else:
        datamodes = ['stdmet']

    # Fetch the data
    for datamode in datamodes:
        data_station = fetch_ndbc_station(
            station_id=str(id_number),
            mode=datamode,
            start_date=start_date,
            end_date=end_date,
            )
        if data_station.empty == False:
            break
    if data_station.empty == False:
        # Do a bunch of formatting
        data_station = data_station.reset_index()

        # Selecting the last monthly variable data, concatenating it
        # with the realtime, and removing duplicates
        if variable == "water_level":
            if "TIDE" in data_station.columns:
                if data_station['TIDE'].isna().all() == False:
                    data_station["DateTime"] = pd.to_datetime(
                        data_station['timestamp'])
                    data_station.loc[(data_station["TIDE"] == "MM"), "TIDE"] =\
                        np.nan
                    data_station.loc[(data_station["TIDE"].astype(float) >= 98)\
                                     | (data_station["TIDE"].astype(float) \
                                        < -50), "TIDE"] = np.nan
                    data_station = data_station[["DateTime", "TIDE"]]
                    data_station = data_station.dropna()
                    data_station["TIDE"] = \
                        data_station["TIDE"].astype(float) * 0.3048
                    data_station["Datum"] = "MLLW"
                    data_station.rename(columns={"TIDE": "OBS"}, inplace=True)
                else:
                    data_station = None
            else:
                data_station = None

        elif variable == "currents":
            if "DEP01" in data_station.columns:
                if data_station['DEP01'].isna().all() == False:
                    data_station["DateTime"] = pd.to_datetime(
                        data_station['timestamp'])
                    data_station.loc[(data_station["DEP01"] == "MM"),\
                                     "DEP01"] = np.nan
                    data_station.loc[(data_station["DEP01"].astype(float) > 5)\
                                     | (data_station["DEP01"].\
                                        astype(float) < 0), "DEP01"] = np.nan
                    data_station.loc[(data_station["DIR01"] == "MM"),\
                                     "DIR01"] = np.nan
                    data_station.loc[(data_station["DIR01"].astype(float)>360)\
                                     | (data_station["DIR01"].\
                                        astype(float) < 0), "DIR01"] = np.nan
                    data_station.loc[(data_station["SPD01"] == "MM"),
                                     "SPD01"] = np.nan
                    data_station.loc[(data_station["SPD01"].astype(float)\
                                      >= 900) | (data_station["SPD01"].\
                                                 astype(float) < 0), \
                                                 "SPD01"] = np.nan
                    data_station = data_station[["DateTime",
                                                 "DEP01",
                                                 "DIR01",
                                                 "SPD01"]]
                    data_station = data_station.dropna()
                    data_station["SPD01"] = (data_station["SPD01"].astype(
                        float) / 100)  # This is to convert from cm/s to m/s
                    data_station.rename(
                        columns={"DIR01": "DIR", "SPD01": "OBS"}, inplace=True)
                else:
                    data_station = None
            else:
                data_station = None

        elif variable == "water_temperature":
            if "OTMP" in data_station.columns:
                if data_station['OTMP'].isna().all() == False:
                    data_station["DateTime"] = pd.to_datetime(
                        data_station['timestamp'])
                    data_station.loc[(data_station["OTMP"] == "MM"),
                                     "OTMP"] = np.nan
                    data_station.loc[(data_station["OTMP"].\
                                      astype(float) >= 98) | (
                            data_station["OTMP"].astype(float) < -50),
                                          "OTMP"] = np.nan
                    data_station.loc[(data_station["DEPTH"].astype(float)\
                                      >= 5000) | (data_station["DEPTH"].\
                                                  astype(float) < 0),\
                                                  "DEPTH"] = np.nan
                    data_station = data_station[["DateTime", "DEPTH", "OTMP"]]
                    data_station = data_station.dropna()
                    data_station.rename(
                        columns={"DEPTH": "DEP01", "OTMP": "OBS"},inplace=True)
                else:
                    data_station = None

            elif "WTMP" in data_station.columns:
                if data_station['WTMP'].isna().all() == False:
                    data_station["DateTime"] = pd.to_datetime(
                        data_station['timestamp'])
                    data_station.loc[(data_station["WTMP"] == "MM"), "WTMP"] =\
                        np.nan
                    data_station.loc[(data_station["WTMP"].astype(float)
                                      >= 98) | (data_station["WTMP"].\
                                                astype(float) < -50),
                                                "WTMP"] = np.nan
                    data_station = data_station[["DateTime", "WTMP"]]
                    data_station = data_station.dropna()
                    data_station.insert(1, "DEP01", pd.to_numeric(0.0))
                    data_station.rename(columns={"WTMP": "OBS"}, inplace=True)
                else:
                    data_station = None
            else:
                data_station = None

        elif variable == "salinity":
            if "SAL" in data_station.columns:
                if data_station['SAL'].isna().all() == False:
                    data_station["DateTime"] = pd.to_datetime(
                        data_station['timestamp'])
                    data_station.loc[(data_station["SAL"] == "MM"),
                                     "SAL"] = np.nan
                    data_station.loc[(data_station["SAL"].astype(float)
                                      >= 98) | (data_station["SAL"].\
                                                astype(float) < 0),
                                                "SAL"] = np.nan
                    data_station = data_station[["DateTime", "DEPTH", "SAL"]]
                    data_station = data_station.dropna()
                    data_station.rename(
                        columns={"DEPTH": "DEP01", "SAL": "OBS"}, inplace=True)
                else:
                    data_station = None
            else:
                data_station = None
    else:
        data_station = None

    if data_station is None:
        logger.error(
            "Retrieve NDBC station %s failed for %s -- station contacted, "
            "but no data available.", str(id_number), variable)
    return data_station
