# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 14:33:17 2024

@author: PL
"""

import pandas as pd
import plotly.express as px
import plotly
import os

def get_error_range(name_var,prop,logger):

    filename = 'error_ranges.csv'
    filepath = os.path.join(prop.path,'conf',filename)
    if os.path.isfile(filepath) is True:
        # Dataframe of error range file!
        df = pd.read_csv(filepath)
        subs = df[df["name_var"]==name_var]

    else:
        # Make file using default values
        errordata = [['salt',3.5,0.5],['temp',3,0.5],['wl',0.15,0.5],
                     ['cu',0.26,0.5]]
        df = pd.DataFrame(errordata, columns=['name_var','X1','X2'])
        subs = df[df["name_var"]==name_var]
        df.to_csv(filepath, sep='\t')

    # Get error ranges for variable
    X1 = pd.to_numeric(subs["X1"])
    X2 = pd.to_numeric(subs["X2"])
    X1 = float(X1)
    X2 = float(X2)

    return X1, X2


def make_skill_maps(output, prop, name_var, logger):
    logger.info("Making skill maps...")
    # First make dataframe from stats table
    df = pd.DataFrame(
        {
            "ID ": output["station_id"],
            "OFS NODE ": output["node"],
            "X ": output["X"],
            "Y ": output["Y"],
            "RMSE ": list(zip(*output["skill"]))[0],
            "R ": list(zip(*output["skill"]))[1],
            "Mean bias ": list(zip(*output["skill"]))[2],
            "Mean bias percent ": list(zip(*output["skill"]))[3],
            "Mean bias, current direction ": list(zip(*output["skill"]))[4],
            "Central freq ": list(zip(*output["skill"]))[5],
            "CF pass/fail ": list(zip(*output["skill"]))[6],
            "Positive outlier freq ": list(zip(*output["skill"]))[7],
            "PO freq pass/fail ": list(zip(*output["skill"]))[8],
            "Negative outlier freq ": list(zip(*output["skill"]))[9],
            "NO freq pass/fail ": list(zip(*output["skill"]))[10],
            "Mean bias standard dev ": list(zip(*output["skill"]))[11],
            "Target RMSE ": list(zip(*output["skill"]))[12],
        }
    )

    # Drop stations with insufficient data points for stats
    if df['RMSE '].dtype == 'object' or df['RMSE '].dtype == 'str':
        df = df[~df['RMSE '].str.contains("data points", na=False)]
        df = df.astype({
                        "RMSE ": 'float',
                        "R ": 'float',
                        "Mean bias ": 'float',
                        "Mean bias percent ": 'float',
                        "Mean bias, current direction ": 'float',
                        "Central freq ": 'float',
                        "CF pass/fail ": 'str',
                        "Positive outlier freq ": 'float',
                        "PO freq pass/fail ": 'str',
                        "Negative outlier freq ": 'float',
                        "NO freq pass/fail ": 'str',
                        "Mean bias standard dev ": 'float',
                        "Target RMSE ": 'float',
                        })
        if df.empty:
            logger.error("DataFrame is empty, cannot make skill maps!")
            return

    df["X "] = pd.to_numeric(df["X "])
    df["Y "] = pd.to_numeric(df["Y "])

    # But first, get data sorted out
    datestrend = (prop.end_date_full).split('T')[0]
    datestrbeg = (prop.start_date_full).split('T')[0]
    if name_var == 'wl':
        title_var = 'water level'
    elif name_var == 'salt':
        title_var = 'salinity'
    elif name_var == 'temp':
        title_var = 'temperature'
    elif name_var == 'cu':
        title_var = 'current speed'
    plottitle = prop.ofs.upper() + ' ' + prop.whichcast + ' ' + title_var +\
        ' skill statistics, ' + datestrbeg + ' - ' + datestrend
    fig = px.scatter_mapbox(df, lat = "Y ", lon = "X ",
                     color="RMSE ", # which column to use to set the color of markers
                     hover_name="ID ", # column added to hover information
                     mapbox_style= "open-street-map",
                     hover_data=["Target RMSE ","Mean bias ","R ","Central freq ", "CF pass/fail ",
                                 "Positive outlier freq ", "PO freq pass/fail ",
                                 "Negative outlier freq ", "NO freq pass/fail "],
                     size="RMSE ", # size of markers
                     zoom = 6,
                     title = plottitle,
                     height = 600
                     )
    fig.update_geos(fitbounds="locations")
    savename = prop.ofs + '_' + name_var + '_' + prop.whichcast + '_RMSE_map' + '.html'
    filepath = os.path.join(prop.path,'data','visual', savename)
    plotly.offline.plot(fig, filename=filepath, auto_open=False)
    logger.info("Skill map for %s complete", name_var)
    #fig.show()
