"""
-*- coding: utf-8 -*-

Documentation for plotting_functions.py

Script Name: plotting_functions.py

Technical Contact(s): Name: AJK

Abstract:
   This module contains shared plotting-related functions used in the skill assessment
   visualizations. These functions were originally part of create_1dplot.py but
   have been separated into this module to improve readability, maintainability,
   and modularity of the code base.

   Functions included here create various types of scalar and vector plots,
   including water level, temperature, salinity, currents, and wind rose
   visualizations.

Language:  Python 3.8+

Estimated Execution Time: Varies depending on input size

Usage:
   Import individual functions as needed:
       from plotting_functions import oned_scalar_plot, oned_vector_plot1, ...

Author Name:  AJK

Revisions:
Date          Author     Description

"""

import os
from datetime import datetime
import urllib.request
import json
import seaborn as sns
import pandas as pd
import numpy as np
#from plotly.validators.scatter.marker import SymbolValidator

# ----------------------------------------
# UTILITY FUNCTIONS
# ----------------------------------------

def make_cubehelix_palette(ncolors, start_val, rot_val, light_val):
    '''
    Function makes and returns a custom cubehelix color palette for plotting.
    The colors within the cubehelix palette (and therefore plots) can be
    distinguished in greyscale to improve accessibility. Colors are returned as HEX
    values because it's easier to handle HEX compared to RGB values.

    Arguments:
    -ncolors = number of dicrete colors in color palette, should correspond to
        number of time series in the plot. integer, 1 <= ncolors <= 1000(?)
    -start_val = starting hue for color palette. float, 0 <= start_val <= 3
    -rot_val = rotations around the hue wheel over the range of the palette.
        Larger (smaller) absolute values increase (decrease) number of different
        colors in palette. float, positive or negative
    -light_val = Intensity of the lightest color in the palette.
        float, 0 (darker) <= light <= 1 (lighter)

    More details:
        https://seaborn.pydata.org/generated/seaborn.cubehelix_palette.html

    '''
    palette_rgb = sns.cubehelix_palette(
        n_colors=ncolors,start=start_val,rot=rot_val,gamma=1.0,
        hue=0.8,light=light_val,dark=0.15,reverse=False,as_cmap=False
        )
    #Convert RGB to HEX numbers
    palette_hex = palette_rgb.as_hex()
    return palette_hex, palette_rgb


def get_markerstyles():
    '''
    Function gets list of marker symbols so they can be
    assigned in for-loop that iterates and makes the plots. This way works so
    that any number of time/data series can be added to the plots and they will
    all have different plot markers.
    '''
    # Get master list of symbols
    # raw_symbols = SymbolValidator().values
    # namestems = []
    # # Get symbol names from master list of symbols and store names in 'namestems'
    # for i in range(0,len(raw_symbols),3):
    #     name = raw_symbols[i+2]
    #     namestems.append(name.replace("-open", "").replace("-dot", ""))

    # # Filter out non-unique namestem entries to create and return 'allmarkerstyles'
    # allmarkerstyles = list(dict.fromkeys(namestems))

    return ['circle','square','diamond','cross','x','triangle-up','pentagon']

def get_title(prop, node, station_id, name_var, logger):
    '''Returns plot title'''
    # If incoming date format is YYYY-MM-DDTHH:MM:SSZ, the chunk below will take out the
    # 'Z' and 'T' to correctly format the date for plotting.
    if "Z" in prop.start_date_full and "Z" in prop.end_date_full:
        start_date = prop.start_date_full.replace("Z", "")
        end_date = prop.end_date_full.replace("Z", "")
        start_date = start_date.replace("T", " ")
        end_date = end_date.replace("T", " ")
    # If the format is YYYYMMDD-HH:MM:SS, the chunk below will format correctly
    else:
        start_date = datetime.strptime(prop.start_date_full,"%Y%m%d-%H:%M:%S")
        end_date = datetime.strptime(prop.end_date_full,"%Y%m%d-%H:%M:%S")
        start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

    # Get the NWS ID (shefcode) if CO-OPS station -- all CO-OPS stations have
    # 7-digit ID
    if station_id[2] == 'CO-OPS' and name_var != 'cu':
        metaurl = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/' +\
            str(station_id[0]) + '.json?units=metric'
        try:
            with urllib.request.urlopen(metaurl) as url:
                metadata = json.load(url)
            nws_id = metadata['stations'][0]["shefcode"]
        except Exception as e:
            logger.error(f"Exception in get_title when getting nws id: {e}")
            nws_id = 'NA'
        nwsline = f"NWS ID:&nbsp;{nws_id}"
    else:
        nwsline = ''

    return f"<b>NOAA/NOS OFS Skill Assessment<br>" \
            f"{station_id[2]} station:&nbsp;{station_id[1]} " \
            f"({station_id[0]})<br>" \
            f"OFS:&nbsp;{prop.ofs.upper()}&nbsp;&nbsp;&nbsp;Node ID:&nbsp;" \
            f"{node}&nbsp;&nbsp;&nbsp;" \
            + nwsline + \
            f"<br>From:&nbsp;{start_date}" \
            f"&nbsp;&nbsp;&nbsp;To:&nbsp;" \
            f"{end_date}<b>"

def get_error_range(name_var,prop,logger):
    '''
    Gets the target error range (X1) for a variable of choice (name_var)

    '''
    filename = 'error_ranges.csv'
    filepath = os.path.join(prop.path,'conf',filename)
    try:
        # Dataframe of error range file!
        df = pd.read_csv(filepath)
        subs = df[df["name_var"]==name_var]
        # Get error ranges for variable
        X1 = pd.to_numeric(subs["X1"]).values[0]
        X2 = pd.to_numeric(subs["X2"]).values[0]
    except (KeyError, FileNotFoundError, IndexError):
        # Make file using default values
        errordata = [['salt',3.5,0.5],['temp',3,0.5],['wl',0.15,0.5],
                     ['cu',0.26,0.5], ['ice_conc',10,0.5]]
        df = pd.DataFrame(errordata, columns=['name_var','X1','X2'])
        subs = df[df["name_var"]==name_var]
        df.to_csv(filepath, index=False)
        X1 = pd.to_numeric(subs["X1"]).values[0]
        X2 = pd.to_numeric(subs["X2"]).values[0]

    return X1, X2

def find_max_data_gap(arr):
    '''
    Finds the maximum data gap (i.e., number of consecutive nans)
    for a time series, and returns it
    '''
    if len(arr) == 0:
        return 0
    # Find indices of nans. Then difference indices to locate consecutive nans --
    # a difference of 1 means consecutive nans, and a data gap is present
    gap_check = (np.diff(np.argwhere(arr.isnull()),axis=0))
    max_count = 0
    current_count = 0
    for x in gap_check:
        if x == 1: # value of 1 indicates data gap
            current_count += 1
        else:
            max_count = max(max_count, current_count)
            current_count = 0
    max_count = max(max_count, current_count) # Handle case where array ends with 1s
    return max_count
