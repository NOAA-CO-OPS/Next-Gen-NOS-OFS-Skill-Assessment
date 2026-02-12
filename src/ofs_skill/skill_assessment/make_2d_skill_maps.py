"""
Create 2D skill assessment maps.

Makes maps of 2D stats using plotly express, and saves interactive maps to file.

Created on Wed Sep 4 14:33:17 2024

@author: PL

NOTE: This file requires additional migration work to add comprehensive type hints
and docstrings. The core functionality has been preserved with updated imports.
"""

import os
import sys
from datetime import datetime
from logging import Logger
from typing import Any

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import plotly.io as pio


def get_stat_name(stat: str) -> str:
    """
    Get full name of a statistic from its shorthand.

    Parameters
    ----------
    stat : str
        Shorthand for statistic used in map

    Returns
    -------
    str
        Full name of statistic used for hover info
    """
    if stat == 'rmse':
        return 'RMSE'
    elif stat == 'diffmean':
        return 'Mean Error'
    elif stat == 'diffmax':
        return 'Max Error'
    elif stat == 'diffmin':
        return 'Min Error'
    elif stat == 'diffstd':
        return 'Error Standard Deviation'
    elif stat == 'cf':
        return 'Central Frequency'
    elif stat == 'pof':
        return 'Positive Outlier Frequency'
    elif stat == 'nof':
        return 'Negative Outlier Frequency'
    elif stat == 'mod':
        return 'Modeled'
    elif stat == 'obs':
        return 'Observed'
    elif stat == 'diffall':
        return 'Error'
    else:
        return 'NoStatName'


def make_2d_skill_maps(
    z: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    time_all: list[str],
    maptype: str,
    sat_source: str,
    prop1: Any,
    logger: Logger,
) -> None:
    """
    Make maps of 2D stats for local users and save them to file.

    Parameters
    ----------
    z : np.ndarray
        2D or 3D array of data to map
    lat : np.ndarray
        Latitude array
    lon : np.ndarray
        Longitude array
    time_all : List[str]
        List of time strings
    maptype : str
        Type of map to create ('rmse', 'diffmean', 'cf', 'pof', 'nof', etc.)
    sat_source : str
        Source of satellite data
    prop1 : Any
        Properties object with configuration
    logger : Logger
        Logger instance

    Returns
    -------
    None
        Saves HTML map to file
    """
    logger.info('Making plotly express maps of 2D stats for %s...', maptype)

    variable = 'sst'
    errorrange = 3

    date_all = []
    for i in range(0, len(time_all)):
        date_all.append(datetime.strptime(time_all[i], '%Y%m%d-%Hz'))

    lat_flat = np.array(lat.flatten())
    lon_flat = np.array(lon.flatten())

    # Make a giant pandas dataframe
    if z.ndim == 3:
        for i in range(0, len(time_all)):
            z_flat = np.array(z[i, :, :].flatten())
            if i == 0:
                df = pd.DataFrame(
                    {
                        'X': lon_flat,
                        'Y': lat_flat,
                        get_stat_name(maptype): z_flat
                    }
                )
                df['Date'] = date_all[i]
            else:
                df2 = pd.DataFrame(
                    {
                        'X': lon_flat,
                        'Y': lat_flat,
                        get_stat_name(maptype): z_flat
                    }
                )
                df2['Date'] = date_all[i]
                df = pd.concat([df, df2], ignore_index=True)
    elif z.ndim == 2:
        z_flat = np.array(z.flatten())
        df = pd.DataFrame(
            {
                'X': lon_flat,
                'Y': lat_flat,
                get_stat_name(maptype): z_flat
            }
        )
        df['Date'] = date_all[i]

    # Convert lat/lon to numeric
    df['X'] = pd.to_numeric(df['X'])
    df['Y'] = pd.to_numeric(df['Y'])

    # make title
    cast = prop1.whichcast.capitalize()
    if maptype == 'obs':
        cast = ''
    datestrend = (prop1.end_date_full).split('-')[0]
    datestrend = datestrend[4:6] + '/' + datestrend[6:] + '/' + datestrend[0:4]
    datestrbeg = (prop1.start_date_full).split('-')[0]
    datestrbeg = datestrbeg[4:6] + '/' + datestrbeg[6:] + '/' + datestrbeg[0:4]
    plottitle = prop1.ofs.upper() + ' ' + cast + ' ' + 'SST ' +\
        '[' + str(sat_source) + '] ' + get_stat_name(maptype) + ', ' +\
        datestrbeg + ' - ' + datestrend
    # Make custom colormap if doing diff/error plot
    if 'diff' in maptype:
        colorscale = [
        [0, '#524094'],  # dark pink
        [0.16666, '#cec7e7'],  # Pink
        [0.16667, '#2082a6'],  # Purple
        [0.33333, '#b1dff0'],  # Light purple
        [0.33334, '#01CBAE'],  # blue
        [0.50000, '#a4fff1'],  # light blue
        [0.50001, '#fff0c2'],  # light yellow
        [0.66666, '#ffc71c'],  # dark yellow
        [0.66667, '#fedac2'],  # light orange
        [0.83333, '#fb761f'],  # Dark orange
        [0.83334, '#f9b7ac'],  # light red
        [1, '#931d0a'],  # dark red
        ]
        range_color = [-9, 9]
        tickvals = [-9, -6, -3, 0, 3, 6, 9]
        cbartitle = 'Error (\u00b0C)'
    elif maptype == 'cf':
        colorscale = [
        [0, '#e35336'],  # dark red
        [0.89999, '#ffcccb'],  # light red
        [0.9, '#92ddc8'],  # light green
        [1, '#5aa17f'],  # dark green
        ]
        range_color = [0, 100]
        tickvals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        cbartitle = 'Central frequency (%)'
    elif maptype == 'pof':
        colorscale = [
        [0, '#5aa17f'],  # dark green
        [0.1, '#92ddc8'],  # light green
        [0.1111, '#ffcccb'],  # light red
        [1, '#e35336'],  # dark red
        ]
        range_color = [0, 10]
        tickvals = [0, 1, 5, 10]
        cbartitle = 'Positive outlier frequency (%)'
    elif maptype == 'nof':
        colorscale = [
        [0, '#5aa17f'],  # dark green
        [0.1, '#92ddc8'],  # light green
        [0.1111, '#ffcccb'],  # light red
        [1, '#e35336'],  # dark red
        ]
        range_color = [0, 10]
        tickvals = [0, 1, 5, 10]
        cbartitle = 'Negative outlier frequency (%)'
    elif maptype == 'rmse':
        colorscale = 'deep'
        range_color = [0, errorrange*2]
        tickvals = np.arange(range_color[0], range_color[1]+1)
        cbartitle = 'RMSE (\u00b0C)'
    elif maptype == 'mod' or maptype == 'obs':
        colorscale = 'deep'
        min_val = 10*(np.floor(np.nanmin(df[get_stat_name(maptype)])/10))
        max_val = 10*(np.ceil(np.nanmax(df[get_stat_name(maptype)])/10))
        range_color = [min_val, max_val]
        tickvals = np.arange(range_color[0], range_color[1]+2, 2)
        cbartitle = get_stat_name(maptype) + ' (\u00b0C)'
    else:
        logger.error('Incorrect map type in make_2d_skill_maps! Abort')
        sys.exit(-1)

    pio.renderers.default = 'browser'
    fig = px.scatter_mapbox(df.dropna(), lat='Y', lon='X',
                     color=get_stat_name(maptype),  # which column to use to set the color of markers
                     mapbox_style='carto-positron',
                     hover_data={'X': ':.2f',
                                 'Y': ':.2f',
                                 get_stat_name(maptype): ':.2f',
                                 'Date': False
                                 },
                     zoom=6,
                     title=plottitle,
                     color_continuous_scale=colorscale,
                     animation_group=get_stat_name(maptype),
                     animation_frame='Date',
                     height=700,
                     width=1000,
                     range_color=range_color
                     )
    sliders = [dict(font={'size': 20})]
    fig.update_layout(
        coloraxis=dict(
            colorbar=dict(
                tickvals=tickvals,
                tickfont=dict(size=16)
                )
            ),
        sliders=sliders,
        coloraxis_colorbar_title_text=cbartitle,
        title_x=0.5,
        title_y=0.9,
        title_font=dict(
            size=20,
            color='black'
            )
        )
    fig.update_traces(marker={'size': 8, 'opacity': 1})
    fig['layout'].pop('updatemenus')
    savepath = os.path.join(prop1.data_skill_2d_px_path, str(prop1.ofs +
                            '_' + cast + '_' + variable + '_' +
                            sat_source + '_' + maptype + '_'
                            + (prop1.start_date_full).split('-')[0] + '-' +
                            (prop1.end_date_full).split('-')[0] + '.html'))

    plotly.offline.plot(fig, filename=savepath, auto_open=False)
