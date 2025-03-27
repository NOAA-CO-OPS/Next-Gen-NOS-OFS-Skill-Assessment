# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 14:33:17 2024

@author: PL
"""
import sys
import os
import numpy as np
import pandas as pd
import plotly.express as px
import plotly
import plotly.io as pio
from datetime import datetime


def make_2d_skill_maps(z,lat,lon,time_all,maptype,prop1,logger):
    ''' This makes maps of 2D stats for local users, and saves them to file '''

    logger.info('Making plotly express maps of 2D stats for %s...',maptype)

    variable = 'temperature'
    errorrange = int(3)

    date_all = []
    for i in range(0,len(time_all)):
        date_all.append(datetime.strptime(time_all[i],'%Y%m%d-%Hz'))

    lat_flat = np.array(lat.flatten())
    lon_flat = np.array(lon.flatten())

    # Make a giant pandas dataframe
    if z.ndim == 3:
        for i in range(0,len(time_all)):
            z_flat = np.array(z[i,:,:].flatten())
            if i == 0:
                df = pd.DataFrame(
                    {
                        "X": lon_flat,
                        "Y": lat_flat,
                        "Z": z_flat
                    }
                )
                df['Date'] = date_all[i]
            else:
                df2 = pd.DataFrame(
                    {
                        "X": lon_flat,
                        "Y": lat_flat,
                        "Z": z_flat
                    }
                )
                df2['Date'] = date_all[i]
                df = pd.concat([df, df2], ignore_index=True)
    elif z.ndim == 2:
        z_flat = np.array(z.flatten())
        df = pd.DataFrame(
            {
                "X": lon_flat,
                "Y": lat_flat,
                "Z": z_flat
            }
        )
        df['Date'] = date_all[i]

    # Convert lat/lon to numeric
    df["X"] = pd.to_numeric(df["X"])
    df["Y"] = pd.to_numeric(df["Y"])

    # make title
    datestrend = (prop1.end_date_full).split('T')[0]
    datestrbeg = (prop1.start_date_full).split('T')[0]


    # Make custom colormap if doing diff/error plot
    if 'diff' in maptype:
        colorscale = [
        [0, '#524094'], # dark pink
        [0.16666, '#cec7e7'],  # Pink
        [0.16667, '#2082a6'], # Purple
        [0.33333, '#b1dff0'], # Light purple
        [0.33334, '#01CBAE'], #blue
        [0.50000, '#a4fff1'], #light blue
        [0.50001, '#fff0c2'], #light yellow
        [0.66666, '#ffc71c'], # dark yellow
        [0.66667, '#fedac2'], # light orange
        [0.83333, '#fb761f'], # Dark orange
        [0.83334, '#f9b7ac'],  # light red
        [1, '#931d0a'], # dark red
        ]
        range_color = [-9,9]
        tickvals = [-9,-6,-3,0,3,6,9]
        cbartitle = 'SST error (\u00b0C)'
        plottitle = prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
            'Sea Surface Temperature Error (\u00b0C)' +\
                ' ' + datestrbeg + ' - ' + datestrend
    elif maptype == 'cf':
        colorscale = [
        [0, '#e35336'], # dark red
        [0.89999, '#ffcccb'], # light red
        [0.9, '#92ddc8'],  # light green
        [1, '#5aa17f'], # dark green
        ]
        range_color = [0,100]
        tickvals = [0,10,20,30,40,50,60,70,80,90,100]
        cbartitle = 'Central frequency (%)'
        plottitle = prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
            'SST Central Frequency (%)' + ' ' + datestrbeg +\
                ' - ' + datestrend
    elif maptype == 'pof':
        colorscale = [
        [0, '#5aa17f'], # dark green
        [0.1, '#92ddc8'], # light green
        [0.1111, '#ffcccb'],  # light red
        [1, '#e35336'], # dark red
        ]
        range_color = [0,10]
        tickvals = [0,1,5,10]
        cbartitle = 'Positive outlier frequency (%)'
        plottitle = prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
            'SST Positive Outlier Frequency (%)' + ' ' + datestrbeg +\
                ' - ' + datestrend
    elif maptype == 'nof':
        colorscale = [
        [0, '#5aa17f'], # dark green
        [0.1, '#92ddc8'], # light green
        [0.1111, '#ffcccb'],  # light red
        [1, '#e35336'], # dark red
        ]
        range_color = [0,10]
        tickvals = [0,1,5,10]
        cbartitle = 'Negative outlier frequency (%)'
        plottitle = prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
            'SST Negative Outlier Frequency (%)' + ' ' + datestrbeg +' - '\
                + datestrend
    elif maptype == 'rmse':
        colorscale = 'Magma'
        range_color = [0,errorrange*2]
        tickvals = np.arange(range_color[0],range_color[1]+1)
        cbartitle = 'RMSE (\u00b0C)'
        plottitle = prop1.ofs.upper() + ' ' + prop1.whichcast + ' ' +\
            'SST RMSE (\u00b0C)' + ' ' + datestrbeg +' - ' + datestrend
    else:
        logger.error('Incorrect map type in make_2d_skill_maps! Abort')
        sys.exit(-1)

    pio.renderers.default = "browser"
    fig = px.scatter_mapbox(df.dropna(), lat = "Y", lon = "X",
                     color="Z", # which column to use to set the color of markers
                     #hover_name="ID", # column added to hover information
                     mapbox_style= "carto-positron",
                     hover_data={'X':':.2f',
                                 'Y':':.2f',
                                 'Z':':.2f',
                                 'Date': False
                                 },
                     #size="RMSE ", # size of markers
                     zoom = 6,
                     title = plottitle,
                     color_continuous_scale=colorscale,
                     animation_group="Z",
                     animation_frame="Date",
                     height = 700,
                     width = 1000,
                     range_color=range_color
                     )
    fig.update_layout(coloraxis=dict(
        colorbar=dict(
            tickvals=tickvals,
            tickfont=dict(size=16)
        )
    ))
    sliders = [
               dict(font={'size': 20}
    )]
    fig.update_layout(sliders=sliders,
                      coloraxis_colorbar_title_text = cbartitle,
                      title_x=0.5,
                      title_y=0.9,
                      title_font=dict(
                          #family="Arial",
                          size=20,
                          color="black"
                      ))
    fig.update_traces(marker={"size": 8, "opacity": 1})
    fig["layout"].pop("updatemenus")
    savepath = os.path.join(prop1.data_skill_2d_table_path, str(prop1.ofs +
                            '_' + prop1.whichcast + '_' + variable + '_' +
                            maptype + '_' + datestrbeg + '-' + datestrend +
                            '.html'))
    # prop1.data_skill_2d_table_path + '/' + prop1.ofs +'_'+ variable +\
    #     '_' + maptype + '_' + datestrbeg +\
    #         '-' + datestrend + '.html'
    plotly.offline.plot(fig, filename=savepath,auto_open=False)
