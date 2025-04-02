"""
-*- coding: utf-8 -*-

Documentation for Scripts create_1dplot.py

Script Name: create_1dplot.py

Technical Contact(s): Name:  FC

Abstract:

   This module is used to create all 1D plots.
   The main function (create_1dplot) controls the iterations over all variables
   and paired datasets (station+node).
   This script uses the ofs control file to list all the paired datasets
   if the paired dataset (or ofs control file) is not found, create_1dplot calls
   the respective modules (ofs and/or skill assessment modules) to create the
   missing file.

Language:  Python 3.8

Estimated Execution Time: < 10sec

Scripts/Programs Called:
 get_skill(start_date_full, end_date_full, datum, path, ofs, whichcast)
 --- This is called in case the paired dataset is not found

 get_node_ofs(start_date_full,end_date_full,path,ofs,whichcast,*args)
 --- This is called in case the ofs control file is not found

Usage: python create_1dplot.py

Arguments:
 -h, --help            show this help message and exit
 -o ofs, --ofs OFS     Choose from the list on the ofs_extents/ folder, you
                       can also create your own shapefile, add it top the
                       ofs_extents/ folder and call it here
 -p Path, --path PATH  Inventary File Path
 -s StartDate_full, --StartDate STARTDATE
                       Start Date
 -e EndDate_full, --EndDate ENDDATE
                       End Date
 -d StartDate_full, --datum",
                      'MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW', 'MLLW',
                      'NAVD', 'STND'",
 -ws whichcast, --Whichcast"
                       'Nowcast', 'Forecast_A', 'Forecast_B'
Output:
Name                 Description
scalar_plot          Standard html scalar timeseries plot of obs and ofs
vector_plot          Standard html vector timeseries plot of obs and ofs
wind_rose            Standard html polar wind rose plot of obs and ofs

Author Name:  FC       Creation Date:  09/20/2023

Revisions:
Date          Author     Description

Remarks:
"""

from datetime import datetime
import sys
import argparse
import logging
import logging.config
import os
import math
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import seaborn as sns
from plotly.validators.scatter.marker import SymbolValidator
import plotly.figure_factory as ff
from matplotlib.dates import date2num
import warnings

# Add parent directory to sys.path
from pathlib import Path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from skill_assessment import get_skill
from obs_retrieval import utils
from model_processing import model_properties
from model_processing import get_fcst_cycle
from model_processing import parse_ofs_ctlfile

warnings.filterwarnings('ignore')

def parameter_validation(prop, logger):
    """ Parameter validation """



def ofs_ctlfile_read(prop, name_var, logger):
    '''
    This reads the OFS control file for a given ofs and variable.
    If not found, it calls the OFS module to create the control file.
    '''
    logger.info(
        f'Trying to extract {prop.ofs} control file for {name_var} from {prop.control_files_path}'
    )

    filename = None
    if prop.ofsfiletype == 'fields':
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model.ctl"
    elif prop.ofsfiletype == 'stations':
        filename = f"{prop.control_files_path}/{prop.ofs}_{name_var}_model_station.ctl"
    else:
        logger.error("Invalid OFS file type.")
        return None

    if not os.path.isfile(filename):
        for i in prop.whichcasts:
            prop.whichcast = i.lower()
            logger.info(f'Running scripts for whichcast = {i}')

            if prop.start_date_full.find('T') == -1:
                prop.start_date_full = prop.start_date_full_before
                prop.end_date_full = prop.end_date_full_before

            get_skill.get_skill(prop, logger)

    # If file exists, use method A to parse it
    if os.path.isfile(filename):
        return parse_ofs_ctlfile.parse_ofs_ctlfile(filename)
    
    logger.info(f'Not able to extract/create {prop.ofs} control file for {name_var} from {prop.control_files_path}')
    return None



def get_title(prop, node, station_id):
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


    return f"<b>NOAA/NOS<br>OFS Skill Assessment<br>" \
           f"OFS:&nbsp;{prop.ofs.upper()}&nbsp;&nbsp;&nbsp;Node ID:&nbsp;" \
           f"{node}&nbsp;&nbsp;&nbsp;Station ID:&nbsp;{station_id}" \
           f"<br>From:&nbsp;{start_date}" \
           f"&nbsp;&nbsp;&nbsp;To:&nbsp;" \
           f"{end_date}<b>"


def get_markerstyles():
    '''
    Function gets list of marker symbols so they can be
    assigned in for-loop that iterates and makes the plots. This way works so
    that any number of time/data series can be added to the plots and they will
    all have different plot markers.
    '''
    # Get master list of symbols
    raw_symbols = SymbolValidator().values
    namestems = []
    # Get symbol names from master list of symbols and store names in 'namestems'
    for i in range(0,len(raw_symbols),3):
        name = raw_symbols[i+2]
        namestems.append(name.replace("-open", "").replace("-dot", ""))

    # Filter out non-unique namestem entries to create and return 'allmarkerstyles'
    allmarkerstyles = list(dict.fromkeys(namestems))
    return allmarkerstyles

def make_cubehelix_palette(
        ncolors, start_val, rot_val, light_val
        ):
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

def oned_scalar_plot(
        now_fores_paired, name_var, station_id, node,
        prop
):
    '''This function creates the standard scalar plots'''
    ''' Updated 5/28/24 for accessibility '''

    # Get marker styles so they can be assigned to different time series below
    #deltat = datetime.strptime(prop.end_date_full,"%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(prop.start_date_full,"%Y-%m-%dT%H:%M:%SZ")
    allmarkerstyles = get_markerstyles()

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired exceeds data_count,
    scale the marker sizes inversely proportional to the data count.
    Otherwise, use the default marker sizes.

    """
    modetype = 'lines+markers'
    marker_opacity = 1
    data_count = 48
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = 6*(data_count/len(list(now_fores_paired[0].DateTime)))
        marker_size_obs = 9**(data_count/len(list(now_fores_paired[0].DateTime)))
    else:
        marker_size = 6
        marker_size_obs = 9

    if name_var == 'wl':
        plot_name = 'Water Level ' + f'at {prop.datum} (<i>meters<i>)'
    elif name_var == 'temp':
        plot_name = 'Water Temperature (<i>degree C<i>)'
    elif name_var == 'salt':
        plot_name = 'Salinity (<i>PSU<i>)'
    elif name_var == 'ice_conc':
        plot_name = 'Sea Ice Concentration (%)'
        #prop.whichcasts = [prop.whichcast]

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2, 0.4, 0.65)
    # Observations are dashed, while model now/forecasts are solid (default)
    if name_var == 'ice_conc':
        obslinestyle = 'solid'
        obsname = 'GLSEA/NIC analysis'
        linewidth = 2.5
    else:
        obslinestyle = 'dash'
        obsname = 'Observations'
        linewidth = 1.5

    # Create figure
    fig = make_subplots(
        rows=1, cols=2, column_widths=[1 - len(prop.whichcasts) * 0.03,
                                       len(prop.whichcasts) * 0.05],
        shared_yaxes=True, horizontal_spacing=0.05
        # subplot_titles = ['Observed','OFS Model'],
    )

    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS), name=obsname,
            hovertemplate='%{y:.2f}', mode=modetype,
            line=dict(color=palette[0], width=linewidth, dash = obslinestyle),
            legendgroup="obs", marker=dict(
                symbol=allmarkerstyles[0], size=marker_size_obs, color=palette[0],
                opacity=marker_opacity, line=dict(width=0.5, color='black'))), 1, 1)

    ## Adding boxplots
    if name_var != 'ice_conc':
        fig.add_trace(
            go.Box(
                y=now_fores_paired[0]['OBS'], boxmean='sd',
                name=obsname, showlegend=False, legendgroup="obs",
                width=.7, line=dict(color=palette[0], width=1.5),
                # fillcolor = 'black',
                marker=dict(color=palette[0])), 1, 2)

    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname='Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname='Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname='Model Nowcast Guidance'
        else:
            seriesname=prop.whichcasts[i].capitalize() + " Guidance"

        fig.add_trace(
            go.Scatter(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS),
                name=seriesname,
                #Updated hover text to show Obs/Fore/Now values instead of bias as per Jiangtao's comments
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=linewidth),
                legendgroup=seriesname,
                #i+1 because observations already used first marker type
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(
                        width=1, color='black'))), 1,1)
        if name_var == 'ice_conc':
            stdevup = now_fores_paired[i].OFS + now_fores_paired[i].STDEV
            stdevup[stdevup > 100] = 100
            stdevdown = now_fores_paired[i].OFS - now_fores_paired[i].STDEV
            stdevdown[stdevdown < 0] = 0
            fig.add_trace(
                go.Scatter(
                    x=list(now_fores_paired[i].DateTime),
                    y=list(stdevup),
                    #Updated hover text to show Obs/Fore/Now values
                    #instead of bias as per user comments
                    name=seriesname.split(' ')[1] + ' +1 sigma',
                    hovertemplate='%{y:.2f}',
                    mode='lines', line=dict(
                        color=palette[i+1],
                        width=0),
                    showlegend=False
                    ), 1,1)
            fig.add_trace(
                go.Scatter(
                    x=list(now_fores_paired[i].DateTime),
                    y=list(stdevdown),
                    #Updated hover text to show Obs/Fore/Now values
                    #instead of bias as per user comments
                    name=seriesname.split(' ')[1] + ' -1 sigma',
                    hovertemplate='%{y:.2f}',
                    mode='lines', line=dict(
                        color=palette[i+1],
                        width=0),
                    fillcolor='rgba('+str(palette_rgb[i+1][0]*255)+\
                        ','+str(palette_rgb[i+1][1]*255)+','+\
                            str(palette_rgb[i+1][2]*255)+','+str(0.1)+')',
                    fill='tonexty',
                    showlegend=False
                    ), 1,1)

        if name_var != 'ice_conc':
            fig.add_trace(
                go.Box(
                    y=now_fores_paired[i]['OFS'], boxmean='sd',
                    name=seriesname,
                    showlegend=False,
                    legendgroup=seriesname,
                    width=.7,
                    line=dict(
                        color=palette[i+1],
                        width=1.5),
                    marker_color=palette[i+1]),
                1, 2)

        ## Figure Config
        if name_var == 'ice_conc':
            figheight = 500
            yoffset = -0.275
        else:
            figheight = 700
            yoffset = -0.425
        fig.update_layout(
            title=dict(
                text=get_title(prop, node, station_id),
                font=dict(size=14, color='black', family='Arial'),
                y=0.97,  # new
                x=0.5, xanchor='center', yanchor='top'),
            xaxis2=dict(tickangle=90),
            transition_ordering="traces first", dragmode="zoom",
            hovermode="x unified", height=figheight, width=900,
            template="plotly_white", margin=dict(
                t=120, b=100), legend=dict(
                orientation="h", yanchor="bottom",
                y=yoffset, xanchor="left", x=-0.05)
                    )

    # Added extra annotation so that user knows that the legend is interactive
    if name_var != 'ice_conc':
        fig.add_annotation(
            text="Click a time series in the legend to hide or show. Double-click to isolate one time series.",
            xref="paper", yref="paper",
            font=dict(size=12, color="grey"),
            x=0, y=-0.5,
            showarrow=False
            )

    # Add range slider
    if name_var != 'ice_conc':
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list(
                        [dict(
                            count=96, label="Forecast Cycle", step="hour",
                            stepmode="todate"), dict(
                            count=1, label="1 month", step="month",
                            stepmode="backward"), dict(
                            count=6, label="6 months", step="month",
                            stepmode="backward"), dict(
                            count=1, label="Year-to-date", step="year",
                            stepmode="todate"), dict(
                            count=1, label="1 year", step="year",
                            stepmode="backward"), dict(step="all")])),
                rangeslider=dict(
                    visible=True), type="date"))


    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Arial",
        tickfont_color="black")
    ##Set y-axes titles
    fig.update_yaxes(
        title_text=f"{plot_name}", titlefont_family="Arial",
        title_font_color="black", tickfont_family="Arial",
        tickfont_color="black", row=1, col=1)
    if name_var == 'ice_conc':
        fig.update_yaxes(
            range=[0,100], row=1, col=1)

    ##naming whichcasts
    naming_ws = "_".join(prop.whichcasts)
    if name_var != 'ice_conc':
        fig.write_html(
            r"" + prop.visuals_1d_station_path + f"/{prop.ofs}_{station_id}_{node}_{name_var}_scalar2_{naming_ws}.html")
    elif name_var == 'ice_conc':
        fig.write_html(
            r"" + prop.visuals_1d_ice_path + f"/{prop.ofs}_{station_id}_{node}_{name_var}_scalar2_{naming_ws}.html")
    else:
        print("Unrecognized name_var! "
                     "No 1D time series plot made.")

def oned_vector_plot1(
        now_fores_paired, name_var, station_id, node,
        prop
):
    '''This function creates the standard vector time series plots'''
    # Choose color & style for observation lines and marker fill.
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2, 0.4, 0.65)
    linestyles = ['solid', 'dot', 'longdash', 'dashdot', 'longdashdot']

    # Create figure
    fig = make_subplots(
        rows=1, cols=2, column_widths=[1 - len(prop.whichcasts) * 0.03,
                                       len(prop.whichcasts) * 0.05],
        shared_yaxes=True, horizontal_spacing=0.05
    )

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired exceeds data_count,
    scale the marker sizes inversely proportional to the data count.
    Otherwise, use the default marker sizes.

    """
    data_count = 48
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = 14*(data_count/len(list(now_fores_paired[0].DateTime)))
    else:
        marker_size = 14


    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS_SPD), name="Observations",
            hovertext=list(now_fores_paired[0].OBS_DIR),
            hovertemplate='<br>Speed (m/s): %{y:.2f}<br>' + 'Direction: %{hovertext:.2f}',
            line=dict(color=palette[0], width=1, dash='dash'),
            mode='lines+markers', legendgroup="obs", marker=dict(
                symbol="arrow", size=marker_size, color=palette[0],
                angle=list(now_fores_paired[0].OBS_DIR), opacity=1,
                angleref='up', line=dict(width=1, color='black')), ),
        1, 1)

    ## Adding boxplots
    fig.add_trace(
        go.Box(
            y=now_fores_paired[0]['OBS_SPD'], boxmean='sd',
            name='Observations', showlegend=False, legendgroup="obs",
            width=0.7, line=dict(color=palette[0], width=1.5),
            marker=dict(color=palette[0])), 1, 2)

    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname='Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname='Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname='Model Nowcast Guidance'
        else:
            seriesname=prop.whichcasts[i].capitalize() + " Guidance"  # + f"{i}",

        fig.add_trace(
            go.Scatter(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS_SPD),
                name=seriesname,
                #Updated hover text to show Obs/Fore/Now values instead of bias as per user comments
                hovertext=list(now_fores_paired[0].OFS_DIR),
                hovertemplate='<br>Speed (m/s): %{y:.2f}<br>' + 'Direction: %{hovertext:.2f}',
                line=dict(
                    color=palette[i+1],
                    width=1, dash=linestyles[i]), mode='lines+markers',
                legendgroup=seriesname,
                marker=dict(
                    symbol="arrow", size=marker_size,
                    color=palette[i+1],
                    angle=list(now_fores_paired[i].OFS_DIR), opacity=1,
                    # 0.6,
                    line=dict(width=1, color='black')), ), 1, 1)

        fig.add_trace(
            go.Box(
                y=now_fores_paired[i]['OFS_SPD'], boxmean='sd',
                name=seriesname,
                showlegend=False,
                legendgroup=seriesname,
                width=.7,
                line=dict(
                    color=palette[i+1],
                    width=1.5),
                marker_color=palette[i+1]),
            1, 2)

    ## Figure Config
    fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id),
            font=dict(size=14, color='black', family='Arial'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top'),
        transition_ordering="traces first", dragmode="zoom",
        hovermode="x unified", height=700, width=900,
        template="plotly_white", margin=dict(
            t=120, b=100), legend=dict(
            orientation="h", yanchor="bottom",
            y=-0.425, xanchor="left", x=-0.05))

    # Added extra annotation so that user knows that the legend is interactive
    fig.add_annotation(
        text="Click a time series in the legend to hide or show. Double-click to isolate one time series.",
        xref="paper", yref="paper",
        font=dict(size=12, color="grey"),
        x=0, y=-0.5,
        showarrow=False
        )

    # Add range slider
    fig.update_layout(  # paper_bgcolor="LightSteelBlue",
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [dict(
                        count=96, label="Forecast Cycle", step="hour",
                        stepmode="todate"), dict(
                        count=1, label="1 month", step="month",
                        stepmode="backward"), dict(
                        count=6, label="6 months", step="month",
                        stepmode="backward"), dict(
                        count=1, label="year-to-date", step="year",
                        stepmode="todate"), dict(
                        count=1, label="1 year", step="year",
                        stepmode="backward"), dict(step="all")])),
            rangeslider=dict(
                visible=True), type="date"))

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Arial",
        tickfont_color="black")
    ##Set y-axes titles
    fig.update_yaxes(
        title_text="Currents (<i>meters/second<i>)",
        titlefont_family="Arial", title_font_color="black",
        tickfont_family="Arial", tickfont_color="black", row=1, col=1)

    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id}_{node}_{name_var}_vector1_{naming_ws}.html")


def oned_vector_plot2a(
        now_fores_paired, logger
):
    '''This function creates the standard wind rose plots'''
    logger.info('oned_vector_plot2a - Start')
    # creating bins based on the data
    bins_mag = (np.linspace(
        0, math.ceil(
            now_fores_paired[0][
                ['OBS_SPD', 'OFS_SPD']].max().max() / 0.25) * 0.25, int(
            (math.ceil(
                now_fores_paired[0][['OBS_SPD',
                                     'OFS_SPD']].max().max() / 0.25)) + 1))).tolist()

    bins = [[0, 11.25, 33.75, 56.25, 78.75, 101.25, 123.75, 146.25, 168.75, 191.25,
             213.75, 236.25, 258.75, 281.25, 303.75, 326.25, 348.75, 360.00],
            ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW',
             'WSW', 'W', 'WNW', 'NW', 'NNW', 'North']]

    now_fores_paired[0]['OBS_mag_binned'] = pd.cut(
        now_fores_paired[0]['OBS_SPD'], bins_mag,
        labels=[f"{bins_mag[i]:.2f} - {bins_mag[i + 1]:.2f}" for i in
                range(len(bins_mag) - 1)])
    now_fores_paired[0]['OBS_dir_binned'] = pd.cut(
        now_fores_paired[0]['OBS_DIR'], bins[0], labels=bins[1])

    df_obs = now_fores_paired[0][
        ['OBS_mag_binned', 'OBS_dir_binned', 'Julian']].copy()

    df_obs = df_obs.replace('North', 'N')

    df_obs.rename(
        columns={'Julian': 'freq'}, inplace=True)  # changing to freq.
    df_obs = df_obs.groupby(['OBS_mag_binned', 'OBS_dir_binned'])
    df_obs = df_obs.count()
    df_obs.reset_index(inplace=True)
    df_obs['percentage'] = df_obs['freq'] / df_obs['freq'].sum()
    df_obs['percentage%'] = df_obs['percentage'] * 100
    df_obs[' Currents (m/s)'] = df_obs['OBS_mag_binned']

    df_ofs_all = []
    max_r = []
    for i in range(len(now_fores_paired)):
        now_fores_paired[i]['OFS_mag_binned'] = pd.cut(
            now_fores_paired[i]['OFS_SPD'], bins_mag,
            labels=[f"{bins_mag[i]:.2f} - {bins_mag[i + 1]:.2f}" for i in
                    range(len(bins_mag) - 1)])
        now_fores_paired[i]['OFS_dir_binned'] = pd.cut(
            now_fores_paired[i]['OFS_DIR'], bins[0], labels=bins[1])

        df_ofs = now_fores_paired[i][
            ['OFS_mag_binned', 'OFS_dir_binned', 'Julian']].copy()
        df_ofs = df_ofs.replace('North', 'N')
        df_ofs.rename(
            columns={'Julian': 'freq'}, inplace=True)  # changing to freq.
        df_ofs = df_ofs.groupby(['OFS_mag_binned', 'OFS_dir_binned'])
        df_ofs = df_ofs.count()
        df_ofs.reset_index(inplace=True)
        df_ofs['percentage'] = df_ofs['freq'] / df_ofs['freq'].sum()
        df_ofs['percentage%'] = df_ofs['percentage'] * 100
        df_ofs[' Currents (m/s)'] = df_ofs['OFS_mag_binned']

        maxi_r = math.ceil(
            max(
                [df_obs.groupby(['OBS_dir_binned'], as_index=False)[
                     'percentage%'].sum().max()['percentage%'],
                 df_ofs.groupby(['OFS_dir_binned'], as_index=False)[
                     'percentage%'].sum().max()[
                     'percentage%']]) / 5) * 5

        max_r.append(maxi_r)
        df_ofs_all.append(df_ofs)

    max_r = max(max_r)

    # Creating subplots
    ### defining the # and disposition of subplots
    if len(df_ofs_all) <= 1:
        totalrows = 1
    elif len(df_ofs_all) >= 2 and len(df_ofs_all) <= 5:
        totalrows = 2
    else:
        totalrows = 3

    totalcol = math.ceil((len(df_ofs_all) + 1) / totalrows)
    f_f = 0
    index = []
    for c_c in range(totalrows):
        for i in range(totalcol):
            f_f += 1
            if f_f <= len(df_ofs_all) + 1:
                index.append([c_c + 1, i + 1])

    index = index[1:]

    # fig_info = [totalrows, totalcol, bins_mag, bins, index, max_r]
    fig_info_data = [[totalrows, totalcol, bins_mag, bins, index, max_r],
                     [df_obs, df_ofs_all]]
    logger.info('oned_vector_plot2a - End')
    return fig_info_data  # fig_info,fig_data


def oned_vector_plot2b(
        fig_info_data, name_var, station_id, node,
        prop, logger
):
    '''This function creates the standard wind rose plots'''

    # Define cubehelix color palette
    ncolors = len(fig_info_data[0][2]) - 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 0.5, -4, 0.85)

    subplot_titles_str = ['Observations']
    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            subplot_titles_str.append('Model Forecast Guidance')
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            subplot_titles_str.append('Model Nowcast Guidance')
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            subplot_titles_str.append('Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle')
        else:
            subplot_titles_str.append(prop.whichcasts[i].capitalize() + " Guidance")

    fig = make_subplots(
        rows=fig_info_data[0][0], cols=fig_info_data[0][1],
        specs=[[{'type': 'polar'}] * fig_info_data[0][1]] *
              fig_info_data[0][0],
        #subplot_titles=['Observations'] + [f'OFS {i.capitalize() + " Guidance"}' for i in prop.whichcasts],
        subplot_titles=subplot_titles_str,
        horizontal_spacing=0.01, vertical_spacing=0.05)


    ## These 2 for loops create the figures
    for i in range(len(fig_info_data[0][2]) - 1):
        fig.add_trace(
            go.Barpolar(
                r=fig_info_data[1][0].loc[fig_info_data[1][0][
                                              ' Currents (m/s)'] == f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}"][
                    'percentage%'],
                name=f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}",
                theta=fig_info_data[0][3][1],
                text=fig_info_data[0][3][0], hovertext=list(
                    fig_info_data[1][0].loc[fig_info_data[1][0][
                                                ' Currents (m/s)'] == f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}"][
                        'percentage%']),
                hovertemplate='<br>Percentage: %{hovertext:.2f}<br>' + 'Direction: %{text:.2f}',
                legendgroup=f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}",
                marker_color=palette[i]),
            1, 1)

    for s_s in range(len(fig_info_data[1][1])):
        for i in range(len(fig_info_data[0][2]) - 1):
            fig.add_trace(
                go.Barpolar(
                    r=fig_info_data[1][1][s_s].loc[
                        fig_info_data[1][1][s_s][
                            ' Currents (m/s)'] == f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}"][
                        'percentage%'],
                    name=f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}",
                    theta=fig_info_data[0][3][1],
                    text=fig_info_data[0][3][0], hovertext=list(
                        fig_info_data[1][1][s_s].loc[
                            fig_info_data[1][1][s_s][
                                ' Currents (m/s)'] == f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}"][
                            'percentage%']),
                    hovertemplate='<br>Percentage: %{hovertext:.2f}<br>' + 'Direction: %{text:.2f}',
                    legendgroup=f"{fig_info_data[0][2][i]:.2f} - {fig_info_data[0][2][i + 1]:.2f}",
                    showlegend=False,
                    marker_color=palette[i]),
                fig_info_data[0][4][s_s][0], fig_info_data[0][4][s_s][1])

    ## Updating the figure/plots to the format we want

    fig.update_traces(text=[f"{i:.1f}" for i in np.arange(0, 360, 22.5)])

    fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id),
            font=dict(size=14, color='black', family='Arial'),
            y=0.975,  # new
            x=0.5, xanchor='center', yanchor='top'),
        # This determines the height of the plot based on # of rows
        height=(fig_info_data[0][0] * 0.9) * 600,
        width=fig_info_data[0][1] * 500, template="seaborn",
        margin=dict(l=20, r=20, t=120, b=0), legend=dict(
            font=dict(size=14, color='black', family='Arial'),
            orientation="h",
            yanchor='bottom',
            y=-0.3 / fig_info_data[0][0], xanchor='center', x=0.5,
            bgcolor='rgba(0,0,0,0)', ))

    # Added extra annotation to explain colors and units
    fig.add_annotation(
        text="Current speed, meters per second",
        xref="paper", yref="paper",
        font=dict(size=12, color="black"),
        x=0.31, y=-0.08,
        showarrow=False
        )

    polars = []
    for p_p in range(1, len(fig_info_data[1][1]) + 2):
        # This is the template for the wind rose plot, one change here will change all
        polar = dict(
            {
                f"polar{p_p}": {
                    "radialaxis": {
                        "angle": 0, "range": [0, fig_info_data[0][5]],
                        "showline": False, "tickfont": {
                            "family": 'Arial', "color": "black", "size": 14
                        }, "linecolor": 'black', "gridcolor": "black",
                        "griddash": "dot", "ticksuffix": '%', "tickangle": 0,
                        "tickwidth": 5, "ticks": "", "showtickprefix": "last",
                        "showticklabels": True
                    }, "angularaxis": {
                        "rotation": 90, "direction": 'clockwise',
                        "gridcolor": "gray", "griddash": "dot", "color": "blue",
                        "linecolor": "gray", "tickfont": {
                            "family": 'Arial', "color": "black", "size": 16
                        }, "ticks": ""
                    }
                }
            })
        polars.append(polar)

    for p_p in polars:
        fig.update_layout(p_p)

    ## This is updating the subplot title
    for i in fig['layout']['annotations']:
        i['font'] = dict(size=16, color='black', family='Arial')
        i['yanchor'] = "bottom"
        i['xanchor'] = "left"
        i['x'] = i['x'] - 0.26 * (2 / fig_info_data[0][1])  # -0.26

    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id}_{node}_{name_var}_vector2_{naming_ws}.html")
    logger.info('oned_vector_plot2b - End')

def oned_vector_plot3(
        now_fores_paired, name_var, station_id, node,
        prop
):
    '''This function creates the standard vector time series plots'''
    # Choose color & style for observation lines and marker fill.
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2, 0.4, 0.65)
    linestyles = ['solid', 'dot', 'longdash', 'dashdot', 'longdashdot']

    # Convert wind directions to radians and calculate u and v component
    cur_dir_rad = np.deg2rad([270 - x for x in now_fores_paired[0].OBS_DIR])
    u=now_fores_paired[0].OBS_SPD * np.cos(cur_dir_rad)*-1
    v=now_fores_paired[0].OBS_SPD * np.sin(cur_dir_rad)*-1
    obs_magnitude=[x for x in now_fores_paired[0].OBS_SPD]

    dimN = np.asarray(u).shape[0]
    reshape_u=np.asarray(u).reshape((dimN,1))
    reshape_v=np.asarray(v).reshape((dimN,1))

    y=reshape_u*0
    date_time_array=np.array(list(now_fores_paired[0].DateTime)).reshape((dimN,1))

    # find out the maximum current speed value in observation as reference
    maxSpd=np.amax(now_fores_paired[0].OBS_SPD)
    overlappingRate=0.75 # (0 1], 1 means no overlapping, smaller value means more overlapping
    dxLength=maxSpd*overlappingRate
    x = np.array([i*dxLength for i in range(dimN)]).reshape((dimN,1))
    x_time = np.array([a for a in date_time_array[:,0]])

    # Create figure object
    fig = go.Figure()

    # to make sure the arrows' directions are correctly shown, scale and scaleratio have to be 1
    scale_value=1
    arrow_scale_value=0.3
    scaleratio_value=1
    angle_value=0.2

    # base figure (including first trace)
    quiver_obs = ff.create_quiver(x, y, u, v,
                                  scale=scale_value,
                                  arrow_scale=arrow_scale_value,
                                  scaleratio=scaleratio_value,
                                  angle=angle_value,
                                  line_color=palette[0])

    # Add quiver plots to figure object with specified legend names
    for trace in quiver_obs.data:
        trace.name = 'Observations'  # Set name for blue quiver plot
        trace.showlegend = True
        trace.hoverinfo='skip'
        fig.add_trace(trace)

    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname='Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname='Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname='Model Nowcast Guidance'
        else:
            seriesname=prop.whichcasts[i].capitalize() + " Guidance"  # + f"{i}",

        # Convert wind directions to radians and calculate u and v component
        cur_dir_rad = np.deg2rad([270 - x for x in now_fores_paired[i].OFS_DIR])
        u=now_fores_paired[i].OFS_SPD * np.cos(cur_dir_rad)*-1
        v=now_fores_paired[i].OFS_SPD * np.sin(cur_dir_rad)*-1

        new_date_time_array=np.array(list(now_fores_paired[i].DateTime))
        new_x = np.array([date2num(a) for a in new_date_time_array])

        # Adjust for differing forecast and observation data counts
        if len(new_x) == len(x) - 1:
            new_u = x * 0
            new_v = x * 0
            # If forecast data count is 1 less than observation data,
            # match counts by setting first forecast value to 0
            new_u[1:,0]=np.array(u)[:]
            new_v[1:,0]=np.array(v)[:]
        elif len(new_x) == len(x):
            new_u = u
            new_v = v

        quiver_ofs = ff.create_quiver(x, y, new_u, new_v,
                                      scale=scale_value,
                                      arrow_scale=arrow_scale_value,
                                      scaleratio=scaleratio_value,
                                      angle=angle_value,
                                      line_color=palette[i+1])
        for trace in quiver_ofs.data:
            trace.name = seriesname  # Set name for red quiver plot
            trace.showlegend = True
            trace.hoverinfo='skip'
            fig.add_trace(trace)

    thex=x[:,0]
    they=y[:,0]
    # Add scatter plot for hover info
    hover_texts = []
    for i in range(len(x)):
        hover_text = (f"Time: {x_time[i]}<br>" +
                      f"Observations:<br>" +
                      f"SPD: {now_fores_paired[0].OBS_SPD[i]:.2f} m/s<br>" +
                      f"DIR: {now_fores_paired[0].OBS_DIR[i]:.2f}°")
        for j in range(len(prop.whichcasts)):
            if prop.whichcasts[j][0].capitalize() == 'F':
                if len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD):
                    hover_text += (f"<br>Model Forecast Guidance: <br>" +
                                   f"SPD: {now_fores_paired[j].OFS_SPD[i]:.2f} m/s<br>" +
                                   f"DIR: {now_fores_paired[j].OFS_DIR[i]:.2f}°")
                elif len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD) - 1 and i > 0:
                    hover_text += (f"<br>Model Forecast Guidance: <br>" +
                                   f"SPD: {now_fores_paired[j].OFS_SPD[i - 1]:.2f} m/s<br>" +
                                   f"DIR: {now_fores_paired[j].OFS_DIR[i - 1]:.2f}°")
            elif prop.whichcasts[j].capitalize() == 'Nowcast':
                if len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD):
                    hover_text += (f"<br>Model Nowcast Guidance: <br>" +
                                   f"SPD: {now_fores_paired[j].OFS_SPD[i]:.2f} m/s<br>" +
                                   f"DIR: {now_fores_paired[j].OFS_DIR[i]:.2f}°")
                elif len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD) - 1 and i > 0:
                    hover_text += (f"<br>Model Nowcast Guidance: <br>" +
                                   f"SPD: {now_fores_paired[j].OFS_SPD[i - 1]:.2f} m/s<br>" +
                                   f"DIR: {now_fores_paired[j].OFS_DIR[i - 1]:.2f}°")
        hover_texts.append(hover_text)

    # create an empty scatter plot for plotting hover
    scatter_hover = go.Scatter(
        x=thex,
        y=they,
        mode='markers',
        marker=dict(size=10, color='rgba(0,0,0,0)'),
        hovertext=hover_texts,
        hoverinfo='text',
        showlegend=False
    )

    fig.add_trace(scatter_hover)

    # Update x-axis to show time format with grid lines every interval and labels every 3 intervals
    step = 3
    filtered_ticktext = [a.strftime('%H:%M %b %d, %Y') for a in x_time[::step]]  # Select corresponding labels
    fig.update_xaxes(
        tickformat='%H:%M %b %d, %Y',
        tickvals=thex,  # Keep grid lines at every tick
        ticktext=["" if i % step != 0 else filtered_ticktext[i // step] for i in range(len(thex))]  # Show labels every 3 intervals
    )

    # Added extra annotation so that user knows that the legend is interactive
    fig.add_annotation(
        text="Current Vectors",
        xref="paper", yref="paper",
        font=dict(size=15, color="black"),
        x=0.05, y=1.05,
        showarrow=False
        )

    # Update layout
    fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id),
            font=dict(size=14, color='black', family='Arial'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top'),
        transition_ordering="traces first", dragmode="zoom",
        xaxis_title='Time',
        height=700, width=820,
        template="plotly_white",
        margin=dict(t=120, b=100),
        yaxis=dict(showticklabels=True,
                   ticks="",
                   range=[-0.5*(thex[-1]-thex[0]),0.5*(thex[-1]-thex[0])]),
        hovermode="x unified",
        yaxis_title="V Component of Current Vectors (<i>meters/second<i>)",
        legend=dict(
            orientation='v',
            yanchor="bottom",
            y=0.8,
            xanchor="left",
            x=1,
            tracegroupgap=0
        )
    )

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Arial",
        tickfont_color="black")

    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id}_{node}_{name_var}_vector3_{naming_ws}.html")

def oned_vector_diff_plot3(
        now_fores_paired, name_var, station_id, node,
        prop
):
    '''This function creates the standard vector time series plots'''
    # Choose color & style for observation lines and marker fill.
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2, 0.4, 0.65)

    # Convert wind directions to radians and calculate u and v component
    cur_dir_rad = np.deg2rad([270 - x for x in now_fores_paired[0].OBS_DIR])
    obs_u=now_fores_paired[0].OBS_SPD * np.cos(cur_dir_rad)*-1
    obs_v=now_fores_paired[0].OBS_SPD * np.sin(cur_dir_rad)*-1
    obs_magnitude=[x for x in now_fores_paired[0].OBS_SPD]

    dimN = np.asarray(obs_u).shape[0]
    reshape_u=np.asarray(obs_u).reshape((dimN,1))
    y=reshape_u*0

    date_time_array=np.array(list(now_fores_paired[0].DateTime)).reshape((dimN,1))

    # Create figure object
    fig = go.Figure()

    # to make sure the arrows' directions are correctly shown, scale and scaleratio have to be 1
    scale_value=1
    arrow_scale_value=0.3
    scaleratio_value=1
    angle_value=0.2

    hover_u=[]
    hover_v=[]
    hover_magnitudes=[]

    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][0].capitalize() == 'F':
            seriesname='Forecast - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname='Nowcast - Obs.'
        else:
            seriesname=prop.whichcasts[i].capitalize() + " Difference Guidance"  # + f"{i}",

        # Convert wind directions to radians and calculate u and v component
        cur_dir_rad = np.deg2rad([270 - x for x in now_fores_paired[i].OFS_DIR])
        ofs_u=now_fores_paired[i].OFS_SPD * np.cos(cur_dir_rad)*-1
        ofs_v=now_fores_paired[i].OFS_SPD * np.sin(cur_dir_rad)*-1

        new_date_time_array=np.array(list(now_fores_paired[i].DateTime))
        new_x = np.array([date2num(a) for a in new_date_time_array])

        # Adjust for differing forecast and observation data counts
        if len(new_x) == len(y) - 1:
            new_u = [0 for i in y]
            new_v = [0 for i in y]
            # If forecast data count is 1 less than observation data,
            # match counts by setting first forecast value to 0
            new_u[1:]=np.array(ofs_u)[:]
            new_v[1:]=np.array(ofs_v)[:]
            new_u[0]=obs_u[0]
            new_v[0]=obs_v[0]
        elif len(new_x) == len(y):
            new_u = ofs_u
            new_v = ofs_v

        u = np.array([a - b for a, b in zip(new_u, obs_u)])
        v = np.array([a - b for a, b in zip(new_v, obs_v)])
        diff_magnitudes = np.array([(a**2 + b**2)**0.5 for a, b in zip(u, v)])
        hover_u.append(u.tolist())
        hover_v.append(v.tolist())
        hover_magnitudes.append(diff_magnitudes.tolist())

        if i == 0:
            # find out the maximum current speed value in observation as reference
            maxSpd=np.amax(diff_magnitudes)
            overlappingRate=0.75 # (0 1], 1 means no overlapping, smaller value means more overlapping
            dxLength=maxSpd*overlappingRate
            x = np.array([i*dxLength for i in range(dimN)]).reshape((dimN,1))
            x_time = np.array([a for a in date_time_array[:,0]])

        quiver_ofs = ff.create_quiver(x, y, u, v,
                                      scale=scale_value,
                                      arrow_scale=arrow_scale_value,
                                      scaleratio=scaleratio_value,
                                      angle=angle_value,
                                      line_color=palette[i+1])
        for trace in quiver_ofs.data:
            trace.name = seriesname  # Set name for red quiver plot
            trace.showlegend = True
            trace.hoverinfo='skip'
            fig.add_trace(trace)

    thex=x[:,0]
    they=y[:,0]
    # Add scatter plot for hover info
    hover_texts = []
    for i in range(len(x)):
        hover_text = (f"Time: {x_time[i]}")
        for j in range(len(prop.whichcasts)):
            if prop.whichcasts[j][0].capitalize() == 'F':
                if len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD):
                    hover_text += (f"<br>Forecast minus Observation: <br>" +
                                   f"U-Component Vector Difference: {hover_u[j][i]:.2f} m/s<br>" +
                                   f"V-Component Vector Difference: {hover_v[j][i]:.2f} m/s")
                elif len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD) - 1 and i > 0:
                    hover_text += (f"<br>Forecast minus Observation: <br>" +
                                   f"U-Component Vector Difference: {hover_u[j][i]:.2f} m/s<br>" +
                                   f"V-Component Vector Difference: {hover_v[j][i]:.2f} m/s")
            elif prop.whichcasts[j].capitalize() == 'Nowcast':
                if len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD):
                    hover_text += (f"<br>Nowcast minus Observation: <br>" +
                                   f"U-Component Vector Difference: {hover_u[j][i]:.2f} m/s<br>" +
                                   f"V-Component Vector Difference: {hover_v[j][i]:.2f} m/s")
                elif len(now_fores_paired[j].OFS_SPD) == len(now_fores_paired[0].OBS_SPD) - 1 and i > 0:
                    hover_text += (f"<br>Nowcast minus Observation: <br>" +
                                   f"U-Component Vector Difference: {hover_u[j][i]:.2f} m/s<br>" +
                                   f"V-Component Vector Difference: {hover_v[j][i]:.2f} m/s")
        hover_texts.append(hover_text)

    # create an empty scatter plot for plotting hover
    scatter_hover = go.Scatter(
        x=thex,
        y=they,
        mode='markers',
        marker=dict(size=10, color='rgba(0,0,0,0)'),
        hovertext=hover_texts,
        hoverinfo='text',
        showlegend=False
    )

    fig.add_trace(scatter_hover)

    # Update x-axis to show time format with grid lines every interval and labels every 3 intervals
    step = 3
    filtered_ticktext = [a.strftime('%H:%M %b %d, %Y') for a in x_time[::step]]  # Select corresponding labels
    fig.update_xaxes(
        tickformat='%H:%M %b %d, %Y',
        tickvals=thex,  # Keep grid lines at every tick
        ticktext=["" if i % step != 0 else filtered_ticktext[i // step] for i in range(len(thex))]  # Show labels every 3 intervals
    )

    # Added extra annotation so that user knows that the legend is interactive
    fig.add_annotation(
        text="Current Vector Differences",
        xref="paper", yref="paper",
        font=dict(size=15, color="black"),
        x=0.05, y=1.05,
        showarrow=False
        )

    # Update layout
    fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id),
            font=dict(size=14, color='black', family='Arial'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top'),
        transition_ordering="traces first", dragmode="zoom",
        xaxis_title='Time',
        height=700, width=760,
        template="plotly_white",
        margin=dict(t=120, b=100),
        yaxis=dict(showticklabels=True,
                   ticks="",
                   range=[-0.5*(thex[-1]-thex[0]),0.5*(thex[-1]-thex[0])]),
        hovermode="x unified",
        yaxis_title="V Component of Current Vector Differences (<i>meters/second<i>)",
        legend=dict(
            orientation='v',
            yanchor="bottom",
            y=0.8,
            xanchor="left",
            x=1,
            tracegroupgap=0
        )
    )

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Arial",
        tickfont_color="black")

    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id}_{node}_{name_var}_difference_vector3_{naming_ws}.html")

def create_1dplot_2nd_part(
        read_ofs_ctl_file, prop, var_info, logger):
    '''
    This is the function that actually create the plots
    it had to be split from the original function due to size (PEP8)
    '''
    logger.info(
        f'Searching for paired dataset for {prop.ofs}, variable {var_info[0]}')

    for i in range(len(read_ofs_ctl_file[1])):
        now_fores_paired = []
        for cast in prop.whichcasts:
            paired_data = None
            # Here we try to open the paired data set, if not found, create.
            prop.whichcast = cast.lower()
            if (os.path.isfile(
                    f"{prop.data_skill_1d_pair_path}/{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int") is False):
                logger.error(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) not found in %s. ",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast,
                    prop.visuals_1d_station_path)
                logger.info(
                    "Calling Skill Assessment module for whichcast %s. ",
                    prop.whichcast
                )

                if prop.ofsfiletype == 'fields' or read_ofs_ctl_file[1][i] >= 0:
                    get_skill.get_skill(prop, logger)


            if (os.path.isfile(
                    f"{prop.data_skill_1d_pair_path}/{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int") is False):
                logger.error(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) not found in %s. ",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast,
                    prop.visuals_1d_station_path)

            else:
                paired_data = pd.read_csv(
                    r"" + f"{prop.data_skill_1d_pair_path}/{prop.ofs}_{var_info[1]}_{read_ofs_ctl_file[-1][i]}_{read_ofs_ctl_file[1][i]}_{prop.whichcast}_pair.int",
                    delim_whitespace=True, names=var_info[2],
                    header=0) #change to skip header for human readability
                #print(read_ofs_ctl_file[-1][i])
                paired_data['DateTime'] = pd.to_datetime(
                    paired_data[['year', 'month', 'day', 'hour', 'minute']])
                logger.info(
                    "Paired dataset (%s_%s_%s_%s_%s_pair.int) found in %s",
                    prop.ofs, var_info[1], read_ofs_ctl_file[-1][i],
                    read_ofs_ctl_file[1][i], prop.whichcast, prop.visuals_1d_station_path)
            if paired_data is not None:
                # NEW! subsample time series if using 6-minute resolution from stations files
                deltat = (paired_data['DateTime'].iloc[-1] - paired_data['DateTime'].iloc[0]).days
                if (prop.ofsfiletype == 'stations'
                    and (deltat > 2 or var_info[1] == 'cu')):
                    paired_data = paired_data.loc[paired_data.groupby(["year","month","day","hour"])
                                                  ["minute"].idxmin()]
                now_fores_paired.append(paired_data)

        if len(now_fores_paired) > 0:
            try:
                if var_info[1] == 'wl' or var_info[1] == 'temp' or var_info[
                    1] == 'salt':
                    logger.info(
                        "Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    oned_scalar_plot(
                        now_fores_paired, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop)
                    oned_scalar_diff_plot(
                        now_fores_paired, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop)
                elif var_info[1] == 'cu':
                    logger.info(
                        "Trying to build timeseries %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    oned_vector_plot1(
                        now_fores_paired, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop)
                    logger.info(
                        "Trying to build wind rose %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    oned_vector_plot2b(
                        oned_vector_plot2a(now_fores_paired, logger),
                        var_info[1], read_ofs_ctl_file[4][i],
                        read_ofs_ctl_file[1][i], prop, logger)
                    logger.info(
                        "Trying to build stick %s plot for paired dataset: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    oned_vector_plot3(
                        now_fores_paired, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop)
                    logger.info(
                        "Trying to build stick %s plot for vector difference: \
    %s_%s_%s_%s_%s_pair.int", var_info[0], prop.ofs, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i], prop.whichcast)
                    oned_vector_diff_plot3(
                        now_fores_paired, var_info[1],
                        read_ofs_ctl_file[-1][i], read_ofs_ctl_file[1][i],
                        prop)
            except Exception as ex:
                logger.info(
                    "Fail to create the plot  \
    ---  %s ...Continuing to next plot", ex)


def create_1dplot(prop, logger):
    '''
    This is the main function for plotting 1d paired datasets
    Specify defaults (can be overridden with command line options)
    '''
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = "conf/logging.conf"
        log_config_file = os.path.join(os.getcwd(), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)
        # Check if config file exists
        if not os.path.isfile(config_file):
            sys.exit(-1)

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger("root")
        logger.info("Using config %s", config_file)
        logger.info("Using log config %s", log_config_file)

    logger.info("--- Starting Visualization Process ---")

    dir_params = utils.Utils().read_config_section("directories", logger)

    # Do forecast_a start and end date reshuffle
    if 'forecast_a' in prop.whichcasts:
        if prop.forecast_hr is None:
            error_message = (
                "prop.forecast_hr is required if prop.whichcast is "
                "forecast_a. Abort!")
            logger.error(error_message)
            sys.exit(-1)
        elif prop.forecast_hr is not None:
            try:
                int(prop.forecast_hr[:-2])
            except ValueError:
                error_message = (f"Please check Forecast Hr format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)
            if prop.forecast_hr[-2:] == 'hr':
                prop.start_date_full, prop.end_date_full =\
                get_fcst_cycle.get_fcst_cycle(prop,logger)
                logger.info(f"Forecast_a: end date reassigned to "
                                 f"{prop.end_date_full}")
            else:
                error_message = (f"Please check Forecast Hr (hr) format - "
                                 f"{prop.forecast_hr}. Abort!")
                logger.error(error_message)
                sys.exit(-1)

    # Start Date and End Date validation
    try:
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        datetime.strptime(prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ")
        datetime.strptime(prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        error_message = (f"Please check Start Date - "
                         f"{prop.start_date_full}, End Date - "
                         f"{prop.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if datetime.strptime(
            prop.start_date_full, "%Y-%m-%dT%H:%M:%SZ") > datetime.strptime(
        prop.end_date_full, "%Y-%m-%dT%H:%M:%SZ"):
        error_message = (f"End Date {prop.end_date_full} "
                         f"is before Start Date {prop.end_date_full}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    if prop.path is None:
        prop.path = dir_params["home"]

    # prop.path validation
    ofs_extents_path = os.path.join(
        prop.path, dir_params["ofs_extents_dir"])
    if not os.path.exists(ofs_extents_path):
        error_message = (f"ofs_extents/ folder is not found. "
                         f"Please check prop.path - {prop.path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)

    # prop.ofs validation
    shape_file = f"{ofs_extents_path}/{prop.ofs}.shp"
    if not os.path.isfile(shape_file):
        error_message = (f"Shapefile {prop.ofs} is not found at "
                         f"the folder {ofs_extents_path}. Abort!")
        logger.error(error_message)
        sys.exit(-1)


    prop.control_files_path = os.path.join(
        prop.path, dir_params["control_files_dir"])
    os.makedirs(prop.control_files_path, exist_ok=True)

    prop.data_observations_1d_station_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["observations_dir"],
        dir_params["1d_station_dir"], )
    os.makedirs(prop.data_observations_1d_station_path, exist_ok=True)

    prop.data_model_1d_node_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["model_dir"],
        dir_params["1d_node_dir"], )
    os.makedirs(prop.data_model_1d_node_path, exist_ok=True)

    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["1d_pair_dir"], )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)

    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["skill_dir"],
        dir_params["stats_dir"], )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)

    prop.visuals_1d_station_path = os.path.join(
        prop.path, dir_params["data_dir"], dir_params["visual_dir"], )
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)

    prop.whichcasts = prop.whichcasts.replace("[", "")
    prop.whichcasts = prop.whichcasts.replace("]", "")
    prop.whichcasts = prop.whichcasts.split(",")

    for variable in ['water_level', 'water_temperature', 'salinity',
                     'currents']:
        if variable == 'water_level':
            name_var = 'wl'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Water Level plots.')
        elif variable == 'water_temperature':
            name_var = 'temp'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Water Temperature plots.')
        elif variable == 'salinity':
            name_var = 'salt'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS', 'OFS', 'BIAS']
            logger.info('Creating Salinity plots.')
        elif variable == 'currents':
            name_var = 'cu'
            list_of_headings = ['Julian', 'year', 'month', 'day', 'hour',
                                'minute', 'OBS_SPD', 'OFS_SPD', 'BIAS_SPD',
                                'OBS_DIR', 'OFS_DIR', 'BIAS_DIR']
            logger.info('Creating Currents plots.')

        var_info = [variable, name_var, list_of_headings]

        read_ofs_ctl_file = ofs_ctlfile_read(
            prop, name_var, logger)

        if read_ofs_ctl_file is not None:
            create_1dplot_2nd_part(
                read_ofs_ctl_file, prop, var_info,
                logger)


def oned_scalar_diff_plot(
        now_fores_paired, name_var, station_id, node,
        prop
):
    '''This function creates the standard scalar plots'''
    ''' Updated 5/28/24 for accessibility '''

    # Get marker styles so they can be assigned to different time series below
    allmarkerstyles = get_markerstyles()

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired exceeds data_count,
    scale the marker sizes inversely proportional to the data count.
    Otherwise, use the default marker sizes.

    """
    modetype = 'lines+markers'
    marker_opacity = 1
    data_count = 48
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = 6*(data_count/len(list(now_fores_paired[0].DateTime)))
        marker_size_obs = 9**(data_count/len(list(now_fores_paired[0].DateTime)))
    else:
        marker_size = 6
        marker_size_obs = 9

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2, 0.4, 0.65)
    # Observations are dashed, while model now/forecasts are solid (default)
    obslinestyle = 'dash'

    if name_var == 'wl':
        plot_name = 'Water Level Difference (M-O) ' + f'at {prop.datum} (<i>meters<i>)'
    elif name_var == 'temp':
        plot_name = 'Water Temperature Difference (M-O) (<i>degree C<i>)'
    elif name_var == 'salt':
        plot_name = 'Salinity Difference (M-O) (<i>PSU<i>)'

    # Create figure
    fig = make_subplots(
        rows=1, cols=2, column_widths=[1 - len(prop.whichcasts) * 0.03,
                                       len(prop.whichcasts) * 0.05],
        shared_yaxes=True, horizontal_spacing=0.05
        # subplot_titles = ['Observed','OFS Model'],
    )


    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname='Model Forecast Minus Observations'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname='Model Forecast Minus Observations, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname='Model Nowcast Minus Observations'
        else:
            seriesname=prop.whichcasts[i].capitalize() + " Guidance"  # + f"{i}",

        fig.add_trace(
            go.Scatter(
                x=list(now_fores_paired[i].DateTime),
                y = [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS, now_fores_paired[i].OBS)],
                name=seriesname,
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=1.5),
                legendgroup=seriesname,
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size, #i+1 because observations already used first marker type
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(width=1, color='black'))), 1,
            1)

        if prop.whichcasts[i].capitalize() == 'Nowcast':
            sdboxName = 'Nowcast - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Forecast_b':
            sdboxName = 'Forecast - Obs.'
        else:
            sdboxName = 'Model'+str(i+1)+' - Obs.'

        fig.add_trace(
            go.Box(
                y =  [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS, now_fores_paired[i].OBS)], boxmean='sd',
                name=sdboxName,
                showlegend=False,
                legendgroup=seriesname,
                width=.7,
                line=dict(
                    color=palette[i+1],
                    width=1.5),
                marker_color=palette[i+1]),
            1, 2)

        ## Figure Config
        fig.update_layout(
            title=dict(
                text=get_title(prop, node, station_id),
                font=dict(size=14, color='black', family='Arial'),
                y=0.97,  # new
                x=0.5, xanchor='center', yanchor='top'),
            transition_ordering="traces first", dragmode="zoom",
            hovermode="x unified", height=700, width=900,
            template="plotly_white", margin=dict(
                t=120, b=100), legend=dict(
                orientation="h", yanchor="bottom",
                y=-0.425, xanchor="left", x=-0.05))

    # Added extra annotation so that user knows that the legend is interactive
    fig.add_annotation(
        text="Click a time series in the legend to hide or show. Double-click to isolate one time series.",
        xref="paper", yref="paper",
        font=dict(size=12, color="grey"),
        x=0, y=-0.5,
        showarrow=False
        )

    # Add range slider
    fig.update_layout(  # paper_bgcolor="LightSteelBlue",
        xaxis=dict(
            rangeselector=dict(
                buttons=list(
                    [dict(
                        count=96, label="Forecast Cycle", step="hour",
                        stepmode="todate"), dict(
                        count=1, label="1 month", step="month",
                        stepmode="backward"), dict(
                        count=6, label="6 months", step="month",
                        stepmode="backward"), dict(
                        count=1, label="Year-to-date", step="year",
                        stepmode="todate"), dict(
                        count=1, label="1 year", step="year",
                        stepmode="backward"), dict(step="all")])),
            rangeslider=dict(
                visible=True), type="date"))

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Arial",
        tickfont_color="black")
    ##Set y-axes titles
    fig.update_yaxes(
        title_text=f"{plot_name}", titlefont_family="Arial",
        title_font_color="black", tickfont_family="Arial",
        tickfont_color="black", row=1, col=1)

    ##naming whichcasts
    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + prop.visuals_1d_station_path + f"/{prop.ofs}_{station_id}_{node}_{name_var}_difference_scalar2_{naming_ws}.html")

# Execution:
if __name__ == "__main__":
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog="python ofs_inventory_station.py", usage="%(prog)s",
        description="OFS Inventory Station", )
    parser.add_argument(
        "-o", "--OFS", required=True, help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""", )
    parser.add_argument(
        "-p", "--Path", required=False,
        help="Inventory File path where ofs_extents/ folder is located", )
    parser.add_argument(
        "-s", "--StartDate_full", required=True,
        help="Start Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-e", "--EndDate_full", required=False,
        help="End Date_full YYYY-MM-DDThh:mm:ssZ e.g. '2023-01-01T12:34:00Z'")
    parser.add_argument(
        "-d", "--Datum", required=True, help="datum: 'MHHW', 'MHW', 'MTL', 'MSL', 'DTL', 'MLW', 'MLLW', \
        'NAVD', 'STND'", )
    parser.add_argument(
        "-ws", "--Whichcasts", required=True,
        help="whichcasts: 'Nowcast', 'Forecast_A', 'Forecast_B'", )
    parser.add_argument(
        "-t", "--FileType", required=False,
        help="OFS output file type to use: 'fields' or 'stations'", )
    parser.add_argument(
        "-f",
        "--Forecast_Hr",
        required=False,
        help="'02hr', '06hr', '12hr', '24hr' ... ", )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.ofs = args.OFS.lower()
    prop1.path = args.Path
    prop1.start_date_full = args.StartDate_full
    prop1.end_date_full = args.EndDate_full
    prop1.whichcasts = args.Whichcasts.lower()
    prop1.datum = args.Datum

    ''' Make stations files default, unless user specifies fields '''
    if args.FileType is None:
        prop1.ofsfiletype = 'stations'
    elif args.FileType is not None:
        prop1.ofsfiletype = args.FileType.lower()
    else:
        print('Check OFS file type argument! Abort.')
        sys.exit(-1)

    ''' Do forecast_a to assess a single forecast cycle'''
    if 'forecast_a' in prop1.whichcasts:
        if args.Forecast_Hr is None:
            print('No forecast cycle input -- defaulting to 00Z. Continuing...')
            prop1.forecast_hr = '00hr'
        elif args.Forecast_Hr is not None:
            prop1.forecast_hr = args.Forecast_Hr

    ''' Enforce end date for whichcasts other than forecast_a'''
    if args.EndDate_full is None and 'forecast_a' not in prop1.whichcasts:
       print('If not using forecast_a, you must set an end date! Abort.')
       sys.exit(-1)

    create_1dplot(prop1, None)
