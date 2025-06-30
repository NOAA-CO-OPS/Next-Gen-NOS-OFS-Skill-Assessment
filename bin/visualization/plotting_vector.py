# -*- coding: utf-8 -*-
"""
Script Name: plotting_vector.py

Technical Contact(s): Name: AJK

Abstract:
   This module includes vector plotting routines including current time series,
   stick plots, wind roses, and vector differences.

Language: Python 3.8

Functions Included:
 - oned_vector_plot1
 - oned_vector_plot2a
 - oned_vector_plot2b
 - oned_vector_plot3
 - oned_vector_diff_plot3

Author Name: AJK       Creation Date: 05/09/2025
Revisions:
Date          Author     Description
------------------------------------------------------------------

"""

import math
import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.figure_factory as ff
from matplotlib.dates import date2num
from .plotting_functions import get_title, \
                                make_cubehelix_palette, \
                                get_error_range, \
                                get_markerstyles, \
                                find_max_data_gap

def oned_vector_plot1(now_fores_paired, name_var, station_id, node, prop, logger):
    '''This function creates the standard vector time series plots'''
    # Choose color & style for observation lines and marker fill.
    ncolors = len(prop.whichcasts) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2.5, 0.9, 0.65)
    linestyles = ['solid', 'dot', 'longdash', 'dashdot', 'longdashdot']

    # Get marker styles so they can be assigned to different time series below
    #deltat = datetime.strptime(prop.end_date_full,"%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(prop.start_date_full,"%Y-%m-%dT%H:%M:%SZ")
    allmarkerstyles = get_markerstyles()

    # Create figure
    fig = make_subplots(
        rows=3, cols=2, column_widths=[1 - len(prop.whichcasts) * 0.03,
                                       len(prop.whichcasts) * 0.1],
        shared_yaxes=True, horizontal_spacing=0.05, vertical_spacing=0.05,
        shared_xaxes=True
    )


    #Get target error range
    X1, X2 = get_error_range(name_var,prop,logger)

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired exceeds data_count,
    scale the marker sizes inversely proportional to the data count.
    Otherwise, use the default marker sizes.

    """
    modetype = 'lines+markers'
    lineopacity=1
    linewidth=1.5
    marker_opacity = 0.5
    data_count = 48
    min_size = 1
    gap_length = 10
    data_count = 48
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = (6**(
            data_count/len(list(now_fores_paired[0].DateTime)))) + (min_size-1)
        marker_size_obs = (9**(
            data_count/len(list(now_fores_paired[0].DateTime)))) + (min_size-1)
    else:
        marker_size = 6
        marker_size_obs = 9
    # Check for long data gaps
    if find_max_data_gap(now_fores_paired[0].OBS_SPD) > gap_length:
        connectgaps = False
    else:
        connectgaps = True

    # Current speed
    fig.add_trace(
        go.Scattergl(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS_SPD), name="Observations",
            hovertext=list(now_fores_paired[0].OBS_DIR),
            hovertemplate='%{y:.2f}',
            connectgaps=connectgaps,
            opacity=lineopacity,
            line=dict(color=palette[0], width=linewidth, dash='dash'),
            mode=modetype, legendgroup="obs", marker=dict(
                symbol=allmarkerstyles[0], size=marker_size_obs, color=palette[0],
                #angle=list(now_fores_paired[0].OBS_DIR),
                opacity=marker_opacity,
                #angleref='up',
                line=dict(width=0, color='black')),
            ), 1, 1
        )

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
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS_SPD),
                name=seriesname,
                #Updated hover text to show Obs/Fore/Now values instead of bias as per user comments
                #hovertext=list(now_fores_paired[0].OFS_DIR),
                hovertemplate='%{y:.2f}',
                connectgaps=connectgaps,
                line=dict(
                    color=palette[i+1],
                    width=linewidth), mode=modetype, opacity=lineopacity,
                legendgroup=seriesname,
                marker=dict(
                    symbol=allmarkerstyles[i+1],
                    size=marker_size,
                    color=palette[i+1],
                    #angle=list(now_fores_paired[i].OFS_DIR),
                    opacity=marker_opacity,
                    # 0.6,
                    line=dict(width=0, color='black')), ), 1, 1)
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
    # Now do current direction
    fig.add_trace(
        go.Scattergl(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS_DIR), name="Observations",
            #hovertext=list(now_fores_paired[0].OBS_DIR),
            hovertemplate='%{y:.2f}',
            connectgaps=connectgaps,
            opacity=lineopacity,
            showlegend=False,
            line=dict(color=palette[0], width=linewidth, dash='dash'),
            mode='lines+markers', legendgroup="obs", marker=dict(
                symbol=allmarkerstyles[0], size=marker_size, color=palette[0],
                #angle=list(now_fores_paired[0].OBS_DIR),
                opacity=marker_opacity,
                #angleref='up',
                line=dict(width=0, color='black')),
            ), 2, 1
        )

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
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS_DIR),
                name=seriesname,
                #Updated hover text to show Obs/Fore/Now values instead of bias as per user comments
                #hovertext=list(now_fores_paired[0].OFS_DIR),
                hovertemplate='%{y:.2f}',
                line=dict(
                    color=palette[i+1],
                    width=1.75, dash=linestyles[i]), mode='lines+markers',
                #legendgroup=seriesname,
                showlegend=False,
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    #angle=list(now_fores_paired[i].OFS_DIR),
                    opacity=1,
                    # 0.6,
                    line=dict(width=0, color='black')), ), 2, 1)

    # Diff plots
    for i in range(len(prop.whichcasts)):
        if prop.whichcasts[i].capitalize() == 'Nowcast':
            sdboxName = 'Nowcast - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Forecast_b':
            sdboxName = 'Forecast - Obs.'
        else:
            sdboxName = 'Model'+str(i+1)+' - Obs.'
        fig.add_trace(
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y = [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS_SPD,
                                                   now_fores_paired[i].OBS_SPD)],
                name=sdboxName,
                hovertemplate='%{y:.2f}',
                mode='lines', line=dict(
                    color=palette[i+1],
                    width=1.5,dash ='dash'),
                legendgroup=sdboxName,
                ), 3, 1)

        fig.add_hline(y=0, line_width=1,
                      line_color="black",
                      #line_dash='dash',
                      row=3, col=1)
        fig.add_hline(y=X1, line_color="orange",
                      line_width=0.75,
                      line_dash='dash',
                      annotation_text= 'Target error range',
                      annotation_position='top left',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=3, col=1)
        fig.add_hline(y=-X1, line_color="orange",
                      line_width=0.75,
                      line_dash='dash',
                      annotation_text= 'Target error range',
                      annotation_position='bottom right',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=3, col=1)
        fig.add_hline(y=X1*2, line_color="red",
                      line_width=0.75,
                      line_dash='dash',
                      annotation_text= '2x target error range',
                      annotation_position='top left',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=3, col=1)
        fig.add_hline(y=-X1*2, line_color="red",
                      line_width=0.75,
                      line_dash='dash',
                      annotation_text= '2x target error range',
                      annotation_position='bottom right',
                      annotation_font_color='black',
                      annotation_font_size=12,
                      row=3, col=1)


        fig.add_trace(
            go.Box(
                y =  [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS_SPD,
                                                    now_fores_paired[i].OBS_SPD)],
                                                    boxmean='sd',
                name=sdboxName,
                showlegend=False,
                legendgroup=sdboxName,
                width=.7,
                line=dict(
                    color=palette[i+1],
                    width=1.5),
                marker_color=palette[i+1]),
            3, 2)

#    figheight = 900
    yoffset = 1.01
    ## Figure Config
    fig.update_layout(
        #margin=dict(t=5),
        xaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            showticklabels=False
            ),
        #xaxis2=dict(tickangle=45),
        xaxis3=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            showticklabels=False
            ),
        xaxis4=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            showticklabels=False
            ),
        xaxis5=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            showticklabels=True),
        yaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            ),
        yaxis3=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            ),
        yaxis5=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            range=[-X1*4,X1*4],
            tickmode='array',
            tickvals=[-X1*4,-X1*3,-X1*2,-X1,0,X1,X1*2,X1*3,X1*4],
            ),
        title=dict(
            text=get_title(prop, node, station_id,name_var,logger),
            font=dict(size=14, color='black', family='Open Sans'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top'),
        transition_ordering="traces first", dragmode="zoom",
        hovermode="x unified", height=700, width=900,
        template="plotly_white", margin=dict(
            t=130, b=100),
        legend=dict(
            orientation="h", yanchor="bottom",
            y=yoffset, xanchor="left", x=-0.05,
            itemsizing='constant',
            font=dict(
                family='Open Sans',
                size=12,
                color='black'
                )
            )
        )

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Open Sans",
        tickfont_color="black",tickfont=dict(size=14))
    fig.update_xaxes(tickangle=45, row=3, col=2)
    ##Set y-axes titles
    fig.update_yaxes(
        title_text="Current speed<br>(<i>meters/second<i>)",
        titlefont_family="Open Sans", title_font_color="black",
        tickfont_family="Open Sans", tickfont=dict(size=14),
        tickfont_color="black", row=1, col=1)
    fig.update_yaxes(
        title_text="Current direction<br>(<i>0-360 deg.<i>)",
        titlefont_family="Open Sans", title_font_color="black",
        tickfont_family="Open Sans", tickfont=dict(size=14),
        tickfont_color="black", row=2, col=1)
    fig.update_yaxes(
        title_text="Speed error<br>(<i>meters/second<i>)",
        titlefont_family="Open Sans", title_font_color="black",
        tickfont_family="Open Sans", tickfont=dict(size=14),
        tickfont_color="black", row=3, col=1)
    naming_ws = "_".join(prop.whichcasts)
    fig.write_html(
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id[0]}_{node}_{name_var}_vector1_{naming_ws}.html")


def oned_vector_plot2a(now_fores_paired, logger):
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


def oned_vector_plot2b(fig_info_data, name_var, station_id, node, prop, logger):
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
            text=get_title(prop, node, station_id,name_var,logger),
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
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id[0]}_{node}_{name_var}_vector2_{naming_ws}.html")
    logger.info('oned_vector_plot2b - End')


def oned_vector_plot3(now_fores_paired, name_var, station_id, node, prop, logger):
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
            text=get_title(prop, node, station_id,name_var,logger),
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
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id[0]}_{node}_{name_var}_vector3_{naming_ws}.html")


def oned_vector_diff_plot3(now_fores_paired, name_var, station_id, node, prop, logger):
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
            text=get_title(prop, node, station_id,name_var,logger),
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
        r"" + f"{prop.visuals_1d_station_path}/{prop.ofs}_{station_id[0]}_{node}_{name_var}_difference_vector3_{naming_ws}.html")
