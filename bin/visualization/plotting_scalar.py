# -*- coding: utf-8 -*-
"""
Script Name: plotting_scalar.py

Technical Contact(s): Name: AJK

Abstract:
   This module includes scalar plotting routines for variables like
   water level, salinity, temperature, and ice concentration.

Language: Python 3.8

Functions Included:
 - oned_scalar_plot
 - oned_scalar_diff_plot

Author Name: AJK       Creation Date: 05/09/2025
Revisions:
Date          Author     Description

06/17/25      PWL        Optimized for year-long runs
------------------------------------------------------------------
"""

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from .plotting_functions import get_markerstyles, \
                                make_cubehelix_palette, \
                                get_title, \
                                get_error_range, \
                                find_max_data_gap


def oned_scalar_plot(now_fores_paired, name_var, station_id, node, prop, logger):
    '''This function creates the standard scalar plots'''
    ''' Updated 5/28/24 for accessibility '''
    # Get marker styles so they can be assigned to different time series below
    #deltat = datetime.strptime(prop.end_date_full,"%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(prop.start_date_full,"%Y-%m-%dT%H:%M:%SZ")
    allmarkerstyles = get_markerstyles()

    #Get target error range
    if name_var != 'ice_conc':
        X1, X2 = get_error_range(name_var,prop,logger)

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired exceeds data_count,
    scale the marker sizes inversely proportional to the data count.
    Otherwise, use the default marker sizes.

    """
    modetype = 'lines+markers'
    lineopacity=1
    marker_opacity = 0.5
    data_count = 48
    min_size = 1
    gap_length = 10
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = (6**(
            data_count/len(list(now_fores_paired[0].DateTime)))) + (min_size-1)
        marker_size_obs = (9**(
            data_count/len(list(now_fores_paired[0].DateTime)))) + (min_size-1)
    else:
        marker_size = 6
        marker_size_obs = 9
    # Check for long data gaps
    if find_max_data_gap(now_fores_paired[0].OBS) > gap_length:
        connectgaps = False
    else:
        connectgaps = True

    if name_var == 'wl':
        plot_name = 'Water Level ' + f'at {prop.datum} (<i>meters<i>)'
    elif name_var == 'temp':
        plot_name = 'Water Temperature (<i>\u00b0C<i>)'
    elif name_var == 'salt':
        plot_name = 'Salinity (<i>PSU<i>)'
    elif name_var == 'ice_conc':
        plot_name = 'Ice Concentration (%)'
        #prop.whichcasts = [prop.whichcast]

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = (len(prop.whichcasts)*1) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2.5, 0.9, 0.65)
    # Observations are dashed, while model now/forecasts are solid (default)
    if name_var == 'ice_conc':
        obslinestyle = 'solid'
        obsname = 'GLSEA/NIC analysis'
        linewidth = 2.5
        nrows = 1
        ncols = 1
        column_widths = [1 - len(prop.whichcasts)*0.03]
        xaxis_share = False
    else:
        obslinestyle = 'dash'
        obsname = 'Observations'
        linewidth = 1.5
        nrows = 2
        ncols = 2
        column_widths = [1 - len(prop.whichcasts) * 0.03,
                        len(prop.whichcasts) * 0.1]
        xaxis_share = True

    # Create figure
    fig = make_subplots(
        rows=nrows, cols=ncols, column_widths=column_widths,
        shared_yaxes=True, horizontal_spacing=0.05,
        vertical_spacing=0.05,
        shared_xaxes=xaxis_share
        # subplot_titles = ['Observed','OFS Model'],
    )

    fig.add_trace(
        go.Scattergl(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS), name=obsname,
            hovertemplate='%{y:.2f}', mode=modetype,
            opacity=lineopacity,
            connectgaps=connectgaps,
            line=dict(color=palette[0], width=linewidth, dash = obslinestyle),
            legendgroup="obs", marker=dict(
                symbol=allmarkerstyles[0], size=marker_size_obs, color=palette[0],
                opacity=marker_opacity, line=dict(width=0, color='black'))), 1, 1)

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
            go.Scattergl(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS),
                name=seriesname,
                opacity=lineopacity,
                #Updated hover text to show Obs/Fore/Now values instead of bias
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
                        width=0, color='black'))), 1,1)
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
    if name_var != 'ice_conc':
        for i in range(len(prop.whichcasts)):
            if prop.whichcasts[i].capitalize() == 'Nowcast':
                sdboxName = 'Nowcast - Obs.'
            elif prop.whichcasts[i].capitalize() == 'Forecast_b':
                sdboxName = 'Forecast - Obs.'
            else:
                sdboxName = 'Model'+str(i+1)+' - Obs.'
            fig.add_trace(
                go.Scattergl(
                    x = list(now_fores_paired[i].DateTime),
                    y = [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS,
                                                       now_fores_paired[i].OBS)],
                    name=sdboxName,
                    connectgaps=connectgaps,
                    hovertemplate='%{y:.2f}',
                    mode=modetype, line=dict(
                        color=palette[i+1],
                        width=1.5,dash ='dash'),
                    legendgroup=sdboxName,
                    marker=dict(
                        symbol=allmarkerstyles[i+1], size=marker_size, #i+1 because observations already used first marker type
                        color=palette[i+1],
                        # 'firebrick',
                        opacity=marker_opacity, line=dict(width=0, color='black'))), 2,
                1)

            fig.add_hline(y=0, line_width=1,
                          line_color="black",
                          #line_dash='dash',
                          row=2, col=1)
            fig.add_hline(y=X1, line_color="orange",
                          line_width=0.75,
                          line_dash='dash',
                          annotation_text= 'Target error range',
                          annotation_position='top left',
                          annotation_font_color='black',
                          annotation_font_size=12,
                          row=2, col=1)
            fig.add_hline(y=-X1, line_color="orange",
                          line_width=0.75,
                          line_dash='dash',
                          annotation_text= 'Target error range',
                          annotation_position='bottom right',
                          annotation_font_color='black',
                          annotation_font_size=12,
                          row=2, col=1)
            fig.add_hline(y=X1*2, line_color="red",
                          line_width=0.75,
                          line_dash='dash',
                          annotation_text= '2x target error range',
                          annotation_position='top left',
                          annotation_font_color='black',
                          annotation_font_size=12,
                          row=2, col=1)
            fig.add_hline(y=-X1*2, line_color="red",
                          line_width=0.75,
                          line_dash='dash',
                          annotation_text= '2x target error range',
                          annotation_position='bottom right',
                          annotation_font_color='black',
                          annotation_font_size=12,
                          row=2, col=1)


            fig.add_trace(
                go.Box(
                    y =  [ofs - obs for ofs, obs in zip(now_fores_paired[i].OFS,
                                                        now_fores_paired[i].OBS)],
                                                        boxmean='sd',
                    name=sdboxName,
                    showlegend=False,
                    legendgroup=sdboxName,
                    width=.7,
                    line=dict(
                        color=palette[i+1],
                        width=linewidth),
                    marker_color=palette[i+1]),
                2, 2)

    ## Figure Config
    if name_var == 'ice_conc':
        figheight = 500
        yoffset = 1.03
        fig.update_layout(
        title=dict(
            text=get_title(prop, node, station_id, name_var, logger),
            font=dict(size=14, color='black', family='Open Sans'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top'),
        legend=dict(
            orientation="h", yanchor="bottom",
            y=yoffset, xanchor="left", x=-0.05,
            itemsizing='constant',
            font=dict(
                family='Open Sans',
                size=12,
                color='black'
                )
            ),
        xaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1
            ),
        yaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            ),
        #xaxis2=dict(tickangle=90),
        transition_ordering="traces first", dragmode="zoom",
        hovermode="x unified", height=figheight, width=900,
        template="plotly_white", margin=dict(
            t=120, b=100),
        # legend=dict(
        #     orientation="h", yanchor="bottom",
        #     y=yoffset, xanchor="left", x=-0.05)
                )
    else:
        figheight = 700
        yoffset = 1.01
        fig.update_layout(
            title=dict(
                text=get_title(prop, node, station_id, name_var, logger),
                font=dict(size=14, color='black', family='Open Sans'),
                y=0.97,  # new
                x=0.5, xanchor='center', yanchor='top'),
            xaxis=dict(
                mirror=True,
                ticks='inside',
                showline=True,
                linecolor='black',
                linewidth=1
                ),
            xaxis2=dict(tickangle=45),
            xaxis3=dict(
                mirror=True,
                ticks='inside',
                showline=True,
                linecolor='black',
                linewidth=1
                ),
            xaxis4=dict(tickangle=45),
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
                range=[-X1*4,X1*4],
                tickmode='array',
                tickvals=[-X1*4,-X1*3,-X1*2,-X1,0,X1,X1*2,X1*3,X1*4],
                ),
            transition_ordering="traces first", dragmode="zoom",
            hovermode="x unified", height=figheight, width=900,
            template="plotly_white", margin=dict(
                t=120, b=100),
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
    # Add annotation if datum mismatch
    if name_var == 'wl':
        filename = f"{prop.control_files_path}/{prop.ofs}_wl_datum_report.csv"
        try:
            df = pd.read_csv(filename)
            has_fail = df.loc[df[df['ID'] == int(station_id[0])].index]\
                ['Datum_conversion_success'] == 'fail'
            if has_fail.bool():
                fig.add_annotation(
                    text="<b>Warning:<br>datum mismatch</b>",
                    xref="x domain", yref="y domain",
                    font=dict(size=14, color="red"),
                    x=0, y=0.0,
                    showarrow=False,
                    row=1, col=1
                    )
        except Exception as e_x:
            logger.error("Cannot find station ID in datum report! "
                         "Exception: %s", e_x)


    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True, spikemode='across', spikesnap='cursor',
        showline=True, showgrid=True, tickfont_family="Open Sans",
        tickfont_color="black",tickfont=dict(size=14))
    ##Set y-axes titles
    fig.update_yaxes(
        mirror=True,
        title_text=f"{plot_name}", titlefont_family="Open Sans",
        title_font_color="black", tickfont_family="Open Sans",
        tickfont_color="black", tickfont=dict(size=14), row=1, col=1)
    fig.update_yaxes(
        title_text=f"Error {plot_name.split(' ')[-1]}", titlefont_family="Open Sans",
        title_font_color="black", tickfont_family="Open Sans",
        tickfont_color="black", tickfont=dict(size=14), row=2, col=1)
    if name_var == 'ice_conc':
        fig.update_yaxes(
            range=[0,100], row=1, col=1)

    ##naming whichcasts
    naming_ws = "_".join(prop.whichcasts)
    if name_var != 'ice_conc':
        fig.write_html(
            r"" + prop.visuals_1d_station_path + f"/{prop.ofs}_{station_id[0]}_{node}_{name_var}_scalar2_{naming_ws}.html")
    elif name_var == 'ice_conc':
        fig.write_html(
            r"" + prop.visuals_1d_ice_path + f"/{prop.ofs}_{station_id[0]}_{node}_{name_var}_scalar2_{naming_ws}.html")
    else:
        print("Unrecognized name_var! "
                     "No 1D time series plot made.")

def oned_scalar_diff_plot(
        now_fores_paired, name_var, station_id, node,
        prop, logger
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
                text=get_title(prop, node, station_id,name_var,logger),
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
        r"" + prop.visuals_1d_station_path + f"/{prop.ofs}_{station_id[0]}_{node}_{name_var}_difference_scalar2_{naming_ws}.html")
