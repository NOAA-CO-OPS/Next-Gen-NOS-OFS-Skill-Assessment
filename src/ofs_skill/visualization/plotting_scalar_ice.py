"""
Script Name: plotting_scalar_ice.py

Technical Contact(s): PWL

Abstract:
   This module includes scalar plotting routines for ice concentration.

Language: Python 3.11

Functions Included:
 - oned_scalar_plot
 - oned_scalar_diff_plot

Author Name: PWL       Creation Date: 10/16/2025
Revisions:

------------------------------------------------------------------
"""
from __future__ import annotations

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .plotting_functions import (
    find_max_data_gap,
    get_error_range,
    get_markerstyles,
    get_title,
    make_cubehelix_palette,
)

# import matplotlib.colors as mcolors


def oned_scalar_plot_ice(now_fores_paired, name_var, station_id, node,
                         prop, logger):
    '''This function creates the standard scalar plots'''
    ''' Updated 5/28/24 for accessibility '''
    # Get marker styles so they can be assigned to different time series below
    allmarkerstyles = get_markerstyles()

    # Get target error range
    X1, X2 = get_error_range(name_var, prop, logger)

    """
    Adjust marker sizes dynamically based on the number of data points.
    If the number of DateTime entries in the first element of now_fores_paired
    exceeds data_count, scale the marker sizes inversely proportional to the
    data count. Otherwise, use the default marker sizes.

    """
    data_count = 48
    min_size = 1
    gap_length = 10
    if len(list(now_fores_paired[0].DateTime)) > data_count:
        marker_size = (
            6**(
                data_count/len(list(now_fores_paired[0].DateTime))
            )
        ) + (min_size-1)
        marker_size_obs = (
            9**(
                data_count/len(list(now_fores_paired[0].DateTime))
            )
        ) + (min_size-1)
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
        # prop.whichcasts = [prop.whichcast]

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = (len(prop.whichcasts)*1) + 1
    palette, palette_rgb = make_cubehelix_palette(ncolors, 2.5, 0.9, 0.65)
    # Observations are dashed, while model now/forecasts are solid (default)
    obslinestyle = 'solid'
    obsname = 'GLSEA/NIC analysis'
    linewidth = 2.5
    modetype = 'lines+markers'
    lineopacity = 1
    marker_opacity = 0.5
    nrows = 2
    ncols = 1
    # column_widths = [1 - len(prop.whichcasts) * 0.03,
    #                 #len(prop.whichcasts) * 0.1
    #                 ]
    xaxis_share = True
    neighborhood = False

    # Create figure
    fig = make_subplots(
        rows=nrows, cols=ncols,
        shared_yaxes=True, horizontal_spacing=0.05,
        vertical_spacing=0.075,
        shared_xaxes=xaxis_share,
        # subplot_titles = ['Observed','OFS Model'],
    )

    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[0].DateTime),
            y=list(now_fores_paired[0].OBS), name=obsname,
            hovertemplate='%{y:.2f}', mode=modetype,
            opacity=lineopacity,
            connectgaps=connectgaps,
            line=dict(color=palette[0], width=linewidth, dash=obslinestyle),
            legendgroup='obs', marker=dict(
            symbol=allmarkerstyles[0],size=marker_size_obs,color=palette[0],
            opacity=marker_opacity, line=dict(width=0, color='black'),
            ),
        ), 1, 1,
    )

    # Set legend for target error ranges
    #showlegend = [True, False, False]
    for i in range(len(prop.whichcasts)):
        # Change name of model time series to make more explanatory
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'

        fig.add_trace(
            go.Scatter(
                x=list(now_fores_paired[i].DateTime),
                y=list(now_fores_paired[i].OFS),
                name=seriesname,
                opacity=lineopacity,
                # Updated hover text to show Obs/Fore/Now values, not bias
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=linewidth,
                ),
                legendgroup=seriesname,
                # i+1 because observations already used first marker type
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(
                        width=0, color='black',
                    ),
                ),
            ), 1, 1,
        )
        if neighborhood:
            stdevup = now_fores_paired[i].OFS + now_fores_paired[i].STDEV
            stdevup[stdevup > 100] = 100
            stdevdown = now_fores_paired[i].OFS - now_fores_paired[i].STDEV
            stdevdown[stdevdown < 0] = 0
            fig.add_trace(
                go.Scatter(
                    x=list(now_fores_paired[i].DateTime),
                    y=list(stdevup),
                    # Updated hover text to show Obs/Fore/Now values
                    # instead of bias as per user comments
                    name=seriesname.split(' ')[1] + ' +1 sigma',
                    hovertemplate='%{y:.2f}',
                    mode='lines', line=dict(
                        color=palette[i+1],
                        width=0,
                    ),
                    showlegend=False,
                ), 1, 1,
            )
            fig.add_trace(
                go.Scatter(
                    x=list(now_fores_paired[i].DateTime),
                    y=list(stdevdown),
                    # Updated hover text to show Obs/Fore/Now values
                    # instead of bias as per user comments
                    name=seriesname.split(' ')[1] + ' -1 sigma',
                    hovertemplate='%{y:.2f}',
                    mode='lines', line=dict(
                        color=palette[i+1],
                        width=0,
                    ),
                    fillcolor='rgba('+str(palette_rgb[i+1][0]*255) +
                    ','+str(palette_rgb[i+1][1]*255)+',' +
                    str(palette_rgb[i+1][2]*255)+','+str(0.1)+')',
                    fill='tonexty',
                    showlegend=False,
                ), 1, 1,
            )

    for i in range(len(prop.whichcasts)):
        if prop.whichcasts[i].capitalize() == 'Nowcast':
            sdboxName = 'Nowcast Error'
        elif prop.whichcasts[i].capitalize() == 'Forecast_b':
            sdboxName = 'Forecast Error'
        else:
            sdboxName = 'Model'+str(i+1)+' - Obs.'
        fig.add_trace(
            go.Scatter(
                x=list(now_fores_paired[i].DateTime),
                y=[
                    ofs - obs for ofs, obs in zip(
                        now_fores_paired[i].OFS,
                        now_fores_paired[i].OBS,
                    )
                ],
                name=sdboxName,
                connectgaps=connectgaps,
                hovertemplate='%{y:.2f}',
                mode=modetype, line=dict(
                    color=palette[i+1],
                    width=1.5, dash='dash',
                ),
                legendgroup=sdboxName,
                marker=dict(
                    symbol=allmarkerstyles[i+1], size=marker_size,
                    color=palette[i+1],
                    # 'firebrick',
                    opacity=marker_opacity, line=dict(width=0, color='black'),
                ),
            ), 2,
            1,
        )
    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[i].DateTime),
            y=np.ones(len(list(now_fores_paired[i].DateTime)))*X1,
            name='Target error range',
            hoverinfo='skip',
            mode='lines',
            line=dict(
                width=0,
                color='red',
            ),
            showlegend=False,
        ), 2, 1,
    )
    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[i].DateTime),
            y=np.ones(len(list(now_fores_paired[i].DateTime)))*-X1,
            name='Target error range',
            hoverinfo='skip',
            mode='lines',
            line=dict(
                width=0,
                color='red',
            ),
            fillcolor='rgba(255,0,0,0.1)',
            fill='tonexty',
            showlegend=False,
        ), 2, 1,
    )
    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[i].DateTime),
            y=np.ones(len(list(now_fores_paired[i].DateTime)))*(X1*2),
            name='Target error range',
            hoverinfo='skip',
            mode='lines',
            line=dict(
                width=0,
                color='red',
            ),
            showlegend=False,
        ), 2, 1,
    )
    fig.add_trace(
        go.Scatter(
            x=list(now_fores_paired[i].DateTime),
            y=np.ones(len(list(now_fores_paired[i].DateTime)))*(-X1*2),
            name='1x and 2x target error ranges',
            hoverinfo='skip',
            mode='lines',
            line=dict(
                width=0,
                color='red',
            ),
            fillcolor='rgba(255,0,0,0.1)',
            fill='tonexty',
            showlegend=True,
        ), 2, 1,
    )

        # ann_text1 = ''
        # ann_text2 = ''
        # fig.add_hline(
        #     y=0, line_width=1,
        #     line_color='black',
        #     # line_dash='dash',
        #     row=2, col=1,
        # )
        # fig.add_hline(
        #     y=X1, line_color='black',
        #     line_width=0,
        #     # line_dash='dash',
        #     annotation_text=ann_text1,
        #     annotation_position='bottom left',
        #     annotation_font_color='black',
        #     annotation_font_size=12,
        #     row=2, col=1,
        # )
        # fig.add_hline(
        #     y=-X1, line_color='black',
        #     line_width=0,
        #     # line_dash='dash',
        #     annotation_text=ann_text1,
        #     annotation_position='top right',
        #     annotation_font_color='black',
        #     annotation_font_size=12,
        #     row=2, col=1,
        # )
        # fig.add_hline(
        #     y=X1*2, line_color='black',
        #     line_width=0,
        #     # line_dash='dash',
        #     annotation_text=ann_text2,
        #     annotation_position='bottom left',
        #     annotation_font_color='black',
        #     annotation_font_size=12,
        #     row=2, col=1,
        # )
        # fig.add_hline(
        #     y=-X1*2, line_color='black',
        #     line_width=0,
        #     # line_dash='dash',
        #     annotation_text=ann_text2,
        #     annotation_position='top right',
        #     annotation_font_color='black',
        #     annotation_font_size=12,
        #     row=2, col=1,
        # )

    # Figure Config
    figheight = 600
    figwidth = 900
    yoffset = 1.03
    fig.update_layout(
        xaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
        ),
        yaxis=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
        ),
        xaxis2=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            tickangle=45,
        ),
        yaxis2=dict(
            mirror=True,
            ticks='inside',
            showline=True,
            linecolor='black',
            linewidth=1,
            range=[-100, 100],
            tickmode='array',
            tickvals=[-100, -75, -50, -25, 0, 25, 50, 75, 100],
        ),
    )
    tmargin = 120
    if len(prop.whichcasts) > 1:
        tmargin = 150
    fig.update_layout(
        transition_ordering='traces first', dragmode='zoom',
        hovermode='x unified', height=figheight, width=figwidth,
        template='plotly_white', margin=dict(
            t=tmargin, b=100,
        ),
        legend=dict(
            orientation='h', yanchor='bottom',
            y=yoffset, xanchor='left', x=-0.05,
            itemsizing='constant',
            font=dict(
                family='Open Sans',
                size=12,
                color='black',
            ),
        ),
        title=dict(
            text=get_title(prop, node, station_id, name_var, logger),
            font=dict(size=14, color='black', family='Open Sans'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top',
        ),
    )

    # Set x-axis moving bar
    fig.update_xaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        showline=True,
        showgrid=True,
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
        range=[
            np.min(now_fores_paired[i].DateTime),
            np.max(now_fores_paired[i].DateTime),
        ],
    )
    # Set y-axes titles
    fig.update_yaxes(
        mirror=True,
        title_text=f'{plot_name}',
        title_font=dict(
            family='Open Sans',
            #size=18,
            color='black'
            ),
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
        range=[0, 100],
        row=1, col=1,
    )
    fig.update_yaxes(
        title_text=f"Error {plot_name.split(' ')[-1]}",
        title_font=dict(
            family='Open Sans',
            #size=18,
            color='black'
            ),
        tickfont=dict(size=14,
                      family='Open Sans',
                      color='black'),
        row=2, col=1,
    )

    # naming whichcasts
    naming_ws = '_'.join(prop.whichcasts)
    output_file = (
        f'{prop.visuals_1d_ice_path}/{prop.ofs}_'
        f'{station_id[0]}_ice_concentration_timeseries_'
        f'{naming_ws}_{prop.ofsfiletype}'
        )
    fig_config = {
    'toImageButtonOptions': {
        'format': 'png',
        'filename': output_file.split('/')[-1],
        'height': figheight,
        'width': figwidth,
        'scale': 1
        }
    }
    logger.debug(f'Writing file: {output_file}')
    fig.write_html(output_file+'.html',config=fig_config)
    logger.debug(f'Finished writing file: {output_file}')
