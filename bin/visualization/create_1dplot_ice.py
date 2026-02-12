"""
Created on Tue Nov  5 15:17:25 2024

@author: PWL

for reference:

"time_all_dt": time_all_dt,
"obs_meanicecover": obs_meanicecover,
"mod_meanicecover": mod_meanicecover,
"obs_stdmic": obs_stdmic,
"mod_stdmic": mod_stdmic,
"icecover_hist": icecover_hist,
"SS": SS,
"rmse_all": rmse_all,
"rmse_either": rmse_either,
"rmse_overlap": rmse_overlap,
"hitrate_mod": hitrate,
"hitrate_obs": hitrate_obs,
"obs_extent": obs_extent,
"mod_extent": mod_extent,
"r_all": r_all,
"r_overlap": r_overlap,
"csi_all": csi_all
"""
from __future__ import annotations

import math
import os
from datetime import datetime

import matplotlib.colors as mcolors
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
from plotly.subplots import make_subplots
from scipy.signal import savgol_filter

from ofs_skill.visualization import plotting_scalar_ice


def make_cubehelix_palette(
        ncolors, start_val, rot_val, light_val,
):
    '''
    Function makes and returns a custom cubehelix color palette for plotting.
    The colors within the cubehelix palette (and therefore plots) can be
    distinguished in greyscale to improve accessibility. Colors are returned as
    HEX values because it's easier to handle HEX compared to RGB values.

    Arguments:
    -ncolors = number of dicrete colors in color palette, should correspond to
        number of time series in the plot. integer, 1 <= ncolors <= 1000(?)
    -start_val = starting hue for color palette. float, 0 <= start_val <= 3
    -rot_val = rotations around the hue wheel over the range of the palette.
        Larger (smaller) absolute values increase (decrease) number of
        different colors in palette. float, positive or negative
    -light_val = Intensity of the lightest color in the palette.
        float, 0 (darker) <= light <= 1 (lighter)

    More details:
        https://seaborn.pydata.org/generated/seaborn.cubehelix_palette.html

    '''
    palette = sns.cubehelix_palette(
        n_colors=ncolors, start=start_val, rot=rot_val, gamma=1.0,
        hue=0.8, light=light_val, dark=0.15, reverse=False, as_cmap=False,
    )
    # Convert RGB to HEX numbers
    palette = palette.as_hex()
    return palette


def create_1dplot_icestats(prop, time_all, logger):
    '''Make time series plots of lake-wide ice concentration
    and extent skill stats, let's go'''

    # First load nowcast and/or forecast stats from file!
    dflist = []
    counter = 0
    for cast in prop.whichcasts:
        df = None
        # df2 = None
        # Here we try to open the paired data set.
        # If not found, return without making plot
        prop.whichcast = cast.lower()
        if (
            os.path.isfile(
                f'{prop.data_skill_stats_path}/skill_{prop.ofs}_'
                f'icestatstseries_{prop.whichcast}.csv',
            ) is False
        ):
            logger.error(
                'Ice stats time series dataset not found in %s. ',
                prop.data_skill_stats_path,
            )

        # If csv is there, proceed
        else:
            counter = counter + 1  # Keep track of # whichcasts
            df = pd.read_csv(
                r'' + f'{prop.data_skill_stats_path}/skill_{prop.ofs}_'
                f'icestatstseries_{prop.whichcast}.csv',
            )
            logger.info(
                'Ice stats time series csv found in %s',
                prop.data_skill_stats_path,
            )

        # Put nowcast & forecast together in list
        if df is not None:
            dflist.append(df)

    # Get dates and make title for figures
    datestrend = str(time_all[len(time_all)-1]).split()
    # datestrbegin = str(time_all[0]).split()
    datestrbegin = prop.start_date_full.split('T')
    titlewc = '_'.join(prop.whichcasts)

    figtitle = prop.ofs.upper() + ' ' + 'ice concentration, ' + \
        datestrbegin[0] +\
        ' - ' + datestrend[0]

    # Figure out what x-axis tick spacing should be --
    # it depends on length of run so axis doesn't overcrowd!
    df['time_all_dt'] = pd.to_datetime(df['time_all_dt'])
    dayselapsed = (
        datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ') -
        datetime.strptime(
            prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ',
        )
    ).days + 1
    xtickspace = dayselapsed/10
    if xtickspace >= 1:
        xtickspace = math.floor(xtickspace)
    elif xtickspace < 1:
        xtickspace = math.ceil(xtickspace)

    nrows = 3
    # Do stats time series
    fig = make_subplots(
        rows=nrows, cols=len(prop.whichcasts), vertical_spacing=0.055,
        subplot_titles=(prop.whichcasts),
        shared_xaxes=True,
    )
    showlegend = [True, False]
    for i in range(len(prop.whichcasts)):
        prop.whichcast = prop.whichcasts[i]
        if prop.whichcast == 'nowcast':
            if (
                os.path.isfile(
                    f'{prop.data_skill_stats_path}/skill_{prop.ofs}_'
                    f'iceonoff_{prop.whichcast}.csv',
                ) is False
            ):
                logger.error(
                    'Ice on/off csv not found in %s. ',
                    prop.data_skill_stats_path,
                )
            # If csv is there, proceed
            else:
                df2 = pd.read_csv(
                    r'' + f'{prop.data_skill_stats_path}/skill_{prop.ofs}_'
                    f'iceonoff_{prop.whichcast}.csv',
                )
                logger.info(
                    'Ice on/off csv found in %s',
                    prop.data_skill_stats_path,
                )

        # subplot 1
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].obs_meanicecover +
                dflist[i].obs_stdmic,
                name='GLSEA +1 sigma',
                hoverinfo='skip',
                mode='lines',
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('dodgerblue'))
                    .strip('()') + ', '+str(0.1) + ')',
                    width=0,
                ),
                showlegend=False,
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].obs_meanicecover -
                dflist[i].obs_stdmic,
                name='GLSEA +/- 1\u03C3',
                hoverinfo='skip',
                line=dict(width=0),
                mode='lines',
                legendgroup='1',
                fillcolor='rgba(' +
                str(mcolors.to_rgb('dodgerblue'))
                .strip('()') + ', ' + str(0.1) + ')',
                fill='tonexty',
                showlegend=showlegend[i],
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].mod_meanicecover +
                dflist[i].mod_stdmic,
                name='OFS +1 sigma',
                hoverinfo='skip',
                mode='lines',
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('magenta'))
                    .strip('()') + ', ' +
                    str(0.1) + ')',
                    width=0,
                ),
                showlegend=False,
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].mod_meanicecover -
                dflist[i].mod_stdmic,
                name='OFS +/- 1\u03C3',
                hoverinfo='skip',
                line=dict(width=0),
                mode='lines',
                legendgroup='1',
                fillcolor='rgba(' +
                str(mcolors.to_rgb('magenta'))
                .strip('()') + ', ' + str(0.1) + ')',
                fill='tonexty',
                showlegend=showlegend[i],
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].icecover_hist,
                name='Climatology (1973-2024)',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='1',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(0,0,0,1)',
                    width=2,
                ),
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].obs_meanicecover,
                name='GLSEA mean',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='1',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('dodgerblue'))
                    .strip('()') + ', ' + str(1) + ')',
                    width=2,
                ),
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].mod_meanicecover,
                name='OFS mean',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='1',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('magenta'))
                    .strip('()') + ', ' + str(1) + ')',
                    width=2,
                ),
            ), row=1, col=i+1,
        )
        # Ice on/off dates
        begindate = time_all[0]
        if prop.whichcast == 'nowcast' and (begindate.month == 11 or
                                           begindate.month == 12):
            df = pd.DataFrame(dflist[i])
            df['time_all_dt'] = pd.to_datetime(df['time_all_dt'])
            if df2['Ice onset'].notna()[0]:
                obs_iceon = pd.to_datetime(df2.iloc[0]['Ice onset'])
                df_ind_obs_on = df[df['time_all_dt'] == obs_iceon].index
                logger.info('obs ice on index: %s', df_ind_obs_on.values)
                logger.info('obs ice on value: %s', obs_iceon)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_obs_on],
                        y=df['obs_meanicecover']
                        [df_ind_obs_on],
                        name='GLSEA ice onset',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                            str(mcolors.to_rgb('dodgerblue'))
                            .strip('()') + ', ' + str(0.6) + ')',
                            size=10,
                            line=dict(
                                color='black',
                                width=0.5,
                            ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )
            if df2['Ice thaw'].notna()[0]:
                obs_iceoff = pd.to_datetime(df2.iloc[0]['Ice thaw'])
                df_ind_obs_off = df[df['time_all_dt'] == obs_iceoff].index
                logger.info('obs ice off index: %s', df_ind_obs_off.values)
                logger.info('obs ice off value: %s', obs_iceoff)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_obs_off],
                        y=df['obs_meanicecover']
                        [df_ind_obs_off],
                        name='GLSEA ice thaw',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                            str(mcolors.to_rgb('dodgerblue'))
                            .strip('()') + ', ' + str(0.6) + ')',
                            size=14,
                            symbol='x',
                            line=dict(
                                color='black',
                                width=0.5,
                            ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )
            if df2['Ice onset'].notna()[1]:
                mod_iceon = pd.to_datetime(df2.iloc[1]['Ice onset'])
                df_ind_mod_on = df[df['time_all_dt'] == mod_iceon].index
                logger.info('mod ice on index: %s', df_ind_mod_on.values)
                logger.info('mod ice on value: %s', mod_iceon)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_mod_on],
                        y=df['mod_meanicecover']
                        [df_ind_mod_on],
                        name='OFS ice onset',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                            str(mcolors.to_rgb('deeppink'))
                            .strip('()') + ', ' + str(0.6) + ')',
                            size=10,
                            line=dict(
                                color='black',
                                width=0.5,
                            ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )
            if df2['Ice thaw'].notna()[1]:
                mod_iceoff = pd.to_datetime(df2.iloc[1]['Ice thaw'])
                df_ind_mod_off = df[df['time_all_dt'] == mod_iceoff].index
                logger.info('mod ice off index: %s', df_ind_mod_off.values)
                logger.info('mod ice off value: %s', mod_iceoff)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_mod_off],
                        y=df['mod_meanicecover']
                        [df_ind_mod_off],
                        name='OFS ice thaw',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                            str(mcolors.to_rgb('deeppink'))
                            .strip('()') + ', ' + str(0.6) + ')',
                            size=14,
                            symbol='x',
                            line=dict(
                                color='black',
                                width=0.5,
                            ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )
            if df2['Ice onset'].notna()[2]:
                clim_iceon = pd.to_datetime(df2.iloc[2]['Ice onset'])
                df_ind_clim_on = df[df['time_all_dt'] == clim_iceon].index
                logger.info('clim ice on index: %s', df_ind_clim_on.values)
                logger.info('clim ice on value: %s', clim_iceon)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_clim_on],
                        y=df['icecover_hist']
                        [df_ind_clim_on],
                        name='Climatology ice onset',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                                      str(mcolors.to_rgb('black'))
                            .strip('()') + ', ' + str(0.5) + ')',
                            size=10,
                            line=dict(
                                color='black',
                                width=0.5,
                                      ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )
            if df2['Ice thaw'].notna()[2]:
                clim_iceoff = pd.to_datetime(df2.iloc[2]['Ice thaw'])
                df_ind_clim_off = df[df['time_all_dt'] == clim_iceoff].index
                logger.info('clim ice off index: %s', df_ind_clim_off.values)
                logger.info('clim ice off value: %s', clim_iceoff)
                fig.add_trace(
                    go.Scatter(
                        x=df['time_all_dt']
                        [df_ind_clim_off],
                        y=df['icecover_hist']
                        [df_ind_clim_off],
                        name='Climatology ice thaw',
                        hovertemplate='%{x}',
                        mode='markers',
                        marker=dict(
                            color='rgba(' +
                                      str(mcolors.to_rgb('black'))
                            .strip('()') + ', ' + str(0.5) + ')',
                            size=14,
                            symbol='x',
                            line=dict(
                                color='black',
                                width=0.5,
                                      ),
                        ),
                        showlegend=False,
                    ), row=1, col=i+1,
                )

    # subplot 2
    for i in range(len(prop.whichcasts)):
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].rmse_all,
                name='RMSE, entire domain',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='2',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('red'))
                    .strip('()') + ', ' + str(1) + ')',
                    width=2,
                ),
            ), row=2, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].rmse_either,
                name='RMSE, areas with ice only',
                hovertemplate='%{y:.2f}',
                legendgroup='2',
                showlegend=showlegend[i],
                mode='lines',
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('sienna'))
                    .strip('()') + ', ' + str(1) + ')',
                    width=2,
                ),
            ), row=2, col=i+1,
        )
    # subplot 3
    for i in range(len(prop.whichcasts)):
        window_length = 5  # Must be an odd number
        if len(dflist[i].SS) < 10:
            window_length = 3
        polyorder = 2
        s = pd.Series(dflist[i].SS)
        interpolated_s = s.bfill().ffill().interpolate()  # Fill NaNs
        if interpolated_s.isna().all() == False:
            smoothed_y = savgol_filter(interpolated_s, window_length, polyorder)
            # Reinsert NaNs
            smoothed_y[np.argwhere(np.isnan(dflist[i].SS))] = np.nan
            # Check for values > 1. Max value is 1!
            smoothed_y[smoothed_y > 1] = 1
        else:
            smoothed_y = interpolated_s
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=smoothed_y,
                name='Skill score, low-pass filter',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='3',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('grey'))
                    .strip('()') + ', ' + str(1) + ')',
                    width=2,
                ),
            ), row=3, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].SS,
                name='Daily skill score',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='3',
                showlegend=showlegend[i],
                line=dict(
                    color='rgba(' +
                    str(mcolors.to_rgb('darkviolet'))
                    .strip('()') + ', ' + str(0.5) + ')',
                    width=1,
                ),
            ), row=3, col=i+1,
        )
        fig.add_hline(
            y=0,
            annotation_text='<i>Model skill > climatology skill</i>',
            annotation_position='top left',
            annotation_font_color='green',
            annotation_font_size=12,
            row=3, col=i+1,
        )
        fig.add_hline(
            y=0,
            line_width=0,
            annotation_text='<i>Model skill < climatology skill</i>',
            annotation_position='bottom left',
            annotation_font_color='red',
            annotation_font_size=12,
            row=3, col=i+1,
        )

    # update axes
    title_text = [
        ['Mean ice conc. (%)', None],
        ['RMSE (%)', None],
        ['Skill score', None],
    ]
    for i in range(len(prop.whichcasts)):
        fig.update_yaxes(
            title_text=title_text[0][i],
            title_font=dict(size=16, color='black'),
            range=[0, 100],
            row=1, col=i+1,
        )
        fig.update_yaxes(
            title_text=title_text[1][i],
            title_font=dict(size=16, color='black'),
            range=[0, 100],
            row=2, col=i+1,
        )
        if np.isnan(dflist[i].SS).all() == False:
            ss_ymin = np.floor(np.nanmin(dflist[i].SS))
            if ss_ymin > -1:
                ss_ymin = -1
        elif np.isnan(dflist[i].SS).all():
            ss_ymin = -1
        fig.update_yaxes(
            title_text=title_text[2][i],
            title_font=dict(size=16, color='black'),
            range=[ss_ymin, 1],
            row=3, col=i+1,
        )

    fig.update_yaxes(
        showline=True, linewidth=1, linecolor='black',
        mirror=True,
    )
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        tickfont=dict(size=16),
        dtick=86400000*xtickspace,
        tickformat='%m/%d',
        range=[
            datetime.strptime(
                prop.start_date_full,
                '%Y-%m-%dT%H:%M:%SZ',
            ), time_all[-1],
        ],
        tick0=datetime.strptime(
            prop.start_date_full,
            '%Y-%m-%dT%H:%M:%SZ',
        ),
    )
    fig.update_xaxes(
        title_text='Time',
        title_font=dict(size=16, color='black'),
        # range=[-1, 1],
        tickangle=45,
        row=3,
    )
    # update layout
    figheight=700
    if len(prop.whichcasts) == 1:
        figwidth = 900
    elif len(prop.whichcasts) > 1:
        figwidth = 1300
    else:
        figwidth = 900
    fig.update_layout(
        title=dict(
            text=figtitle,
            font=dict(size=20, color='black'),
            y=1,  # new
            x=0.5, xanchor='center', yanchor='top',
        ),
        yaxis1=dict(tickfont=dict(size=16)),
        yaxis2=dict(tickfont=dict(size=16)),
        yaxis3=dict(tickfont=dict(size=16)),
        yaxis4=dict(tickfont=dict(size=16)),
        yaxis5=dict(tickfont=dict(size=16)),
        yaxis6=dict(tickfont=dict(size=16)),
        transition_ordering='traces first',
        dragmode='zoom',
        hovermode='x unified',
        height=figheight,
        width=figwidth,
        legend_tracegroupgap=120,
        xaxis_tickangle=-45,
        template='plotly_white',
        margin=dict(
            t=50, b=50,
        ),
        legend=dict(
            font=dict(size=16, color='black'),
            bgcolor='rgba(0,0,0,0)',
        ),
    )
    # write to file
    output_file = (
        f'{prop.visuals_stats_ice_path}/{prop.ofs}_'
        f'{titlewc}_iceconcseries'
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

    ######
    # Now do ice extents plot
    ######
    nrows = 2
    fig = make_subplots(
        rows=nrows, cols=len(prop.whichcasts), vertical_spacing=0.055,
        subplot_titles=(prop.whichcasts),
        shared_xaxes=True,
    )
    figtitle = prop.ofs.upper() + ' ' + 'ice extent and overlap, ' + \
        datestrbegin[0] + ' - ' + datestrend[0]
    for i in range(len(prop.whichcasts)):
        prop.whichcast = prop.whichcasts[i]
        # subplot 1
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].obs_extent,
                name='GLSEA ice extent',
                hovertemplate='%{y:.2f}',
                mode='lines',
                legendgroup='1',
                showlegend=showlegend[i],
                line=dict(
                     color='rgba(' +
                    str(mcolors.to_rgb('dodgerblue'))
                    .strip('()') + ', ' + str(1)+')',
                     width=2,
                     # dash='dash',
                ),
            ), row=1, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].mod_extent,
                name='OFS ice extent',
                hovertemplate='%{y:.2f}',
                legendgroup='1',
                showlegend=showlegend[i],
                mode='lines',
                line=dict(
                     color='rgba(' +
                    str(mcolors.to_rgb('magenta'))
                    .strip('()') + ', ' + str(1)+')',
                     width=2,
                     # dash='dash',
                ),
            ), row=1, col=i+1,
        )
    # subplot 2
    for i in range(len(prop.whichcasts)):
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=(dflist[i].csi_all)*100,
                name='<b>Critical Success Index</b><br>(GLSEA & OFS percent overlap)',
                hovertemplate='%{y:.2f}',
                legendgroup='2',
                showlegend=showlegend[i],
                mode='lines',
                line=dict(
                     color='rgba(' +
                    str(mcolors.to_rgb('lightseagreen'))
                    .strip('()') + ', ' + str(1)+')',
                     width=3,
                     # dash='dash',
                ),
            ), row=2, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].csi_falsealarms*100,
                name='<b>False alarms</b><br>(OFS ice, no GLSEA ice)',
                hovertemplate='%{y:.2f}',
                legendgroup='2',
                showlegend=showlegend[i],
                mode='lines',
                line=dict(
                     color='rgba(' +
                    str(
                        mcolors.to_rgb(
                            'purple',
                        ),
                    )
                    .strip('()') + ', ' + str(1)+')',
                    width=1.25,
                ),
                # showlegend=False
            ), row=2, col=i+1,
        )
        fig.add_trace(
            go.Scatter(
                x=dflist[i].time_all_dt,
                y=dflist[i].csi_misses*100,
                name='<b>Misses</b><br>(GLSEA ice, no OFS ice)',
                hovertemplate='%{y:.2f}',
                legendgroup='2',
                showlegend=showlegend[i],
                mode='lines',
                line=dict(
                     color='rgba(' +
                    str(
                        mcolors.to_rgb(
                            'red',
                        ),
                    )
                    .strip('()') + ', ' + str(1)+')',
                    width=1.25,
                ),
            ), row=2, col=i+1,
        )
    # update axes
    title_text = [
        ['Ice extent (%)', None],
        ['Percent of<br>combined ice extent', None],
    ]
    for i in range(len(prop.whichcasts)):
        fig.update_yaxes(
            title_text=title_text[0][i],
            title_font=dict(size=16, color='black'),
            range=[0, 100],
            row=1, col=i+1,
        )
        fig.update_yaxes(
            title_text=title_text[1][i],
            title_font=dict(size=16, color='black'),
            range=[0, 100],
            row=2, col=i+1,
        )
    fig.update_yaxes(
        showline=True, linewidth=1, linecolor='black',
        mirror=True,
    )
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        tickfont=dict(size=16),
        dtick=86400000*xtickspace,
        tickformat='%m/%d',
        range=[
            datetime.strptime(
                prop.start_date_full,
                '%Y-%m-%dT%H:%M:%SZ',
            ), time_all[-1],
        ],
    )
    fig.update_xaxes(
        title_text='Time',
        title_font=dict(size=16, color='black'),
        tickangle=45,
        row=2,
    )
    # update layout
    figheight=500
    if len(prop.whichcasts) == 1:
        figwidth = 900
    elif len(prop.whichcasts) > 1:
        figwidth = 1300
    else:
        figwidth = 900
    fig.update_layout(
        title=dict(
            text=figtitle,
            font=dict(size=20, color='black'),
            y=1,  # new
            x=0.5, xanchor='center', yanchor='top',
        ),
        yaxis1=dict(tickfont=dict(size=16)),
        yaxis2=dict(tickfont=dict(size=16)),
        yaxis3=dict(tickfont=dict(size=16)),
        yaxis4=dict(tickfont=dict(size=16)),
        legend_tracegroupgap=130,
        transition_ordering='traces first',
        dragmode='zoom',
        hovermode='x unified',
        height=figheight,
        width=figwidth,
        xaxis_tickangle=-45,
        template='plotly_white',
        margin=dict(t=50, b=50),
        legend=dict(
            font=dict(size=16, color='black'),
            bgcolor='rgba(0,0,0,0)',
        ),
    )
    # write to file
    output_file = (
        f'{prop.visuals_stats_ice_path}/{prop.ofs}_'
        f'{titlewc}_iceextentseries'
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


def create_1dplot_ice(prop, inventory, time_all, logger):
    '''Create 1D time series at obs station locations, and save a plot'''

    # First load nowcast and/or forecast stats from file!
    counter = 0
    for i in range(0, len(inventory['ID'])):
        dflist = []
        for cast in prop.whichcasts:
            df = None
            # Here we try to open the paired data set(s).
            # If not found, return without making plot
            prop.whichcast = cast.lower()
            if (
                os.path.isfile(
                    f'{prop.data_skill_ice1dpair_path}/'
                    f'{prop.ofs}_'
                    f'iceconc_'
                    f"{inventory.at[i,'ID']}_"
                    f"{inventory.at[i,'NODE']}_"
                    f'{prop.whichcast}_pair.int',
                ) is False
            ):
                logger.error(
                    'Ice 1D time series .int NOT found in %s. ',
                    prop.data_skill_ice1dpair_path,
                )

            # If csv is there, proceed
            else:
                counter = counter + 1  # Keep track of # whichcasts
                df = pd.read_csv(
                    r'' + f'{prop.data_skill_ice1dpair_path}/'
                          f'{prop.ofs}_'
                          f'iceconc_'
                          f"{inventory.at[i,'ID']}_"
                          f"{inventory.at[i,'NODE']}_"
                          f'{prop.whichcast}_pair.int',
                )
                logger.info(
                    'Ice 1D time series .int for %s found in %s',
                    inventory.at[i, 'ID'],
                    prop.data_skill_ice1dpair_path,
                )

                df['DateTime'] = pd.to_datetime(df['DateTime'])
            # Put nowcast & forecast together in list
            if df is not None:
                dflist.append(df)
        # Do station ice plots
        plotting_scalar_ice.oned_scalar_plot_ice(
            dflist,
            'ice_conc',
            [
                inventory.at[i, 'ID'],
                inventory.at[i, 'Name'],
                inventory.at[i, 'Source'],
            ],
            inventory.at[i, 'NODE'],
            prop, logger,
        )
