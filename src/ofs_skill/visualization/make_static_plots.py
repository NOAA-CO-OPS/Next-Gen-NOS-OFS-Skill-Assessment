"""
Created on Thu Oct 23 12:35:41 2025

@author: PWL
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import UTC, datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import ofs_skill.visualization.plotting_functions as plotting_functions


def combine_obs_across_casts(now_fores_paired, prop):
    """Combine observations from multiple casts, dedup, and filter by date range."""
    obs_df = None
    for i in range(len(now_fores_paired)):
        obs_df = pd.concat([obs_df, now_fores_paired[i]], ignore_index=True)
        obs_df = obs_df.drop_duplicates(subset=['DateTime'], ignore_index=True)
        if 'nowcast' in prop.whichcasts and 'forecast_a' in prop.whichcasts:
            pass
        else:
            try:
                start_dt = datetime.strptime(prop.start_date_full, '%Y%m%d-%H:%M:%S')
                end_dt = datetime.strptime(prop.end_date_full, '%Y%m%d-%H:%M:%S')
            except ValueError:
                start_dt = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
                end_dt = datetime.strptime(prop.end_date_full, '%Y-%m-%dT%H:%M:%SZ')
            obs_df = obs_df.loc[((obs_df['DateTime'] >= start_dt) & (obs_df['DateTime'] <= end_dt))]
            now_fores_paired[i] = now_fores_paired[i].loc[((
                now_fores_paired[i].DateTime >= start_dt) & (now_fores_paired[i].DateTime <= end_dt))]
    return obs_df, now_fores_paired


def add_nowcast_forecast_vline(axs, prop, now_fores_paired, nrows=1):
    """Add nowcast-forecast transition line to matplotlib axes."""
    max_datetime = now_fores_paired[0].DateTime.max().replace(tzinfo=UTC)
    for i in range(len(now_fores_paired)):
        if now_fores_paired[i].DateTime.max() > now_fores_paired[0].DateTime.max():
            max_datetime = now_fores_paired[i].DateTime.max().replace(tzinfo=UTC)
    if max_datetime > datetime.now(UTC) and 'nowcast' in prop.whichcasts:
        try:
            dt_n = datetime.strptime(prop.start_date_full, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            dt_n = datetime.strptime(prop.start_date_full, '%Y%m%d-%H:%M:%S')
        if nrows == 1:
            axes_list = [axs]
        else:
            axes_list = [axs[i] for i in range(nrows)]
        for ax in axes_list:
            ax.axvline(x=dt_n, color='r', linestyle='--', lw=1)
            ax.annotate('Nowcast-forecast transition',
                xy=(dt_n, ax.get_ylim()[1]),
                xytext=(85, 0),
                textcoords='offset points',
                ha='right',
                va='bottom',
                fontsize=10,
                color='r')


def get_title_static(prop, node, station_id, name_var, logger):
    '''Returns plot title'''
    # If incoming date format is YYYY-MM-DDTHH:MM:SSZ, the chunk below will
    # take out the 'Z' and 'T' to correctly format the date for plotting.
    if 'Z' in prop.start_date_full and 'Z' in prop.end_date_full:
        start_date = prop.start_date_full.replace('Z', '')
        end_date = prop.end_date_full.replace('Z', '')
        start_date = start_date.replace('T', ' ')
        end_date = end_date.replace('T', ' ')
    # If the format is YYYYMMDD-HH:MM:SS, the chunk below will format correctly
    else:
        start_date = datetime.strptime(prop.start_date_full, '%Y%m%d-%H:%M:%S')
        end_date = datetime.strptime(prop.end_date_full, '%Y%m%d-%H:%M:%S')
        start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

    # Get the NWS ID (shefcode) if CO-OPS station -- all CO-OPS stations have
    # 7-digit ID
    if station_id[2] == 'CO-OPS' and name_var != 'cu':
        metaurl =\
        'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/' +\
        str(station_id[0]) + '.json?units=metric'
        try:
            with urllib.request.urlopen(metaurl) as url:
                metadata = json.load(url)
            nws_id = metadata['stations'][0]['shefcode']
        except Exception as e:
            logger.error(f'Exception in get_title when getting nws id: {e}')
            nws_id = 'NA'
        nwsline = f'NWS ID: {nws_id}   '
    else:
        nwsline = ''

    return f'NOAA/NOS OFS Skill Assessment\n' \
        f'{station_id[2]} station: {station_id[1]} ' \
        f'({station_id[0]})\n' \
        f'OFS: {prop.ofs.upper()}    Node ID: ' \
        f'{node}    ' \
        + nwsline + \
        f'\nFrom: {start_date}  ' \
        f'To: ' \
        f'{end_date}'


def scalar_plots(now_fores_paired, name_var, station_id, node, prop, logger):
    '''
    1D static plots for the O&M/overview dashboard. Writes plots to a .png file
    '''

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = (len(prop.whichcasts)*1) + 1
    palette, palette_rgb = plotting_functions.make_cubehelix_palette(
        ncolors, 2.5, 0.9, 0.65,
    )
    image_type = 'png'
    dpi = 100

    # Get target error range
    if name_var != 'ice_conc':
        X1, _ = plotting_functions.get_error_range(name_var, prop, logger)

    # Settings and stuff
    if name_var == 'wl':
        plot_name = 'Water Level ' + f'at {prop.datum} (meters)'
        save_name = 'water_level'
    elif name_var == 'temp':
        plot_name = 'Water Temperature (\u00b0C)'
        save_name = 'water_temperature'
    elif name_var == 'salt':
        plot_name = 'Salinity (PSU)'
        save_name = 'salinity'
    elif name_var == 'ice_conc':
        plot_name = 'Ice Concentration (%)'
        save_name = 'ice_concentration'

    figtitle = get_title_static(
        prop, node, station_id, name_var, logger,
    )

    # Combine obs from different casts into one main obs array
    obs_df, now_fores_paired = combine_obs_across_casts(now_fores_paired, prop)

    # --- Do plots, huzzah --------------------------------------------
    fig, axs = plt.subplots(1, 1)
    fig.set_figheight(5)
    fig.set_figwidth(12)
    fig.suptitle(figtitle, fontsize=16)
    # -----------------------------------------------------------------
    axs.plot(
        list(obs_df.DateTime),
        list(obs_df.OBS),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
    )
    for i in range(len(prop.whichcasts)):
        # Series names
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance,\n' + prop.forecast_hr[:-1] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'

        axs.plot(
            list(now_fores_paired[i].DateTime),
            list(now_fores_paired[i].OFS),
            label=seriesname,
            color=palette[i+1],
        )

    axs.grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs.legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs.set_ylabel(plot_name, fontsize=14)
    axs.set_yticks(axs.get_yticks()[::1])
    axs.tick_params(axis='both', which='major', labelsize=12)
    axs.set_xlabel('Time', fontsize=14)

    # Check if end datetime is > current date
    add_nowcast_forecast_vline(axs, prop, now_fores_paired, nrows=1)
    plt.gcf().autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.align_ylabels()
    naming_ws = '_'.join(prop.whichcasts)
    filename = f'{prop.ofs}_{station_id[0]}_{save_name}_timeseries_' +\
        f'{naming_ws}_{prop.ofsfiletype}.{image_type}'
    filepath = os.path.join(prop.om_files, filename)
    fig.savefig(filepath, format=image_type, dpi=dpi, bbox_inches='tight')


def vector_plots(now_fores_paired, name_var, station_id, node, prop, logger):
    '''
    Static 1D vector plots used for the O&M/overview dashboard.
    Writes plots to a .png file
    '''

    '''
    Make a color palette with entries for each whichcast plus observations.
    The 'cubehelix' palette linearly varies hue AND intensity
    so that colors can be distingushed by colorblind users or in greyscale.
    '''
    ncolors = (len(prop.whichcasts)*1) + 1
    palette, palette_rgb = plotting_functions.make_cubehelix_palette(
        ncolors, 2.5, 0.9, 0.65,
    )
    image_type = 'png'
    dpi = 100

    # Get target error range
    if name_var != 'ice_conc':
        X1, _ = plotting_functions.get_error_range(name_var, prop, logger)

    # Combine obs from different casts into one main obs array
    obs_df, now_fores_paired = combine_obs_across_casts(now_fores_paired, prop)

    figtitle = get_title_static(
        prop, node, station_id, name_var, logger,
    )

    # --- Do plots, huzzah --------------------------------------------
    nrows = 2
    fig, axs = plt.subplots(nrows, 1)
    fig.set_figheight(8)
    fig.set_figwidth(12)
    fig.suptitle(figtitle, fontsize=16)
    # -----------------------------------------------------------------

    axs[0].plot(
        list(obs_df.DateTime),
        list(obs_df.OBS_SPD),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
        linestyle='--',
    )
    axs[1].plot(
        list(obs_df.DateTime),
        list(obs_df.OBS_DIR),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
        linestyle='--',
    )
    for i in range(len(prop.whichcasts)):
        # Series names
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance,\n' + prop.forecast_hr[:-1] +\
                'z cycle'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'

        axs[0].plot(
            list(now_fores_paired[i].DateTime),
            list(now_fores_paired[i].OFS_SPD),
            label=seriesname,
            color=palette[i+1],
        )
        axs[1].plot(
            list(now_fores_paired[i].DateTime),
            list(now_fores_paired[i].OFS_DIR),
            label=seriesname,
            color=palette[i+1],
        )


    axs[0].grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs[0].legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs[0].set_ylabel('Current speed\n(m/s)', fontsize=16)
    axs[0].set_yticks(axs[0].get_yticks()[::1])
    axs[0].tick_params(axis='both', which='major', labelsize=12)
    plt.gcf().autofmt_xdate()

    axs[1].grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs[1].legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs[1].set_ylabel('Current direction\n(0-360 deg.)', fontsize=14)
    axs[1].set_yticks(axs[1].get_yticks()[::1])
    axs[1].tick_params(axis='both', which='major', labelsize=12)
    plt.gcf().autofmt_xdate()
    add_nowcast_forecast_vline(axs, prop, now_fores_paired, nrows=nrows)

    axs[1].set_xlabel('Time', fontsize=16)

    plt.gcf().autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.align_ylabels()
    naming_ws = '_'.join(prop.whichcasts)
    filename = f'{prop.ofs}_{station_id[0]}_currents_timeseries_{naming_ws}_' \
        + f'{prop.ofsfiletype}.{image_type}'
    filepath = os.path.join(prop.om_files, filename)
    fig.savefig(filepath, format=image_type, dpi=dpi, bbox_inches='tight')
    plt.close('all')


def bar_plots(data, info, ytitle, prop, logger):
    '''
    Writes static bar plots from the forecast horizon skill module for the O&M
    overview dashboard.
    Writes plots to a .png file.
    '''

    image_type = 'png'
    dpi = 100

    #Format y axis label
    ytitle=ytitle.replace('<br>', '\n').replace('<i>','').replace(' or error','')

    # Get title
    figtitle = get_title_static(prop, info[1], [info[2],info[3],info[4]], info[0], logger)

    # Get target error range
    X1, _ = plotting_functions.get_error_range(info[0], prop, logger)

    # --- Do plots, huzzah --------------------------------------------
    fig, axs = plt.subplots(2, 1)
    fig.set_figheight(14)
    fig.set_figwidth(12)
    fig.suptitle(figtitle, fontsize=22)
    # -----------------------------------------------------------------
    # --- First Subplot ---

    # Bar colors based on threshold, also set ymax
    colors = []
    if 'RMSE' in ytitle:
        namestat = 'rmse'
        # Set y max limit
        ymaxmult = np.ceil(np.nanmax(data[1][0])/X1)
        if ymaxmult < 2:
            ymaxmult = 2
        ymax = X1*ymaxmult
        axhline = X1
        axhlinetext = 'Target error range'
        for value in data[1][0]:
            if -X1 <= value <= X1:
                colors.append('palegreen')
            else:
                colors.append('lightcoral')
    elif 'central frequency' in ytitle:
        namestat = 'cfreq'
        ymax = 100
        axhline = 90
        axhlinetext = '90% acceptance criteria'
        for value in data[1][0]:
            if value >= 90:
                colors.append('palegreen')
            else:
                colors.append('lightcoral')
    axs[0].bar(data[0][0],
               data[1][0],
               color=colors,
               edgecolor='black',
               linewidth=1
               )
    axs[0].set_ylabel(ytitle,
                      fontsize=20,
                      )
    axs[0].tick_params(axis='y', labelsize=18)
    axs[0].set_xlabel('Forecast horizon (hours)',
                      fontsize=20,
                      )
    axs[0].set_xticklabels(data[0][0],
                           rotation=45,
                           ha='right',
                           fontsize=18,
                           )
    axs[0].set_ylim(0, ymax) # Set a consistent y-limit for comparison
    axs[0].axhline(y=axhline,
                   color='red',
                   linewidth=1,
                   linestyle='--',
                   label=axhlinetext)
    # Display the legend to show the label
    axs[0].legend(fontsize=18,
                  #frameon=False,
                  loc='lower right',
                  facecolor='white',
                  framealpha=0.75
                  )

    # --- Second Subplot ---
    # Bar colors based on threshold
    colors = []
    if 'RMSE' in ytitle:
        # Set y max limit
        ymaxmult = np.ceil(np.nanmax(data[1][1])/X1)
        if ymaxmult < 2:
            ymaxmult = 2
        ymax = X1*ymaxmult
        for value in data[1][1]:
            if -X1 <= value <= X1:
                colors.append('palegreen')
            else:
                colors.append('lightcoral')
    elif 'central frequency' in ytitle:
        for value in data[1][1]:
            if value >= 90:
                colors.append('palegreen')
            else:
                colors.append('lightcoral')
    axs[1].bar(data[0][1],
               data[1][1],
               color=colors,
               edgecolor='black',
               linewidth=1
               )
    axs[1].set_xticklabels(data[0][1],
                           rotation=45,
                           ha='right',
                           fontsize=18,
                           )
    axs[1].set_xlabel('Model cycle',
                      fontsize=20,)
    axs[1].set_ylabel(ytitle,
                      fontsize=20,
                      )
    axs[1].tick_params(axis='y', labelsize=18)
    axs[1].set_ylim(0, ymax) # Set a consistent y-limit
    axs[1].axhline(y=axhline,
                   color='red',
                   linestyle='--',
                   linewidth=1,
                   label=axhlinetext
                   )
    axs[1].legend(fontsize=18,
                  #frameon=False,
                  loc='lower right',
                  facecolor='white',
                  framealpha=0.75
                  )


    # Adjust layout to prevent titles/labels from overlapping
    # Increase vertical space
    plt.tight_layout(pad=3)
    filename = f'{prop.ofs}_{info[2]}_{info[7]}_{namestat}_bars.{image_type}'
    filepath = os.path.join(prop.om_files, filename)
    fig.savefig(filepath, format=image_type, dpi=dpi, bbox_inches='tight')
    plt.close('all')
