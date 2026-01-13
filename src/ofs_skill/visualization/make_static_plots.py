"""
Created on Thu Oct 23 12:35:41 2025

@author: PWL
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

import ofs_skill.visualization.plotting_functions as plotting_functions


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
        nwsline = f'NWS ID: {nws_id}'
    else:
        nwsline = ''

    return f'NOAA/NOS OFS Skill Assessment\n' \
        f'{station_id[2]} station: {station_id[1]} ' \
        f'({station_id[0]})\n' \
        f'OFS: {prop.ofs.upper()} Node ID: ' \
        f'{node} ' \
        + nwsline + \
        f'\nFrom: {start_date} ' \
        f'To: ' \
        f'{end_date}'


def scalar_plots(now_fores_paired, name_var, station_id, node, prop, logger):
    '''
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

    # --- Do plots, huzzah --------------------------------------------
    fig, axs = plt.subplots(2, 1)
    fig.set_figheight(8)
    fig.set_figwidth(12)
    fig.suptitle(figtitle, fontsize=16)
    # -----------------------------------------------------------------
    axs[0].plot(
        list(now_fores_paired[0].DateTime),
        list(now_fores_paired[0].OBS),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
    )
    for i in range(len(prop.whichcasts)):
        # Series names
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
            sdboxName = 'Forecast - Obs.'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
            sdboxName = 'Forecast_a - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
            sdboxName = 'Nowcast - Obs.'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'
            sdboxName = 'unknown'

        axs[0].plot(
            list(now_fores_paired[i].DateTime),
            list(now_fores_paired[i].OFS),
            label=seriesname,
            color=palette[i+1],
        )
        axs[1].plot(
            list(now_fores_paired[i].DateTime),
            [
                ofs - obs for ofs, obs in zip(
                    now_fores_paired[i].OFS,
                    now_fores_paired[i].OBS,
                )
            ],
            label=sdboxName,
            color=palette[i+1],
            linestyle='--',
        )
    axs[1].fill_between(
        list(now_fores_paired[i].DateTime),
        np.ones(len(list(now_fores_paired[i].DateTime)))*X1,
        np.ones(len(list(now_fores_paired[i].DateTime)))*-X1,
        alpha=0.1,
        linewidth=0,
        facecolor='orange',
        label='Target error range',
    )
    axs[1].fill_between(
        list(now_fores_paired[i].DateTime),
        np.ones(len(list(now_fores_paired[i].DateTime)))*2*X1,
        np.ones(len(list(now_fores_paired[i].DateTime)))*2*-X1,
        alpha=0.1,
        linewidth=0,
        facecolor='red',
        label='2x target error range',
    )

    axs[0].grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs[0].legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs[0].set_ylabel(plot_name, fontsize=16)
    axs[0].set_yticks(axs[0].get_yticks()[::1])
    axs[0].tick_params(axis='both', which='major', labelsize=12)
    plt.gcf().autofmt_xdate()

    axs[1].axhline(y=0, color='black', linewidth=1)
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    axs[1].grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs[1].legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs[1].set_ylim([-X1*3, X1*3])
    error_units = plot_name.split(' ')[-1]
    axs[1].set_ylabel('Error ' + error_units, fontsize=16)
    axs[1].set_xlabel('Time', fontsize=16)
    axs[1].set_yticks(axs[1].get_yticks()[::1])
    axs[1].tick_params(axis='y', which='major', labelsize=12)
    axs[1].tick_params(axis='x', which='major', labelsize=12)

    plt.gcf().autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.align_ylabels()
    naming_ws = '_'.join(prop.whichcasts)
    filename = f'{prop.ofs}_{station_id[0]}_{save_name}_timeseries_' +\
        f'{naming_ws}_{prop.ofsfiletype}.{image_type}'
    filepath = os.path.join(prop.om_files, filename)
    fig.savefig(filepath, format=image_type, dpi=200, bbox_inches='tight')


def vector_plots(now_fores_paired, name_var, station_id, node, prop, logger):
    '''
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

    # Get target error range
    if name_var != 'ice_conc':
        X1, _ = plotting_functions.get_error_range(name_var, prop, logger)

    figtitle = get_title_static(
        prop, node, station_id, name_var, logger,
    )

    # --- Do plots, huzzah --------------------------------------------
    fig, axs = plt.subplots(3, 1)
    fig.set_figheight(10)
    fig.set_figwidth(12)
    fig.suptitle(figtitle, fontsize=16)
    # -----------------------------------------------------------------

    axs[0].plot(
        list(now_fores_paired[0].DateTime),
        list(now_fores_paired[0].OBS_SPD),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
        linestyle='--',
    )
    axs[1].plot(
        list(now_fores_paired[0].DateTime),
        list(now_fores_paired[0].OBS_DIR),
        label='Observations',
        color=palette[0],
        linewidth=1.5,
        linestyle='--',
    )
    for i in range(len(prop.whichcasts)):
        # Series names
        if prop.whichcasts[i][-1].capitalize() == 'B':
            seriesname = 'Model Forecast Guidance'
            sdboxName = 'Forecast - Obs.'
        elif prop.whichcasts[i][-1].capitalize() == 'A':
            seriesname = 'Model Forecast Guidance, ' + prop.forecast_hr[:-2] +\
                'z cycle'
            sdboxName = 'Forecast_a - Obs.'
        elif prop.whichcasts[i].capitalize() == 'Nowcast':
            seriesname = 'Model Nowcast Guidance'
            sdboxName = 'Nowcast - Obs.'
        else:
            seriesname = prop.whichcasts[i].capitalize() + ' Guidance'
            sdboxName = 'unknown'

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
        axs[2].plot(
            list(now_fores_paired[i].DateTime),
            [
                ofs - obs for ofs, obs in zip(
                    now_fores_paired[i].OFS_SPD,
                    now_fores_paired[i].OBS_SPD,
                )
            ],
            label=sdboxName,
            color=palette[i+1],
            linestyle='--',
        )
    axs[2].fill_between(
        list(now_fores_paired[i].DateTime),
        np.ones(len(list(now_fores_paired[i].DateTime)))*X1,
        np.ones(len(list(now_fores_paired[i].DateTime)))*-X1,
        alpha=0.1,
        linewidth=0,
        facecolor='orange',
        label='Target error range',
    )
    axs[2].fill_between(
        list(now_fores_paired[i].DateTime),
        np.ones(len(list(now_fores_paired[i].DateTime)))*2*X1,
        np.ones(len(list(now_fores_paired[i].DateTime)))*2*-X1,
        alpha=0.1,
        linewidth=0,
        facecolor='red',
        label='2x target error range',
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
    axs[1].set_ylabel('Current direction\n(0-360 deg.)', fontsize=16)
    axs[1].set_yticks(axs[1].get_yticks()[::1])
    axs[1].tick_params(axis='both', which='major', labelsize=12)
    plt.gcf().autofmt_xdate()

    axs[2].axhline(y=0, color='black', linewidth=1)
    axs[2].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    axs[2].grid(True, color='grey', linestyle='--', linewidth=0.5)
    axs[2].legend(
        loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12,
        frameon=False,
    )
    axs[2].set_ylim([-X1*3, X1*3])
    axs[2].set_ylabel('Speed error\n(m/s)', fontsize=16)
    axs[2].set_xlabel('Time', fontsize=16)
    axs[2].set_yticks(axs[2].get_yticks()[::1])
    axs[2].tick_params(axis='y', which='major', labelsize=12)
    axs[2].tick_params(axis='x', which='major', labelsize=12)

    plt.gcf().autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.align_ylabels()
    naming_ws = '_'.join(prop.whichcasts)
    filename = f'{prop.ofs}_{station_id[0]}_currents_timeseries_{naming_ws}_' \
        + f'{prop.ofsfiletype}.{image_type}'
    filepath = os.path.join(prop.om_files, filename)
    fig.savefig(filepath, format=image_type, dpi=200, bbox_inches='tight')
    plt.close('all')
