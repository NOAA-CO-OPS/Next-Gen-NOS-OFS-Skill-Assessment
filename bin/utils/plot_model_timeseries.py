"""
Created on Fri Nov 21 10:38:14 2025

@author: PWL
"""

from __future__ import annotations

import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils


def get_plot_title(plotinfo, logger):
    '''
    '''
    start_str = datetime.strftime(plotinfo['start'],'%Y/%m/%d %H:%M:%S')
    end_str = datetime.strftime(plotinfo['end'],'%Y/%m/%d %H:%M:%S')


    return f'<b>{plotinfo["ofs"].upper()} {plotinfo["whichcast"]} time series<br>' \
            f'Station ID:&nbsp;{plotinfo["station_id"]} &nbsp;&nbsp;&nbsp;' \
            f'Node ID:&nbsp;{plotinfo["node"]}' \
            f'<br>From:&nbsp;{start_str}' \
            f'&nbsp;&nbsp;&nbsp;To:&nbsp;' \
            f'{end_str}<b>'

def get_variable_names(name_var):
    '''
    '''
    if name_var == 'wl':
        plot_name = 'Water Level (<i>meters<i>)'
        save_name = 'water_level'
    elif name_var == 'temp':
        plot_name = 'Water Temperature (<i>\u00b0C<i>)'
        save_name = 'water_temperature'
    elif name_var == 'salt':
        plot_name = 'Salinity (<i>PSU<i>)'
        save_name = 'salinity'
    elif name_var == 'cu':
        plot_name = ['Current speed<br>(<i>meters/second<i>)',
                     'Current direction<br>(<i>0-360 deg.<i>)']
        save_name = 'currents'

    return plot_name, save_name

def main(logger):
    '''This function plot time series from .prd files'''

    # Directories from conf file
    dir_params = utils.Utils().read_config_section('directories', logger)
    home_path = dir_params['home']
    # Logger
    if logger is None:
        config_file = utils.Utils().get_config_file()
        log_config_file = 'conf/logging.conf'
        log_config_file = os.path.join(Path(home_path), log_config_file)

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit()

        # Creater logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using config %s', config_file)
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting Visualization Process ---')
    prd_folder = os.path.join(
        home_path, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir'], )
    save_path = os.path.join(prd_folder,'prd_plots')
    os.makedirs(save_path, exist_ok=True)

    # First look in folder and get list of all .prd files.
    all_files = os.listdir(prd_folder)
    # Filter for .prd files
    all_prd = [item for item in all_files if '.prd' in item]

    # Plot settings
    ncols = 1

    # Now loop through all files and make plot for each one
    if len(all_prd) > 0:
        for file in all_prd:
            try:
                # Reset plot settings to default
                sharexaxis = False
                nrows = 1
                # Make full filepath
                file_path = os.path.join(prd_folder,file)
                # Parse .prd filename into dict
                plotinfo = {
                    'station_id': file.split('_')[0],
                    'ofs': file.split('_')[1],
                    'var_name': file.split('_')[2],
                    'node': file.split('_')[3],
                    'whichcast': file.split('_')[4]
                    }

                # Get long variable names
                plotinfo['plot_name'], plotinfo['save_name'] = \
                    get_variable_names(plotinfo['var_name'])
                # Load data
                try:
                    df = pd.read_csv(file_path, sep=r'\s+', header=None)
                except FileNotFoundError:
                    logger.error('No .prd file found! Moving to next file...')
                    continue
                # Reformat df
                df['DateTime'] = pd.to_datetime(
                    dict(
                        year=df[1],
                        month=df[2],
                        day=df[3],
                        hour=df[4],
                        minute=df[5],
                    )
                )
                df = df.rename(columns={6: 'OFS'})
                if plotinfo['var_name'] == 'cu':
                    df = df.rename(columns={7: 'OFS_DIR'})
                    nrows = 2
                    sharexaxis=True
                # Figure stuff
                fig = make_subplots(rows=nrows,
                                    cols=ncols,
                                    shared_xaxes=sharexaxis)
                fig.add_trace(
                    go.Scattergl(
                        x=df['DateTime'],
                        y=df['OFS'],
                        showlegend=False,
                        hovertemplate='%{y:.2f}',
                        mode='lines',
                        opacity=1,
                        connectgaps=False,
                        line=dict(color='red',
                                  width=1.5,
                                  ),
                    ), 1, 1,
                )
                if nrows == 2:
                    fig.add_trace(
                        go.Scattergl(
                            x=df['DateTime'],
                            y=df['OFS_DIR'],
                            showlegend=False,
                            hovertemplate='%{y:.2f}',
                            mode='lines',
                            opacity=1,
                            connectgaps=False,
                            line=dict(color='blue',
                                      width=1.5,
                                      ),
                        ), 2, 1,
                    )
                # Figure Config
                figheight = 500
                yoffset = 1.01
                plotinfo['start'] = df['DateTime'].iloc[0]
                plotinfo['end'] = df['DateTime'].iloc[-1]
                fig.update_layout(
                    title=dict(
                        text=get_plot_title(plotinfo, logger),
                        font=dict(size=14, color='black', family='Open Sans'),
                        y=0.97,  # new
                        x=0.5, xanchor='center', yanchor='top',
                    ),
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
                    ),
                    yaxis2=dict(
                        mirror=True,
                        ticks='inside',
                        showline=True,
                        linecolor='black',
                        linewidth=1,
                    ),
                    transition_ordering='traces first', dragmode='zoom',
                    hovermode='x unified', height=figheight, width=900,
                    template='plotly_white', margin=dict(
                        t=100, b=100,
                    ),
                    # legend=dict(
                    #     orientation='h', yanchor='bottom',
                    #     y=yoffset, xanchor='left', x=-0.05,
                    #     itemsizing='constant',
                    #     font=dict(
                    #         family='Open Sans',
                    #         size=12,
                    #         color='black',
                    #     ),
                    # ),
                )

                # Set x-axis moving bar
                fig.update_xaxes(
                    showspikes=True,
                    spikemode='across',
                    spikesnap='cursor',
                    showline=True,
                    showgrid=True,
                    tickfont_family='Open Sans',
                    tickfont_color='black',
                    tickfont=dict(size=14),
                )
                # Set y-axes titles
                if nrows == 2:
                    fig.update_yaxes(
                        mirror=True,
                        title_text=plotinfo['plot_name'][1],
                        titlefont_family='Open Sans',
                        title_font_color='black',
                        tickfont_family='Open Sans',
                        tickfont_color='black',
                        tickfont=dict(size=14),
                        row=2, col=1,
                    )
                    plotinfo['plot_name'] = plotinfo['plot_name'][0]
                fig.update_yaxes(
                    mirror=True,
                    title_text=f'{plotinfo["plot_name"]}',
                    titlefont_family='Open Sans',
                    title_font_color='black',
                    tickfont_family='Open Sans',
                    tickfont_color='black',
                    tickfont=dict(size=14),
                    row=1, col=1,
                )

                # Save file
                fig.write_html(
                    r'' + save_path + f'/{plotinfo["ofs"]}_'
                    f'{plotinfo["station_id"]}_{plotinfo["save_name"]}_modelseries_'
                    f'{plotinfo["whichcast"]}.html',
                )
                logger.info('Completed %s plot for %s!',
                            plotinfo['var_name'],
                            plotinfo['station_id'])
            except Exception as ex:
                logger.error('Caught exception: %s', ex)
    else:
        logger.info('No .prd output files found! No plots were made.')

if __name__ == '__main__':
    main(None)
