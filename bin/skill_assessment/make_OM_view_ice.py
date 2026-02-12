"""
Created on Thu Sep 18 08:50:00 2025

This script aggregates skill assessment output from all OFS, and makes overview
plots and tables. More specifically,
    1) .int files for all OFS and all variables are collected to make overview
    scorecard plots;
    2) CSV skill tables for all OFS and all variables are combined to one
    master plotly table & summary central frequency plot;
    3) plotly html plots for all OFS and all variables are converted to static
    .png images, and collected into a PDF.


@author: PWL
"""
from __future__ import annotations

import argparse
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.signal import savgol_filter
import matplotlib.colors as mcolors

from ofs_skill.model_processing import model_properties
from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization import plotting_functions


def parameter_validation(argu_list, logger):
    """ Parameter validation """

    path, whichcast, ofs_extents_path = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
    )

    # path validation
    if not os.path.exists(ofs_extents_path):
        error_message = f"""ofs_extents/ folder is not found. Please
        check path - {path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # whichcast validation
    if (
        'nowcast' not in whichcast and
        'forecast_b' not in whichcast and
        'forecast_a' not in whichcast and
        'all' not in whichcast
    ):
        error_message = f'Incorrect whichcast: {whichcast}! Exiting.'
        logger.error(error_message)
        sys.exit(-1)


def list_ofs():
    '''
    Returns a list of all OFS, sorted alphabetically
    '''
    return ['leofs', 'lmhofs', 'loofs', 'lsofs']


def is_df_nans(df):
    '''
    Checks if stats dataframe is full of nans, which happens if there are
    no .int files available to collect.
    '''
    df_indexed = df.set_index('ofs')
    return df_indexed.isna().all().all()


def make_bar_plots(df1, df2, prop, cast, logger):
    '''
    Takes a pandas dataframe, prop, cast (nowcast or forecast_b) and writes
    a summary scorecard plotly plot that includes all OFS and all variables
    '''
    col_labels = df1['ofs']
    col_labels = [item.upper() for item in col_labels]
    # Title
    titlestr = 'GLOFS ' + cast.rstrip('_b') + ' ice skill overview,<br>' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard
    # Marker (OFS) colors
    ofs_colors = ['#e377c2','#17becf','#ff7f0e','#9467bd']
    # Outline (stat) colors
    csi_stat_colors = ['rgba(' +
                       str(mcolors.to_rgb('lightseagreen'))
                       .strip('()') + ', ' + str(1)+')','rgba(' +
                       str(mcolors.to_rgb('purple'))
                       .strip('()') + ', ' + str(0.75)+')','rgba(' +
                       str(mcolors.to_rgb('coral'))
                       .strip('()') + ', ' + str(0.75)+')']
    rmse_stat_colors = ['rgba(' +
                        str(mcolors.to_rgb('grey'))
                        .strip('()') + ', ' + str(0.75) + ')']
    # Make figure
    fig = make_subplots(
        rows=2, cols=1, vertical_spacing=0.06,
        shared_xaxes=True,
    )
    stat_list = df1.columns[1:]
    for i,stat in enumerate(stat_list):
        fig.add_trace(
            go.Bar(
                x=col_labels,
                y=df1[stat],
                name=stat,
                width=0.5,
                marker_color=csi_stat_colors[i],
                marker_line_color=ofs_colors,
                marker_line_width=0,
                textposition='outside',
            ),
            row=1, col=1,
        )
    stat_list = ['RMSE, all']
    for i,stat in enumerate(stat_list):
        fig.add_trace(
            go.Bar(
                x=col_labels,
                y=df2[stat],
                name=stat,
                width=0.5,
                marker_color=rmse_stat_colors[i],
                marker_line_color=ofs_colors,
                marker_line_width=0,
                textposition='outside',
            ),
            row=2, col=1,
        )

    # # Update layout for shared color axis and overall figure properties
    figwidth = 600
    figheight = figwidth*1
    fig.update_layout(
        bargap=0.2,
        barmode='stack',
        title_text=titlestr,
        title_x=0.5,
        title_y=0.96,
        title_font=dict(size=24, color='black', family='Open Sans'),
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin={'l': 50, 'r': 50, 'b': 100, 't': 100},
        width=figwidth, height=figheight,
        autosize=False,
    )
    fig.update_yaxes(
        tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        }, title_text='CSI',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        #scaleanchor='x',
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        range=[0,1],
        row=1, col=1,
    )
    fig.update_yaxes(
        tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        }, title_text='Ice conc. RMSE (%)',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        #scaleanchor='x',
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        range=[0,int(np.ceil(np.nanmax(df2['RMSE, all'].values)/10))*10],
        row=2, col=1,
    )
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        row=1, col=1,
    )
    fig.update_xaxes(
        tickangle=45,
        tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        },
        title_text='OFS',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        row=2, col=1,
    )

    # Write to file
    # prop.stat = prop.stat.rstrip('*')
    output_file = (
        f'{prop.om_files}/bars_ice_{cast}_'
        f'_all_GLOFS'
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
    fig.write_html(output_file+'.html',config=fig_config,auto_open=False)
    logger.debug(f'Finished writing file: {output_file}')
    logger.info('Wrote bar plot to file for %s', cast)

def make_scatter_plot(prop, df, cast, logger):
    nrows = 2
    ncols = 2
    # Do stats time series
    fig = make_subplots(
        rows=nrows, cols=ncols, vertical_spacing=0.1,
        horizontal_spacing=0.1
    )

    figtitle = 'GLOFS ' + cast.strip('_b') + ' ice conc.,<br>' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard
    ofs_colors = ['#e377c2','#17becf','#ff7f0e','#9467bd']
    x_names = ['', 'GLSEA daily mean ice conc. (%)']
    y_names = ['OFS daily mean ice conc. (%)', '']
    rownum = [1,1,2,2]
    colnum = [1,2,1,2]
    figheight=600
    figwidth=figheight*1.08
    for i,ofs in enumerate(list_ofs()):
        # Filter df by ofs
        df_filt = df[df['OFS'] == ofs]
        axmax = 10*(np.ceil((df_filt[[
            'mod_meanicecover', 'obs_meanicecover']].max().max())/10))
        if df_filt.empty:
            # Add annotation that OFS is missing/has no data
            fig.add_annotation(
                    text=f'No data for {ofs}!',
                    xref='x domain', yref='y domain',
                    font=dict(size=14, color='red'),
                    x=0, y=0.0,
                    showarrow=False,
                    row=rownum[i], col=colnum[i],
            )
            axmax = 1
            #continue

        fig.add_trace(
            go.Scatter(
                x=df_filt['obs_meanicecover'],
                y=df_filt['mod_meanicecover'],
                name=ofs.upper(),
                mode='markers',
                marker=dict(
                    color=ofs_colors[i], # Sets all markers to red
                    #size=10,
                    line=dict(
                        color='black',
                        width=0
                        )
                    ),
                hovertemplate='GLSEA: %{x:.2f}<br>OFS: %{y:.2f}<extra></extra>',
                ), row=rownum[i], col=colnum[i]
            )
        fig.update_yaxes(
            title_text=y_names[colnum[i]-1],
            title_font=dict(size=16, color='black'),
            range=[0,axmax],
            tickfont=dict(size=16, color='black'),
            row=rownum[i], col=colnum[i]
        )
        fig.update_xaxes(
            title_text=x_names[rownum[i]-1],
            title_font=dict(size=16, color='black'),
            range=[0,axmax],
            tickfont=dict(size=16, color='black'),
            row=rownum[i], col=colnum[i]
        )
    fig.update_yaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
    )
    fig.update_xaxes(
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
    )
    fig.update_layout(
        title=dict(
            text=figtitle,
            font=dict(size=20,
                      color='black'),
            y=0.95,  # new
            x=0.5,
            #automargin=True,
            xanchor='center',
            yanchor='top',
        ),
        legend_tracegroupgap=130,
        transition_ordering='traces first',
        dragmode='zoom',
        #hovermode='x unified',
        height=figheight,
        width=figwidth,
        autosize=False,
        yaxis=dict(
            scaleanchor="x",
            scaleratio=1,
            ),
        template='plotly_white',
        margin=dict(t=75, b=50, l=50, r=50),
        legend=dict(
            font=dict(size=16, color='black'),
            bgcolor='rgba(0,0,0,0)',
        ),
    )
    # Write to file
    output_file = (
        f'{prop.om_files}/scatter_ice_{cast}_'
        f'all_GLOFS'
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
    fig.write_html(output_file+'.html',config=fig_config,auto_open=False)
    logger.debug(f'Finished writing file: {output_file}')
    logger.info('Wrote GLOFS scatter plot to file for %s', cast)

def make_summary_series(prop, df, cast, logger):
    '''
    '''
    nrows = 2
    ncols = 2
    # Do stats time series
    fig = make_subplots(
        rows=nrows, cols=ncols, vertical_spacing=0.075,
        subplot_titles='',
        shared_xaxes=True,
    )
    figtitle = 'GLOFS ' + cast.strip('_b') + ' ice skill summary, ' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard
    #showlegend = [True, False]
    ofs_colors = ['#e377c2','#17becf','#ff7f0e','#9467bd']
    list_stats = ['obs_meanicecover',
                  'mod_meanicecover',
                  'csi_all',
                  'rmse_all']
    stats_names = ['GLSEA mean <br>ice conc. (%)',
                   'OFS mean <br>ice conc. (%)',
                   'CSI (smoothed)',
                   'RMSE ice conc. (%)']
    rownum = [1,1,2,2]
    colnum = [1,2,1,2]
    ymin = [0,0,0,0]
    ymax = [100,100,1,100]
    figheight=500
    figwidth=figheight*2.5
    showlegend = [True, False, False, False]
    for i,ofs in enumerate(list_ofs()):
        # Filter df by ofs
        df_filt = df[df['OFS'] == ofs]
        if df_filt.empty:
            continue
        for j,stat in enumerate(list_stats):
            if stat == 'csi_all':
                # Do low-pass filter
                window_length = 7  # Must be an odd number
                if len(df_filt['csi_all']) < 10:
                    window_length = 3
                polyorder = 2
                s = pd.Series(df_filt['csi_all'])
                interpolated_s = s.bfill().ffill().interpolate()  # Fill NaNs
                if interpolated_s.isna().all() == False:
                    smoothed_y = savgol_filter(interpolated_s,
                                               window_length,
                                               polyorder)
                    # Reinsert NaNs
                    smoothed_y[np.argwhere(np.isnan(df_filt['csi_all']))] = np.nan
                    # Check for values > 1. Max value is 1!
                    smoothed_y[smoothed_y > 1] = 1
                    df_filt['csi_all'] = smoothed_y
                else:
                    df_filt['csi_all'] = interpolated_s
            # Done with low-pass filter
            # Start figs
            fig.add_trace(
                go.Scatter(
                    x=df_filt['time_all_dt'],
                    y=df_filt[stat],
                    name=ofs.upper(),
                    hovertemplate='%{y:.2f}',
                    mode='lines',
                    #legendgroup='1',
                    showlegend=showlegend[j],
                    line=dict(color=ofs_colors[i],
                         width=2,
                         # dash='dash',
                    ),
                ), row=rownum[j], col=colnum[j],
            )
            fig.update_yaxes(
                title_text=stats_names[j],
                title_font=dict(size=16, color='black'),
                range=[ymin[j], ymax[j]],
                row=rownum[j], col=colnum[j],
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
        tickfont=dict(size=16, color='black'),
        tickangle=45
    )
    fig.update_layout(
        title=dict(
            text=figtitle,
            font=dict(size=20, color='black'),
            y=0.97,  # new
            x=0.5, xanchor='center', yanchor='top',
        ),
        yaxis1=dict(tickfont=dict(size=16, color='black')),
        yaxis2=dict(tickfont=dict(size=16, color='black')),
        yaxis3=dict(tickfont=dict(size=16, color='black')),
        yaxis4=dict(tickfont=dict(size=16, color='black')),
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
    # Write to file
    output_file = (
        f'{prop.om_files}/series_ice_{cast}_'
        f'all_GLOFS'
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
    fig.write_html(output_file+'.html',config=fig_config,auto_open=False)
    logger.debug(f'Finished writing file: {output_file}')
    logger.info('Wrote scorecard/flag plot to file for %s', cast)

def collect_stats_files(prop, ofs, var, cast, logger):
    '''
    Finds and loads all available .csv stat files for an OFS, then combines them
    into one big pandas dataframe. Calculates RMSE and CSI metrics for
    entire OFS, and returns those stats in pandas dataframe.
    '''

    # Get all file names for var and ofs
    stat_file = [
        file for file in
        os.listdir(prop.data_skill_stats_path)
        if ofs in file and cast in file and 'icestatstseries' in file
    ]
    try:  # Check to see if any files were found
        stat_file[0]
    except IndexError:
        logger.warning(
            'No files for %s in %s, moving to next '
            'variable.', var, ofs,
        )
        # Return 5 nans for 5 non-existent stats
        return None
        #return [np.nan, np.nan, np.nan, np.nan, np.nan]

    # If files exist, keep going
    # Get error range
    error_range, _ = plotting_functions.get_error_range(
        var, prop, logger,
    )
    # read CSV
    stats = pd.read_csv(
        r'' + f'{prop.data_skill_stats_path}/'
        f'{stat_file[0]}',)
    stats['DateTime'] = pd.to_datetime(stats['time_all_dt'],)

    return stats
    #return [csi,fa,miss,rmse,rmse_ice]

def make_OM_view_ice(prop, logger):
    '''
    Top-level function that calls (directly or indirectly) all other functions.
    '''

    # Specify defaults (can be overridden with command line options)
    if logger is None:
        log_config_file = 'conf/logging.conf'
        log_config_file = (
            Path(__file__).parent.parent.parent /
            log_config_file
        ).resolve()

        # Check if log file exists
        if not os.path.isfile(log_config_file):
            sys.exit(-1)

        # Create logger
        logging.config.fileConfig(log_config_file)
        logger = logging.getLogger('root')
        logger.info('Using log config %s', log_config_file)

    logger.info('--- Starting O&M ice dashboard processing ---')

    # Parameter validation & paths
    dir_params = utils.Utils().read_config_section('directories', logger)
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    argu_list = (
        prop.path,
        prop.whichcasts,
        ofs_extents_path,
    )
    parameter_validation(argu_list, logger)
    logger.info('Parameter validation complete!')

    # Path to .int files
    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_ice_pair_dir'],
    )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)
    # Path to CSV tables
    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'], dir_params['stats_ice_dir'],
    )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)
    # Path to save O&M files
    prop.om_files = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['om_dir'],
    )
    os.makedirs(prop.om_files, exist_ok=True)


    # Reformat whichcasts argument
    prop.whichcasts = prop.whichcasts.replace('[', '')
    prop.whichcasts = prop.whichcasts.replace(']', '')
    prop.whichcasts = prop.whichcasts.split(',')

    # Next loop through each OFS, each variable, load control files,
    # loop through stations in each control file, and collect .int files,
    # csv tables, and plots.
    for cast in prop.whichcasts:
        logger.info('Starting file collection for %s!', cast)
        # Set up blank array for each whichcast to hold stats
        ofs_stats = {
            'Hits': [],
            'False alarms': [],
            'Misses': [],
            'RMSE, all': [],
            'RMSE, ice': [],
        }
        # Set up blank var for each whichcast to append time series
        df_ofs = None

        for ofs in list_ofs():
            logger.info(
                'Collecting station output files for %s...',
                ofs.upper(),
            )
            # Make a stats dict that will be appended to master OFS dict
            stats_dict = {
                'Hits': [],
                'False alarms': [],
                'Misses': [],
                'RMSE, all': [],
                'RMSE, ice': [],
                }
            # Collect stat files
            stats = collect_stats_files(prop, ofs, 'ice_conc', cast, logger)
            # Append time series to df
            try:
                stats['OFS'] = ofs
            except TypeError:
                continue
            df_ofs = pd.concat([df_ofs, stats], axis=0)

            # Do some OFS-wide stats
            # But first, filter out no ice
            stats['csi_sum'] = stats['csi_all'] + stats['csi_misses'] + \
                stats['csi_falsealarms']
            stats.loc[stats['csi_sum'] == 0, ['csi_all','csi_falsealarms',
                                              'csi_misses']] = np.nan
            # Get Means for bar plots
            stats_dict['Hits'].append(stats[
                'csi_all'].mean())
            stats_dict['False alarms'].append(stats[
                'csi_falsealarms'].mean())
            stats_dict['Misses'].append(stats[
                'csi_misses'].mean())
            stats_dict['RMSE, all'].append(stats[
                'rmse_all'].mean())
            stats_dict['RMSE, ice'].append(stats[
                'rmse_either'].mean())

            # Make master array for means/bar plots.
            # Each row is an OFS, columns are csi, false alarms, misses,
            # RMSE, and RMSE ice only
            for stat in list(stats_dict.keys()):
                if len(stats_dict[stat]) == 0:
                    stats_dict[stat] = [np.nan]
                row_data = {
                    'ofs': ofs,
                    stat: stats_dict[stat][0],
                }
                ofs_stats[stat].append(row_data)

        # Get date range for table & scorecard plots below
        try:
            prop.start_date_scorecard = datetime.strftime(
                df_ofs['DateTime'].min(),'%m/%d/%Y'
                )
            prop.end_date_scorecard = datetime.strftime(
                df_ofs['DateTime'].max(),'%m/%d/%Y'
                )
        except TypeError:
            logger.error('No dates available for strftime '
                         'conversion! That happened '
                         'because there are no stats/files '
                         'found. Exiting...')
            sys.exit()
        # Make time series & scatter plot
        try:
            make_summary_series(prop, df_ofs, cast, logger)
            make_scatter_plot(prop, df_ofs, cast, logger)
        except Exception as ex:
            logger.error('Exception caught in make_summary_series: %s', ex)

        # Dataframe for each panel in bar subplots
        df1 = pd.concat([pd.DataFrame(ofs_stats['Hits']),
                         pd.DataFrame(ofs_stats['False alarms'])
                         ['False alarms'],
                         pd.DataFrame(ofs_stats['Misses'])
                         ['Misses']],axis=1)
        df2 = pd.concat([pd.DataFrame(ofs_stats['RMSE, all']),
                         pd.DataFrame(ofs_stats['RMSE, ice'])
                         ['RMSE, ice']],axis=1)
        if not is_df_nans(df1) and not is_df_nans(df2):
            # Bar plots!
            try:
                make_bar_plots(df1, df2, prop, cast, logger)
                logger.info('Finished O&M ice plots! Good bye.')
            except Exception as ex:
                logger.error('Exception caught in make_scorecard_plot: %s', ex)
        else:
            logger.warning('Ice stats dictionary is full of NaNs. No scorecard '
                           'plots will be made.')

    logger.info('Program complete.')

# Execution:
if __name__ == '__main__':

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python make_OM_view_ice.py',
        usage='%(prog)s',
        description='make generalized plots of ice skill for quick O&M checks',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=True,
        help='Home path',
    )
    parser.add_argument(
        '-ws',
        '--Whichcasts',
        required=True,
        help="whichcast: 'Nowcast','Forecast_A','Forecast_B', all",
    )

    args = parser.parse_args()

    prop1 = model_properties.ModelProperties()
    prop1.path = args.Path
    prop1.whichcasts = args.Whichcasts.lower()

    # Exclude forecast_a
    if 'forecast_a' in prop1.whichcast:
        print('Forecast_a is not available! Use nowcast and/or forecast_b.')
        sys.exit(-1)
    # Go!
    make_OM_view_ice(prop1, None)
