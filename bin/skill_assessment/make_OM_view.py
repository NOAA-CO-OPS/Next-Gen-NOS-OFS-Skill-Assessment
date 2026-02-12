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
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ofs_skill.model_processing import model_properties
from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization import plotting_functions


def parameter_validation(argu_list, logger):
    """ Parameter validation """

    path, whichcast, filetype, ofs_extents_path = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
    )

    # path validation
    if not os.path.exists(ofs_extents_path):
        error_message = f"""ofs_extents/ folder is not found. Please
        check path - {path}. Abort!"""
        logger.error(error_message)
        sys.exit(-1)

    # filetype validation
    if filetype not in ['stations', 'fields']:
        error_message = f'Filetype should be fields or stations: {filetype}!'
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
    return [
        'cbofs', 'ciofs', 'dbofs', 'gomofs', 'leofs', 'lmhofs', 'loofs',
        'lsofs', 'ngofs2', 'sfbofs', 'sscofs', 'tbofs', 'wcofs',
    ]


def is_df_nans(df):
    '''
    Checks if stats dataframe is full of nans, which happens if there are
    no .int files available to collect.
    '''
    df_indexed = df.set_index('ofs')
    return df_indexed.isna().all().all()


def get_csv_headings(var, logger):
    '''
    Returns a list of .int table(CSV) headings for a given variable (var)
    '''
    if var != 'cu':
        return [
            'Julian', 'year', 'month', 'day', 'hour',
            'minute', 'OBS', 'OFS', 'BIAS',
        ]
    else:
        return [
            'Julian', 'year', 'month', 'day', 'hour',
            'minute', 'OBS_SPD', 'OFS_SPD', 'BIAS_SPD',
            'OBS_DIR', 'OFS_DIR', 'BIAS_DIR',
        ]


def make_scorecard_plot(df1, df2, prop, cast, logger):
    '''
    Takes a pandas dataframe, prop, cast (nowcast or forecast_b) and writes
    a summary scorecard plotly plot that includes all OFS and all variables
    '''
    col_labels = df1['ofs']
    df1 = df1.set_index('ofs')
    df2 = df2.set_index('ofs')
    # Set colorscales
    # CF
    colorscale_cf = [
        [0, '#e35336'],  # dark red
        [0.89999, '#ffcccb'],  # light red
        [0.9, '#92ddc8'],  # light green
        [1, '#5aa17f'],  # dark green
    ]
    tickvals_cf = [0, 25, 50, 75, 90, 100]
    ticktext_cf = ['0%', '25%', '50%', '75%', '90%', '100%']
    c_title_cf = 'Central freq. (%)'
    cminmax_cf = [0, 100]
    # RMSE
    max_val = (np.nanmax(df2.values))
    if max_val > 5:
        max_val = 5
    threshold = 1/max_val
    colorscale_rmse = [
        [0, '#5aa17f'],  # dark green
        [threshold, '#92ddc8'],  # light green
        [threshold+0.0001, '#ffcccb'],  # light red
        [1, '#e35336'],  # dark red
    ]
    cmax = int(np.ceil(max_val))
    cminmax_rmse = [0, cmax]
    tickvals_rmse = np.linspace(
        0, cminmax_rmse[1], cminmax_rmse[1]+1,
        endpoint=True,
    )
    ticktext_rmse = tickvals_rmse
    c_title_rmse = 'RMSE / X'

    # Set other stuff up
    titlestr = 'OFS ' + cast.rstrip('_b') + ' skill overview,<br>' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard

    # Make figure
    fig = make_subplots(
        rows=2, cols=1, vertical_spacing=0.025,
        shared_xaxes=True,
    )
    df1 = df1.round(2)
    text_df = df1.astype(str).T
    # Replace 'nan' string values with your custom string, e.g., 'N/A'
    text_df = text_df.replace('nan', 'No data')
    # Convert to a list of lists for the 'text' attribute
    text_values = text_df.values.tolist()
    fig.add_trace(
        go.Heatmap(
            z=df1.T,
            x=col_labels,
            y=df1.columns,
            coloraxis='coloraxis1',
            text=text_values,
            hovertemplate='OFS: %{y}<br>'
            'Variable: %{x}<br>'
            'Central freq.: %{text}<br>'
            '<extra></extra>',
        ),
        row=1, col=1,
    )
    df2 = df2.round(2)
    text_df = df2.astype(str).T
    # Replace 'nan' string values with your custom string, e.g., 'N/A'
    text_df = text_df.replace('nan', 'No data')
    # Convert to a list of lists for the 'text' attribute
    text_values = text_df.values.tolist()
    fig.add_trace(
        go.Heatmap(
            z=df2.T,
            x=col_labels,
            y=df2.columns,
            coloraxis='coloraxis2',
            text=text_values,
            hovertemplate='OFS: %{y}<br>'
            'Variable: %{x}<br>'
            'RMSE / X: %{text}<br>'
            '<extra></extra>',
        ),
        row=2, col=1,
    )
    # Update layout for shared color axis and overall figure properties
    figwidth = 1000
    figheight = 700
    fig.update_layout(
        title_text=titlestr,
        title_x=0.5,
        title_y=0.96,
        title_font=dict(size=24, color='black', family='Open Sans'),
        paper_bgcolor='white',
        plot_bgcolor='white',
        coloraxis1=dict(
            colorscale=colorscale_cf,
            cmin=cminmax_cf[0],
            cmax=cminmax_cf[1],
            colorbar=dict(
                thickness=20,
                x=1.025,
                y=0.96,
                len=0.4,
                xanchor='left',
                yanchor='top',
                tickvals=tickvals_cf,
                ticktext=ticktext_cf,
                title=c_title_cf,
            ),
        ),
        coloraxis2=dict(
            colorscale=colorscale_rmse,
            cmin=cminmax_rmse[0],
            cmax=cminmax_rmse[1],
            colorbar=dict(
                thickness=20,
                x=1.025,
                y=0.45,
                len=0.4,
                xanchor='left',
                yanchor='top',
                tickvals=tickvals_rmse,
                ticktext=ticktext_rmse,
                title=c_title_rmse,
            ),
        ),
        margin={'l': 50, 'r': 50, 'b': 100, 't': 50},
        width=figwidth, height=figheight,
        autosize=False,
    )
    fig.update_traces(xgap=7, ygap=7)
    fig.update_yaxes(
        type='category', tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        }, title_text='Variable',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        scaleanchor='x', row=1, col=1,
    )
    fig.update_yaxes(
        type='category', tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        }, title_text='Variable',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        scaleanchor='x', row=2, col=1,
    )
    fig.update_xaxes(
        tickangle=45, tickfont={
            'size': 14, 'color': 'black', 'family': 'Open Sans',
        }, title_text='OFS',
        title_font={'size': 20, 'color': 'black', 'family': 'Open Sans'},
        row=2, col=1,
    )

    # Write to file
    # prop.stat = prop.stat.rstrip('*')
    output_file = (
        f'{prop.om_files}/scorecard_{cast}_'
        f'all_OFS'
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


def load_csv_tables(prop, ofs, var, cast, logger):
    '''
    Loads skill tables (CSV files) to a pandas dataframe for a given OFS,
    variable (var), and cast (nowcast or forecast_b). Returns the dataframe
    after filtering it to include the most relevant columns (df_filt).
    '''
    # Example file name:
    # skill_cbofs_currents_forecast_b_fields.csv
    # Define variable names that are in the file names
    if var == 'wl':
        var_name = 'water_level'
    elif var == 'temp':
        var_name = 'water_temperature'
    elif var == 'salt':
        var_name = 'salinity'
    elif var == 'cu':
        var_name = 'currents'
    # Load file!
    try:
        df = pd.read_csv(
            os.path.join(
                prop.data_skill_stats_path,
                f'skill_{ofs}_{var_name}_{cast}_{prop.ofsfiletype}.csv',
            ),
        )
    except FileNotFoundError:
        logger.warning('%s CSV skill table for %s not found!', ofs, var_name)
        return None

    # Clean it up a bit
    df['variable'] = var_name
    df['ofs'] = ofs
    df_filt = df[[
        'ofs', 'ID', 'variable', 'rmse', 'bias', 'central_freq',
        'central_freq_pass_fail', 'pos_outlier_freq',
        'pos_outlier_freq_pass_fail', 'neg_outlier_freq',
        'neg_outlier_freq_pass_fail', 'start_date', 'end_date',
    ]]
    return df_filt


def collect_int_files(prop, ofs, var, cast, logger):
    '''
    Finds and loads all available .int files for an OFS, then combines them
    into one big pandas dataframe. Calculates RMSE and central frequency for
    entire OFS, and returns those two stats.
    '''
    # Get variable's error range
    X1, _ = plotting_functions.get_error_range(var, prop, logger)

    # Empty array holds each variable's .int files
    all_ints = None

    # Get all .int file names for var and ofs
    ofs_int_files = [
        file for file in
        os.listdir(prop.data_skill_1d_pair_path)
        if ofs in file and var in file and cast in file
    ]
    try:  # Check to see if any .int files were found
        ofs_int_files[0]
    except IndexError:
        logger.warning(
            'No .int files for %s in %s, moving to next '
            'variable.', var, ofs,
        )
        cf = np.nan
        rmse = np.nan
        return [cf, rmse]
    # Get list of headings to parse .int file
    list_of_headings = get_csv_headings(var, logger)
    # Get error range
    error_range, _ = plotting_functions.get_error_range(
        var, prop, logger,
    )
    # If .int files exist, loop through them
    for file in ofs_int_files:
        paired_data = pd.read_csv(
            r'' + f'{prop.data_skill_1d_pair_path}/'
            f'{file}', sep=r'\s+', names=list_of_headings,  # Do i need this?
            header=0,
        )
        paired_data['DateTime'] = pd.to_datetime(
            paired_data[['year', 'month', 'day', 'hour', 'minute']],
        )
        # logger.info("Loaded %s .int for %s in %s!",
        #            var, file.split('_')[2], ofs)
        # Collect .ints to one big dataframe
        try:
            all_ints = pd.concat([all_ints, paired_data], axis=0)
        except Exception as e_x:
            logger.error(
                'Exception caught when combining .int '
                'files! Error: %s', e_x,
            )
            continue
    # Now that we have all .int files, do some OFS-wide stats
    try:
        all_ints = all_ints.dropna(subset=['BIAS'])
        bias = np.array(all_ints['BIAS'])
    except KeyError:
        all_ints = all_ints.dropna(subset=['BIAS_SPD'])
        bias = np.array(all_ints['BIAS_SPD'])

    # Do central frequency & RMSE
    if len(bias) > 10:  # Impose limits on number of data points
        cf = ((((-error_range <= bias) &
                (bias <= error_range)).sum())/len(bias))*100
        rmse = (np.nanmean(bias**2)**0.5)/X1
    else:
        cf = np.nan
        rmse = np.nan
        logger.warning(
            'Not enough data points to calculate '
            'stats for %s in %s.', var, ofs,
        )

    return [cf, rmse]


def make_skill_table(prop, cast, df, logger):
    '''
    Makes a plotly table of skill stats for all OFS and all variables, and
    saves it.
    '''
    logger.info('Starting construction of master skill table...')
    columns_to_drop = [
        'central_freq_pass_fail',
        'pos_outlier_freq_pass_fail',
        'neg_outlier_freq_pass_fail',
    ]
    df = df.drop(columns_to_drop, axis=1)
    df = df.rename(
        columns={
            'ofs': 'OFS',
            'ID': 'Station ID',
            'variable': 'Variable',
            'rmse': 'RMSE',
            'bias': 'Mean bias',
            'central_freq': 'Central frequency',
            'pos_outlier_freq': 'Positive outlier frequency',
            'neg_outlier_freq': 'Negative outlier frequency',
            'start_date': 'Start (UTC)',
            'end_date': 'End (UTC)',
        },
    )
    # Make very large plotly table
    # colors = np.empty((len(df),len(df.columns)),dtype=object)
    # cf_np = np.array(df['Central frequency'])
    # for i in range(len(cf_np)):
    #     if cf_np[i] < 90:
    #         colors[i,:] = 'lightsalmon'
    #     else:
    #         colors[i,:] = 'paleturquoise'
    # colors = colors.T
    # Make table
    titlestr = 'OFS ' + cast.rstrip('_b') + ' skill statistics, ' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns),
                    fill_color='lightsalmon',
                    # align='left',
                    font=dict(color='black', size=14, weight='bold'),
                ),
                cells=dict(
                    values=df.transpose().values.tolist(),
                    # fill_color=colors,
                    # align='left'
                ),
            ),
        ],
    )
    fig.update_layout(
        updatemenus=[
            {
                'buttons': [
                    {
                        'label': c,
                        'method': 'restyle',
                        'args': [
                            {
                                'cells': {
                                    'values': df.T.values
                                    if c == 'All OFS'
                                    else df.loc[df['OFS'].eq(c)].T.values,
                                },
                            },
                        ],
                    }
                    for c in ['All OFS'] + df['OFS'].unique().tolist()
                ],
            },
        ],
        title_text=titlestr,
        title_x=0.5,
        title_font=dict(size=24, color='black', family='Open Sans'),
    )
    figwidth = 1000
    figheight = 700
    output_file = (
        f'{prop.om_files}/table_skill_{cast}_'
        f'all_OFS'
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
    logger.info('Finished constructing master skill table!')


def make_summary_plot(prop, cast, df, logger):
    '''
    Plots central frequency for all OFS, variables, and stations for a
    whichcast, and saves the plot to file.
    '''

    # Marker colors
    alpha = str(0.6)
    n_colors = int(np.ceil(len(list(df['variable'].unique()))))
    colors = px.colors.sample_colorscale(
        'viridis', [n/(n_colors - 1) for n in range(n_colors)],
    )
    # colors = colors[:-1]
    colors = [color.split(')')[0] + ', ' + alpha + ')' for color in colors]
    colors = [
        color.split('b')[0] + 'ba' + color.split('b')[1]
        for color in colors
    ]
    symbols = ['circle', 'diamond', 'square', 'triangle-up']
    # Create a Figure object
    fig = go.Figure()

    # Add scatter traces
    for i, var in enumerate(list(df['variable'].unique())):
        # Get subset of df
        df_filt = df[df['variable'] == var]
        fig.add_trace(
            go.Scatter(
                x=df_filt['ofs'],
                y=df_filt['central_freq'],
                mode='markers',
                marker=dict(
                    size=18, color=colors[i], symbol=symbols[i],
                    line=dict(color='black', width=0.75),
                ),
                name=var.replace('_', ' ').capitalize(),
                customdata=np.stack(
                    (
                        list(df_filt['ID']),
                        list(df_filt['variable']),
                    ), axis=1,
                ),
                # customdata=df_filt['ID'],
                hovertemplate='OFS: %{x}<br>'
                'Station ID: %{customdata[0]}<br>'
                'Variable: %{customdata[1]}<br>'
                'Central freq.: %{y}<br>'
                '<extra></extra>',
                hoverlabel=dict(
                    font=dict(
                        family='Open Sans',
                        size=16,
                        color='black',
                    ),
                ),
            ),
        )

    # Update the x-axis to be categorical
    fig.update_xaxes(
        type='category', title_text='OFS',
        title_font={
            'size': 24, 'color': 'black',
            'family': 'Open Sans',
        },
        tickfont={
            'size': 16, 'color': 'black', 'family': 'Open Sans',
            'style': 'italic',
        },
        showline=True,  # Show the axis line
        linewidth=1,    # Set line width
        linecolor='black',  # Set line color
        mirror=True,     # Mirror the line on the opposite side
    )
    fig.update_yaxes(
        title_text='Central frequency (%)',
        title_font={
            'size': 22, 'color': 'black',
            'family': 'Open Sans',
        },
        tickfont={
            'size': 16,
            'color': 'black',
            'family': 'Open Sans',
        },
        showline=True,  # Show the axis line
        linewidth=1,    # Set line width
        linecolor='black',  # Set line color
        mirror=True,     # Mirror the line on the opposite side
    )
    fig.add_hline(
        y=90, line_color='red',
        line_width=1.25,
        line_dash='dash',
        annotation_text='<b>90% acceptance criteria</b>',
        annotation_position='top right',
        annotation_font_color='black',
        annotation_font_size=13,
        annotation_font_family='Open Sans',
    )
    # Update layout for title and y-axis label
    titlestr = 'OFS ' + cast.rstrip('_b') + ' central frequency summary, ' + \
        prop.start_date_scorecard + ' - ' + prop.end_date_scorecard
    figwidth = 1000
    figheight = 700
    fig.update_layout(
        title={
            'text': titlestr,
            'font': {'family': 'Open Sans', 'size': 24, 'color': 'black'},
        },
        transition_ordering='traces first',
        dragmode='zoom',
        # hovermode="x unified",
        width=figwidth,
        template='plotly_white',
        yaxis=dict(range=[-2, 102]),
        legend=dict(
            font=dict(
                size=16, color='black', family='Open Sans',
                style='italic',
            ),
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
        ),
        # margin={"l":50, "r": 50, "b": 50, "t": 125},
    )

    output_file = (
        f'{prop.om_files}/scatter_cf_{cast}_'
        f'all_OFS'
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
    logger.info('Finished drawing master central frequency scatter plot!')


def make_OM_view(prop, logger):
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

    logger.info('--- Starting O&M dashboard processing ---')

    # Parameter validation & paths
    dir_params = utils.Utils().read_config_section('directories', logger)
    ofs_extents_path = os.path.join(prop.path, dir_params['ofs_extents_dir'])
    argu_list = (
        prop.path,
        prop.whichcasts,
        prop.ofsfiletype,
        ofs_extents_path,
    )
    parameter_validation(argu_list, logger)
    logger.info('Parameter validation complete!')
    # Path to .int files
    prop.data_skill_1d_pair_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir'],
    )
    os.makedirs(prop.data_skill_1d_pair_path, exist_ok=True)
    # Path to CSV tables
    prop.data_skill_stats_path = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['stats_dir'],
    )
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)
    # Path to save O&M files
    prop.om_files = os.path.join(
        prop.path, dir_params['data_dir'], dir_params['visual_dir'],
        dir_params['om_dir'],
    )
    os.makedirs(prop.om_files, exist_ok=True)
    # Path to control files
    prop.control_files_path = os.path.join(
        prop.path, dir_params['control_files_dir'],
    )
    os.makedirs(prop.control_files_path, exist_ok=True)

    # Reformat whichcasts argument
    prop.whichcasts = prop.whichcasts.replace('[', '')
    prop.whichcasts = prop.whichcasts.replace(']', '')
    prop.whichcasts = prop.whichcasts.split(',')

    # Next loop through each OFS, each variable, load control files,
    # loop through stations in each control file, and collect .int files,
    # csv tables, and plots.
    for cast in prop.whichcasts:
        logger.info('Starting file collection for %s!', cast)
        ofs_stats = {
            'central frequency': [],
            'RMSE*': [],
        }  # Set up blank array for each whichcast
        skill_csvs = None
        for ofs in list_ofs():
            logger.info(
                'Collecting station output files for %s...',
                ofs.upper(),
            )
            # Make a stats dict -- can include more stats here
            stats_dict = {
                'central frequency': [],
                'RMSE*': [],
            }
            for var in ['wl', 'temp', 'salt', 'cu']:
                # Collect .int files
                stats = collect_int_files(prop, ofs, var, cast, logger)
                stats_dict['central frequency'].append(stats[0])
                stats_dict['RMSE*'].append(stats[1])

                # Collect CSV skill tables
                try:
                    skill_csvs = pd.concat(
                        [
                            skill_csvs, load_csv_tables(
                                prop, ofs, var, cast, logger,
                            ),
                        ], axis=0,
                    )
                except ValueError:
                    pass
                except Exception as e_x:
                    logger.error(
                        'Unexpected exception caught when combining CSV '
                        'files! Error: %s', e_x,
                    )
            # Done with all variables for a given OFS!
            # First append CF for each OFS to a master array.
            # Each row is an OFS, columns are CF for each variable
            for stat in list(stats_dict.keys()):
                if len(stats_dict[stat]) == 0:
                    stats_dict[stat] = [np.nan, np.nan, np.nan, np.nan]
                row_data = {
                    'ofs': ofs,
                    'water level': stats_dict[stat][0],
                    'temperature': stats_dict[stat][1],
                    'salinity': stats_dict[stat][2],
                    'current speed': stats_dict[stat][3],
                }
                ofs_stats[stat].append(row_data)

        # Done with all OFS!
        # Next, save the collected CSV tables
        # First reformat dates
        try:
            date_types = ['start_date', 'end_date']
            for date_type in date_types:
                for index, row in skill_csvs.iterrows():
                    if 'T' in row[date_type]:
                        split_str = str(row[date_type]).split('T')
                        date_str = split_str[0] + ' ' + split_str[1][0:5]
                        skill_csvs.loc[index, date_type] = date_str
                    else:
                        split_str = str(row[date_type]).split('-')
                        date_str = split_str[0][0:4] + '-' +\
                            split_str[0][4:6] + '-' + split_str[0][6:] + ' ' \
                            + split_str[1][0:5]
                        skill_csvs.loc[index, date_type] = date_str
            # Save table to CSV
            skill_csvs.to_csv(
                os.path.join(
                    prop.data_skill_stats_path,
                    f'skill_{cast}_{prop.ofsfiletype}_all_OFS.csv',
                ),
                index=False,
            )
            # Get date range for table & scorecard plots below
            prop.start_date_scorecard = datetime.strftime(
                pd.to_datetime(
                    skill_csvs['start_date'],
                ).min(), '%m/%d/%Y %H:%M',
            )
            prop.end_date_scorecard = datetime.strftime(
                pd.to_datetime(
                    skill_csvs['end_date'],
                ).max(), '%m/%d/%Y %H:%M',
            )
            # Make a plotly table of all skill stats, and save it too
            make_skill_table(prop, cast, skill_csvs, logger)
            # Make a graphic of all OFS central frequency for all vars
            make_summary_plot(prop, cast, skill_csvs, logger)
        except AttributeError:
            logger.info('No CSV files were collected! Cannot make summary '
                         'table or scatter plot. Proceeding...')
        except Exception as ex:
            logger.error('Unspecified error caught: %s. Cannot make summary '
                         'table or scatter plot. Proceeding...', ex)
        # Now do scorecard. First check if there is data
        # Set up pandas dataframe with all OFS for a whichcast
        df1 = pd.DataFrame(ofs_stats['central frequency'])
        df2 = pd.DataFrame(ofs_stats['RMSE*'])
        if not is_df_nans(df1) and not is_df_nans(df2):
            # Scorecard plots!
            try:
                make_scorecard_plot(df1, df2, prop, cast, logger)
                logger.info('Finished O&M views! Good bye.')
            except Exception as ex:
                logger.error('Exception caught in make_scorecard_plot: %s', ex)
        else:
            logger.warning('Stats dictionary is full of NaNs because no .int '
                           'files were available. No scorecard plots will be '
                           'made.')

    logger.info('Program complete.')

# Execution:
if __name__ == '__main__':

    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python make_OM_view.py',
        usage='%(prog)s',
        description='make generalized plots for quick O&M checks',
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=True,
        help='Home path',
    )
    parser.add_argument(
        '-t',
        '--FileType',
        required=True,
        help="OFS output file type to use: 'fields' or 'stations'",
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
    prop1.ofsfiletype = args.FileType.lower()

    # Exclude forecast_a
    if 'forecast_a' in prop1.whichcast:
        print('Forecast_a is not available! Use nowcast and/or forecast_b.')
        sys.exit(-1)
    # Go!
    make_OM_view(prop1, None)
