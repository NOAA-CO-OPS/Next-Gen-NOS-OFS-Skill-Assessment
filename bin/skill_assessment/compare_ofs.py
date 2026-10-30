"""OFS Skill Assessment Comparison Tool.

This script compares the performance of two Operational Forecast Systems (OFS)
by evaluating them on a shared set of overlapping stations. The workflow includes:
1. Identifying overlapping stations via shapefile intersection.
2. Isolating the assessment to only those overlapping stations by copying
   the restricted inventory and clearing binary caches.
3. Running 1D skill assessments (`create_1dplot`) for both models.
4. Generating comparative Plotly time series for paired variables.
5. Generating Plotly statistical comparisons (RMSE, Bias, Central Frequency)
   across all stations, including interactive Map views with dropdowns.

Created on Mon Jul  6 13:58:20 2026
@author: PWL
"""

import argparse
import glob
import logging.config
import math
import os
import shutil
import sys

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ofs_skill.model_processing import check_model_files, model_properties
from ofs_skill.utils import cache_manifest


class MockProp:  # pylint: disable=too-few-public-methods
    """Mock model property container holding path information."""

    def __init__(self, path):
        self.path = path


def _extract_scalar(val):
    """Force output to be a pure Python float to avoid pandas Series ambiguity."""
    try:
        if hasattr(val, 'iloc'):
            return float(val.iloc[0])
        if isinstance(val, list | tuple):
            return float(val[0])
        return float(val)
    except (ValueError, TypeError, IndexError, AttributeError):
        return 0.0


def fetch_error_range(short_var, base_path, logger):
    """Retrieve error target threshold for a short variable name."""
    try:
        # pylint: disable=import-outside-toplevel
        from ofs_skill.visualization.plotting_functions import get_error_range

        val = get_error_range(short_var, MockProp(base_path), logger)[0]
        return _extract_scalar(val)
    except ImportError:
        pass

    try:
        # pylint: disable=import-outside-toplevel
        from plotting_functions import get_error_range

        val = get_error_range(short_var, MockProp(base_path), logger)[0]
        return _extract_scalar(val)
    except ImportError:
        pass

    logger.warning(
        'Could not import get_error_range. Using direct CSV fallback for target error.'
    )
    config_path = os.path.join(base_path, 'conf', 'error_ranges.csv')
    defaults = {
        'salt': 3.5,
        'temp': 3.0,
        'wl': 0.15,
        'cu': 0.26,
        'ice_conc': 10.0,
        'cu_dir': 22.5,
    }

    if os.path.exists(config_path):
        try:
            df_err = pd.read_csv(config_path)
            match = df_err[df_err['name_var'] == short_var]
            if not match.empty:
                col = 'x1' if 'x1' in match.columns else 'X1'
                return float(match.iloc[0][col])
        except (KeyError, ValueError, OSError, pd.errors.EmptyDataError) as err:
            logger.warning('Error reading %s: %s', config_path, err)

    return defaults.get(short_var, 0.0)


def _resolve_model_keys(ofs1, ofs2, filetype1, filetype2):
    """Resolve legend names and column suffixes for model identifiers."""
    ofs1_key = ofs1
    ofs2_key = ofs2
    ofs1_name = ofs1.upper()
    ofs2_name = ofs2.upper()

    if ofs1 == ofs2:
        ofs1_key = f'{ofs1}_{filetype1}'
        ofs2_key = f'{ofs2}_{filetype2}'
        ofs1_name = f'{ofs1.upper()} ({filetype1})'
        ofs2_name = f'{ofs2.upper()} ({filetype2})'

    return ofs1_key, ofs2_key, ofs1_name, ofs2_name


def _extract_cu_depth_bins(found_ofs1, prefix, suffix):
    """Dynamically extract unique depth bins from matched current files."""
    depth_bins = []
    for filepath in found_ofs1:
        basename = os.path.basename(filepath)
        if basename.startswith(prefix) and suffix in basename:
            full_node_str = basename[len(prefix) :].split(suffix)[0]
            depth_bin = full_node_str.split('_')[0]
            if depth_bin not in depth_bins:
                depth_bins.append(depth_bin)
    return depth_bins


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements
def generate_comparisons(
    ofs1,
    ofs2,
    overlap_csv,
    var_selection,
    whichcasts,
    home_path,
    datum,
    _start_date,
    _end_date,
    filetype1,
    filetype2,
    dir_params,
    logger,
):
    """Ingest paired datasets for overlapping stations and create interactive Plotly time series."""
    logger.info('--- Starting Comparison Plotting ---')

    try:
        inventory = pd.read_csv(overlap_csv)
        station_col = 'ID' if 'ID' in inventory.columns else inventory.columns[0]
        overlap_stations = inventory[station_col].astype(str).unique().tolist()
    except (KeyError, ValueError, OSError, pd.errors.EmptyDataError) as err:
        logger.error('Failed to read inventory CSV: %s', err)
        sys.exit(1)

    # Resolve configured directories
    data_dir = dir_params.get('data_dir', 'data')
    skill_dir = dir_params.get('skill_dir', 'skill')
    pair_dir_name = dir_params.get('1d_pair_dir', '1d_pair')
    visual_dir_name = dir_params.get('visual_dir', 'visual')

    pair_dir = os.path.join(home_path, data_dir, skill_dir, pair_dir_name)
    visual_dir = os.path.join(home_path, data_dir, visual_dir_name, 'comparisons')
    os.makedirs(visual_dir, exist_ok=True)

    vars_to_process = var_selection.split(',')
    casts_to_process = [c.strip() for c in whichcasts.split(',')]

    generated_any_plots = False

    for var in vars_to_process:
        var = var.strip()
        display_var = var.replace('_', ' ').title()

        var_map = {
            'water_level': 'wl',
            'water_temperature': 'temp',
            'salinity': 'salt',
            'currents': 'cu',
        }
        short_var = var_map.get(var, var)

        if short_var == 'wl':
            y_title = f'Water Level<br>(<i>meters {datum}</i>)'
            unit = 'm'
        elif short_var == 'temp':
            y_title = 'Water Temperature<br>(<i>\u00b0C</i>)'
            unit = '\u00b0C'
        elif short_var == 'cu':
            y_title = 'Current Speed<br>(<i>knots</i>)'
            unit = 'knots'
        elif short_var == 'salt':
            y_title = 'Salinity<br>(<i>PSU</i>)'
            unit = 'PSU'
        else:
            y_title = display_var
            unit = ''

        if short_var == 'cu':
            col_names = [
                'Julian',
                'year',
                'month',
                'day',
                'hour',
                'minute',
                'OBS_SPD',
                'OFS_SPD',
                'BIAS_SPD',
                'OBS_DIR',
                'OFS_DIR',
                'BIAS_DIR',
            ]
        else:
            col_names = [
                'Julian',
                'year',
                'month',
                'day',
                'hour',
                'minute',
                'OBS',
                'OFS',
                'BIAS',
            ]

        x1 = fetch_error_range(short_var, home_path, logger)
        if short_var == 'cu':
            x1 *= 1.943844

        for cast in casts_to_process:
            cast_file = cast.lower()
            cast_title = cast_file.replace('_b', '')

            for station in overlap_stations:
                if short_var == 'cu':
                    ofs1_wildcard = os.path.join(
                        pair_dir,
                        f'{ofs1}_{short_var}_{station}_*_{cast}_{filetype1}_pair.int',
                    )
                    found_ofs1 = glob.glob(ofs1_wildcard)

                    if not found_ofs1:
                        logger.warning(
                            'Missing %s pair files for station %s, cast %s, file type %s. Skipping.',
                            ofs1,
                            station,
                            cast,
                            filetype1,
                        )
                        continue

                    depth_bins = _extract_cu_depth_bins(
                        found_ofs1,
                        prefix=f'{ofs1}_{short_var}_{station}_',
                        suffix=f'_{cast}_',
                    )
                else:
                    depth_bins = ['']

                for depth_bin in depth_bins:
                    if short_var == 'cu' and depth_bin:
                        station_key = f'{station}_{depth_bin}'
                        ofs1_pattern = os.path.join(
                            pair_dir,
                            f'{ofs1}_{short_var}_{station}_{depth_bin}_*_'
                            f'{cast}_{filetype1}_pair.int',
                        )
                        ofs2_pattern = os.path.join(
                            pair_dir,
                            f'{ofs2}_{short_var}_{station}_{depth_bin}_*_'
                            f'{cast}_{filetype2}_pair.int',
                        )
                    else:
                        station_key = station
                        ofs1_pattern = os.path.join(
                            pair_dir,
                            f'{ofs1}_{short_var}_{station}_*_{cast}_{filetype1}_pair.int',
                        )
                        ofs2_pattern = os.path.join(
                            pair_dir,
                            f'{ofs2}_{short_var}_{station}_*_{cast}_{filetype2}_pair.int',
                        )

                    logger.info(
                        'Processing comparison for %s, variable %s, cast %s',
                        station_key,
                        short_var,
                        cast,
                    )

                    ofs1_files = glob.glob(ofs1_pattern)
                    ofs2_files = glob.glob(ofs2_pattern)

                    if not ofs1_files or not ofs2_files:
                        logger.warning(
                            'Missing complete pair files for %s. Skipping.',
                            station_key,
                        )
                        continue

                    try:
                        df1 = pd.read_csv(
                            ofs1_files[0], sep=r'\s+', names=col_names, header=0
                        )
                        df2 = pd.read_csv(
                            ofs2_files[0], sep=r'\s+', names=col_names, header=0
                        )

                        date_cols = ['year', 'month', 'day', 'hour', 'minute']
                        df1['DateTime'] = pd.to_datetime(df1[date_cols])
                        df2['DateTime'] = pd.to_datetime(df2[date_cols])

                        df1 = df1.drop_duplicates(subset=['DateTime']).reset_index(
                            drop=True
                        )
                        df2 = df2.drop_duplicates(subset=['DateTime']).reset_index(
                            drop=True
                        )

                        if short_var == 'cu':
                            df1['OBS'] = df1['OBS_SPD'] * 1.943844
                            df1['OFS'] = df1['OFS_SPD'] * 1.943844
                            df2['OBS'] = df2['OBS_SPD'] * 1.943844
                            df2['OFS'] = df2['OFS_SPD'] * 1.943844

                        ofs1_key, ofs2_key, ofs1_name, ofs2_name = _resolve_model_keys(
                            ofs1, ofs2, filetype1, filetype2
                        )

                        merged = pd.merge(
                            df1[['DateTime', 'OBS', 'OFS']],
                            df2[['DateTime', 'OFS']],
                            on='DateTime',
                            suffixes=(f'_{ofs1_key}', f'_{ofs2_key}'),
                        )

                        if merged.empty:
                            logger.warning(
                                'No overlapping timeframe for %s. Skipping.',
                                station_key,
                            )
                            continue

                        generated_any_plots = True

                        hover_tmpl = (
                            '<b>Time:</b> %{x|%m/%d/%Y %H:%M}'
                            f'<br><b>%{{data.name}}:</b> %{{y:.2f}} {unit}<extra></extra>'
                        )

                        merged[f'Error_{ofs1_key}'] = (
                            merged[f'OFS_{ofs1_key}'] - merged['OBS']
                        )
                        merged[f'Error_{ofs2_key}'] = (
                            merged[f'OFS_{ofs2_key}'] - merged['OBS']
                        )

                        fig_ts = make_subplots(
                            rows=2,
                            cols=1,
                            shared_xaxes=True,
                            vertical_spacing=0.08,
                            row_heights=[0.7, 0.3],
                        )

                        fig_ts.add_trace(
                            go.Scatter(
                                x=merged['DateTime'],
                                y=merged['OBS'],
                                name='Observation',
                                mode='lines',
                                hovertemplate=hover_tmpl,
                                line=dict(color='red', width=2),
                            ),
                            row=1,
                            col=1,
                        )
                        fig_ts.add_trace(
                            go.Scatter(
                                x=merged['DateTime'],
                                y=merged[f'OFS_{ofs1_key}'],
                                name=ofs1_name,
                                mode='lines',
                                hovertemplate=hover_tmpl,
                                line=dict(color='#d55e00', width=1.5),
                                opacity=0.8,
                                legendgroup=ofs1_key,
                            ),
                            row=1,
                            col=1,
                        )
                        fig_ts.add_trace(
                            go.Scatter(
                                x=merged['DateTime'],
                                y=merged[f'OFS_{ofs2_key}'],
                                name=ofs2_name,
                                mode='lines',
                                hovertemplate=hover_tmpl,
                                line=dict(color='#0072b2', width=1.5),
                                opacity=0.8,
                                legendgroup=ofs2_key,
                            ),
                            row=1,
                            col=1,
                        )

                        min_dt = merged['DateTime'].min()
                        max_dt = merged['DateTime'].max()

                        if x1 > 0:
                            x2 = x1 * 2
                            fig_ts.add_trace(
                                go.Scatter(
                                    x=[min_dt, max_dt, max_dt, min_dt],
                                    y=[x1, x1, -x1, -x1],
                                    fill='toself',
                                    fillcolor='rgba(255, 165, 0, 0.3)',
                                    line=dict(color='rgba(255,255,255,0)'),
                                    name=f'Target Error (\u00B1{x1:.2f} {unit})',
                                    hoverinfo='skip',
                                ),
                                row=2,
                                col=1,
                            )
                            fig_ts.add_trace(
                                go.Scatter(
                                    x=[min_dt, max_dt, max_dt, min_dt],
                                    y=[x2, x2, -x2, -x2],
                                    fill='toself',
                                    fillcolor='rgba(255, 0, 0, 0.15)',
                                    line=dict(color='rgba(255,255,255,0)'),
                                    name=f'2x Target Error (\u00B1{x2:.2f} {unit})',
                                    hoverinfo='skip',
                                ),
                                row=2,
                                col=1,
                            )

                        fig_ts.add_trace(
                            go.Scatter(
                                x=[min_dt, max_dt],
                                y=[0, 0],
                                mode='lines',
                                name='Zero Error',
                                showlegend=False,
                                hoverinfo='skip',
                                line=dict(color='black', dash='dash', width=1),
                            ),
                            row=2,
                            col=1,
                        )
                        fig_ts.add_trace(
                            go.Scatter(
                                x=merged['DateTime'],
                                y=merged[f'Error_{ofs1_key}'],
                                name=f'{ofs1_name} Error',
                                mode='lines',
                                hovertemplate=hover_tmpl,
                                line=dict(color='#d55e00', width=1.5),
                                opacity=0.8,
                                showlegend=False,
                                legendgroup=ofs1_key,
                            ),
                            row=2,
                            col=1,
                        )
                        fig_ts.add_trace(
                            go.Scatter(
                                x=merged['DateTime'],
                                y=merged[f'Error_{ofs2_key}'],
                                name=f'{ofs2_name} Error',
                                mode='lines',
                                hovertemplate=hover_tmpl,
                                line=dict(color='#0072b2', width=1.5),
                                opacity=0.8,
                                showlegend=False,
                                legendgroup=ofs2_key,
                            ),
                            row=2,
                            col=1,
                        )

                        ts_title = (
                            f'<b>Time Series Comparison: {station_key} - '
                            f'{display_var} ({cast_title})</b>'
                        )
                        fig_ts.update_layout(
                            title=dict(
                                text=ts_title,
                                font=dict(size=18, color='black', family='Open Sans'),
                                y=0.98,
                                x=0.5,
                                xanchor='center',
                                yanchor='top',
                            ),
                            template='plotly_white',
                            hovermode='x unified',
                            hoverlabel=dict(
                                bgcolor='white',
                                bordercolor='#cccccc',
                                font=dict(
                                    family='Open Sans', size=13, color='#333333'
                                ),
                            ),
                            legend=dict(
                                orientation='h',
                                yanchor='bottom',
                                y=1.02,
                                xanchor='left',
                                x=0,
                                font=dict(size=16, color='black'),
                                itemclick='toggle',
                                itemdoubleclick=False,
                            ),
                            margin=dict(t=120, b=120),
                            height=780,
                            width=900,
                        )
                        fig_ts.update_xaxes(
                            mirror=True,
                            ticks='inside',
                            showline=True,
                            linecolor='black',
                            linewidth=1,
                            showspikes=True,
                            spikemode='across',
                            spikesnap='cursor',
                            showgrid=True,
                            tickfont=dict(
                                family='Open Sans', color='black', size=14
                            ),
                            minor=dict(
                                ticklen=4,
                                tickcolor='black',
                                ticks='inside',
                                showgrid=False,
                            ),
                            tickformat='%H:%M<br>%m/%d',
                            hoverformat='%b %d, %Y, %H:%M UTC',
                        )
                        fig_ts.update_xaxes(
                            title_text='<br>Time (UTC)',
                            titlefont=dict(
                                family='Open Sans', color='black', size=18
                            ),
                            rangeslider=dict(
                                visible=True,
                                thickness=0.06,
                                bordercolor='black',
                                borderwidth=1,
                            ),
                            row=2,
                            col=1,
                        )
                        fig_ts.update_yaxes(
                            title_text=y_title,
                            titlefont=dict(
                                family='Open Sans', color='black', size=17
                            ),
                            mirror=True,
                            ticks='inside',
                            showline=True,
                            linecolor='black',
                            linewidth=1,
                            tickfont=dict(
                                family='Open Sans', color='black', size=14
                            ),
                            minor=dict(
                                ticklen=4,
                                tickcolor='black',
                                ticks='inside',
                                showgrid=False,
                            ),
                            zeroline=(short_var == 'wl'),
                            zerolinewidth=1,
                            zerolinecolor='black',
                            row=1,
                            col=1,
                        )
                        fig_ts.update_yaxes(
                            title_text=f'Error ({unit})' if unit else 'Error',
                            titlefont=dict(
                                family='Open Sans', color='black', size=17
                            ),
                            mirror=True,
                            ticks='inside',
                            showline=True,
                            linecolor='black',
                            linewidth=1,
                            tickfont=dict(
                                family='Open Sans', color='black', size=14
                            ),
                            minor=dict(
                                ticklen=4,
                                tickcolor='black',
                                ticks='inside',
                                showgrid=False,
                            ),
                            zeroline=False,
                            row=2,
                            col=1,
                        )

                        ts_out = os.path.join(
                            visual_dir,
                            f'{ofs1_key}_vs_{ofs2_key}_{short_var}_{station_key}_'
                            f'{cast_file}_timeseries.html',
                        )
                        fig_ts.write_html(ts_out)

                    except (KeyError, ValueError, OSError) as err:
                        logger.error('Error plotting TS for %s: %s', station_key, err)

    if not generated_any_plots:
        logger.error('No overlapping paired data files found. Pair generation either failed or no valid stations overlap.')
        sys.exit(1)


def _add_scatter_corridors(fig, axis_range, safe_x, safe_y):
    """Add pass corridor rectangles and center safe zone to 1-to-1 scatter figure."""
    # OFS1 Pass Corridor (Vertical)
    fig.add_trace(
        go.Scatter(
            x=[safe_x[0], safe_x[1], safe_x[1], safe_x[0], safe_x[0]],
            y=[axis_range[0], axis_range[0], axis_range[1], axis_range[1], axis_range[0]],
            fill='toself',
            fillcolor='rgba(200, 200, 200, 0.15)',
            mode='lines',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip',
        )
    )
    # OFS2 Pass Corridor (Horizontal)
    fig.add_trace(
        go.Scatter(
            x=[axis_range[0], axis_range[1], axis_range[1], axis_range[0], axis_range[0]],
            y=[safe_y[0], safe_y[0], safe_y[1], safe_y[1], safe_y[0]],
            fill='toself',
            fillcolor='rgba(200, 200, 200, 0.15)',
            mode='lines',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip',
        )
    )
    # Center Safe Zone
    fig.add_trace(
        go.Scatter(
            x=[safe_x[0], safe_x[1], safe_x[1], safe_x[0], safe_x[0]],
            y=[safe_y[0], safe_y[0], safe_y[1], safe_y[1], safe_y[0]],
            fill='toself',
            fillcolor='rgba(0, 255, 0, 0.15)',
            mode='lines',
            line=dict(width=1, color='green'),
            showlegend=False,
            name='Both OFS pass',
            hoverinfo='skip',
        )
    )


def _add_scatter_annotations(fig, stat_key, ofs1_name, ofs2_name, stat_display):
    """Add directional comparison text annotations to the 1:1 scatter plot."""
    if stat_key != 'bias':
        fig.add_annotation(
            text=f'Higher {stat_display} for {ofs2_name}',
            xref='paper',
            yref='paper',
            x=0.02,
            y=0.98,
            xanchor='left',
            yanchor='top',
            showarrow=False,
            font=dict(family='Open Sans', size=14, color='black'),
            bgcolor='rgba(255, 255, 255, 0.8)',
            borderwidth=0,
        )
        fig.add_annotation(
            text=f'Higher {stat_display} for {ofs1_name}',
            xref='paper',
            yref='paper',
            x=0.98,
            y=0.02,
            xanchor='right',
            yanchor='bottom',
            showarrow=False,
            font=dict(family='Open Sans', size=14, color='black'),
            bgcolor='rgba(255, 255, 255, 0.8)',
            borderwidth=0,
        )
        return

    anno_size = 12
    quadrants = [
        (
            0.02,
            0.98,
            'left',
            'top',
            f'<i>{ofs2_name} overprediction,<br>{ofs1_name} underprediction</i>',
        ),
        (
            0.98,
            0.02,
            'right',
            'bottom',
            f'<i>{ofs2_name} underprediction,<br>{ofs1_name} overprediction</i>',
        ),
        (
            0.02,
            0.02,
            'left',
            'bottom',
            f'<i>{ofs2_name} underprediction,<br>{ofs1_name} underprediction</i>',
        ),
        (
            0.98,
            0.98,
            'right',
            'top',
            f'<i>{ofs2_name} overprediction,<br>{ofs1_name} overprediction</i>',
        ),
    ]
    for x_pos, y_pos, x_anc, y_anc, text in quadrants:
        fig.add_annotation(
            text=text,
            xref='paper',
            yref='paper',
            x=x_pos,
            y=y_pos,
            xanchor=x_anc,
            yanchor=y_anc,
            showarrow=False,
            font=dict(family='Open Sans', size=anno_size, color='black'),
            bgcolor='rgba(255, 255, 255, 0.8)',
            borderwidth=0,
        )


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements
def generate_stat_comparisons(
    ofs1,
    ofs2,
    var_selection,
    whichcasts,
    home_path,
    start_date,
    end_date,
    filetype1,
    filetype2,
    dir_params,
    logger,
    make_bar_plots=False,
):
    """Read skill stat CSVs and plot interactive 1-to-1 scatters and maps."""
    start_str = start_date.replace('T', ' ').replace('Z', '')
    end_str = end_date.replace('T', ' ').replace('Z', '')

    variable_keywords = {
        'water_level_hw': 'Water Level high tide',
        'water_level_lw': 'Water Level low tide',
        'water_level': 'Water Level',
        'temperature': 'Temperature',
        'water_temperature': 'Temperature',
        'currents_dir': 'Current direction',
        'currents': 'Current speed',
        'salinity': 'Salinity',
    }
    cast_keywords = {
        'nowcast': 'Nowcast',
        'forecast_b': 'Forecast (B)',
        'forecast_a': 'Forecast (A)',
        'hindcast': 'Hindcast',
    }

    logger.info('--- Starting Stats Comparison Plotting (Plotly) ---')

    data_dir = dir_params.get('data_dir', 'data')
    skill_dir = dir_params.get('skill_dir', 'skill')
    stats_dir_name = dir_params.get('stats_dir', 'stats')
    visual_dir_name = dir_params.get('visual_dir', 'visual')

    stats_dir = os.path.join(home_path, data_dir, skill_dir, stats_dir_name)
    vis_dir = os.path.join(home_path, data_dir, visual_dir_name, 'comparisons')
    os.makedirs(vis_dir, exist_ok=True)

    ofs1_file = os.path.join(stats_dir, f'skill_{ofs1}_all_{filetype1}.csv')
    ofs2_file = os.path.join(stats_dir, f'skill_{ofs2}_all_{filetype2}.csv')

    if not os.path.exists(ofs1_file):
        ofs1_file = os.path.join(home_path, f'skill_{ofs1}_all_{filetype1}.csv')
    if not os.path.exists(ofs2_file):
        ofs2_file = os.path.join(home_path, f'skill_{ofs2}_all_{filetype2}.csv')

    if not os.path.exists(ofs1_file) or not os.path.exists(ofs2_file):
        logger.error(
            'Stats files not found. Searched %s and %s.',
            stats_dir,
            home_path,
        )
        sys.exit(1)

    try:
        df1 = pd.read_csv(ofs1_file)
        df2 = pd.read_csv(ofs2_file)

        if 'source_file' in df1.columns:
            df1 = df1[df1['source_file'].str.contains(filetype1, na=False)]
        if 'source_file' in df2.columns:
            df2 = df2[df2['source_file'].str.contains(filetype2, na=False)]

        df1['ID'] = df1['ID'].astype(str)
        df2['ID'] = df2['ID'].astype(str)

        ofs1_key, ofs2_key, ofs1_name, ofs2_name = _resolve_model_keys(
            ofs1, ofs2, filetype1, filetype2
        )

        merged = pd.merge(
            df1,
            df2,
            on=['ID', 'variable', 'type'],
            suffixes=(f'_{ofs1_key}', f'_{ofs2_key}'),
        )

        if merged.empty:
            logger.error('No overlapping stations found in the stats files. Nothing to plot.')
            sys.exit(1)

        stats_to_plot = {
            'rmse': 'RMSE',
            'bias': 'Bias',
            'central_freq': 'Central Frequency',
        }

        vars_to_process_raw = [v.strip() for v in var_selection.split(',')]
        vars_to_process = []
        for v in vars_to_process_raw:
            if v == 'water_level':
                vars_to_process.extend(
                    ['water_level', 'water_level_hw', 'water_level_lw']
                )
            elif v == 'currents':
                vars_to_process.extend(['currents', 'currents_dir'])
            else:
                vars_to_process.append(v)

        vars_to_process = list(dict.fromkeys(vars_to_process))
        casts_to_process = [c.strip() for c in whichcasts.split(',')]

        for var in vars_to_process:
            csv_var = variable_keywords.get(var, var)

            for cast in casts_to_process:
                csv_cast = cast_keywords.get(cast, cast)
                cast_str = str(cast)
                cast_file = cast_str.lower()
                cast_title = cast_file.replace('_b', '')

                var_data = merged[
                    (merged['variable'] == csv_var) & (merged['type'] == csv_cast)
                ].reset_index(drop=True)

                if var_data.empty:
                    logger.warning(
                        'No statistical data found for variable: %s (CSV: %s), cast: %s (CSV: %s).',
                        var,
                        csv_var,
                        cast,
                        csv_cast,
                    )
                    continue

                display_var = var.replace('_', ' ').title()

                var_lower = var.strip().lower()
                if 'water_level' in var_lower or var_lower == 'wl':
                    base_var = 'wl'
                elif 'temperature' in var_lower or var_lower == 'temp':
                    base_var = 'temp'
                elif 'salinity' in var_lower or var_lower == 'salt':
                    base_var = 'salt'
                elif var_lower in ['currents_dir', 'cu_dir']:
                    base_var = 'cu_dir'
                elif 'current' in var_lower or var_lower == 'cu':
                    base_var = 'cu'
                else:
                    base_var = var.replace(' ', '_').lower()

                file_var = var.replace(' ', '_').lower()
                x1 = fetch_error_range(base_var, home_path, logger)

                lon_col = f'X_{ofs1_key}' if f'X_{ofs1_key}' in var_data.columns else 'X'
                lat_col = f'Y_{ofs1_key}' if f'Y_{ofs1_key}' in var_data.columns else 'Y'

                fig_map = go.Figure()
                stat_traces_map = {k: [] for k in stats_to_plot}
                map_trace_idx = 0

                has_coords = (
                    lon_col in var_data.columns
                    and lat_col in var_data.columns
                    and not var_data[lon_col].dropna().empty
                )
                if has_coords:
                    mean_lon = var_data[lon_col].dropna().mean()
                    mean_lat = var_data[lat_col].dropna().mean()
                    lon_diff = var_data[lon_col].max() - var_data[lon_col].min()
                    lat_diff = var_data[lat_col].max() - var_data[lat_col].min()

                    if lat_diff == 0 and lon_diff == 0:
                        zoom_level = 10.0
                    else:
                        zoom_lon = (
                            math.log2(360 / lon_diff) if lon_diff > 0 else 15.0
                        )
                        zoom_lat = (
                            math.log2(180 / lat_diff) if lat_diff > 0 else 15.0
                        )
                        zoom_level = min(zoom_lon, zoom_lat) - 0.25
                else:
                    mean_lon, mean_lat, zoom_level = -95, 38, 4

                for stat_key, stat_display in stats_to_plot.items():
                    stat1 = f'{stat_key}_{ofs1_key}'
                    stat2 = f'{stat_key}_{ofs2_key}'

                    if (
                        stat1 not in var_data.columns
                        or stat2 not in var_data.columns
                    ):
                        continue
                    if (
                        var_data[stat1].isna().all()
                        and var_data[stat2].isna().all()
                    ):
                        continue

                    bar_hover = (
                        '<b>Station ID:</b> %{x}<br><b>Model:</b> %{data.name}'
                        f'<br><b>{stat_display}:</b> %{{y:.3f}}<extra></extra>'
                    )

                    if make_bar_plots:
                        fig_bar = go.Figure()
                        fig_bar.add_trace(
                            go.Bar(
                                x=var_data['ID'],
                                y=var_data[stat1],
                                name=ofs1_name,
                                hovertemplate=bar_hover,
                                marker_color='#d55e00',
                            )
                        )
                        fig_bar.add_trace(
                            go.Bar(
                                x=var_data['ID'],
                                y=var_data[stat2],
                                name=ofs2_name,
                                hovertemplate=bar_hover,
                                marker_color='#0072b2',
                            )
                        )

                        if stat_key == 'central_freq':
                            fig_bar.add_hline(
                                y=90,
                                line_dash='solid',
                                line_color='red',
                                annotation_text='90% Target',
                                annotation_position='top left',
                                annotation_font=dict(color='red', size=13),
                            )
                        elif stat_key == 'rmse' and x1 > 0:
                            fig_bar.add_hline(
                                y=x1,
                                line_dash='solid',
                                line_color='red',
                                annotation_text=f'Target Error ({x1:.2f})',
                                annotation_position='top left',
                                annotation_font=dict(color='red', size=13),
                            )
                        elif stat_key == 'bias' and x1 > 0:
                            fig_bar.add_hline(
                                y=x1,
                                line_dash='solid',
                                line_color='red',
                                annotation_text=f'+Target Error (+{x1:.2f})',
                                annotation_position='top left',
                                annotation_font=dict(color='red', size=13),
                            )
                            fig_bar.add_hline(
                                y=-x1,
                                line_dash='solid',
                                line_color='red',
                                annotation_text=f'-Target Error (-{x1:.2f})',
                                annotation_position='bottom left',
                                annotation_font=dict(color='red', size=13),
                            )

                        bar_title = (
                            f'<b>Station-by-Station {stat_display} Comparison: '
                            f'{display_var} ({cast_title})</b>'
                        )
                        fig_bar.update_layout(
                            barmode='group',
                            title=dict(
                                text=bar_title,
                                font=dict(size=14, color='black', family='Open Sans'),
                                y=0.97,
                                x=0.5,
                                xanchor='center',
                                yanchor='top',
                            ),
                            template='plotly_white',
                            hovermode='x unified',
                            hoverlabel=dict(
                                bgcolor='white',
                                bordercolor='#cccccc',
                                font=dict(
                                    family='Open Sans', size=13, color='#333333'
                                ),
                            ),
                            legend=dict(
                                orientation='h',
                                yanchor='bottom',
                                y=1.02,
                                xanchor='left',
                                x=0,
                                font=dict(size=16, color='black'),
                                itemclick=False,
                                itemdoubleclick=False,
                            ),
                            margin=dict(t=100, b=100),
                            height=550,
                            width=1000,
                        )
                        fig_bar.update_xaxes(
                            title_text='Station ID',
                            titlefont=dict(
                                family='Open Sans', color='black', size=18
                            ),
                            mirror=True,
                            ticks='inside',
                            showline=True,
                            linecolor='black',
                            linewidth=1,
                            tickangle=45,
                            tickfont=dict(
                                family='Open Sans', color='black', size=14
                            ),
                            minor=dict(
                                ticklen=4,
                                tickcolor='black',
                                ticks='inside',
                                showgrid=False,
                            ),
                        )
                        fig_bar.update_yaxes(
                            title_text=stat_display,
                            titlefont=dict(
                                family='Open Sans', color='black', size=17
                            ),
                            mirror=True,
                            ticks='inside',
                            showline=True,
                            linecolor='black',
                            linewidth=1,
                            showgrid=True,
                            tickfont=dict(
                                family='Open Sans', color='black', size=14
                            ),
                            minor=dict(
                                ticklen=4,
                                tickcolor='black',
                                ticks='inside',
                                showgrid=False,
                            ),
                            zeroline=(stat_key == 'bias'),
                            zerolinewidth=1,
                            zerolinecolor='black',
                        )

                        out_file_cat = os.path.join(
                            vis_dir,
                            f'{ofs1_key}_vs_{ofs2_key}_{file_var}_{cast_file}_'
                            f'{stat_key}_stations.html',
                        )
                        fig_bar.write_html(out_file_cat)

                    # 1-to-1 Scatter Plot
                    fig_stat_scat = go.Figure()
                    stat_scat_hover = (
                        '<b>Station ID:</b> %{customdata}<br><b>'
                        f'{ofs1_name}:</b> %{{x:.3f}}<br><b>'
                        f'{ofs2_name}:</b> %{{y:.3f}}<extra></extra>'
                    )

                    s1_num = pd.to_numeric(var_data[stat1], errors='coerce')
                    s2_num = pd.to_numeric(var_data[stat2], errors='coerce')

                    if stat_key == 'central_freq':
                        pass_1 = s1_num >= 90
                        pass_2 = s2_num >= 90
                    elif stat_key == 'rmse' and x1 > 0:
                        pass_1 = s1_num <= x1
                        pass_2 = s2_num <= x1
                    elif stat_key == 'bias' and x1 > 0:
                        pass_1 = s1_num.abs() <= x1
                        pass_2 = s2_num.abs() <= x1
                    else:
                        pass_1 = pd.Series(True, index=var_data.index)
                        pass_2 = pd.Series(True, index=var_data.index)

                    fail_1 = ~pass_1
                    fail_2 = ~pass_2

                    min_val = min(s1_num.min(), s2_num.min())
                    max_val = max(s1_num.max(), s2_num.max())

                    if stat_key == 'central_freq':
                        min_val = min(min_val, 85)
                        max_val = max(max_val, 100)
                    elif stat_key == 'rmse' and x1 > 0:
                        max_val = max(max_val, x1 * 2 * 1.1)
                        min_val = min(min_val, 0)
                    elif stat_key == 'bias' and x1 > 0:
                        min_val = min(min_val, -x1 * 2 * 1.1)
                        max_val = max(max_val, x1 * 2 * 1.1)

                    axis_range = None
                    if pd.notna(min_val) and pd.notna(max_val):
                        diff = max_val - min_val
                        buffer = diff * 0.1 if diff != 0 else 0.1
                        axis_range = [min_val - buffer, max_val + buffer]

                        safe_x, safe_y = None, None
                        if stat_key == 'central_freq':
                            safe_x, safe_y = [90, axis_range[1]], [90, axis_range[1]]
                        elif stat_key == 'rmse' and x1 > 0:
                            safe_x, safe_y = [axis_range[0], x1], [axis_range[0], x1]
                        elif stat_key == 'bias' and x1 > 0:
                            safe_x, safe_y = [-x1, x1], [-x1, x1]

                        if safe_x is not None and safe_y is not None:
                            _add_scatter_corridors(
                                fig_stat_scat, axis_range, safe_x, safe_y
                            )

                        fig_stat_scat.add_trace(
                            go.Scatter(
                                x=axis_range,
                                y=axis_range,
                                mode='lines',
                                name='1:1 Line',
                                hoverinfo='skip',
                                showlegend=False,
                                line=dict(color='black', dash='dash', width=1),
                            )
                        )

                    cat_masks = {
                        'Both Pass': (pass_1 & pass_2, '#009E73'),
                        f'{ofs1_name} Fails, {ofs2_name} Passes': (
                            fail_1 & pass_2,
                            '#56B4E9',
                        ),
                        f'{ofs2_name} Fails, {ofs1_name} Passes': (
                            pass_1 & fail_2,
                            '#E69F00',
                        ),
                        'Both Fail': (fail_1 & fail_2, '#D55E00'),
                    }

                    is_first_stat = not any(stat_traces_map.values())

                    for label, (mask, color) in cat_masks.items():
                        if not mask.any():
                            continue
                        subset = var_data[mask]

                        fig_stat_scat.add_trace(
                            go.Scatter(
                                x=subset[stat1],
                                y=subset[stat2],
                                mode='markers',
                                customdata=subset['ID'],
                                hovertemplate=stat_scat_hover,
                                name=label,
                                showlegend=True,
                                marker=dict(
                                    size=10,
                                    color=color,
                                    opacity=0.8,
                                    line=dict(color='black', width=1),
                                ),
                            )
                        )

                        if (
                            lon_col in subset.columns
                            and lat_col in subset.columns
                        ):
                            custom_data = list(
                                zip(
                                    subset['ID'],
                                    subset[stat1],
                                    subset[stat2],
                                )
                            )
                            map_hover = (
                                '<b>Station ID:</b> %{customdata[0]}'
                                f'<br><b>Status:</b> {label}'
                                f'<br><b>{ofs1_name} {stat_display}:</b> %{{customdata[1]:.3f}}'
                                f'<br><b>{ofs2_name} {stat_display}:</b> %{{customdata[2]:.3f}}'
                                '<extra></extra>'
                            )
                            fig_map.add_trace(
                                go.Scattermap(
                                    lon=subset[lon_col],
                                    lat=subset[lat_col],
                                    mode='markers',
                                    customdata=custom_data,
                                    hovertemplate=map_hover,
                                    name=label,
                                    visible=is_first_stat,
                                    marker=dict(size=12, color=color, opacity=0.8),
                                )
                            )
                            stat_traces_map[stat_key].append(map_trace_idx)
                            map_trace_idx += 1

                    _add_scatter_annotations(
                        fig_stat_scat,
                        stat_key,
                        ofs1_name,
                        ofs2_name,
                        stat_display,
                    )

                    scat_title = (
                        f'<b>{ofs1_name} vs {ofs2_name} {stat_display}:<br>'
                        f'{display_var} ({cast_title})</b><br>'
                        f'<span style="font-size:16px">{start_str} to {end_str}</span>'
                    )
                    fig_stat_scat.update_layout(
                        title=dict(
                            text=scat_title,
                            font=dict(size=18, color='black', family='Open Sans'),
                            y=0.98,
                            x=0.5,
                            xanchor='center',
                            yanchor='top',
                        ),
                        template='plotly_white',
                        hovermode='closest',
                        hoverlabel=dict(
                            bgcolor='white',
                            bordercolor='#cccccc',
                            font=dict(
                                family='Open Sans', size=13, color='#333333'
                            ),
                        ),
                        legend=dict(
                            orientation='h',
                            yanchor='bottom',
                            y=1.02,
                            xanchor='left',
                            x=0,
                            font=dict(size=12, color='black'),
                            itemclick='toggle',
                            itemdoubleclick=False,
                            entrywidth=0.48,
                            entrywidthmode='fraction',
                        ),
                        margin=dict(t=125, b=100),
                        height=660,
                        width=650,
                    )
                    fig_stat_scat.update_xaxes(
                        title_text=f'{ofs1_name} {stat_display}',
                        range=axis_range,
                        titlefont=dict(
                            family='Open Sans', color='black', size=18
                        ),
                        mirror=True,
                        ticks='inside',
                        showline=True,
                        linecolor='black',
                        linewidth=1,
                        showgrid=True,
                        tickfont=dict(
                            family='Open Sans', color='black', size=14
                        ),
                        minor=dict(
                            ticklen=4,
                            tickcolor='black',
                            ticks='inside',
                            showgrid=False,
                        ),
                        zeroline=(stat_key == 'bias'),
                        zerolinewidth=1,
                        zerolinecolor='black',
                    )
                    fig_stat_scat.update_yaxes(
                        title_text=f'{ofs2_name} {stat_display}',
                        range=axis_range,
                        titlefont=dict(
                            family='Open Sans', color='black', size=18
                        ),
                        mirror=True,
                        ticks='inside',
                        showline=True,
                        linecolor='black',
                        linewidth=1,
                        showgrid=True,
                        tickfont=dict(
                            family='Open Sans', color='black', size=14
                        ),
                        minor=dict(
                            ticklen=4,
                            tickcolor='black',
                            ticks='inside',
                            showgrid=False,
                        ),
                        scaleanchor='x',
                        scaleratio=1,
                        zeroline=(stat_key == 'bias'),
                        zerolinewidth=1,
                        zerolinecolor='black',
                    )

                    out_file_scat = os.path.join(
                        vis_dir,
                        f'{ofs1_key}_vs_{ofs2_key}_{file_var}_{cast_file}_'
                        f'{stat_key}_scatter.html',
                    )
                    fig_stat_scat.write_html(out_file_scat)

                if map_trace_idx > 0:
                    total_traces = len(fig_map.data)
                    buttons = []
                    first_valid_stat = None

                    for stat_k, stat_disp in stats_to_plot.items():
                        if stat_traces_map[stat_k]:
                            if first_valid_stat is None:
                                first_valid_stat = stat_disp

                            viz_array = [False] * total_traces
                            for idx in stat_traces_map[stat_k]:
                                viz_array[idx] = True

                            map_dyn_title = (
                                f'<b>{ofs1_name} vs {ofs2_name} Comparison Map: '
                                f'{display_var} ({cast_title})<br>Statistic: '
                                f'{stat_disp}</b><br>'
                                f'<span style="font-size:16px">{start_str} to {end_str}</span>'
                            )
                            buttons.append(
                                dict(
                                    label=f'{stat_disp} View',
                                    method='update',
                                    args=[
                                        {'visible': viz_array},
                                        {'title.text': map_dyn_title},
                                    ],
                                )
                            )

                    map_main_title = (
                        f'<b>{ofs1_name} vs {ofs2_name} Comparison Map: '
                        f'{display_var} ({cast_title})<br>Statistic: '
                        f'{first_valid_stat}</b><br>'
                        f'<span style="font-size:16px">{start_str} to {end_str}</span>'
                    )
                    fig_map.update_layout(
                        title=dict(
                            text=map_main_title,
                            font=dict(size=18, color='black', family='Open Sans'),
                            y=0.98,
                            x=0.5,
                            xanchor='center',
                            yanchor='top',
                        ),
                        map_style='carto-positron',
                        map=dict(
                            center=dict(lat=mean_lat, lon=mean_lon),
                            zoom=zoom_level,
                        ),
                        legend=dict(
                            orientation='h',
                            yanchor='top',
                            y=-0.05,
                            xanchor='center',
                            x=0.5,
                            font=dict(size=12, color='black'),
                            itemclick='toggle',
                            itemdoubleclick=False,
                            entrywidth=0.48,
                            entrywidthmode='fraction',
                        ),
                        updatemenus=[
                            dict(
                                type='dropdown',
                                direction='down',
                                x=0.01,
                                xanchor='left',
                                y=0.99,
                                yanchor='top',
                                buttons=buttons,
                                font=dict(color='black'),
                            )
                        ],
                        annotations=[
                            dict(
                                text='<i>Select mapped statistics from dropdown:</i>',
                                x=0.01,
                                y=1.0,
                                xref='paper',
                                yref='paper',
                                showarrow=False,
                                xanchor='left',
                                yanchor='bottom',
                                font=dict(color='black', size=13),
                            )
                        ],
                        margin=dict(t=120, b=100, l=40, r=40),
                        height=700,
                        width=800,
                    )
                    out_file_map = os.path.join(
                        vis_dir,
                        f'{ofs1_key}_vs_{ofs2_key}_{file_var}_{cast_file}_'
                        'all_stats_map.html',
                    )
                    fig_map.write_html(out_file_map)

        logger.info('Saved stats comparisons to %s', vis_dir)
    except (KeyError, ValueError, OSError) as err:
        logger.error('Error generating stat comparisons: %s', err)


def setup_logger(home_path, config_file_arg):
    """Set up the logger by reading conf/logging.conf."""
    try:
        # pylint: disable=import-outside-toplevel
        from ofs_skill.obs_retrieval import utils
        config_file = utils.Utils(config_file_arg).get_config_file()
    except (FileNotFoundError, ImportError) as err:
        print(f'CRITICAL ERROR: {err}')
        sys.exit(1)

    log_config_file = os.path.join(home_path, 'conf', 'logging.conf')

    if not os.path.isfile(log_config_file):
        print(f'CRITICAL ERROR: Log config file not found at {log_config_file}')
        sys.exit(1)

    if not os.path.isfile(config_file):
        print(f'CRITICAL ERROR: Config file not found at {config_file}')
        sys.exit(1)

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using config %s', config_file)
    logger.info('Using log config %s', log_config_file)

    return logger


def run_skill_assessment(ofs_name, filetype, args, logger, create_1dplot):
    """Configure and run create_1dplot for a given OFS."""
    logger.info('--- Running 1D Plot Assessment for %s ---', ofs_name.upper())

    prop = model_properties.ModelProperties()
    prop.ofs = ofs_name.lower()
    prop.path = args.home_path
    prop.start_date_full = args.start_date
    prop.end_date_full = args.end_date
    prop.whichcasts = args.whichcasts
    prop.datum = args.datum
    prop.ofsfiletype = filetype
    prop.stationowner = args.station_owner
    prop.horizonskill = False
    prop.forecast_hr = 'now'
    prop.var_list = args.var_selection
    prop.aux_vars = ''
    prop.filecheck = False
    prop.config_file = args.config
    prop.user_input_location = False

    create_1dplot(prop, logger)


def setup_overlap_inventories(ofs1, ofs2, args, dir_params, logger):
    """Copy overlap inventory to OFS-specific files and invalidate stale caches."""
    control_dir = os.path.join(args.home_path, dir_params.get('control_files_dir', 'control_files'))
    overlap_name = f'{ofs1}_{ofs2}_overlap'
    overlap_csv = os.path.join(control_dir, f'inventory_all_{overlap_name}.csv')

    if not os.path.exists(overlap_csv):
        logger.error(
            'Overlap inventory not found at %s. Cannot proceed.', overlap_csv
        )
        sys.exit(1)

    ofs1_csv = os.path.join(control_dir, f'inventory_all_{ofs1}.csv')
    ofs2_csv = os.path.join(control_dir, f'inventory_all_{ofs2}.csv')

    logger.info(
        'Setting %s and %s inventories to overlapping stations only...',
        ofs1,
        ofs2,
    )
    shutil.copy2(overlap_csv, ofs1_csv)
    shutil.copy2(overlap_csv, ofs2_csv)

    for ofs, target_csv in [(ofs1, ofs1_csv), (ofs2, ofs2_csv)]:
        overlap_sig = cache_manifest.inventory_signature(
            ofs, args.start_date, args.end_date, args.station_owner.split(',')
        )
        cache_manifest.record_artifact(
            target_csv, overlap_sig, args.home_path, logger
        )

    data_dir = dir_params.get('data_dir', 'data')
    inventory_dir = dir_params.get('inventory_dir', 'inventory')

    cache_dirs = [
        control_dir,
        os.path.join(args.home_path, data_dir, inventory_dir),
    ]
    for c_dir in cache_dirs:
        if not os.path.exists(c_dir):
            continue
        for f in os.listdir(c_dir):
            if f.endswith(('.pkl', '.parquet', '.json')) and (
                f.startswith(ofs1) or f.startswith(ofs2)
            ):
                cached_file = os.path.join(c_dir, f)
                try:
                    os.remove(cached_file)
                    logger.info('Deleted cached binary to force rebuild: %s', cached_file)
                except OSError as err:
                    logger.debug('Could not remove cache file %s: %s', cached_file, err)

    for ofs in [ofs1, ofs2]:
        sig = cache_manifest.inventory_signature(
            ofs, args.start_date, args.end_date, args.station_owner.split(',')
        )
        for c_dir in cache_dirs:
            for ext in ['.pkl', '.parquet', '.json']:
                cached_file = os.path.join(c_dir, f'{sig}{ext}')
                if os.path.exists(cached_file):
                    try:
                        os.remove(cached_file)
                        logger.info('Deleted cache signature binary: %s', cached_file)
                    except OSError as err:
                        logger.debug('Could not remove %s: %s', cached_file, err)

    return overlap_csv


def main(args):
    """Run shapefile intersection, restricted assessment, and comparisons."""
    ofs1, ofs2 = args.ofs1.lower(), args.ofs2.lower()

    # We must setup the logger BEFORE fetching the configs through utils
    logger = setup_logger(args.home_path, args.config)

    # Unconditionally add both legacy bin directories to sys.path so the scripts can be found
    vis_dir = os.path.join(args.home_path, 'bin', 'visualization')
    utils_dir = os.path.join(args.home_path, 'bin', 'utils')

    for dynamic_dir in [vis_dir, utils_dir]:
        if not os.path.exists(dynamic_dir):
            logger.error("CRITICAL ERROR: Could not find 'bin' directory at %s", dynamic_dir)
            sys.exit(1)
        if dynamic_dir not in sys.path:
            sys.path.insert(0, dynamic_dir)

    # Use utils from the module, fallback to local path if not in env
    try:
        from ofs_skill.obs_retrieval import utils
    except ImportError:
        import utils

    # pylint: disable=import-outside-toplevel
    from create_1dplot import create_1dplot
    from get_shapefile_intersection import get_shapefile_intersection

    # Parse configured directories to match pipeline outputs
    dir_params = utils.Utils(args.config).read_config_section('directories', logger)

    if not args.skip_1d_runs:
        logger.info('=== Pre-checking Model Files ===')
        for ofs_name, f_type in [(ofs1, args.filetype1), (ofs2, args.filetype2)]:
            pre_prop = model_properties.ModelProperties()
            pre_prop.ofs = ofs_name
            pre_prop.path = args.home_path
            pre_prop.start_date_full = args.start_date
            pre_prop.end_date_full = args.end_date
            pre_prop.whichcasts = args.whichcasts.split(',')
            pre_prop.ofsfiletype = f_type
            pre_prop.config_file = args.config

            logger.info('Verifying files for %s...', ofs_name.upper())
            check_model_files(pre_prop, logger)
            logger.info('Model file check was successful for %s.', ofs_name.upper())

    logger.info('=== Computing Shapefile Intersection ===')
    get_shapefile_intersection(
        shp1=ofs1,
        shp2=ofs2,
        home_path=args.home_path,
        stationowner=args.station_owner,
        config_file=args.config,
        logger=logger,
    )

    logger.info('=== Applying Overlap Restrictions ===')
    overlap_csv = setup_overlap_inventories(ofs1, ofs2, args, dir_params, logger)

    if not args.skip_1d_runs:
        logger.info('=== Running Assessment on Overlapping Stations ===')
        for ofs_name, f_type in [(ofs1, args.filetype1), (ofs2, args.filetype2)]:
            run_skill_assessment(ofs_name, f_type, args, logger, create_1dplot)

    logger.info('=== Generating Data Comparisons ===')
    generate_comparisons(
        ofs1,
        ofs2,
        overlap_csv,
        args.var_selection,
        args.whichcasts,
        args.home_path,
        args.datum,
        args.start_date,
        args.end_date,
        args.filetype1,
        args.filetype2,
        dir_params,
        logger,
    )

    logger.info('=== Generating Stats Comparisons ===')
    generate_stat_comparisons(
        ofs1,
        ofs2,
        args.var_selection,
        args.whichcasts,
        args.home_path,
        args.start_date,
        args.end_date,
        args.filetype1,
        args.filetype2,
        dir_params,
        logger,
        make_bar_plots=args.make_bar_plots,
    )

    logger.info('Program Complete!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run shapefile intersection, restricted skill assessment, and comparisons.'
    )
    parser.add_argument('-o1', '--ofs1', required=True, help='First OFS to overlap')
    parser.add_argument('-o2', '--ofs2', required=True, help='Second OFS to overlap')
    parser.add_argument(
        '-p', '--home_path', required=True, help='Path to package installation'
    )
    parser.add_argument(
        '-s', '--start_date', required=True, help='Assessment start date'
    )
    parser.add_argument(
        '-e', '--end_date', required=True, help='Assessment end date'
    )
    parser.add_argument(
        '-vs',
        '--var_selection',
        default='water_level,water_temperature,salinity,currents',
        help='Variables to assess',
    )
    parser.add_argument(
        '-ws', '--whichcasts', default='nowcast', help='Whichcasts to assess'
    )
    parser.add_argument(
        '-so',
        '--station_owner',
        default='co-ops,ndbc,usgs,chs',
        help='Station providers',
    )
    parser.add_argument('-d', '--datum', default='MLLW', help='Datum')
    parser.add_argument(
        '-t1',
        '--filetype1',
        default='stations',
        help='OFS filetype for ofs1: fields or stations',
    )
    parser.add_argument(
        '-t2',
        '--filetype2',
        default='stations',
        help='OFS filetype for ofs2: fields or stations',
    )
    parser.add_argument('-c', '--config', help='Path to config file')
    parser.add_argument(
        '-b',
        '--make_bar_plots',
        action='store_true',
        help='Also generate station-by-station grouped bar plots for each '
        'skill statistic (off by default; scatter plots are always made).',
    )
    parser.add_argument(
        '-skip',
        '--skip_1d_runs',
        action='store_true',
        help='Skip the 1D pipelines if necessary files are already present.',
    )

    main(parser.parse_args())
