"""Summary bar plots: RMSE and central frequency, station-by-station.

Reads the per-station skill stats CSV emitted by
``get_skill._skill_for_variable`` and produces two stacked bar charts
(one per metric) covering every station / bin / extremum row in the
file.  Output:

* PNG (matplotlib) into ``prop.om_files`` -- gated on ``prop.static_plots``.
* HTML (plotly) into ``prop.visuals_1d_station_path`` -- always written.

Color encoding mirrors the forecast-horizon ``bar_plots`` in
``make_static_plots``: palegreen / lightcoral against the
``target_error_range`` (RMSE) and 90% central-frequency thresholds.
"""
from __future__ import annotations

import os
from collections.abc import Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import ofs_skill.visualization.plotting_functions as plotting_functions

_PASS = 'palegreen'
_FAIL = 'lightcoral'
_PASS_HEX = '#98FB98'
_FAIL_HEX = '#F08080'


def _x_labels(df: pd.DataFrame, name_var: str) -> list[str]:
    """One label per CSV row.  Currents bins get ``ID@<depth>m`` so
    multiple bins on the same station do not collide on the x-axis."""
    if name_var == 'cu' and 'obs_water_depth' in df.columns:
        labels = []
        for sid, depth in zip(df['ID'], df['obs_water_depth']):
            try:
                labels.append(f'{sid}@{float(depth):.1f}m')
            except (TypeError, ValueError):
                labels.append(str(sid))
        return labels
    return [str(sid) for sid in df['ID']]


def _rmse_colors(values: Sequence[float | None], x1: float,
                 hex_form: bool = False) -> list[str]:
    pass_c, fail_c = (_PASS_HEX, _FAIL_HEX) if hex_form else (_PASS, _FAIL)
    out = []
    for v in values:
        if v is None:
            out.append(fail_c)
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            out.append(fail_c)
            continue
        if not np.isfinite(fv):
            out.append(fail_c)
            continue
        out.append(pass_c if -x1 <= fv <= x1 else fail_c)
    return out


def _cf_colors(values: Sequence[float | None],
               hex_form: bool = False) -> list[str]:
    pass_c, fail_c = (_PASS_HEX, _FAIL_HEX) if hex_form else (_PASS, _FAIL)
    out = []
    for v in values:
        if v is None:
            out.append(fail_c)
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            out.append(fail_c)
            continue
        if not np.isfinite(fv):
            out.append(fail_c)
            continue
        out.append(pass_c if fv >= 90 else fail_c)
    return out


def _figure_title(prop, variable: str, name_var: str) -> str:
    """Title for the summary figure.  Doesn't reuse ``get_title_static``
    because that helper is per-station (queries CO-OPS metadata for a
    single NWS ID).  Summary plots cover all stations, so we just print
    OFS / variable / whichcast / date range."""
    start = prop.start_date_full.replace('Z', '').replace('T', ' ')
    end = prop.end_date_full.replace('Z', '').replace('T', ' ')
    forecast_tag = ''
    if 'forecast_a' in getattr(prop, 'whichcasts', []) and \
            getattr(prop, 'forecast_hr', None):
        forecast_tag = f' ({prop.forecast_hr} cycle)'
    return (
        f'NOAA/NOS OFS Skill Assessment -- station summary\n'
        f'OFS: {prop.ofs.upper()}    Variable: {variable}    '
        f'Whichcast: {prop.whichcast}{forecast_tag}\n'
        f'From: {start}    To: {end}'
    )


def _ymax_for_rmse(values: Sequence[float], x1: float) -> float:
    arr = np.array(values, dtype=float)
    if not np.isfinite(arr).any():
        return x1 * 2
    mult = np.ceil(np.nanmax(arr) / x1) if x1 > 0 else 2
    if mult < 2:
        mult = 2
    return float(x1 * mult)


def write_static_summary_bars(df: pd.DataFrame, name_var: str, variable: str,
                              prop, logger) -> str | None:
    """Two-row PNG figure: RMSE on top, CF on bottom."""
    x1, _ = plotting_functions.get_error_range(name_var, prop, logger)
    labels = _x_labels(df, name_var)
    rmse_vals = list(df['rmse'])
    cf_vals = list(df['central_freq'])

    fig, axs = plt.subplots(2, 1)
    fig.set_figheight(14)
    fig.set_figwidth(max(12, 0.5 * len(labels) + 6))
    fig.suptitle(_figure_title(prop, variable, name_var),
                 fontsize=16, fontweight='bold')

    # --- RMSE ---
    rmse_ymax = _ymax_for_rmse(rmse_vals, x1)
    x_positions = np.arange(len(labels))
    axs[0].bar(x_positions, rmse_vals, color=_rmse_colors(rmse_vals, x1),
               edgecolor='black', linewidth=1)
    axs[0].set_ylabel('RMSE', fontsize=18)
    axs[0].tick_params(axis='y', labelsize=14)
    axs[0].set_xticks(x_positions)
    axs[0].set_xticklabels(labels, rotation=60, ha='right', fontsize=12)
    axs[0].set_ylim(0, rmse_ymax)
    axs[0].axhline(y=x1, color='red', linewidth=1, linestyle='--',
                   label=f'Target error range (±{x1})')
    axs[0].legend(fontsize=14, loc='upper right',
                  facecolor='white', framealpha=0.75)

    # --- Central frequency ---
    axs[1].bar(x_positions, cf_vals, color=_cf_colors(cf_vals),
               edgecolor='black', linewidth=1)
    axs[1].set_ylabel('Central frequency (%)', fontsize=18)
    axs[1].tick_params(axis='y', labelsize=14)
    axs[1].set_xticks(x_positions)
    axs[1].set_xticklabels(labels, rotation=60, ha='right', fontsize=12)
    axs[1].set_xlabel('Station', fontsize=16)
    axs[1].set_ylim(0, 100)
    axs[1].axhline(y=90, color='red', linewidth=1, linestyle='--',
                   label='90% acceptance criteria')
    axs[1].legend(fontsize=14, loc='lower right',
                  facecolor='white', framealpha=0.75)

    fig.tight_layout(rect=(0, 0, 1, 0.97))

    filename = (
        f'{prop.ofs}_summary_barplot_{variable}_{prop.whichcast}_'
        f'{prop.ofsfiletype}.png'
    )
    out_path = os.path.join(prop.om_files, filename)
    fig.savefig(out_path, format='png', dpi=100, bbox_inches='tight')
    plt.close(fig)
    logger.info('Wrote summary bar PNG: %s', out_path)
    return out_path


def write_html_summary_bars(df: pd.DataFrame, name_var: str, variable: str,
                            prop, logger) -> str | None:
    """Two-row plotly figure: RMSE on top, CF on bottom."""
    x1, _ = plotting_functions.get_error_range(name_var, prop, logger)
    labels = _x_labels(df, name_var)
    rmse_vals = [float(v) if pd.notna(v) else None for v in df['rmse']]
    cf_vals = [float(v) if pd.notna(v) else None for v in df['central_freq']]

    custom = list(zip(
        df.get('ID', pd.Series([''] * len(df))).astype(str),
        df.get('NODE', pd.Series([''] * len(df))).astype(str),
        df.get('obs_water_depth', pd.Series([''] * len(df))).astype(str),
        df.get('central_freq_pass_fail',
               pd.Series([''] * len(df))).astype(str),
    ))

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=(
            f'RMSE per station (target ±{x1})',
            'Central frequency per station (≥ 90% pass)',
        ),
    )
    for annot in fig['layout']['annotations']:
        annot['font'] = dict(family='Open Sans', size=14, color='black')
    fig.add_trace(
        go.Bar(
            x=labels, y=rmse_vals,
            marker=dict(
                color=_rmse_colors(rmse_vals, x1, hex_form=True),
                line=dict(color='black', width=1),
            ),
            customdata=custom,
            hovertemplate=(
                'Station: %{customdata[0]}<br>'
                'Node: %{customdata[1]}<br>'
                'Obs depth: %{customdata[2]} m<br>'
                'RMSE: %{y:.3f}<extra></extra>'
            ),
            name='RMSE',
            showlegend=False,
        ),
        row=1, col=1,
    )
    fig.add_hline(y=x1, line_dash='dash', line_color='red', row=1, col=1,
                  annotation_text=f'Target error range (±{x1})',
                  annotation_position='top right')
    fig.add_hline(y=-x1, line_dash='dash', line_color='red',
                  row=1, col=1)

    fig.add_trace(
        go.Bar(
            x=labels, y=cf_vals,
            marker=dict(
                color=_cf_colors(cf_vals, hex_form=True),
                line=dict(color='black', width=1),
            ),
            customdata=custom,
            hovertemplate=(
                'Station: %{customdata[0]}<br>'
                'Node: %{customdata[1]}<br>'
                'Obs depth: %{customdata[2]} m<br>'
                'CF: %{y:.2f}%<br>'
                'Pass/fail: %{customdata[3]}<extra></extra>'
            ),
            name='CF',
            showlegend=False,
        ),
        row=2, col=1,
    )
    fig.add_hline(y=90, line_dash='dash', line_color='red', row=2, col=1,
                  annotation_text='90% acceptance', annotation_position='top right')

    axis_title_font = dict(family='Open Sans', color='black')
    axis_tick_font = dict(family='Open Sans', size=14, color='black')
    fig.update_yaxes(title_text='RMSE', title_font=axis_title_font,
                     tickfont=axis_tick_font, row=1, col=1)
    fig.update_yaxes(title_text='Central frequency (%)',
                     title_font=axis_title_font,
                     tickfont=axis_tick_font,
                     range=[0, 100], row=2, col=1)
    fig.update_xaxes(title_text='Station', title_font=axis_title_font,
                     tickfont=axis_tick_font,
                     row=2, col=1, tickangle=-60)
    fig.update_xaxes(tickfont=axis_tick_font, row=1, col=1)
    fig_height = 850
    fig_width = 1100
    fig.update_layout(
        title=dict(
            text='<b>' + _figure_title(prop, variable, name_var)
                .replace('\n', '<br>') + '</b>',
            font=dict(size=14, color='black', family='Open Sans'),
            y=0.97,
            x=0.5, xanchor='center', yanchor='top',
        ),
        font=dict(family='Open Sans', color='black'),
        template='plotly_white',
        height=fig_height,
        width=fig_width,
        margin=dict(l=80, r=40, t=150, b=120),
    )

    filename = (
        f'{prop.ofs}_summary_barplot_{variable}_{prop.whichcast}_'
        f'{prop.ofsfiletype}.html'
    )
    out_path = os.path.join(prop.visuals_1d_station_path, filename)
    fig_config = {
        'toImageButtonOptions': {
            'format': 'png',
            'filename': filename.removesuffix('.html'),
            'height': fig_height,
            'width': fig_width,
            'scale': 1,
        }
    }
    fig.write_html(out_path, config=fig_config)
    logger.info('Wrote summary bar HTML: %s', out_path)
    return out_path


def _csv_path(prop, variable: str) -> str:
    return os.path.join(
        prop.data_skill_stats_path,
        f'skill_{prop.ofs}_{variable}_{prop.whichcast}_'
        f'{prop.ofsfiletype}.csv',
    )


def _process_one(variable: str, name_var: str, prop, logger) -> None:
    csv_path = _csv_path(prop, variable)
    if not os.path.isfile(csv_path):
        logger.warning(
            'Summary bar plot: skill stats CSV not found, skipping: %s',
            csv_path,
        )
        return
    try:
        df = pd.read_csv(csv_path)
    except (OSError, pd.errors.ParserError) as ex:
        logger.warning(
            'Summary bar plot: failed to read %s (%s) -- skipping',
            csv_path, ex)
        return
    if df.empty or 'rmse' not in df.columns or \
            'central_freq' not in df.columns:
        logger.warning(
            'Summary bar plot: %s missing rmse/central_freq columns or '
            'has no rows -- skipping', csv_path)
        return
    if 'X' in df.columns:
        df = df.copy()
        df['X'] = pd.to_numeric(df['X'], errors='coerce')
        df = df.sort_values(by='X', kind='mergesort').reset_index(drop=True)

    write_html_summary_bars(df, name_var, variable, prop, logger)
    if getattr(prop, 'static_plots', False):
        write_static_summary_bars(df, name_var, variable, prop, logger)


def make_summary_bars(prop, var_info, logger) -> None:
    """Public entry point.

    ``var_info`` follows the create_1dplot convention:
    ``[variable, name_var, list_of_headings]``.

    For ``water_level`` we additionally emit ``_hw`` and ``_lw`` summary
    plots when those CSVs exist.
    """
    variable, name_var = var_info[0], var_info[1]
    _process_one(variable, name_var, prop, logger)
    if name_var == 'wl':
        for suffix in ('hw', 'lw'):
            _process_one(f'{variable}_{suffix}', name_var, prop, logger)
