"""
Created July 2025

@author: PWL
"""

from __future__ import annotations

import copy
import glob
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ofs_skill.utils import plot_units
from ofs_skill.visualization import make_static_plots, plotting_functions

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))


def make_table(grouped, info, prop, stat, by='modelcycles'):
    """
    Writes a table to file for a given statistic. Each table row is a station
    ID, and each column is either a model cycle (``by='modelcycles'``) or a
    6-hour forecast-horizon bin (``by='horizonbins'``). Two separate CSVs are
    produced so both scorecard flavours (model-cycle and forecast-horizon)
    can be built downstream by make_flag_images.
    Called by plotting functions below.

    PARAMETERS:
    ----------
    grouped: a grouped pandas dataframe created using 'groupby'
    info: list of station info strings -->
        info[0] = variable
        info[1] = model node
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
        info[5] = list of all station IDs
        info[6] = 'doextraplots' boolean True/False
        info[7] = full variable name
    prop: model properties object containing date range, etc.
    stat: a string describing the statistic used in the grouped dataframe, e.g.
    'cf' for central frequency. Used in the file name.
    by: 'modelcycles' or 'horizonbins' -- controls the column axis and the
        output filename so the two scorecard tables don't collide.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a table to file.
    """

    # Get error range
    # X1, X2 = plotting_functions.get_error_range(info[0],prop,logger)
    # Make dataframe from 'groupby' pandas series
    df_grouped = pd.DataFrame(grouped)
    df_grouped = df_grouped.transpose()
    # Column labels come from the groupby index (model cycles or hour-bin
    # edges). Stringify them so appending station rows with pandas.concat
    # aligns on a stable, consistent set of column names. Without this,
    # integer hour-bin labels that differ per station get mangled into
    # duplicated columns (6, 6.1, 6.2, ...) on concat.
    df_grouped.columns = [str(c) for c in df_grouped.columns]
    # Add station ID
    df_grouped.insert(loc=0, column='ID', value=info[2])
    # Append to existing file if there is one
    filename = f'{prop.ofs}_{info[0]}_{by}_{stat}.csv'
    filepath = os.path.join(prop.data_horizon_1d_pair_path, filename)
    if os.path.isfile(filepath) and info[2] != info[5][0]:
        df_file = pd.read_csv(filepath)
        df_file['ID'] = df_file['ID'].astype(str)
        # Align on the union of columns so each station row lands under the
        # correct cycle/bin column (missing entries stay NaN => 'no data').
        df_grouped = pd.concat([df_file, df_grouped], ignore_index=True)
        df_grouped.to_csv(filepath, index=False)
    else:
        df_grouped.to_csv(filepath, index=False)


def get_yaxis_label(name_var, logger):
    '''
    Takes a variable name (wl, salt, temp, cu, cu_dir, ice_conc, or the
    long form such as 'water_level') and returns the quantity name plus
    its HTML-formatted unit suffix, for use in figure y-axis labels.

    Both strings come from ofs_skill.utils.plot_units, the single source
    of truth for plot units. An unrecognized variable now yields
    ('Unknown', '') instead of raising: the previous units if/elif chain
    had no else branch, so any name_var outside {wl, temp, salt, cu}
    raised UnboundLocalError on the return.
    Called by plotting functions below.
    '''
    label_text = plot_units.quantity_label(name_var, logger)
    units = plot_units.unit_suffix(name_var, html=True, logger=logger)

    return label_text, units


def _format_cycle_label(col):
    """Format a model-cycle column name (YYYYMMDD-HHz-forecast) as MM/DD HH:00."""
    return (
        col.split('-')[0][4:6] + '/' + col.split('-')[0][6:8] + ' ' + col.split('-')[1][0:2] + ':00'
    )


def _format_horizonbin_label(col):
    """Format a forecast-horizon-bin column (an integer bin edge) as e.g. 0-6."""
    try:
        top = int(float(col))
    except (TypeError, ValueError):
        return str(col)
    return f'{max(top - 6, 0)}-{top}'


def _cf_scorecard_cmap():
    """Continuous red/green colormap + norm for the CF scorecard.

    Central frequency is a 0-100% score with a hard pass/fail break at 90%:
      * below 90%  -> red gradient (darker = worse)
      * at/above 90% -> green gradient (darker = better)
    The 90% break is placed proportionally (0.9 of the color range) so the
    colorbar can carry even 10% increments while still marking the threshold.
    """
    import matplotlib as mpl
    from matplotlib.colors import LinearSegmentedColormap, Normalize

    # Red ramp for [0, 90), green ramp for [90, 100]. The break lives at the
    # 0.9 position of the [0, 1] colormap domain (== 90 on a 0-100 Normalize).
    cmap = LinearSegmentedColormap.from_list(
        'cf_passfail',
        [
            (0.00, '#b2182b'),  # dark red   (0%)
            (0.45, '#f4a582'),  # light red
            (0.8999, '#fddbc7'),  # very light red, just below threshold
            (0.90, '#d9f0d3'),  # very light green, at threshold
            (0.95, '#7fbf7b'),  # medium green
            (1.00, '#1b7837'),  # dark green (100%)
        ],
    )
    cmap.set_bad('white')  # NaN / no-data cells render white
    norm = Normalize(vmin=0, vmax=100)
    return cmap, norm, mpl


def _make_scorecard(
    prop,
    logger,
    by,
    col_formatter,
    xaxis_title,
    file_paths,
    suffix,
    title_prefix=None,
    row_sort=None,
    row_label_fmt=None,
    left_margin=0.12,
    title_fontsize=22,
    cbar_frac=0.625,
    cbar_gap=0.02,
):
    """
    Build one matplotlib scorecard figure from the given per-variable CF
    tables. Rows are station IDs, columns are either model cycles or 6-hour
    forecast-horizon bins (per ``by``). Cells are colored by central
    frequency using the pass/fail colormap; missing cells are white.

    PARAMETERS:
    ----------
    prop: model properties object (paths, ofs, dates).
    logger: logging interface.
    by: 'modelcycles' or 'horizonbins' -- selects the column axis semantics.
    col_formatter: function mapping a raw column label to a display label.
    xaxis_title: x-axis title string for every subplot.
    file_paths: list of CF-table CSV paths, one per variable, to include as
        subplots on this figure.
    suffix: output filename suffix, e.g. 'modelcycles', 'horizonbins',
        'currents_cb0102_modelcycles' -- controls the saved PNG name.
    title_prefix: optional string prepended to the figure suptitle (used to
        name the current station on per-station currents scorecards).
    row_sort: optional callable key applied to sort the y-axis (station/bin)
        rows before plotting. Defaults to as-loaded order.
    row_label_fmt: optional callable to format each raw row label for display.

    RETURNS:
    --------
    NOTHING. Writes a PNG to prop.visuals_horizon_path.
    """
    import matplotlib

    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    if not file_paths:
        return

    cmap, norm, _mpl = _cf_scorecard_cmap()

    # Dates for the title
    if 'T' in prop.start_date_full:
        start_date_title = prop.start_date_full.split('T')[0]
        end_date_title = prop.end_date_full.split('T')[0]
    else:
        s = prop.start_date_full.split('-')[0]
        start_date_title = s[0:4] + '-' + s[4:6] + '-' + s[6:]
        e = prop.end_date_full.split('-')[0]
        end_date_title = e[0:4] + '-' + e[4:6] + '-' + e[6:]

    n_vars = len(file_paths)
    # Lay the per-variable subplots in a single row (1 x n_vars). A trailing
    # thin GridSpec row holds the full-width horizontal colorbar.
    grid_rows, grid_cols = 1, max(n_vars, 1)

    per_var_w = 7.0
    per_row_h = 8.0
    fig = plt.figure(
        figsize=(max(per_var_w * grid_cols, 9), per_row_h * grid_rows + 1.8),
    )
    gs = fig.add_gridspec(
        nrows=grid_rows + 1,
        ncols=grid_cols,
        height_ratios=[1] * grid_rows + [0.05],
        # Extra left/top margin so long y-labels and the suptitle are not
        # clipped by the page edge. Currents scorecards use a wider left
        # margin (passed in) because their per-station titles + bin labels
        # need more room. hspace controls the gap between the subplots and
        # the colorbar row (smaller for currents to tighten it up).
        top=0.88,
        bottom=0.10,
        left=left_margin,
        right=0.96,
        hspace=0.6,
        wspace=0.32,
    )
    axes = [fig.add_subplot(gs[0, j]) for j in range(n_vars)]
    # Colorbar axis: keep the same thickness but set its length relative to
    # the full-width GridSpec cell via cbar_frac. Pull it up toward the plots
    # by cbar_gap (a fraction of figure height) to reduce dead white space.
    cax_full = fig.add_subplot(gs[grid_rows, :])
    cax_full.set_axis_off()
    cb_pos = cax_full.get_position()
    cb_w = cb_pos.width * cbar_frac
    cax = fig.add_axes(
        [
            cb_pos.x0 + (cb_pos.width - cb_w) / 2.0,  # centered horizontally
            cb_pos.y0 + cbar_gap,  # nudge up toward the plots
            cb_w,
            cb_pos.height,  # same thickness
        ]
    )

    mesh = None
    for j, (ax, filepath) in enumerate(zip(axes, file_paths)):
        try:
            name_var = os.path.basename(filepath).split('_')[1]
            title_var, _ = get_yaxis_label(name_var, logger)
            df = pd.read_csv(filepath)
            df = df.set_index('ID')
            # Optionally reorder the rows (e.g. current bins b01..bNN).
            if row_sort is not None:
                df = df.loc[sorted(df.index, key=row_sort)]
            # Sort columns chronologically (cycles) or numerically (bins).
            if by == 'horizonbins':
                ordered = sorted(df.columns, key=lambda c: float(c))
            else:
                ordered = sorted(
                    df.columns,
                    key=lambda c: (c.split('-')[0], c.split('-')[1][0:2]),
                )
            df = df[ordered]
            col_labels = [col_formatter(c) for c in df.columns]
            if row_label_fmt is not None:
                row_labels = [row_label_fmt(str(r)) for r in df.index]
            else:
                row_labels = [str(r) for r in df.index]
            data = np.ma.masked_invalid(df.to_numpy(dtype=float))

            mesh = ax.pcolormesh(
                np.arange(data.shape[1] + 1),
                np.arange(data.shape[0] + 1),
                data,
                cmap=cmap,
                norm=norm,
                edgecolors='white',
                linewidth=1.5,
            )
            ax.set_title(title_var, fontsize=20, fontweight='bold')
            # X-axis: for model-cycle scorecards the label count can be large
            # (one per cycle over a week+), so thin the tick labels to avoid
            # overcrowding while keeping every cell. Horizon-bin scorecards
            # only have a handful of bins, so show them all.
            n_cols = data.shape[1]
            if by == 'modelcycles' and n_cols > 12:
                step = int(np.ceil(n_cols / 12))
            else:
                step = 1
            tick_pos = np.arange(0, n_cols, step) + 0.5
            tick_lab = [col_labels[i] for i in range(0, n_cols, step)]
            ax.set_xticks(tick_pos)
            ax.set_xticklabels(tick_lab, rotation=45, ha='right', fontsize=13)
            # Y-axis: each variable has its own station set (different IDs and
            # counts), so label every subplot's rows independently rather than
            # sharing one axis. When a subplot has many rows thin the labels
            # so they stay readable.
            n_rows = data.shape[0]
            y_step = int(np.ceil(n_rows / 30)) if n_rows > 30 else 1
            ax.set_yticks(np.arange(0, n_rows, y_step) + 0.5)
            ax.set_yticklabels([row_labels[i] for i in range(0, n_rows, y_step)], fontsize=12)
            ax.set_ylabel('Station ID', fontsize=18)
            ax.set_xlabel(xaxis_title, fontsize=18)
            ax.tick_params(axis='x', labelsize=13)
            ax.set_ylim(0, n_rows)
            ax.invert_yaxis()  # first row (e.g. b01) at top
            ax.set_aspect('auto')
        except Exception as e_x:
            logger.error(
                'Caught exception building %s scorecard subplot for %s! ' 'Error: %s',
                by,
                filepath,
                e_x,
            )
            continue

    titlestr = (
        f'{prop.ofs.upper()} forecast central frequency, ' f'{start_date_title} to {end_date_title}'
    )
    if title_prefix:
        titlestr = f'{title_prefix}\n{titlestr}'
    fig.suptitle(titlestr, fontsize=title_fontsize, fontweight='bold', y=0.975)

    if mesh is not None:
        # Sparse tick set so labels don't overlap on the (narrower) currents
        # colorbars, while still marking the 90% pass/fail threshold. A black
        # divider line reinforces the 90% break.
        cb_ticks = [0, 25, 50, 75, 90, 100]
        cbar = fig.colorbar(
            mesh,
            cax=cax,
            orientation='horizontal',
            ticks=cb_ticks,
        )
        cbar.ax.tick_params(labelsize=15)
        cbar.ax.set_xticklabels([f'{v}%' for v in cb_ticks])
        # Emphasize the 90% acceptance threshold (red below, green at/above).
        cbar.ax.axvline(90, color='black', linewidth=2)
        cbar.set_label('Central frequency  (pass \u2265 90%)', fontsize=18)
    else:
        cax.set_visible(False)

    output_file = os.path.join(prop.visuals_horizon_path, f'{prop.ofs}_cf_scorecard_{suffix}.png')
    logger.debug('Writing file: %s', output_file)
    try:
        fig.savefig(output_file, dpi=150)
    except Exception as e_x:
        logger.error('Could not save %s scorecard! Error: %s', suffix, e_x)
    finally:
        plt.close(fig)
    logger.info('Wrote %s central-frequency scorecard for %s.', suffix, prop.ofs)


def make_flag_images(prop, logger):
    """
    Plotting function that writes 'scorecard' (flag) plots based on pass/fail
    acceptance criteria for central-frequency statistics, rendered with
    matplotlib. Each figure has one subplot per assessed variable with
    station IDs on the y-axis:

      * ``{ofs}_cf_scorecard_modelcycles.png`` -- model cycles on the x-axis
      * ``{ofs}_cf_scorecard_horizonbins.png`` -- 6-hour forecast-horizon
        bins on the x-axis

    Currents typically have far more stations (ADCP bins) than the scalar
    variables. To avoid an unreadable, overcrowded figure, currents are
    handled specially: the remaining scalar variables (wl/temp/salt) share
    the main scorecards, while currents get ONE scorecard PER current station
    (``{ofs}_cf_scorecard_currents_{station}_{modelcycles,horizonbins}.png``)
    whose rows are that station's depth bins sorted b01 (top) -> largest
    (bottom).

    Cells are colored by central frequency (red = fail, green = pass at the
    90% acceptance criterion); missing cells are white.
    Called by do_horizon_skill.merge_obs_series_scalar

    PARAMETERS:
    ----------
    prop: model properties object containing date range, paths, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes scorecard plots to file.
    """

    def _tables(by):
        return sorted(
            glob.glob(os.path.join(prop.data_horizon_1d_pair_path, f'{prop.ofs}_*_{by}_cf.csv'))
        )

    def _is_currents(fp):
        # CF filenames are '{ofs}_{namevar}_{by}_cf.csv'; currents -> 'cu'.
        return os.path.basename(fp).split('_')[1] == 'cu'

    def _bin_key(row_id):
        # Current bin IDs look like 'cb0102_b07'; sort by the trailing bin
        # number so b01 is first (top) and the largest bin is last (bottom).
        m = re.search(r'[bB](\d+)$', str(row_id))
        return int(m.group(1)) if m else 0

    def _station_of(row_id):
        # 'cb0102_b07' -> 'cb0102'
        return re.sub(r'_[bB]\d+$', '', str(row_id))

    for by, fmt, xtitle in (
        ('modelcycles', _format_cycle_label, 'Model cycle'),
        ('horizonbins', _format_horizonbin_label, 'Forecast horizon (hours)'),
    ):
        all_tables = _tables(by)
        if not all_tables:
            logger.info('No %s CF tables found for %s scorecard; skipping.', by, prop.ofs)
            continue
        currents = [f for f in all_tables if _is_currents(f)]
        scalars = [f for f in all_tables if not _is_currents(f)]

        # Scalar variables (wl/temp/salt) share the main scorecard, laid out
        # as a single row of subplots (1 x n).
        if scalars:
            _make_scorecard(
                prop,
                logger,
                by=by,
                col_formatter=fmt,
                xaxis_title=xtitle,
                file_paths=scalars,
                suffix=by,
                # Added explicit left_margin and smaller title_fontsize
                # to prevent y-axis and title clipping
                left_margin=0.18,
                title_fontsize=18,
                cbar_frac=1.0,
                cbar_gap=0.03,
            )
        # Currents: one scorecard per current station, rows = depth bins
        # sorted b01 -> largest. Split the single cu CF table into per-station
        # temporary tables and render each on its own figure.
        if currents:
            cu_table = currents[0]  # there is one cu CF table per 'by'
            try:
                cu_df = pd.read_csv(cu_table)
            except Exception as e_x:
                logger.error('Could not read currents CF table %s! Error: %s', cu_table, e_x)
                cu_df = None
            if cu_df is not None and 'ID' in cu_df.columns:
                stations = sorted({_station_of(i) for i in cu_df['ID']})
                for station in stations:
                    sub = cu_df[cu_df['ID'].map(_station_of) == station]
                    if sub.empty:
                        continue
                    # Write a temp per-station CF table so _make_scorecard can
                    # consume it with the same filename convention (var='cu').
                    tmp_path = os.path.join(
                        prop.data_horizon_1d_pair_path, f'{prop.ofs}_cu_{station}_{by}_cf.csv'
                    )
                    try:
                        sub.to_csv(tmp_path, index=False)
                        _make_scorecard(
                            prop,
                            logger,
                            by=by,
                            col_formatter=fmt,
                            xaxis_title=xtitle,
                            file_paths=[tmp_path],
                            suffix=f'currents_{station}_{by}',
                            title_prefix=f'Current station {station}',
                            row_sort=_bin_key,
                            # Currents single-station scorecards need a wider
                            # left margin (long bin labels) and a slightly
                            # smaller title so neither is clipped by the page.
                            left_margin=0.25,  # Increased from 0.18 to prevent long bin label clipping
                            title_fontsize=15,  # Decreased from 17 to prevent multi-line title clipping
                            # Longer colorbar (0.625 * 1.5) so the % labels
                            # don't overlap on the single-subplot width, and
                            # nudge it up to reduce the gap to the plot.
                            cbar_frac=1.0,
                            cbar_gap=0.03,
                        )
                    finally:
                        # The temp table is only an intermediate; remove it so
                        # it isn't re-globbed as a variable next time.
                        try:
                            os.remove(tmp_path)
                        except OSError:
                            pass


def make_horizonbin_plots(df_all, info, prop, logger):
    """
    Here we make bar subplots for each OFS station with 2 rows and 1 column.
    The first row is a bar plot showing RMSE and mean error (y axis) across
    6-hour forecast horizon bins (x axis), with each variable's target error
    range superimposed. The second row is a bar plot showing RMSE and mean
    error (y axis) across each model cycle (x axis), with each variable's
    target error range superimposed.
    Called by do_horizon_skill.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    do_horizon_skill.horizon_skill.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model node
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
        info[5] = list of all station IDs
        info[6] = 'doextraplots' boolean True/False
        info[7] = full variable name
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    """

    # Resolve the target error range, the axis label and the unit
    # through one key. info[7] (the long variable name) is the only one
    # that separates current direction from current speed -- info[0]
    # collapses both to 'cu' -- so a threshold read under info[0] would
    # be printed next to a unit read under info[7].
    label_var = plot_units.resolve_variable(info[7], info[0])
    error_range, _ = plotting_functions.get_error_range(
        plot_units.canonical_key(label_var) or info[0], prop, logger)

    # Stats
    n_threshold = 10
    # Filter out groups with n < n_threshold
    df_filt_mc = df_all.groupby('model_cycle')
    df_filt_mc = df_filt_mc.filter(lambda x: x['error'].count() > n_threshold)
    df_filt_hb = df_all.groupby('hour_bins')
    df_filt_hb = df_filt_hb.filter(lambda x: x['error'].count() > n_threshold)

    rmse_hours = np.round(
        np.sqrt(
            df_filt_hb.groupby(
                'hour_bins',
            )['square_error'].mean(),
        ),
        decimals=2,
    )
    error_hours = np.round(
        df_filt_hb.groupby('hour_bins')['error'].mean(),
        decimals=2,
    )
    rmse_hours_cycle = np.round(
        np.sqrt(
            df_filt_mc.groupby(
                'model_cycle',
            )['square_error'].mean(),
        ),
        decimals=2,
    )
    error_hours_cycle = np.round(
        df_filt_mc.groupby('model_cycle')['error'].mean(),
        decimals=2,
    )

    # r_hours = df_all.groupby('hour_bins')[['OBS','OFS']].corr().iloc[0::2,-1]
    # mean_hours = df_all.groupby('hour_bins')[['OFS','OBS']].mean()
    # mean_obs_hours = df_all.groupby('hour_bins')['OBS'].mean()
    # std_hours = df_all.groupby('hour_bins')[['OFS','OBS']].std()
    # std_obs_hours = df_all.groupby('hour_bins')['OBS'].std()
    # Plots
    # Make hour bin (x axis) labels
    barlabels = [_format_horizonbin_label(top) for top in rmse_hours.index]
    # Make model cycle bin (x axis) labels
    model_cycles = sorted(df_filt_mc['model_cycle'].unique())
    cyclelabels = []
    for i in range(len(model_cycles)):
        cyclestr = (
            model_cycles[i].split('-')[0][4:6]
            + '/'
            + model_cycles[i].split('-')[0][6:8]
            + ' '
            + model_cycles[i].split('-')[1][0:2]
            + ':00'
        )
        cyclelabels.append(cyclestr)

    # Create lists for looping
    xlabels = [barlabels, cyclelabels]
    ydatahours = [np.array(rmse_hours), np.array(error_hours)]
    ydatacycles = [np.array(rmse_hours_cycle), np.array(error_hours_cycle)]
    # colorramps = ['deep', 'dense']
    # Figure set-up
    figheight = 700
    figwidth = 800
    nrows = 2
    fig = make_subplots(
        rows=nrows,
        cols=1,
        vertical_spacing=0.2,
        # shared_xaxes=True
    )
    showlegend = [True, False]
    # Loop 'n plot the things 'n stuff
    for i in range(nrows):
        try:
            if i == 0:
                ydata = ydatahours
            else:
                ydata = ydatacycles
            # n_colors = len(ydata[0])
            # colors = px.colors.sample_colorscale(
            #     colorramps[i], [n/(n_colors - 1) for n in range(n_colors)],
            # )
            # Define colors for rmse and mean error (me)
            # based on target error range
            rmsecolors = []
            for value in ydata[0]:
                if -error_range <= value <= error_range:
                    rmsecolors.append('palegreen')
                else:
                    rmsecolors.append('lightcoral')
            mecolors = []
            for value in ydata[1]:
                if -error_range <= value <= error_range:
                    mecolors.append('palegreen')
                else:
                    mecolors.append('lightcoral')
            # Plot
            fig.add_trace(
                go.Bar(
                    x=xlabels[i],
                    y=ydata[0],
                    name=plot_units.with_unit('RMSE', label_var, html=True,
                                              logger=logger),
                    marker_color=rmsecolors,
                    marker_line_color='black',
                    marker_line_width=1.5,
                    textposition='outside',
                    showlegend=showlegend[i],
                ),
                row=i + 1,
                col=1,
            )
            fig.add_trace(
                go.Bar(
                    x=xlabels[i],
                    y=ydata[1],
                    name=plot_units.with_unit('Mean error', label_var,
                                              html=True, logger=logger),
                    marker_color=mecolors,
                    marker_line_color='dodgerblue',
                    marker_line_width=1.5,
                    # line=dict(color='magenta'),
                    showlegend=showlegend[i],
                ),
                row=i + 1,
                col=1,
            )
            if ydata[1] is None:
                fig.add_annotation(
                    text='<b>Not enough data points to calculate RMSE!</b>',
                    xref='x domain',
                    yref='y domain',
                    font={'size': 14, 'color': 'red'},
                    x=0,
                    y=0.0,
                    showarrow=False,
                    row=i + 1,
                    col=1,
                )
                logger.info(
                    'Added low data points warning label to plot ' 'for station %s',
                    info[2],
                )
            fig.add_hline(
                y=error_range,
                line_color='darkorange',
                line_width=1.25,
                line_dash='dash',
                annotation_text=(
                    '<b>Target error range '
                    f'(+{plot_units.value_with_unit(error_range, label_var)})'
                    '</b>'),
                annotation_position='top left',
                annotation_font_color='black',
                annotation_font_size=13,
                row=i + 1,
                col=1,
            )
            fig.add_hline(
                y=-error_range,
                line_color='darkorange',
                line_width=1.25,
                line_dash='dash',
                annotation_text=(
                    '<b>Target error range '
                    f'(-{plot_units.value_with_unit(error_range, label_var)})'
                    '</b>'),
                annotation_position='bottom right',
                annotation_font_color='black',
                annotation_font_size=13,
                row=i + 1,
                col=1,
            )
            fig.add_hline(
                y=0,
                line_color='black',
                line_width=1,
                row=i + 1,
                col=1,
            )
        except Exception as e_x:
            logger.error(
                'Caught exception in make_horizonbin_plots loop! ' 'Error: %s. Skipping plot!',
                e_x,
            )
            return
    try:
        yaxis_label, unit_label = get_yaxis_label(label_var, logger)
        yaxistitle = yaxis_label + '<br>RMSE or error' + unit_label
        fig.update_yaxes(
            title_text=yaxistitle,
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            tickfont={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # tickfont_family='Open Sans',
            # titlefont_family='Open Sans',
        )
        fig.update_yaxes(
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=True,
        )
        # Rotate tick labels 45 degrees on BOTH subplot x-axes (horizon bins
        # on top, model cycles on bottom) so they are consistently angled.
        fig.update_xaxes(
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=True,
            tickangle=45,
            tickfont={'size': 14, 'color': 'black', 'family': 'Open Sans'},
            # tickfont_family='Open Sans'
        )
        fig.update_xaxes(
            title_text='Forecast horizon (hours)',
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # titlefont_family='Open Sans',
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text='Model cycle',
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # titlefont_family='Open Sans',
            row=2,
            col=1,
        )
        # Update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(
            df_all['DateTime'].min(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        prop111.end_date_full = datetime.strftime(
            df_all['DateTime'].max(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        figtitle = plotting_functions.get_title(
            prop111,
            info[1],
            info[2:5],
            info[0],
            logger,
        )

        fig.update_layout(
            title={
                'text': figtitle,
                'font': dict(size=14, color='black', family='Open Sans'),
                'y': 0.97,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
            },
            yaxis1={
                'tickfont': dict(size=16),
                'range': [-error_range * 2, error_range * 2],
            },
            yaxis2={
                'tickfont': dict(size=16),
                'range': [-error_range * 2, error_range * 2],
            },
            transition_ordering='traces first',
            dragmode='zoom',
            hovermode='x unified',
            height=figheight,
            width=figwidth,
            template='plotly_white',
            barmode='group',
            # Generous top margin so multi-line titles (esp. currents, which
            # include station name + bin + node info) do not overlap the top
            # subplot.
            margin={'t': 150, 'b': 50},
            legend={
                'font': dict(
                    family='Open Sans',
                    size=14,
                    color='black',
                ),
            },
        )
        output_file = f'{prop.visuals_horizon_path}/{prop.ofs}_' f'{info[2]}_{info[7]}_rmse_bars'
        fig_config = {
            'toImageButtonOptions': {
                'format': 'png',
                'filename': output_file.split('/')[-1],
                'height': figheight,
                'width': figwidth,
                'scale': 1,
            }
        }
        logger.debug(f'Writing file: {output_file}')
        fig.write_html(output_file + '.html', config=fig_config)
        logger.debug(f'Finished writing file: {output_file}')
        if prop.static_plots:
            xydata = [xlabels, [ydatahours[0], ydatacycles[0]]]
            make_static_plots.bar_plots(xydata, info, yaxistitle, prop, logger)
    except Exception as e_x:
        logger.error(
            'Caught exception in make_horizonbin_plots formatting! ' 'Error: %s. Skipping plot!',
            e_x,
        )
        return
    # logger.info("Wrote bar plot for %s from make_horizonbin_plots",
    #             info[2])


def make_horizonbin_freq_plots(df_all, info, prop, logger):
    """
    Here we make bar subplots for each OFS station with 2 rows and 1 column.
    The first row is a bar plot showing central frequency (y axis) across
    6-hour forecast horizon bins (x axis), with the 90% acceptance criteria
    superimposed. The second row is a bar plot showing central frequency
    (y axis) across each model cycle (x axis), with the 90% acceptance criteria
    superimposed
    Called by do_horizon_skill.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    do_horizon_skill.horizon_skill.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model node
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
        info[5] = list of all station IDs
        info[6] = 'doextraplots' boolean True/False
        info[7] = full variable name
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    """
    # See make_horizonbin_plots: the long variable name is the key that
    # separates current direction from current speed.
    label_var = plot_units.resolve_variable(info[7], info[0])
    error_range, _ = plotting_functions.get_error_range(
        plot_units.canonical_key(label_var) or info[0], prop, logger)

    # Stats
    n_threshold = 20
    # Filter out groups with n < n_threshold
    df_filt_mc = df_all.groupby('model_cycle')
    df_filt_mc = df_filt_mc.filter(lambda x: x['error'].count() > n_threshold)
    df_filt_hb = df_all.groupby('hour_bins')
    df_filt_hb = df_filt_hb.filter(lambda x: x['error'].count() > n_threshold)

    # Stats
    cf_hours = np.round(
        100
        * (
            df_filt_hb.groupby('hour_bins')['error'].apply(
                lambda x: ((x <= error_range) & (x >= -error_range)).sum()
            )
        )
        / df_filt_hb.groupby('hour_bins')['error'].count(),
        decimals=2,
    )
    cf_hours_cycle = np.round(
        100
        * (
            df_filt_mc.groupby('model_cycle')['error'].apply(
                lambda x: ((x <= error_range) & (x >= -error_range)).sum()
            )
        )
        / df_filt_mc.groupby('model_cycle')['error'].count(),
        decimals=2,
    )
    # Make tables: one keyed by model cycle, one by 6-hour forecast-horizon
    # bin. Both feed the two scorecard flavours in make_flag_images.
    make_table(cf_hours_cycle, info, prop, 'cf', by='modelcycles')
    make_table(cf_hours, info, prop, 'cf', by='horizonbins')
    # Plots
    # Make hour bin (x axis) labels
    barlabels = [_format_horizonbin_label(top) for top in cf_hours.index]
    # Make model cycle bin (x axis) labels
    model_cycles = sorted(df_filt_mc['model_cycle'].unique())
    cyclelabels = []
    for i in range(len(model_cycles)):
        cyclestr = (
            model_cycles[i].split('-')[0][4:6]
            + '/'
            + model_cycles[i].split('-')[0][6:8]
            + ' '
            + model_cycles[i].split('-')[1][0:2]
            + ':00'
        )
        cyclelabels.append(cyclestr)

    # Create lists for looping
    xlabels = [barlabels, cyclelabels]
    ydatahours = [np.array(cf_hours)]
    ydatacycles = [np.array(cf_hours_cycle)]
    # colorramps = ['deep', 'dense']
    # Figure set-up
    figheight = 700
    figwidth = 800
    nrows = 2
    fig = make_subplots(rows=nrows, cols=1, vertical_spacing=0.2)
    showlegend = [False, False]
    # Loop 'n plot the things 'n stuff
    for i in range(nrows):
        try:
            if i == 0:
                ydata = ydatahours
            else:
                ydata = ydatacycles
            # n_colors = len(ydata[0])
            # colors = px.colors.sample_colorscale(
            #     colorramps[i], [n/(n_colors - 1) for n in range(n_colors)],
            # )
            colors = ['palegreen' if val >= 90 else 'lightcoral' for val in ydata[0]]

            fig.add_trace(
                go.Bar(
                    x=xlabels[i],
                    y=ydata[0],
                    name='Central frequency',
                    marker_color=colors,
                    marker_line_color='black',
                    marker_line_width=0.75,
                    textposition='outside',
                    showlegend=showlegend[i],
                ),
                row=i + 1,
                col=1,
            )
            if ydata[0] is None:
                fig.add_annotation(
                    text='<b>Not enough data points to calculate stats!</b>',
                    xref='x domain',
                    yref='y domain',
                    font={'size': 14, 'color': 'red'},
                    x=0,
                    y=0.0,
                    showarrow=False,
                    row=i + 1,
                    col=1,
                )
                logger.error(
                    'Added low data points warning label to plot ' 'for station %s',
                    info[2],
                )
            fig.add_hline(
                y=90,
                line_color='darkred',
                line_width=1.25,
                line_dash='dash',
                annotation_text='<b>90% acceptance criteria</b>',
                annotation_position='top left',
                annotation_font_color='black',
                annotation_font_size=13,
                row=i + 1,
                col=1,
            )
            fig.add_hline(
                y=0,
                line_color='black',
                line_width=1,
                row=i + 1,
                col=1,
            )
        except Exception as e_x:
            logger.error(
                'Caught exception in make_horizonbin_freq_plots loop! ' 'Error: %s. Skipping plot!',
                e_x,
            )
            return
    try:
        yaxis_label, _ = get_yaxis_label(label_var, logger)
        yaxistitle = yaxis_label + '<br>central frequency' + ' (%)'
        fig.update_yaxes(
            title_text=yaxistitle,
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            tickfont={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # titlefont_family='Open Sans',
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
            tickangle=45,
            tickfont={'size': 14, 'color': 'black', 'family': 'Open Sans'},
        )
        fig.update_xaxes(
            title_text='Forecast horizon (hours)',
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # titlefont_family='Open Sans',
            row=1,
            col=1,
        )
        fig.update_xaxes(
            title_text='Model cycle',
            title_font={'size': 16, 'color': 'black', 'family': 'Open Sans'},
            # titlefont_family='Open Sans',
            row=2,
            col=1,
        )
        # update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(
            df_all['DateTime'].min(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        prop111.end_date_full = datetime.strftime(
            df_all['DateTime'].max(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        figtitle = plotting_functions.get_title(
            prop111,
            info[1],
            info[2:5],
            info[0],
            logger,
        )

        fig.update_layout(
            title={
                'text': figtitle,
                'font': dict(size=14, color='black', family='Open Sans'),
                'y': 0.97,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
            },
            yaxis1={
                'tickfont': dict(size=16),
                'range': [0, 100],
            },
            yaxis2={
                'tickfont': dict(size=16),
                'range': [0, 100],
            },
            transition_ordering='traces first',
            dragmode='zoom',
            hovermode='x unified',
            height=figheight,
            width=figwidth,
            template='plotly_white',
            # barmode='group',
            # Generous top margin so multi-line titles (esp. currents) do not
            # overlap the top subplot.
            margin={'t': 150, 'b': 50},
            legend={
                'font': dict(
                    family='Open Sans',
                    size=14,
                    color='black',
                ),
            },
        )
        output_file = f'{prop.visuals_horizon_path}/{prop.ofs}_' f'{info[2]}_{info[7]}_cfreq_bars'
        fig_config = {
            'toImageButtonOptions': {
                'format': 'png',
                'filename': output_file.split('/')[-1],
                'height': figheight,
                'width': figwidth,
                'scale': 1,
            }
        }
        logger.debug(f'Writing file: {output_file}')
        fig.write_html(output_file + '.html', config=fig_config)
        if prop.static_plots:
            xydata = [xlabels, [ydatahours[0], ydatacycles[0]]]
            make_static_plots.bar_plots(xydata, info, yaxistitle, prop, logger)
        logger.debug(f'Finished writing file: {output_file}')
    except Exception as e_x:
        logger.error(
            'Caught exception in make_horizonbin_freq_plots '
            'formatting! Error: %s. Skipping plot!',
            e_x,
        )
        return
    # logger.info("Wrote bar plot for %s from make_horizonbin_freq_plots",
    #             info[2])


def make_timeseries_plots(df_all, forecast_cols_sort, info, prop, logger):
    """
    Here we make subplots (2x1) time series of observations and all model
    cycles for each OFS station. First row is a time series of obs and model
    data. Second row is a time series of error (model minus obs) for each
    model cycle, with each variable's target error range superimposed.

    A dropdown menu lets the user isolate a single model cycle (obs stays
    visible) or choose 'Show all' to display every cycle at once (default).
    Called by do_horizon_skill.horizon_skill

    PARAMETERS:
    ----------
    df_all: giant pandas dataframe that was reshaped in
    do_horizon_skill.horizon_skill.
    forecast_cols_sort: list of model cycle dates as datetime objects sorted in
    ascending order. The main loop below iterates over this list.
    info: list of station info strings -->
        info[0] = variable
        info[1] = model node
        info[2] = station ID
        info[3] = station full name
        info[4] = station provider/owner
        info[5] = list of all station IDs
        info[6] = 'doextraplots' boolean True/False
        info[7] = full variable name
    prop: model properties object containing date range, etc.
    logger: logging interface.

    RETURNS:
    --------
    NOTHING.
    Writes a plot to file.
    """

    # See make_horizonbin_plots: the long variable name is the key that
    # separates current direction from current speed.
    label_var = plot_units.resolve_variable(info[7], info[0])
    error_range, _ = plotting_functions.get_error_range(
        plot_units.canonical_key(label_var) or info[0], prop, logger)

    # Sort out observation data
    df_obs = df_all[['DateTime', 'OBS']]
    df_obs = df_obs.sort_values(by='DateTime')
    df_obs = df_obs.drop_duplicates(subset='DateTime', keep='first')
    # Figure set-up
    figwidth = 900
    figheight = 600
    nrows = 2
    fig = make_subplots(
        rows=nrows,
        cols=1,
        vertical_spacing=0.055,
        # subplot_titles=(prop.whichcasts),
        shared_xaxes=True,
    )
    n_colors = len(forecast_cols_sort)
    colors = px.colors.sample_colorscale(
        'Turbo',
        [n / (n_colors - 1) for n in range(n_colors)],
    )
    # Track which trace belongs to which model cycle so the dropdown can
    # toggle visibility. Index 0 is always the (always-visible) obs trace.
    # cycle_trace_map[i] -> list of trace indices for forecast_cols_sort[i].
    cycle_trace_map = [[] for _ in forecast_cols_sort]
    # Add traces -- first do observation
    try:
        fig.add_trace(
            go.Scatter(
                x=df_obs['DateTime'],
                y=df_obs['OBS'],
                mode='lines',
                name='Observations',
                line={'color': 'black', 'width': 2, 'dash': 'dash'},
                # Show only the y-value, rounded to 2 decimals, consistently
                # across every trace (no date/time header).
                hovertemplate='%{y:.2f}<extra></extra>',
            ),
            row=1,
            col=1,
        )

        # Next add all model + error time series
        trace_idx = 1  # obs is trace 0
        for i, fcst_col in enumerate(forecast_cols_sort):
            trace_name = (
                fcst_col.split('-')[0][4:6]
                + '/'
                + fcst_col.split('-')[0][6:]
                + '/'
                + fcst_col.split('-')[0][0:4]
                + ' '
                + fcst_col.split('-')[1][0:2]
                + 'Z'
            )
            df_filt = df_all[df_all['model_cycle'] == fcst_col]
            fig.add_trace(
                go.Scatter(
                    x=df_filt['DateTime'],
                    y=df_filt['OFS'],
                    mode='lines',
                    showlegend=False,
                    name=trace_name,
                    line={'color': colors[i], 'width': 1.25},
                    hovertemplate='%{y:.2f}<extra></extra>',
                ),
                row=1,
                col=1,
            )
            cycle_trace_map[i].append(trace_idx)
            trace_idx += 1
            fig.add_trace(
                go.Scatter(
                    x=df_filt['DateTime'],
                    y=df_filt['error'],
                    mode='lines',
                    name=trace_name,
                    line={'color': colors[i], 'width': 1.25},
                    hovertemplate='%{y:.2f}<extra></extra>',
                ),
                row=2,
                col=1,
            )
            cycle_trace_map[i].append(trace_idx)
            trace_idx += 1
    except Exception as e_x:
        logger.error(
            'Caught exception in make_timeseries_plots loop!' 'Skipping plot. Error: %s',
            e_x,
        )
    try:
        # add target error ranges
        fig.add_hline(
            y=0,
            line={'width': 1},
            row=2,
            col=1,
        )
        fig.add_hline(
            y=error_range,
            line_color='red',
            line_width=1,
            line_dash='dash',
            annotation_text=(
                'Target error range '
                f'(+{plot_units.value_with_unit(error_range, label_var)})'),
            annotation_position='top left',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2,
            col=1,
        )
        fig.add_hline(
            y=-error_range,
            line_color='red',
            line_width=1,
            line_dash='dash',
            annotation_text=(
                'Target error range '
                f'(-{plot_units.value_with_unit(error_range, label_var)})'),
            annotation_position='bottom right',
            annotation_font_color='black',
            annotation_font_size=12,
            row=2,
            col=1,
        )
        # Set figure properties
        yaxis_label, unit_label = get_yaxis_label(label_var, logger)
        # yrange_error = np.ceil(np.nanmax(np.abs(df_all['error'])))
        # Long variable names (e.g. 'Water temperature') crowd the vertical
        # y-axis title and collide with the error-subplot title below. For
        # long labels, put the (multi-word) variable name on its own line and
        # the qualifier/units on the next so both subplot titles stay compact.
        if len(yaxis_label) > 12:
            value_title = f'{yaxis_label}<br>{unit_label.strip()}'
            error_title = f'{yaxis_label} error<br>{unit_label.strip()}'
        else:
            value_title = f'{yaxis_label}{unit_label}'
            error_title = f'{yaxis_label} error{unit_label}'
        fig.update_yaxes(
            title_text=value_title,
            # range=[0, 100],
            row=1,
            col=1,
        )
        fig.update_yaxes(
            title_text=error_title,
            range=[-error_range * 3, error_range * 3],
            row=2,
            col=1,
        )
        fig.update_yaxes(
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=True,
            title_font={'size': 14, 'color': 'black'},
            title_standoff=8,
        )
        fig.update_xaxes(
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=True,
            tickfont={'size': 16},
        )
        fig.update_xaxes(
            title_text='Time',
            title_font={'size': 16, 'color': 'black'},
            row=2,
        )

        # Build the model-cycle dropdown menu. Each option toggles the
        # visibility of a single cycle's traces (obs always visible); the
        # first option ('Show all') keeps every trace visible.
        total_traces = 1 + sum(len(t) for t in cycle_trace_map)  # obs + cycles

        def _vis_mask(selected_cycle_idx):
            # selected_cycle_idx=None -> show all
            mask = [False] * total_traces
            mask[0] = True  # obs always on
            if selected_cycle_idx is None:
                for traces in cycle_trace_map:
                    for t in traces:
                        mask[t] = True
            else:
                for t in cycle_trace_map[selected_cycle_idx]:
                    mask[t] = True
            return mask

        buttons = [
            {
                'label': 'Show all',
                'method': 'update',
                'args': [{'visible': _vis_mask(None)}],
            }
        ]
        for i, fcst_col in enumerate(forecast_cols_sort):
            cyclestr = (
                fcst_col.split('-')[0][4:6]
                + '/'
                + fcst_col.split('-')[0][6:8]
                + '/'
                + fcst_col.split('-')[0][0:4]
                + ' '
                + fcst_col.split('-')[1][0:2]
                + 'Z'
            )
            buttons.append(
                {
                    'label': cyclestr,
                    'method': 'update',
                    'args': [{'visible': _vis_mask(i)}],
                }
            )

        # update layout
        prop111 = copy.deepcopy(prop)
        prop111.start_date_full = datetime.strftime(
            df_obs['DateTime'].min(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        prop111.end_date_full = datetime.strftime(
            df_obs['DateTime'].max(),
            '%Y-%m-%dT%H:%M:%SZ',
        )
        figtitle = plotting_functions.get_title(
            prop111,
            info[1],
            info[2:5],
            info[0],
            logger,
        )

        # The title is horizontally centered within the top margin band. It
        # can wrap to several lines (esp. currents: station name + bin + node
        # info), so size the top margin to the title's line count: scalar
        # variables have ~4-line titles and need little whitespace, while
        # currents titles wrap to ~6 lines and need more room. The dropdown
        # menu + label sit in the band between the title and the plot,
        # centered in the gap.
        n_title_lines = figtitle.count('<br>') + 1
        if n_title_lines >= 6:
            top_margin, title_y, menu_y = 230, 0.97, 1.16
        elif n_title_lines >= 5:
            top_margin, title_y, menu_y = 195, 0.97, 1.15
        else:  # ~4-line scalar titles
            top_margin, title_y, menu_y = 165, 0.97, 1.14
        fig.update_layout(
            title={
                'text': figtitle,
                'font': dict(size=14, color='black', family='Open Sans'),
                'y': title_y,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
            },
            updatemenus=[
                {
                    'buttons': buttons,
                    'direction': 'down',
                    'showactive': True,
                    'x': 1.0,
                    'xanchor': 'right',
                    'y': menu_y,
                    'yanchor': 'middle',
                    'pad': {'r': 5, 't': 5},
                    'bgcolor': 'white',
                    'bordercolor': 'black',
                    'borderwidth': 1,
                    'font': {'size': 12, 'color': 'black', 'family': 'Open Sans'},
                }
            ],
            yaxis1={'tickfont': dict(size=16)},
            yaxis2={
                'tickfont': dict(size=16),
                'range': [-error_range * 2, error_range * 2],
            },
            transition_ordering='traces first',
            dragmode='zoom',
            hovermode='x unified',
            height=figheight,
            width=figwidth,
            legend_tracegroupgap=140,
            legend_traceorder='normal',
            template='plotly_white',
            margin={'t': top_margin, 'b': 50},
            legend={
                'font': dict(size=16, color='black'),
                'bgcolor': 'rgba(0,0,0,0)',
            },
        )
        # Appended rather than passed as update_layout(annotations=[...]):
        # that keyword replaces the whole annotation list, which would drop
        # the target-error-range labels add_hline puts on the error subplot.
        fig.add_annotation(
            text='Model cycle:',
            showarrow=False,
            x=0.78,
            xref='paper',
            xanchor='right',
            y=menu_y,
            yref='paper',
            yanchor='middle',
            font={'size': 15, 'color': 'black', 'family': 'Open Sans'},
        )
        output_file = f'{prop.visuals_horizon_path}/{prop.ofs}_' f'{info[2]}_{info[7]}_cycle_series'
        fig_config = {
            'toImageButtonOptions': {
                'format': 'png',
                'filename': output_file.split('/')[-1],
                'height': figheight,
                'width': figwidth,
                'scale': 1,
            }
        }
        logger.debug(f'Writing file: {output_file}')
        fig.write_html(output_file + '.html', config=fig_config)
        logger.debug(f'Finished writing file: {output_file}')
    except Exception as e_x:
        logger.error(
            'Caught exception in make_timeseries_plots ' 'formatting! Error: %s. Skipping plot!',
            e_x,
        )
        return
