"""OFS Data Processing Pipeline Tracking and Visualization Utility.

This script audits and tracks the progression of physical oceanographic and
meteorological station data through a linear 7-stage processing pipeline
within an Operational Forecast System (OFS). It validates baseline inventory
existence against configured control files and directory assets, outputs a
structured summary matrix in CSV format, and generates highly optimized,
dual-axis heatmap charts.

Pipeline Developmental Stages:
    1. **In Inventory**: Station is registered in the baseline inventory and
       configured to monitor the given parameter.
    2. **In OBS CTL**: Station is successfully declared in the observation
       control file (`.ctl`).
    3. **OBS Generated**: The raw observation file (`.obs`) has been pulled
       and written to disk.
    4. **In Model CTL**: Station is declared in the numerical model's
       control file.
    5. **PRD Generated**: The model prediction file (`.prd`) has been successfully
       simulated and written.
    6. **INT Generated**: The interpolated paired file (`.int`) mapping observation
       to model grids has been successfully built.
    7. **HTML Generated**: The final localized web-visualization product (`.html`)
       is complete.

Drop reasons (joined from the station-drop ledger):
    The 7-stage matrix says how far a station got; it does not say why it
    stopped. When the skill run has left a combined station-drop ledger at
    `control_files/station_ledger_{ofs}.csv`, three extra columns are
    appended to the summary CSV -- `Drop_Stage`, `Drop_Reason`, and
    `Bins_Pruned`.

    `Drop_Stage`/`Drop_Reason` carry the earliest recorded stage that
    dropped the station and its explanation (e.g. `node_match` / "nearest
    model location 6.2 km away (> 4.0 km cutoff)"). They are populated only
    when this audit's own matrix agrees that the station stalled: a ledger
    reason is shown only if the station has a `No` at or after the column
    that stage would have blocked (see `LEDGER_STAGE_MIN_COLUMN`). A station
    that reached all seven stages is never annotated as dropped, whatever
    the ledger holds -- ledger rows are per bin for ADCP currents and can be
    left over from a run whose stages this run never re-executed.

    `Bins_Pruned` counts CO-OPS ADCP virtual bins the skill run removed
    because they sat below the model bottom. That is routine for shallow
    stations and is deliberately kept out of `Drop_Stage`: the parent
    station is still assessed on its remaining bins. It becomes the drop
    reason only when the station is itself missing from the model control
    file, i.e. nothing survived the prune.

    Ledger rows stamped `whichcast=all` describe artifacts shared by every
    cast and are applied to each audited whichcast; rows for a different
    `filetype` (a `-t fields` run) are skipped, since this tool audits the
    `stations` product. All three columns are left blank when no ledger file
    is present, so older output directories still audit cleanly. The
    heatmaps are unaffected: they select their columns by name.

Visual Chunking & Provider Grouping Rules (threshold = MAX_STATIONS_PER_PLOT):
    - **Combined Mode**: If the cumulative unique stations for a variable is at or
      below the threshold, all observation data sources are consolidated into one
      unified master chart (`_combined.png`).
    - **Provider Split Mode**: If total stations exceed the threshold, the script
      groups stations strictly by data provider (e.g., `CO-OPS`, `NDBC`, `USGS`),
      generating separate layout charts per supplier.
    - **Deep Sub-Chunking**: If a single provider's underlying station list itself
      exceeds the threshold, it is sub-divided into cleanly numbered multi-part
      files (e.g., `_usgs_pt_1.png`, `_usgs_pt_2.png`).

Dependencies:
    - Standard Library: `os`, `csv`, `argparse`, `pathlib`
    - Data Processing: `pandas`, `numpy`
    - Visualization: `matplotlib`, `seaborn`
    - Internal Domain: `ofs_skill.obs_retrieval.utils`,
      `ofs_skill.obs_retrieval.currents_bins_override`,
      `ofs_skill.model_processing.station_ledger`

Author: PWL
Date: June 2026
"""

import argparse
import csv
import logging
import os
from pathlib import Path

from ofs_skill.model_processing.station_ledger import stage_rank
from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.currents_bins_override import split_virtual_currents_id

# ==========================================
# CONFIGURATION
# ==========================================

HTML_VAR_MAP = {
    'cu': 'currents',
    'wl': 'water_level',
    'temp': 'temperature',
    'salt': 'salinity'
}

ALLOWED_WHICHCASTS = ['nowcast', 'forecast_b', 'forecast_a', 'hindcast']

# The station-drop ledger names variables the way the pipeline does; this
# tool uses its own shorter term for temperature. Translate so the join on
# the ledger cannot silently miss every temperature row.
LEDGER_VAR_TERMS = {
    'water_level': 'water_level',
    'water_temperature': 'temperature',
    'salinity': 'salinity',
    'currents': 'currents',
}

# Whichcast stamp the ledger uses for stages whose artifacts are shared by
# every cast (inventory, control files, station matching).
LEDGER_CAST_ALL = 'all'

# Product this tool audits. The combined ledger holds `stations` and `fields`
# rows side by side, told apart by its `filetype` column.
LEDGER_FILETYPE = 'stations'

# Earliest 7-stage column a ledger drop can explain. A station dropped at
# `obs_ctl` never reaches the observation control file (column 2); one
# dropped at `node_match` never reaches the model control file (column 4);
# one that failed pairing has its `.obs` but no usable pair, which shows up
# from the `.prd`/`.int` columns onwards (column 5). A ledger reason whose
# column is later than this station's first `No` describes a stall that did
# not happen here, so it is not shown.
LEDGER_STAGE_MIN_COLUMN = {
    'inventory': 2,
    'inventory_variable_flag': 2,
    'obs_ctl': 2,
    'node_match': 4,
    'node_match_collision': 4,
    'depth_match': 4,
    'model_ctl': 4,
    'id_mismatch': 5,
    'temporal_overlap': 5,
    'pairing': 5,
}

# Fallback for a stage this tool has not been taught about: require only
# that the station stalled somewhere.
DEFAULT_LEDGER_STAGE_COLUMN = 1

# Ledger stage recorded per ADCP bin rather than per station. Surfaced in
# its own column instead of as the parent station's drop reason.
LEDGER_BIN_STAGE = 'depth_match'

# Maximum stations rendered in a single heatmap before the layout splits by
# provider and, if still too dense, sub-chunks a provider into numbered parts.
MAX_STATIONS_PER_PLOT = 35

# ==========================================
# VISUALIZATION FUNCTION
# ==========================================

def generate_visualizations(csv_file_path, home_dir, ofs):
    """Reads the pipeline summary CSV and generates ultra-tight, side-by-side heatmaps.

    This function processes pipeline completion statuses across 7 linear tracking
    stages for every monitored variable and forecast type (whichcast). Each grid
    cell represents a stage's completion status (Green/True for completed, Red/False
    for missing) and is explicitly annotated with a bold, single-character indicator
    denoting its original data provider (e.g., 'C' for CO-OPS, 'U' for USGS, 'N' for NDBC).

    To preserve high visual fidelity and text readability:
    1. **Adaptive Chunking Strategy**: If the total station count for a variable is
       at or below MAX_STATIONS_PER_PLOT, all providers are bundled together into a
       single master plot file (`_combined.png`). If the station count exceeds the
       threshold, the function pivots to group stations strictly by their data
       provider, outputting one file per source.
    2. **Deep Sub-chunking**: If a single data provider's internal station count
       exceeds MAX_STATIONS_PER_PLOT, it is sub-divided into numbered files (e.g.,
       `_usgs_pt_1.png`, `_usgs_pt_2.png`).
    3. **Anti-Squishing Layout**: The vertical dimensions are automatically scaled
       with a floor constraint of 6.5 inches. This prevents sparse station selections
       from flattening when squeezed by the top and bottom text margins.
    4. **Dual X-Axis Layout**: Structural tick marks and stage names are mirrored
       identically across both the top and bottom of the heatmap frame. Top labels are
       automatically offset and aligned up-and-away from the grid to maintain space.
    5. **Ultra-Tight Compression**: Spacing parameters (`pad=4` and `y=0.99`) are
       tightened to squeeze out blank white space between titles, labels, and figures.

    Args:
        csv_file_path (str or pathlib.Path): Path to the generated CSV matrix output
            containing the 7-stage pipeline boolean data for all stations.
        home_dir (str or pathlib.Path): Root or target destination directory where
            the finalized visualization image assets will be compiled and written.
        ofs (str): Name of the operational forecast system being audited
            (e.g., 'cbofs', 'dbofs', 'necofs').

    Returns:
        None: Saves generated high-resolution (150 DPI) PNG visualization charts
        directly into the target `home_dir`.

    Raises:
        ImportError: Raised and caught internally if any dependency (`pandas`,
            `matplotlib`, `seaborn`, or `numpy`) is missing from the environment,
            safely skipping visual generation without killing execution flow.

    File Output Naming Conventions:
        - Small Network:   `pipeline_viz_{ofs}_{variable}_combined.png`
        - Segmented Group: `pipeline_viz_{ofs}_{variable}_{provider_name}.png`
        - Split Part:      `pipeline_viz_{ofs}_{variable}_{provider_name}_pt_{part_number}.png`
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        import seaborn as sns
        from matplotlib.colors import ListedColormap
    except ImportError:
        print('\n[Visualization Skipped] Missing required libraries.')
        print('To generate visualizations, install them using: '
              'pip install pandas matplotlib seaborn numpy')
        return

    print('\nGenerating visual heatmaps from CSV summary...')

    df = pd.read_csv(csv_file_path)
    if df.empty:
        print('CSV is empty. No visualizations to generate.')
        return

    stages = [
        '1_In_Inventory', '2_In_OBS_CTL', '3_OBS_Generated',
        '4_In_Model_CTL', '5_PRD_Generated', '6_INT_Generated', '7_HTML_Generated'
    ]

    # Convert Yes/No strings to 1 (True/Green) and 0 (False/Red) for the heatmap
    plot_df = df.copy()
    for stage in stages:
        plot_df[stage] = plot_df[stage].map({'Yes': 1, 'No': 0})

    # Custom Red/Green colormap
    cmap = ListedColormap(['#ff6666', '#66cc66'])

    # Helper to clean and map provider text to an abbreviated single character symbol
    def get_symbol(provider_name):
        if pd.isna(provider_name) or not str(provider_name).strip():
            return ''
        p = str(provider_name).strip().upper()
        if 'CO-OPS' in p:
            return 'C'
        if 'NDBC' in p:
            return 'N'
        if 'USGS' in p:
            return 'U'
        return p[0] if p else ''

    variables = plot_df['Variable'].unique()

    for var in variables:
        var_df = plot_df[plot_df['Variable'] == var]
        whichcasts = var_df['Whichcast'].unique()

        # Get a sorted list of all unique stations for this variable
        all_stations = sorted(var_df['Station_ID'].unique())

        # -----------------------------------------------------------------
        # STRATEGY: Determine chunking approach based on total station counts
        # -----------------------------------------------------------------
        if len(all_stations) <= MAX_STATIONS_PER_PLOT:
            # Everything fits in one heatmap -> do not split
            station_groups = [('combined', all_stations, '_combined')]
        else:
            # Too many stations -> group by station provider
            station_to_prov = var_df.set_index('Station_ID')['Provider'].to_dict()
            prov_map_groups = {}
            for st in all_stations:
                prov = station_to_prov.get(st, 'Unknown')
                prov_map_groups.setdefault(prov, []).append(st)

            station_groups = []
            # Sort providers alphabetically for consistent tracking
            for prov in sorted(prov_map_groups.keys()):
                st_list = sorted(prov_map_groups[prov])
                prov_clean = prov.lower().replace('-', '_').replace(' ', '_')

                if len(st_list) <= MAX_STATIONS_PER_PLOT:
                    # Provider stations fit inside a single image
                    station_groups.append((prov, st_list, f'_{prov_clean}'))
                else:
                    # NEW: Provider stations exceed max limit -> sub-chunk into parts
                    sub_chunks = [
                        st_list[i:i + MAX_STATIONS_PER_PLOT]
                        for i in range(0, len(st_list), MAX_STATIONS_PER_PLOT)]
                    for idx, sub_chunk in enumerate(sub_chunks):
                        display_name = f'{prov} (Part {idx + 1})'
                        group_suffix = f'_{prov_clean}_pt_{idx + 1}'
                        station_groups.append((display_name, sub_chunk, group_suffix))
        # -----------------------------------------------------------------

        for group_name, station_chunk, group_suffix in station_groups:
            ncols = len(whichcasts)

            # Sizing for layout height scales safely to fit the entire provider block
            fig_height = max(6.5, len(station_chunk) * 0.45)
            fig_width = max(6, ncols * 4.5) + 2

            fig, axes = plt.subplots(
                nrows=1, ncols=ncols, sharey=True,
                figsize=(fig_width, fig_height))
            if ncols == 1:
                axes = [axes]

            # Dynamically build a clean legend subtitle from present providers in this chunk
            chunk_df = var_df[var_df['Station_ID'].isin(station_chunk)]
            unique_provs = sorted(
                {str(p).strip() for p in chunk_df['Provider'].dropna().unique()
                 if str(p).strip()})
            prov_legend_parts = [f'{get_symbol(p)} = {p}' for p in unique_provs if get_symbol(p)]
            prov_legend_str = f"({', '.join(prov_legend_parts)})" if prov_legend_parts else ''

            # Station -> provider lookup is identical across whichcasts; build once
            provider_map = var_df.set_index('Station_ID')['Provider'].to_dict()

            for i, wc in enumerate(whichcasts):
                ax = axes[i]

                wc_df = var_df[(var_df['Whichcast'] == wc)
                               & (var_df['Station_ID'].isin(station_chunk))]
                wc_df = wc_df.set_index('Station_ID').reindex(station_chunk)[stages]

                # Construct cell marking matrices
                annot_df = pd.DataFrame('', index=wc_df.index, columns=wc_df.columns)
                for st in wc_df.index:
                    prov = provider_map.get(st, '')
                    annot_df.loc[st] = get_symbol(prov)

                # Draw Heatmap
                sns.heatmap(
                    wc_df,
                    cmap=cmap,
                    cbar=False,
                    linewidths=0.5,
                    linecolor='black',
                    vmin=0, vmax=1,
                    annot=annot_df,
                    fmt='',
                    annot_kws={'fontsize': 10, 'weight': 'bold', 'color': '#333333'},
                    ax=ax
                )

                # Set to an ultra-tight pad of 4 to squeeze out empty white space
                ax.set_title(f'{wc.upper()}', fontsize=12, pad=4)
                ax.set_xlabel('')
                ax.set_ylabel('')

                # Configure structured tick labels
                clean_labels = [s.replace('_', ' ') for s in stages]
                ax.set_xticks(np.arange(len(stages)) + 0.5)
                ax.set_xticklabels(clean_labels)
                ax.tick_params(axis='x', top=True, labeltop=True, bottom=True, labelbottom=True)

                for label in ax.get_xticklabels():
                    label.set_rotation(45)
                    if label.get_position()[1] > 0.5:
                        label.set_ha('left')
                    else:
                        label.set_ha('right')

                # Lock down Station IDs flat/horizontal
                for label in ax.get_yticklabels():
                    label.set_rotation(0)
                    label.set_va('center')

            # Customize main title text depending on whether it is split or combined
            if len(station_groups) > 1:
                title_text = (f'Pipeline Tracking: {ofs.upper()} | Variable: {var} '
                              f'| Provider: {group_name}\n{prov_legend_str}')
            else:
                title_text = (f'Pipeline Tracking: {ofs.upper()} | Variable: {var}'
                              f'\n{prov_legend_str}')

            fig.suptitle(title_text, fontsize=16, y=0.99)
            axes[0].set_ylabel('Station ID', fontsize=12)

            plt.tight_layout()

            # Format output filename dynamically based on the group suffix
            out_file = Path(home_dir) / f'pipeline_viz_{ofs}_{var}{group_suffix}.png'

            plt.savefig(out_file, dpi=150, bbox_inches='tight')
            plt.close()

            print(f'  -> Saved visualization: {out_file.name}')


# ==========================================
# STATION-DROP LEDGER JOIN
# ==========================================

def _load_ledger_reasons(dir_ctl, ofs, target_whichcasts,
                         filetype=LEDGER_FILETYPE):
    """Load drop stages/reasons from the combined station-drop ledger.

    The skill run writes ``control_files/station_ledger_{ofs}.csv``, which
    records *why* a station stopped progressing. This tool records *how far*
    each station got. Joining the two turns a bare "No" in the stage matrix
    into an explanation.

    Per-bin ADCP prunings are kept apart from station-level drops. A bin
    removed for sitting below the model bottom does not stop its parent
    station, which is still assessed on the bins that remain, so those
    records are returned as a count rather than as the station's reason.

    Args:
        dir_ctl (str or pathlib.Path): Control-files directory to look in.
        ofs (str): OFS identifier, used to build the ledger file name.
        target_whichcasts (list of str): Whichcasts this run is auditing.
            Ledger rows stamped ``all`` (stages whose artifacts are shared
            by every cast) are expanded across all of them.
        filetype (str): Product being audited. Ledger rows carrying a
            different ``filetype`` are skipped, so a ``-t fields`` run's
            drop reasons are not attributed to the stations audit.

    Returns:
        tuple: ``(reasons, bin_prunes)``. ``reasons`` maps
        ``(station_id, variable_term, whichcast)`` to ``(stage, reason)``
        with lower-cased station IDs, ADCP virtual bin IDs collapsed to
        their parent station, and the earliest pipeline stage winning when
        a station was recorded at more than one. ``bin_prunes`` maps the
        same key to ``(bin_count, sample_reason)`` for below-bottom bin
        removals. Both are empty when no ledger file is present, so the
        join is a no-op on older output directories.
    """
    reasons = {}
    bin_prunes = {}
    ledger_path = Path(os.path.join(dir_ctl, f'station_ledger_{ofs}.csv'))
    if not ledger_path.exists():
        print(f'  -> No station ledger at {ledger_path}; '
              f'drop reasons will be blank.')
        return reasons, bin_prunes

    try:
        with open(ledger_path, newline='') as ledger_file:
            rows = list(csv.DictReader(ledger_file))
    except (OSError, csv.Error, UnicodeDecodeError):
        print(f'  -> Warning: could not read {ledger_path}; '
              f'drop reasons will be blank.')
        return reasons, bin_prunes

    for row in rows:
        if (row.get('record_type') or 'drop').strip() != 'drop':
            continue
        # A blank filetype means an older/hand-made ledger: accept it rather
        # than silently dropping every row.
        row_filetype = (row.get('filetype') or '').strip()
        if row_filetype and row_filetype != filetype:
            continue
        # A leading quote is the ledger's spreadsheet-formula guard.
        raw_id = (row.get('station_id') or '').strip().lstrip("'").lower()
        if not raw_id:
            continue
        station_id, bin_num = split_virtual_currents_id(raw_id)
        ledger_var = (row.get('variable') or '').strip()
        var_term = LEDGER_VAR_TERMS.get(ledger_var, ledger_var)
        stage = (row.get('stage') or '').strip()
        reason = (row.get('reason') or '').strip().lstrip("'")
        row_cast = (row.get('whichcast') or '').strip()
        casts = (target_whichcasts if row_cast == LEDGER_CAST_ALL
                 else [row_cast])
        for cast in casts:
            key = (station_id, var_term, cast)
            if stage == LEDGER_BIN_STAGE and bin_num is not None:
                count, sample = bin_prunes.get(key, (0, reason))
                bin_prunes[key] = (count + 1, sample)
                continue
            current = reasons.get(key)
            # Earliest stage wins: it is the one that actually stopped the
            # station, and later records are consequences of it.
            if current is None or stage_rank(stage) < stage_rank(current[0]):
                reasons[key] = (stage, reason)
    return reasons, bin_prunes


def _first_failed_stage(stage_flags):
    """Return the 1-based index of the earliest ``False`` stage, or None.

    ``stage_flags`` is the seven booleans of one summary row, in pipeline
    order. ``None`` means the station completed every stage.
    """
    for index, reached in enumerate(stage_flags, start=1):
        if not reached:
            return index
    return None


def _explain_row(stage_flags, reason_entry, prune_entry):
    """Pick the drop annotation for one summary row.

    A ledger reason is only shown when this audit agrees the station
    stalled, and only when the stall is at or after the column that stage
    would have blocked. That keeps a stale record from a previous run -- or
    a per-bin currents record -- from labelling a station the auditor's own
    matrix reports as complete.

    Args:
        stage_flags (list of bool): The row's seven stage outcomes, in
            pipeline order.
        reason_entry (tuple or None): ``(stage, reason)`` from the ledger.
        prune_entry (tuple or None): ``(bin_count, sample_reason)`` of
            below-bottom ADCP bins removed for this station.

    Returns:
        tuple: ``(drop_stage, drop_reason, bins_pruned_text)``.
    """
    bins_pruned, prune_reason = prune_entry or (0, '')
    bins_text = str(bins_pruned) if bins_pruned else ''
    first_no = _first_failed_stage(stage_flags)
    if first_no is None:
        # The station reached the end of the pipeline. Whatever the ledger
        # says, it was not dropped in this run.
        return '', '', bins_text

    if reason_entry:
        stage, reason = reason_entry
        min_column = LEDGER_STAGE_MIN_COLUMN.get(
            stage, DEFAULT_LEDGER_STAGE_COLUMN)
        if first_no >= min_column:
            return stage, reason, bins_text

    # Nothing else explains a currents station that never reached the model
    # control file, so an exhaustive bin prune is the explanation.
    if bins_pruned and first_no == LEDGER_STAGE_MIN_COLUMN[LEDGER_BIN_STAGE]:
        return LEDGER_BIN_STAGE, (
            f'{bins_pruned} ADCP bin(s) removed as below the model bottom; '
            f'e.g. {prune_reason}'), bins_text
    return '', '', bins_text


# ==========================================
# MAIN SCRIPT
# ==========================================

def main(args):
    ofs = args.OFS.lower()
    var_selection = args.Var_Selection.lower()
    # Do not lowercase the config path: filesystem paths are case-sensitive on Linux.
    conf_path = args.config

    raw_wc_str = ' '.join(args.Whichcasts)
    clean_wc_str = raw_wc_str.replace('[', ' ').replace(']', ' ').replace(',', ' ')
    parsed_whichcasts = [wc.strip().lower() for wc in clean_wc_str.split() if wc.strip()]

    target_whichcasts = []
    for wc in parsed_whichcasts:
        if wc not in ALLOWED_WHICHCASTS:
            print(f"Error: Invalid whichcast '{wc}'. Allowed choices are: "
                  f"{', '.join(ALLOWED_WHICHCASTS)}")
            raise SystemExit(1)
        if wc not in target_whichcasts:
            target_whichcasts.append(wc)

    if not target_whichcasts:
        print(f'Error: No valid whichcasts provided. Allowed choices are: '
              f"{', '.join(ALLOWED_WHICHCASTS)}")
        raise SystemExit(1)

    if var_selection == 'all':
        target_vars = ['cu', 'wl', 'temp', 'salt']
    else:
        target_vars = [var_selection]

    home_dir = Path(args.Path)
    # A relative -c is looked for under the working directory first, then
    # under the installation root, so an external -p still finds the
    # shipped config instead of dying on a path that cannot exist.
    # Split into components before resolving: resolve_asset_path joins its
    # *parts with os.path.join, so handing it the whole 'conf/ofs_dps.conf'
    # string would splice a forward slash into an otherwise native path and
    # yield a mixed-separator path like 'C:\...\conf/ofs_dps.conf' on Windows.
    resolved_conf = (
        conf_path if os.path.isabs(conf_path)
        else utils.resolve_asset_path(home_dir, *Path(conf_path).parts)
    )
    try:
        dir_params = utils.Utils(resolved_conf).read_config_section(
            'directories', logging.getLogger(__name__))
    except FileNotFoundError as exc:
        print('No configuration file found! Please check the path.')
        raise SystemExit(1) from exc

    dir_ctl = Path(os.path.join(home_dir, dir_params['control_files_dir']))
    dir_obs = Path(os.path.join(
        home_dir, dir_params['data_dir'], dir_params['observations_dir'],
        dir_params['1d_station_dir']))
    dir_prd = Path(os.path.join(
        home_dir, dir_params['data_dir'], dir_params['model_dir'],
        dir_params['1d_node_dir']))
    dir_int = Path(os.path.join(
        home_dir, dir_params['data_dir'], dir_params['skill_dir'],
        dir_params['1d_pair_dir']))
    dir_html = Path(os.path.join(home_dir, dir_params['data_dir'], dir_params['visual_dir'], ))

    output_csv = Path(os.path.join(home_dir, f'pipeline_summary_{ofs}_{var_selection}.csv'))

    def get_filenames(dir_path, ext):
        if not dir_path.exists():
            print(f'  -> Warning: Directory {dir_path} does not exist.')
            return []
        return [f.name.lower() for f in dir_path.glob(f'*{ext}')]

    print('Pre-fetching directory file lists...')
    obs_files = get_filenames(dir_obs, '.obs')
    prd_files = get_filenames(dir_prd, '.prd')
    int_files = get_filenames(dir_int, '.int')
    html_files = get_filenames(dir_html, '.html')

    ledger_reasons, ledger_bin_prunes = _load_ledger_reasons(
        dir_ctl, ofs, target_whichcasts)

    all_csv_rows = []

    for var in target_vars:
        print(f'\n================ Processing Variable: {var.upper()} ================')
        stations_tracker = {}
        html_var_term = HTML_VAR_MAP.get(var, var)

        inv_filename = f'inventory_all_{ofs}.csv'
        inv_path = Path(os.path.join(dir_ctl, inv_filename))

        if not Path(inv_path).exists():
            print(f'Error: Inventory file {inv_path} not found. Skipping {var} baseline.')
            continue

        print(f'Reading baseline from {inv_path}...')
        with open(inv_path) as f:
            reader = csv.DictReader(f)
            var_flag_column = f'has_{var}'

            for row in reader:
                station_id = row.get('ID', '').strip().lower()
                if not station_id:
                    continue

                if var_flag_column in row and row[var_flag_column].strip().upper() != 'TRUE':
                    continue

                # UPDATED: Capturing data source 'Source' key as 'provider'
                stations_tracker[station_id] = {
                    'inv': True,
                    'obs_ctl': False,
                    'mod_ctl': False,
                    'provider': row.get('Source', 'Unknown').strip()
                }

        if not stations_tracker:
            print(f"No valid stations found for variable '{var}' in the inventory. Skipping.")
            continue

        print(f'Tracking {len(stations_tracker)} baseline station(s) for {var}.')

        obs_ctl_path = Path(os.path.join(dir_ctl, f'{ofs}_{var}_station.ctl'))
        mod_ctl_path = Path(os.path.join(dir_ctl, f'{ofs}_{var}_model_station.ctl'))

        obs_ctl_content = ''
        if obs_ctl_path.exists():
            with open(obs_ctl_path) as f:
                obs_ctl_content = f.read().lower()

        mod_ctl_content = ''
        if mod_ctl_path.exists():
            with open(mod_ctl_path) as f:
                mod_ctl_content = f.read().lower()
        else:
            mod_ctl_path = Path(os.path.join(dir_ctl, f'{ofs}_{var}_model.ctl'))
            if mod_ctl_path.exists():
                with open(mod_ctl_path) as f:
                    mod_ctl_content = f.read().lower()

        # NOTE: stage detection below relies on substring matching (station ID is a
        # substring of the .ctl content / output filename). This deliberately lets a
        # base station match its per-bin ADCP files (e.g. 'cb0402' -> 'cb0402_b01...'),
        # but it is approximate: a station ID that is itself a substring of a longer ID
        # can register a false positive. The tool is an at-a-glance auditor, not an
        # exact-match validator.
        for st_id in sorted(stations_tracker.keys()):
            if st_id in obs_ctl_content:
                stations_tracker[st_id]['obs_ctl'] = True
            if st_id in mod_ctl_content:
                stations_tracker[st_id]['mod_ctl'] = True

            s = stations_tracker[st_id]

            for wc in target_whichcasts:
                req_obs = [st_id, ofs, var]
                req_prd_int = [st_id, ofs, var, wc]
                req_html = [st_id, ofs, html_var_term, wc]

                obs_found = any(
                    all(term in fname for term in req_obs) for fname in obs_files)
                prd_found = any(
                    all(term in fname for term in req_prd_int) for fname in prd_files)
                int_found = any(
                    all(term in fname for term in req_prd_int) for fname in int_files)
                html_found = any(
                    all(term in fname for term in req_html) for fname in html_files)

                # Explain the first stage that stopped this station, when
                # the skill run's ledger recorded one and this audit agrees
                # the station actually stalled there.
                ledger_key = (st_id, html_var_term, wc)
                drop_stage, drop_reason, bins_pruned = _explain_row(
                    [s['inv'], s['obs_ctl'], obs_found, s['mod_ctl'],
                     prd_found, int_found, html_found],
                    ledger_reasons.get(ledger_key),
                    ledger_bin_prunes.get(ledger_key),
                )

                # UPDATED: Included 'Provider' key into output rows mapping
                all_csv_rows.append({
                    'Station_ID': st_id,
                    'Variable': html_var_term,
                    'Whichcast': wc,
                    'Provider': s.get('provider', 'Unknown'),
                    '1_In_Inventory': 'Yes' if s['inv'] else 'No',
                    '2_In_OBS_CTL': 'Yes' if s['obs_ctl'] else 'No',
                    '3_OBS_Generated': 'Yes' if obs_found else 'No',
                    '4_In_Model_CTL': 'Yes' if s['mod_ctl'] else 'No',
                    '5_PRD_Generated': 'Yes' if prd_found else 'No',
                    '6_INT_Generated': 'Yes' if int_found else 'No',
                    '7_HTML_Generated': 'Yes' if html_found else 'No',
                    'Drop_Stage': drop_stage,
                    'Drop_Reason': drop_reason,
                    'Bins_Pruned': bins_pruned
                })

    if not all_csv_rows:
        print('\nNo data was processed. Exiting without writing CSV.')
        return

    print(f'\nWriting summary to {output_csv}...')
    # UPDATED: Included 'Provider' column in header fieldnames
    fieldnames = [
        'Station_ID', 'Variable', 'Whichcast', 'Provider',
        '1_In_Inventory', '2_In_OBS_CTL', '3_OBS_Generated',
        '4_In_Model_CTL', '5_PRD_Generated', '6_INT_Generated', '7_HTML_Generated',
        'Drop_Stage', 'Drop_Reason', 'Bins_Pruned'
    ]

    def write_csv(csv_path):
        with open(csv_path, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_csv_rows)
        return csv_path

    final_csv_path = None
    try:
        final_csv_path = write_csv(output_csv)
    except PermissionError:
        output_csv_fallback = Path(os.path.join(
            home_dir, f'pipeline_summary_{ofs}_{var_selection}_2.csv'))
        print(f'Permission denied for {output_csv}. Trying {output_csv_fallback}...')
        final_csv_path = write_csv(output_csv_fallback)

    generate_visualizations(final_csv_path, home_dir, ofs)
    print('Pipeline check complete!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Track station processing through the entire 7-stage OFS pipeline.')
    parser.add_argument(
        '--OFS', '-o', type=str, required=True,
        help="Name of the Operational Forecast System (e.g., 'necofs').")
    parser.add_argument(
        '--Var_Selection', '-vs', type=str, required=True,
        choices=['cu', 'wl', 'temp', 'salt', 'all'],
        help='Variable type to search for.')
    parser.add_argument(
        '--Whichcasts', '-ws', type=str, nargs='+', required=True,
        help='Whichcast type(s) to search for.')
    parser.add_argument(
        '--Path', '-p', type=str, default='.',
        help='Path to the home directory.')
    parser.add_argument(
        '-c', '--config', type=str, default='conf/ofs_dps.conf',
        help='Path to configuration file.')

    parsed_args = parser.parse_args()
    main(parsed_args)
