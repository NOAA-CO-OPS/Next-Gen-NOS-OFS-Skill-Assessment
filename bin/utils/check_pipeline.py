import argparse
import csv
import os
from pathlib import Path

from ofs_skill.obs_retrieval import utils

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

# ==========================================
# VISUALIZATION FUNCTION
# ==========================================

def generate_visualizations(csv_file_path, home_dir, ofs):
    """
    Reads the pipeline summary CSV and generates combined heatmaps.
    Each box is annotated with a symbol representing its data provider.

    Grouping Rule:
    - If total stations <= MAX_STATIONS_PER_PLOT, all providers are kept together in 1 file.
    - If total stations > MAX_STATIONS_PER_PLOT, stations are grouped by provider.
    - If an individual provider itself has > MAX_STATIONS_PER_PLOT stations, it is
      sub-chunked into numbered parts (e.g., _usgs_pt_1, _usgs_pt_2).
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        import seaborn as sns
        from matplotlib.colors import ListedColormap
    except ImportError:
        print('\n[Visualization Skipped] Missing required libraries.')
        print('To generate visualizations, install them using: pip install pandas matplotlib seaborn numpy')
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
        elif 'NDBC' in p:
            return 'N'
        elif 'USGS' in p:
            return 'U'
        else:
            return p[0] if p else ''

    variables = plot_df['Variable'].unique()
    MAX_STATIONS_PER_PLOT = 35

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
                    sub_chunks = [st_list[i:i + MAX_STATIONS_PER_PLOT] for i in range(0, len(st_list), MAX_STATIONS_PER_PLOT)]
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

            fig, axes = plt.subplots(nrows=1, ncols=ncols, sharey=True, figsize=(fig_width, fig_height))
            if ncols == 1:
                axes = [axes]

            # Dynamically build a clean legend subtitle from present providers in this chunk
            chunk_df = var_df[var_df['Station_ID'].isin(station_chunk)]
            unique_provs = sorted([str(p).strip() for p in chunk_df['Provider'].dropna().unique() if str(p).strip()])
            prov_legend_parts = [f'{get_symbol(p)} = {p}' for p in unique_provs if get_symbol(p)]
            prov_legend_str = f"({', '.join(prov_legend_parts)})" if prov_legend_parts else ''

            for i, wc in enumerate(whichcasts):
                ax = axes[i]

                wc_df = var_df[(var_df['Whichcast'] == wc) & (var_df['Station_ID'].isin(station_chunk))]
                provider_map = var_df.set_index('Station_ID')['Provider'].to_dict()
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
                title_text = f'Pipeline Tracking: {ofs.upper()} | Variable: {var} | Provider: {group_name}\n{prov_legend_str}'
            else:
                title_text = f'Pipeline Tracking: {ofs.upper()} | Variable: {var}\n{prov_legend_str}'

            fig.suptitle(title_text, fontsize=16, y=0.99)
            axes[0].set_ylabel('Station ID', fontsize=12)

            plt.tight_layout()

            # Format output filename dynamically based on the group suffix
            out_file = Path(home_dir) / f'pipeline_viz_{ofs}_{var}{group_suffix}.png'

            plt.savefig(out_file, dpi=150, bbox_inches='tight')
            plt.close()

            print(f'  -> Saved visualization: {out_file.name}')


# ==========================================
# MAIN SCRIPT
# ==========================================

def main(args):
    ofs = args.OFS.lower()
    var_selection = args.Var_Selection.lower()
    conf_path = args.config.lower()

    raw_wc_str = ' '.join(args.Whichcasts)
    clean_wc_str = raw_wc_str.replace('[', ' ').replace(']', ' ').replace(',', ' ')
    parsed_whichcasts = [wc.strip().lower() for wc in clean_wc_str.split() if wc.strip()]

    target_whichcasts = []
    for wc in parsed_whichcasts:
        if wc not in ALLOWED_WHICHCASTS:
            print(f"Error: Invalid whichcast '{wc}'. Allowed choices are: {', '.join(ALLOWED_WHICHCASTS)}")
            raise SystemExit
        if wc not in target_whichcasts:
            target_whichcasts.append(wc)

    if not target_whichcasts:
        print(f"Error: No valid whichcasts provided. Allowed choices are: {', '.join(ALLOWED_WHICHCASTS)}")
        raise SystemExit

    if var_selection == 'all':
        target_vars = ['cu', 'wl', 'temp', 'salt']
    else:
        target_vars = [var_selection]

    home_dir = Path(args.Path)
    try:
        dir_params = utils.Utils(os.path.join(home_dir, conf_path)).read_config_section('directories', None)
    except FileNotFoundError:
        print('No configuration file found! Please check the path.')
        raise SystemExit

    dir_ctl = Path(os.path.join(home_dir, dir_params['control_files_dir']))
    dir_obs = Path(os.path.join(home_dir, dir_params['data_dir'], dir_params['observations_dir'], dir_params['1d_station_dir'], ))
    dir_prd = Path(os.path.join(home_dir, dir_params['data_dir'], dir_params['model_dir'], dir_params['1d_node_dir'], ))
    dir_int = Path(os.path.join(home_dir, dir_params['data_dir'], dir_params['skill_dir'], dir_params['1d_pair_dir'], ))
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

                obs_found = any(all(term in fname for term in req_obs) for fname in obs_files)
                prd_found = any(all(term in fname for term in req_prd_int) for fname in prd_files)
                int_found = any(all(term in fname for term in req_prd_int) for fname in int_files)
                html_found = any(all(term in fname for term in req_html) for fname in html_files)

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
                    '7_HTML_Generated': 'Yes' if html_found else 'No'
                })

    if not all_csv_rows:
        print('\nNo data was processed. Exiting without writing CSV.')
        return

    print(f'\nWriting summary to {output_csv}...')
    # UPDATED: Included 'Provider' column in header fieldnames
    fieldnames = [
        'Station_ID', 'Variable', 'Whichcast', 'Provider',
        '1_In_Inventory', '2_In_OBS_CTL', '3_OBS_Generated',
        '4_In_Model_CTL', '5_PRD_Generated', '6_INT_Generated', '7_HTML_Generated'
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
        output_csv_fallback = Path(os.path.join(home_dir, f'pipeline_summary_{ofs}_{var_selection}_2.csv'))
        print(f'Permission denied for {output_csv}. Trying {output_csv_fallback}...')
        final_csv_path = write_csv(output_csv_fallback)

    generate_visualizations(final_csv_path, home_dir, ofs)
    print('Pipeline check complete!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Track station processing through the entire 7-stage OFS pipeline.')
    parser.add_argument('--OFS', '-o', type=str, required=True, help="Name of the Operational Forecast System (e.g., 'necofs').")
    parser.add_argument('--Var_Selection', '-vs', type=str, required=True, choices=['cu', 'wl', 'temp', 'salt', 'all'], help='Variable type to search for.')
    parser.add_argument('--Whichcasts', '-ws', type=str, nargs='+', required=True, help='Whichcast type(s) to search for.')
    parser.add_argument('--Path', '-p', type=str, default='.', help='Path to the home directory.')
    parser.add_argument('-c', '--config', type=str, default='conf/ofs_dps.conf', help='Path to configuration file.')

    parsed_args = parser.parse_args()
    main(parsed_args)
