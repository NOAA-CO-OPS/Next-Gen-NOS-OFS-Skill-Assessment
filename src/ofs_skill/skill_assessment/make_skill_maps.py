"""
Create skill assessment maps.

This module makes plotly express maps of skill assessment output. Each
observation station is mapped, with markers color-coded by RMSE,
Central Frequency, and Mean Bias. Includes a dropdown to toggle views.

Created on Wed Sep 4 14:33:17 2024

@author: PL
"""

import math
import os
from logging import Logger
from typing import Any

import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go

from ofs_skill.skill_assessment.nos_metrics import get_error_threshold


def make_skill_maps(
    output: dict[str, list],
    prop: Any,
    name_var: str,
    logger: Logger,
) -> None:
    """
    Create interactive skill assessment maps with a dropdown toggle.
    """
    logger.info('Making skill maps...')

    # First make dataframe from stats table
    df = pd.DataFrame(
        {
            'ID ': output['station_id'],
            'OFS NODE ': output['node'],
            'X ': output['X'],
            'Y ': output['Y'],
            'RMSE ': list(zip(*output['skill']))[0],
            'R ': list(zip(*output['skill']))[1],
            'Mean bias ': list(zip(*output['skill']))[2],
            'Mean bias percent ': list(zip(*output['skill']))[3],
            'Mean bias, current direction ': list(zip(*output['skill']))[4],
            'Central freq ': list(zip(*output['skill']))[5],
            'CF pass/fail ': list(zip(*output['skill']))[6],
            'Positive outlier freq ': list(zip(*output['skill']))[7],
            'PO freq pass/fail ': list(zip(*output['skill']))[8],
            'Negative outlier freq ': list(zip(*output['skill']))[9],
            'NO freq pass/fail ': list(zip(*output['skill']))[10],
            'Max duration PO': list(zip(*output['skill']))[11],
            'Max duration PO pass/fail': list(zip(*output['skill']))[12],
            'Max duration NO': list(zip(*output['skill']))[13],
            'Max duration NO pass/fail': list(zip(*output['skill']))[14],
            'Worst case outlier freq': list(zip(*output['skill']))[15],
            'Worst case outlier freq pass/fail': list(zip(*output['skill']))[16],
            'Mean bias standard dev ': list(zip(*output['skill']))[17],
            'Target RMSE ': list(zip(*output['skill']))[18],
        }
    )

    # Drop stations with insufficient data points for stats
    if df['RMSE '].dtype == 'object' or df['RMSE '].dtype == 'str':
        df = df[~df['RMSE '].str.contains('data points', na=False)]
        df = df.astype({
                        'RMSE ': 'float',
                        'R ': 'float',
                        'Mean bias ': 'float',
                        'Mean bias percent ': 'float',
                        'Mean bias, current direction ': 'float',
                        'Central freq ': 'float',
                        'CF pass/fail ': 'str',
                        'Positive outlier freq ': 'float',
                        'PO freq pass/fail ': 'str',
                        'Negative outlier freq ': 'float',
                        'NO freq pass/fail ': 'str',
                        'Max duration PO': 'float',
                        'Max duration PO pass/fail': 'str',
                        'Max duration NO': 'float',
                        'Max duration NO pass/fail': 'str',
                        'Worst case outlier freq': 'float',
                        'Worst case outlier freq pass/fail': 'str',
                        'Mean bias standard dev ': 'float',
                        'Target RMSE ': 'float',
                        })
        if df.empty:
            logger.error('DataFrame is empty, cannot make skill maps!')
            return

    df['X '] = pd.to_numeric(df['X '], errors='coerce')
    df['Y '] = pd.to_numeric(df['Y '], errors='coerce')

    # Drop stations with missing or sentinel (0,0) coordinates so they don't
    # skew the auto-zoom / center-of-mass calculation below.
    df = df[df['X '].notna() & df['Y '].notna()
            & ~((df['X '] == 0) & (df['Y '] == 0))]
    if df.empty:
        logger.error('No stations with valid coordinates; cannot make skill maps!')
        return

    # Absolute mean bias for marker sizing
    df['Abs mean bias '] = df['Mean bias '].abs()

    datestrend = (prop.end_date_full).split('T')[0]
    datestrbeg = (prop.start_date_full).split('T')[0]

    title_map = {'wl': 'water level', 'salt': 'salinity', 'temp': 'temperature', 'cu': 'current speed'}
    unit_map = {'wl': 'm', 'salt': 'PSU', 'temp': '\u00b0C', 'cu': 'm/s'}
    title_var = title_map.get(name_var, name_var)
    title_unit = unit_map.get(name_var, name_var)

    map_width = 1000
    map_height = 650

    # Calculate the center coordinates of your data
    center_lat = df['Y '].mean()
    center_lon = df['X '].mean()

    # Calculate the optimal starting zoom level
    lat_diff = df['Y '].max() - df['Y '].min()
    lon_diff = df['X '].max() - df['X '].min()

    if lat_diff == 0 and lon_diff == 0:
        zoom_level = 10.0
    else:
        zoom_lon = math.log2(360 / lon_diff) if lon_diff > 0 else 15.0
        zoom_lat = math.log2(180 / lat_diff) if lat_diff > 0 else 15.0
        zoom_level = min(zoom_lon, zoom_lat) - 0.25

    # Base Titles
    plottitle_rmse = f"{prop.ofs.upper()} {prop.whichcast.split('_')[0]} {title_var} RMSE statistics, {datestrbeg} - {datestrend}"
    plottitle_cf = f"{prop.ofs.upper()} {prop.whichcast.split('_')[0]} {title_var} central frequency, {datestrbeg} - {datestrend}"
    plottitle_mb = f"{prop.ofs.upper()} {prop.whichcast.split('_')[0]} {title_var} mean bias, {datestrbeg} - {datestrend}"

    # ========================================================
    #                     CALCULATE RMSE LOGIC
    # ========================================================
    target_error, _ = get_error_threshold(
        name_var,
        os.path.join(prop.path, 'conf', 'error_ranges.csv'),
    )
    actual_max_rmse = df['RMSE '].max()
    min_rmse_extent = target_error * 2
    rmse_cap = target_error * 6.0

    # display_max_rmse = max(actual_max_rmse, min_rmse_extent)
    # display_max_rmse = min(display_max_rmse, rmse_cap)
    display_max_rmse = target_error * 4
    norm_target_rmse = target_error / display_max_rmse

    color_scale_rmse = [
        [0, '#5aa17f'],
        [norm_target_rmse, '#92ddc8'],
        [norm_target_rmse, '#ffcccb'],
        [1, '#e35336']
    ]

    # if actual_max_rmse > display_max_rmse:
    #     tick_values_rmse = [0, target_error, display_max_rmse]
    #     tick_labels_rmse = ['0', f'Target ({target_error})', f'Max ({display_max_rmse:.2f}+)']
    # elif actual_max_rmse <= target_error:
    tick_values_rmse = [0, target_error, display_max_rmse]
    tick_labels_rmse = ['0', f'Target ({target_error})', f'Scale Limit ({display_max_rmse:.2f})']
    # else:
    #     tick_values_rmse = [0, target_error, display_max_rmse]
    #     tick_labels_rmse = ['0', f'Target ({target_error})', f'Max ({display_max_rmse:.2f})']

    # ========================================================
    #                     CALCULATE CF LOGIC
    # ========================================================
    # Central frequency is always reported as a percentage (0-100) by
    # nos_metrics.central_frequency; the NOS skill-assessment pass threshold is 90 %.
    target_cf = 90
    cf_upper_bound = 100

    actual_max_cf = df['Central freq '].max()
    display_max_cf = max(actual_max_cf, cf_upper_bound)

    if display_max_cf == 0:
        norm_target_cf = 1
    else:
        norm_target_cf = target_cf / display_max_cf

    color_scale_cf = [
        [0, '#e35336'],
        [norm_target_cf, '#ffcccb'],
        [norm_target_cf, '#92ddc8'],
        [1, '#5aa17f']
    ]

    if actual_max_cf < target_cf:
        tick_values_cf = [0, target_cf, display_max_cf]
        tick_labels_cf = ['0', f'Target ({target_cf} %)', f'Scale Limit ({display_max_cf} %)']
    else:
        tick_values_cf = [0, target_cf, display_max_cf]
        tick_labels_cf = ['0', f'Target ({target_cf} %)', f'Max ({display_max_cf} %)']

    # ========================================================
    #                 CALCULATE MEAN BIAS LOGIC
    # ========================================================
    mb_cap = target_error * 3.0
    actual_max_mb = df['Mean bias '].max()
    actual_min_mb = df['Mean bias '].min()

    # NEW: Full Diverging Scale (Blue -> White -> Red)
    # Hard jumps at the +/- target threshold to maintain visual boundaries.
    # The acceptable colors (-Target to +Target) are more saturated/darker to stand out.
    color_scale_mb = [
        [0.0, '#08519c'],           # -3x target (Dark Blue)
        [1/3, '#3182bd'],           # -Target limit (Medium Blue)
        [1/3, '#6baed6'],           # -Target acceptable (Darker Pale Blue)
        [0.5, '#ffffff'],           # 0 Bias (White / Perfect)
        [2/3, '#ffe066'],           # +Target acceptable (Darker Pale Yellow)
        [2/3, '#ffb300'],           # +Target limit (Rich Golden Yellow)
        [1.0, '#cc7a00']            # +3x target (Dark Gold/Ochre)
    ]

    label_min_mb = f'Min ({-mb_cap:.2f}-)' if actual_min_mb < -mb_cap else f'Min ({-mb_cap:.2f})'
    label_max_mb = f'Max ({mb_cap:.2f}+)' if actual_max_mb > mb_cap else f'Max ({mb_cap:.2f})'

    tick_values_mb = [-mb_cap, -target_error, 0, target_error, mb_cap]
    tick_labels_mb = [label_min_mb, f'-Target ({-target_error})', '0', f'+Target ({target_error})', label_max_mb]

    # ========================================================
    #                     BUILD COMBINED FIGURE
    # ========================================================

    # 1. Define custom data arrays to strictly control hover order
    cols_rmse = [
        'RMSE ', 'Target RMSE ', 'Mean bias ', 'Central freq ',
        'CF pass/fail ', 'R ', 'Positive outlier freq ',
        'PO freq pass/fail ', 'Negative outlier freq ', 'NO freq pass/fail '
    ]

    cols_cf = [
        'Central freq ', 'CF pass/fail ', 'RMSE ', 'Target RMSE ',
        'Mean bias ', 'R ', 'Positive outlier freq ',
        'PO freq pass/fail ', 'Negative outlier freq ', 'NO freq pass/fail '
    ]

    cols_mb = [
        'Mean bias ', 'RMSE ', 'Target RMSE ', 'Central freq ',
        'CF pass/fail ', 'R ', 'Positive outlier freq ',
        'PO freq pass/fail ', 'Negative outlier freq ', 'NO freq pass/fail '
    ]

    # Helper function to build a clean, perfectly ordered hover template
    def build_hovertemplate(cols):
        template = '<b>%{hovertext}</b><br><br>'
        for i, col in enumerate(cols):
            label = col.strip() # Cleans up the trailing spaces in your column names
            template += f'{label}: %{{customdata[{i}]}}<br>'
        template += '<extra></extra>' # Hides the secondary trace name box
        return template

    # Build a (colored trace, black outline trace) pair per view.
    # Returns plotly Scattermapbox traces ready to add to the final figure.
    def build_view(color_col, size_col, custom_cols, coloraxis_id):
        fig_tmp = px.scatter_mapbox(
            df, lat='Y ', lon='X ', color=color_col, hover_name='ID ',
            custom_data=custom_cols, size=size_col,
        )
        trace = fig_tmp.data[0]
        trace.marker.coloraxis = coloraxis_id
        trace.hovertemplate = build_hovertemplate(custom_cols)

        outline = px.scatter_mapbox(
            df, lat='Y ', lon='X ', hover_name='ID ', size=size_col,
        ).data[0]
        outline.marker.color = 'black'
        outline.marker.size = trace.marker.size * 1.1
        outline.hovertemplate = None
        outline.hoverinfo = 'skip'
        outline.showlegend = False
        return trace, outline

    rmse_trace, rmse_outline = build_view('RMSE ', 'RMSE ', cols_rmse, 'coloraxis')
    cf_trace, cf_outline = build_view('Central freq ', 'Central freq ', cols_cf, 'coloraxis2')
    mb_trace, mb_outline = build_view('Mean bias ', 'Abs mean bias ', cols_mb, 'coloraxis3')

    # 5. Assemble Final Figure
    fig = go.Figure()

    # Traces 0 & 1: RMSE (Visible by default)
    fig.add_trace(rmse_outline)
    fig.add_trace(rmse_trace)

    # Traces 2 & 3: CF (Hidden by default)
    cf_outline.visible = False
    cf_trace.visible = False
    fig.add_trace(cf_outline)
    fig.add_trace(cf_trace)

    # Traces 4 & 5: Mean Bias (Hidden by default)
    mb_outline.visible = False
    mb_trace.visible = False
    fig.add_trace(mb_outline)
    fig.add_trace(mb_trace)

    # 5. Layout & Dropdown Menus
    fig.update_layout(
        mapbox_style='carto-positron',
        mapbox=dict(zoom=zoom_level, center=dict(lat=center_lat, lon=center_lon)),
        height=map_height,
        width=map_width,
        title=dict(text=plottitle_rmse, font=dict(color='black', size=22), x=0.5),

        # Primary Colorbar (RMSE) - Visible by default
        coloraxis=dict(
            colorscale=color_scale_rmse,
            cmin=0, cmax=display_max_rmse,
            colorbar=dict(
                title=dict(text=f'RMSE ({title_unit})', font=dict(color='black')),
                tickfont=dict(color='black'),
                tickvals=tick_values_rmse,
                ticktext=tick_labels_rmse,
                thickness=20,
                len=0.6
            )
        ),

        # Secondary Colorbar (CF) - Hidden by default
        coloraxis2=dict(
            colorscale=color_scale_cf,
            cmin=0, cmax=display_max_cf,
            showscale=False,
            colorbar=dict(
                title=dict(text='Central frequency (%)', font=dict(color='black')),
                tickfont=dict(color='black'),
                tickvals=tick_values_cf,
                ticktext=tick_labels_cf,
                thickness=20,
                len=0.6
            )
        ),

        # Tertiary Colorbar (Mean Bias) - Hidden by default
        coloraxis3=dict(
            colorscale=color_scale_mb,
            cmin=-mb_cap, cmax=mb_cap, # Bounded entirely between -3x and +3x target
            showscale=False,
            colorbar=dict(
                title=dict(text=f'Mean bias ({title_unit})', font=dict(color='black')),
                tickfont=dict(color='black'),
                tickvals=tick_values_mb,
                ticktext=tick_labels_mb,
                thickness=20,
                len=0.6
            )
        ),

        annotations=[
            dict(
                text='<i>Select mapped statistics from dropdown:</i>',
                x=0.01,
                y=1.01,
                xref='paper',
                yref='paper',
                showarrow=False,
                xanchor='left',
                yanchor='bottom',
                font=dict(color='black', size=13)
            )
        ],

        # The Dropdown Menu
        updatemenus=[
            dict(
                type='dropdown',
                direction='down',
                x=0.01,
                xanchor='left',
                y=0.99,
                yanchor='top',
                buttons=list([
                    dict(
                        label='RMSE view',
                        method='update',
                        args=[
                            {'visible': [True, True, False, False, False, False]},
                            {'title.text': plottitle_rmse,
                             'coloraxis.showscale': True,
                             'coloraxis2.showscale': False,
                             'coloraxis3.showscale': False}
                        ]
                    ),
                    dict(
                        label='Central Frequency view',
                        method='update',
                        args=[
                            {'visible': [False, False, True, True, False, False]},
                            {'title.text': plottitle_cf,
                             'coloraxis.showscale': False,
                             'coloraxis2.showscale': True,
                             'coloraxis3.showscale': False}
                        ]
                    ),
                    dict(
                        label='Mean Bias view',
                        method='update',
                        args=[
                            {'visible': [False, False, False, False, True, True]},
                            {'title.text': plottitle_mb,
                             'coloraxis.showscale': False,
                             'coloraxis2.showscale': False,
                             'coloraxis3.showscale': True}
                        ]
                    )
                ])
            )
        ]
    )

    # 6. Save the single combined file
    savename = prop.ofs + '_' + name_var + '_' + prop.whichcast + '_Skill_Map.html'
    filepath = os.path.join(prop.plotly_maps, savename)

    plotly.offline.plot(fig, filename=filepath, auto_open=False, config={'scrollZoom': True})
    logger.info('Combined interactive map for %s complete', name_var)
