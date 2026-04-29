"""
Create skill assessment maps.

This module makes plotly express maps of skill assessment output. Each
observation station is mapped, with markers color-coded by RMSE between
observations and model. Clicking on a station shows all skill statistics.

Created on Wed Sep 4 14:33:17 2024

@author: PL
"""

import os
import math
from logging import Logger
from typing import Any

import pandas as pd
import plotly
import plotly.express as px


def make_skill_maps(
    output: dict[str, list],
    prop: Any,
    name_var: str,
    logger: Logger,
) -> None:
    """
    Create interactive skill assessment maps.

    Generates a Plotly Express map showing observation stations color-coded
    by RMSE values. Clicking on stations reveals detailed skill statistics.

    Parameters
    ----------
    output : Dict[str, List]
        Dictionary containing skill assessment results with keys:
        - 'station_id': List of station IDs
        - 'node': List of model node IDs
        - 'X': List of longitudes
        - 'Y': List of latitudes
        - 'skill': List of skill metric tuples
    prop : Any
        Properties object containing:
        - path: Base path for configuration
        - ofs: OFS name
        - whichcast: Forecast type
        - start_date_full: Start date string
        - end_date_full: End date string
        - visuals_1d_station_path: Path to save visualizations
    name_var : str
        Variable name ('wl', 'temp', 'salt', or 'cu')
    logger : Logger
        Logger instance for logging messages

    Returns
    -------
    None
        Saves HTML map to visuals_1d_station_path
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

    df['X '] = pd.to_numeric(df['X '])
    df['Y '] = pd.to_numeric(df['Y '])

    # But first, get data sorted out
    datestrend = (prop.end_date_full).split('T')[0]
    datestrbeg = (prop.start_date_full).split('T')[0]
    # Map variable names to display titles
    title_map = {'wl': 'water level', 'salt': 'salinity', 'temp': 'temperature', 'cu': 'current speed'}
    title_var = title_map.get(name_var, name_var)

    plottitle = f"{prop.ofs.upper()} {prop.whichcast} {title_var} skill statistics, {datestrbeg} - {datestrend}"
# 1. Calculate the center coordinates of your data
    center_lat = df['Y '].mean()
    center_lon = df['X '].mean()

    # 2. Calculate the optimal starting zoom level
    lat_diff = df['Y '].max() - df['Y '].min()
    lon_diff = df['X '].max() - df['X '].min()

    if lat_diff == 0 and lon_diff == 0:
        zoom_level = 10 # Default zoom if there's only one station
    else:
        # Base math for calculating mapbox zoom levels.
        # Subtracting ~1.2 provides a nice visual buffer so edge points aren't cut off.
        zoom_lon = math.log2(360 / lon_diff) if lon_diff > 0 else 15
        zoom_lat = math.log2(180 / lat_diff) if lat_diff > 0 else 15
        zoom_level = min(zoom_lon, zoom_lat) - 1.2

    # 3. Create the map with the dynamic center and zoom_level
    fig = px.scatter_mapbox(df, lat='Y ', lon='X ',
                     color='RMSE ',
                     hover_name='ID ',
                     mapbox_style='carto-positron',
                     hover_data=['Target RMSE ', 'Mean bias ', 'R ', 'Central freq ', 'CF pass/fail ',
                                 'Positive outlier freq ', 'PO freq pass/fail ',
                                 'Negative outlier freq ', 'NO freq pass/fail '],
                     size='RMSE ',
                     title=plottitle,
                     height=600,
                     zoom=zoom_level,
                     center=dict(lat=center_lat, lon=center_lon)
                     )
    savename = prop.ofs + '_' + name_var + '_' + prop.whichcast + '_RMSE_map' + '.html'
    filepath = os.path.join(prop.visuals_1d_station_path, savename)
    plotly.offline.plot(fig, filename=filepath, auto_open=False, config={'scrollZoom': True})
    logger.info('Skill map for %s complete', name_var)
