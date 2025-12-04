'''
Reads JSON file output from leaflet routine and visualizes as a cartopy plot.
'''
import argparse

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_leafletJSON(fname):
    data = pd.read_json(fname)

    lats = np.array([np.array(x) for x in data['lats']])
    lons = np.array([np.array(x) for x in data['lons']])
    sst = np.array([np.array(x) for x in data['sst']],dtype=float)

    """Plot the sea surface temperature on a Cartopy map."""
    # Create a new figure with a specified projection (PlateCarree for global maps)
    fig = plt.figure(figsize=(15, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Add coastlines and features for better visualization
    ax.coastlines()
    ax.gridlines(draw_labels=True)
    #ax.add_feature(cfeature.LAKES, linestyle=':')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.STATES)

    # Create a scatter plot with longitude, latitude, and sea surface temperature
    # Use scatter for point data
    scatter = ax.pcolor(lons, lats, sst, cmap='seismic', transform=ccrs.PlateCarree())

    if 'sst' in fname:
        label='SST (°C)'
    elif 'ssh' in fname:
        label='SSH (m)'

    plt.colorbar(scatter, orientation='vertical', label=label,pad=0.1)

    # Set the title
    plt.title(fname)

    fig.savefig(str(fname+'.jpg'))

    # Show the plot
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python plot_leafletJSON.py', usage='%(prog)s',
        description='Make cartopy plot of leaflet JSON file.', )
    parser.add_argument(
        '-f', '--filename', required=True, help="""
        Path to JSON file to plot.""", )

    args = parser.parse_args()
    plot_leafletJSON(args.filename)
