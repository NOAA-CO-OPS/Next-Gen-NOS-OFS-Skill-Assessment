'''
Reads JSON file output from leaflet routine and visualizes as a cartopy plot.
'''
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

fname = 'sfbofs_20240615-00z_sst_model_old.json'
plot_leafletJSON(fname)

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


    # Add a color bar to show the SST scale
    cbar = plt.colorbar(scatter, orientation='vertical', label='SST (°C)',pad=0.1)

    # Set the title
    plt.title(fname)

    # Show the plot
    plt.show()
    fig.savefig(str(fname+'.jpg'))
