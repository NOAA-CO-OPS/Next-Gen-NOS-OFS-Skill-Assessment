#!/usr/bin/env python
"""Plot a ROMS grid from a NetCDF file using cartopy.

Produces a depth-colored map of grid elements (quadrilateral cells built
from psi-point corners) with cartopy coastlines and land overlay.

By default the full domain is plotted.  Use --extent to zoom in, and
--show-edges to draw individual cell edges (useful at regional scales).

Dependencies
------------
cartopy, geopandas (optional, for --from-shapefile), matplotlib, netCDF4, numpy

Examples
--------
  # Full domain, no cell edges
  python plot_romsgrid.py grid.nc grid_overview.png

  # Zoom to Chesapeake Bay with visible cell edges
  python plot_romsgrid.py grid.nc chesapeake.png --extent -77.5 -75.5 36.5 39.5 --show-edges

  # Include land cells
  python plot_romsgrid.py grid.nc full_grid.png --all-cells
"""

import argparse
import time

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
from matplotlib.collections import PolyCollection


def _load_grid(nc_file):
    """Read key arrays from a ROMS grid NetCDF file."""
    ds = nc.Dataset(nc_file, 'r')
    data = {
        'lon_psi': np.ma.filled(ds.variables['lon_psi'][:], np.nan),
        'lat_psi': np.ma.filled(ds.variables['lat_psi'][:], np.nan),
        'lon_rho': np.ma.filled(ds.variables['lon_rho'][:], np.nan),
        'lat_rho': np.ma.filled(ds.variables['lat_rho'][:], np.nan),
        'mask_rho': np.ma.filled(ds.variables['mask_rho'][:], 0),
        'h': np.ma.filled(ds.variables['h'][:], np.nan),
    }
    ds.close()
    return data


def _build_elements(grid, water_only=True):
    """Build polygon vertices, depths, and centre coords for interior cells.

    Returns (verts, depths, centers_lon, centers_lat) where verts has shape
    (n, 4, 2) — four corners per quadrilateral.
    """
    lon_psi = grid['lon_psi']
    lat_psi = grid['lat_psi']
    mask = grid['mask_rho']
    h = grid['h']
    lon_rho = grid['lon_rho']
    lat_rho = grid['lat_rho']

    neta_psi, nxi_psi = lon_psi.shape

    e_range = np.arange(1, neta_psi)
    x_range = np.arange(1, nxi_psi)
    ee, xx = np.meshgrid(e_range, x_range, indexing='ij')
    ee_flat = ee.ravel()
    xx_flat = xx.ravel()

    if water_only:
        keep = mask[ee_flat, xx_flat] == 1
        ee_flat = ee_flat[keep]
        xx_flat = xx_flat[keep]

    n = len(ee_flat)
    verts = np.empty((n, 4, 2), dtype=np.float64)
    verts[:, 0, 0] = lon_psi[ee_flat - 1, xx_flat - 1]
    verts[:, 0, 1] = lat_psi[ee_flat - 1, xx_flat - 1]
    verts[:, 1, 0] = lon_psi[ee_flat - 1, xx_flat]
    verts[:, 1, 1] = lat_psi[ee_flat - 1, xx_flat]
    verts[:, 2, 0] = lon_psi[ee_flat, xx_flat]
    verts[:, 2, 1] = lat_psi[ee_flat, xx_flat]
    verts[:, 3, 0] = lon_psi[ee_flat, xx_flat - 1]
    verts[:, 3, 1] = lat_psi[ee_flat, xx_flat - 1]

    depths = h[ee_flat, xx_flat]
    centers_lon = lon_rho[ee_flat, xx_flat]
    centers_lat = lat_rho[ee_flat, xx_flat]

    return verts, depths, centers_lon, centers_lat


def plot_grid(nc_file, out_file, extent=None, show_edges=False,
              water_only=True, dpi=200, figsize=(16, 12)):
    """Render the grid and save to *out_file*."""
    matplotlib.use('Agg')

    print('Reading grid...')
    grid = _load_grid(nc_file)
    verts, depths, centers_lon, centers_lat = _build_elements(
        grid, water_only=water_only,
    )
    print(f'{len(verts):,} elements')

    # Spatial filter when zoomed in
    if extent is not None:
        lon0, lon1, lat0, lat1 = extent
        buf = 0.5
        in_view = (
            (centers_lon >= lon0 - buf) & (centers_lon <= lon1 + buf)
            & (centers_lat >= lat0 - buf) & (centers_lat <= lat1 + buf)
        )
        verts = verts[in_view]
        depths = depths[in_view]
        print(f'  {len(verts):,} elements in view')

    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': proj})

    norm = mcolors.LogNorm(vmin=max(depths.min(), 1), vmax=depths.max())
    cmap = plt.cm.ocean_r

    edge_kw = {
        'edgecolors': '#333333' if show_edges else 'none',
        'linewidths': 0.3 if show_edges else 0,
    }

    t0 = time.time()
    pc = PolyCollection(
        verts,
        array=depths,
        cmap=cmap,
        norm=norm,
        transform=proj,
        **edge_kw,
    )
    ax.add_collection(pc)

    ax.add_feature(cfeature.LAND, facecolor='#d9d9d9', zorder=2)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5, zorder=3)

    if extent is not None:
        ax.set_extent(extent, crs=proj)
    else:
        ax.set_extent([
            centers_lon.min() - 1, centers_lon.max() + 1,
            centers_lat.min() - 1, centers_lat.max() + 1,
        ], crs=proj)

    gl = ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    gl.top_labels = False
    gl.right_labels = False

    cb = fig.colorbar(pc, ax=ax, orientation='vertical', shrink=0.7, pad=0.02)
    cb.set_label('Depth (m)', fontsize=12)

    title = f'ROMS Grid — {len(verts):,} elements (colored by depth)'
    ax.set_title(title, fontsize=14)

    fig.savefig(out_file, dpi=dpi, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {out_file} ({time.time() - t0:.1f}s)')


def main():
    parser = argparse.ArgumentParser(
        description='Plot a ROMS grid from a NetCDF file using cartopy.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('nc_file', help='Input ROMS grid NetCDF file')
    parser.add_argument('out_file', help='Output image path (e.g. grid.png)')
    parser.add_argument(
        '--extent', nargs=4, type=float, metavar=('LON0', 'LON1', 'LAT0', 'LAT1'),
        help='Map extent: west east south north (decimal degrees)',
    )
    parser.add_argument(
        '--show-edges', action='store_true',
        help='Draw individual cell edges (best for zoomed-in views)',
    )
    parser.add_argument(
        '--all-cells', action='store_true',
        help='Include land cells (default: water only)',
    )
    parser.add_argument('--dpi', type=int, default=200, help='Output DPI (default: 200)')
    parser.add_argument(
        '--figsize', nargs=2, type=float, default=[16, 12], metavar=('W', 'H'),
        help='Figure size in inches (default: 16 12)',
    )

    args = parser.parse_args()
    plot_grid(
        args.nc_file,
        args.out_file,
        extent=args.extent,
        show_edges=args.show_edges,
        water_only=not args.all_cells,
        dpi=args.dpi,
        figsize=tuple(args.figsize),
    )


if __name__ == '__main__':
    main()
