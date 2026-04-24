#!/usr/bin/env python
"""Convert a ROMS grid NetCDF file to ESRI Shapefile.

This utility reads a ROMS (Regional Ocean Modeling System) grid definition
NetCDF file and produces an ESRI Shapefile in one of three output modes.
All output is in WGS 84 / EPSG:4326 geographic coordinates.

Input
-----
The NetCDF file must contain the standard ROMS Arakawa-C grid variables:

    lon_rho, lat_rho   Cell-centre coordinates     (eta_rho x xi_rho)
    lon_psi, lat_psi   Cell-vertex coordinates      (eta_psi x xi_psi)
    mask_rho            Land/water mask (1=water)    (eta_rho x xi_rho)
    h                   Bathymetric depth (metres)   (eta_rho x xi_rho)

Output modes
------------
boundary (default)
    A single polygon that traces the outer perimeter of the rho grid
    (bottom row -> right column -> top row reversed -> left column reversed).
    Useful for clipping other datasets to the model domain.

--elements
    Every interior grid cell emitted as an individual quadrilateral polygon.
    Vertices are taken from the psi grid, which provides the true ROMS cell
    corners (rho points are cell centres, psi points are cell vertices).
    Each feature carries attributes: eta, xi (grid indices), mask, depth (m),
    lon_ctr, lat_ctr (rho-point centre of the cell).

--nodes
    Every rho-point emitted as a point feature.  Attributes: eta, xi, mask,
    depth.

The --water-only flag (applicable to --elements and --nodes) restricts output
to features where mask_rho == 1, which substantially reduces file size for
grids with large land areas.

Dependencies
------------
geopandas, netCDF4, numpy, shapely >= 2.0

Examples
--------
  # Grid perimeter boundary polygon
  python romsgrid_to_shapefile.py grid.nc boundary.shp

  # All water-cell elements as polygons (~400 MB for a 1.7M-cell grid)
  python romsgrid_to_shapefile.py grid.nc elements.shp --elements --water-only

  # All cells including land
  python romsgrid_to_shapefile.py grid.nc elements.shp --elements

  # Water rho-point nodes as points
  python romsgrid_to_shapefile.py grid.nc nodes.shp --nodes --water-only
"""

import argparse
import time

import geopandas as gpd
import netCDF4 as nc
import numpy as np
from shapely import polygons as shapely_polygons
from shapely.geometry import Polygon


def _read_grid(nc_file):
    """Read and return key arrays from a ROMS grid NetCDF file."""
    ds = nc.Dataset(nc_file, 'r')
    data = {
        'lon_rho': np.ma.filled(ds.variables['lon_rho'][:], np.nan),
        'lat_rho': np.ma.filled(ds.variables['lat_rho'][:], np.nan),
        'lon_psi': np.ma.filled(ds.variables['lon_psi'][:], np.nan),
        'lat_psi': np.ma.filled(ds.variables['lat_psi'][:], np.nan),
        'mask_rho': np.ma.filled(ds.variables['mask_rho'][:], 0),
        'h': np.ma.filled(ds.variables['h'][:], np.nan),
    }
    ds.close()
    return data


def write_boundary(grid, shp_file):
    """Write a single polygon tracing the grid perimeter (all 4 edges)."""
    lon = grid['lon_rho']
    lat = grid['lat_rho']

    # Trace the outer perimeter of the rho grid:
    #   bottom row (left->right), right column (bottom->top),
    #   top row (right->left), left column (top->bottom)
    bottom = list(zip(lon[0, :], lat[0, :]))
    right = list(zip(lon[1:, -1], lat[1:, -1]))
    top = list(zip(lon[-1, -2::-1], lat[-1, -2::-1]))
    left = list(zip(lon[-2:0:-1, 0], lat[-2:0:-1, 0]))

    coords = bottom + right + top + left
    coords.append(coords[0])

    poly = Polygon(coords)
    gdf = gpd.GeoDataFrame({'ID': [1]}, geometry=[poly], crs='EPSG:4326')
    gdf.to_file(shp_file)
    print(f'Boundary shapefile: {shp_file}  (1 polygon, {len(coords)} vertices)')


def write_elements(grid, shp_file, water_only=True):
    """Write each interior grid cell as a polygon using psi-point corners.

    In the ROMS Arakawa-C grid, rho-points are cell centres and psi-points
    are cell vertices.  Interior rho cell (e, x) — for e in [1, neta_psi]
    and x in [1, nxi_psi] — has corners at:

        psi[e-1, x-1]  psi[e-1, x]
        psi[e,   x-1]  psi[e,   x]
    """
    lon_psi = grid['lon_psi']
    lat_psi = grid['lat_psi']
    mask = grid['mask_rho']
    h = grid['h']
    lon_rho = grid['lon_rho']
    lat_rho = grid['lat_rho']

    neta_psi, nxi_psi = lon_psi.shape

    # Interior rho indices that have full psi corner coverage:
    # psi[e-1,x-1] .. psi[e,x] must all be valid psi indices
    e_range = np.arange(1, neta_psi)  # 1 .. neta_psi-1
    x_range = np.arange(1, nxi_psi)   # 1 .. nxi_psi-1

    # Build (e, x) index arrays for all interior cells
    ee, xx = np.meshgrid(e_range, x_range, indexing='ij')
    ee_flat = ee.ravel()
    xx_flat = xx.ravel()

    if water_only:
        keep = mask[ee_flat, xx_flat] == 1
        ee_flat = ee_flat[keep]
        xx_flat = xx_flat[keep]

    n = len(ee_flat)
    print(f'Building {n:,} element polygons...')
    t0 = time.time()

    # Vectorised corner coordinates — shape (n, 5, 2) for closed quads
    coords = np.empty((n, 5, 2), dtype=np.float64)
    # Corner 0: psi[e-1, x-1]
    coords[:, 0, 0] = lon_psi[ee_flat - 1, xx_flat - 1]
    coords[:, 0, 1] = lat_psi[ee_flat - 1, xx_flat - 1]
    # Corner 1: psi[e-1, x]
    coords[:, 1, 0] = lon_psi[ee_flat - 1, xx_flat]
    coords[:, 1, 1] = lat_psi[ee_flat - 1, xx_flat]
    # Corner 2: psi[e, x]
    coords[:, 2, 0] = lon_psi[ee_flat, xx_flat]
    coords[:, 2, 1] = lat_psi[ee_flat, xx_flat]
    # Corner 3: psi[e, x-1]
    coords[:, 3, 0] = lon_psi[ee_flat, xx_flat - 1]
    coords[:, 3, 1] = lat_psi[ee_flat, xx_flat - 1]
    # Close the polygon
    coords[:, 4, :] = coords[:, 0, :]

    # Shapely 2.x vectorised polygon creation
    geom = shapely_polygons(coords)

    gdf = gpd.GeoDataFrame(
        {
            'eta': ee_flat.astype(np.int32),
            'xi': xx_flat.astype(np.int32),
            'mask': mask[ee_flat, xx_flat].astype(np.int8),
            'depth': h[ee_flat, xx_flat].astype(np.float32),
            'lon_ctr': lon_rho[ee_flat, xx_flat].astype(np.float32),
            'lat_ctr': lat_rho[ee_flat, xx_flat].astype(np.float32),
        },
        geometry=geom,
        crs='EPSG:4326',
    )

    print(f'  Polygons built in {time.time() - t0:.1f}s — writing shapefile...')
    t1 = time.time()
    gdf.to_file(shp_file)
    print(
        f'Elements shapefile: {shp_file}  '
        f'({n:,} polygons, {time.time() - t1:.1f}s write)'
    )


def write_nodes(grid, shp_file, water_only=True):
    """Write each rho-point as a point feature."""
    lon = grid['lon_rho']
    lat = grid['lat_rho']
    mask = grid['mask_rho']
    h = grid['h']

    neta, nxi = lon.shape
    ee, xx = np.meshgrid(np.arange(neta), np.arange(nxi), indexing='ij')
    ee_flat = ee.ravel()
    xx_flat = xx.ravel()

    if water_only:
        keep = mask[ee_flat, xx_flat] == 1
        ee_flat = ee_flat[keep]
        xx_flat = xx_flat[keep]

    n = len(ee_flat)
    print(f'Building {n:,} node points...')
    t0 = time.time()

    lons = lon[ee_flat, xx_flat]
    lats = lat[ee_flat, xx_flat]

    from shapely import points as shapely_points

    geom = shapely_points(np.column_stack([lons, lats]))

    gdf = gpd.GeoDataFrame(
        {
            'eta': ee_flat.astype(np.int32),
            'xi': xx_flat.astype(np.int32),
            'mask': mask[ee_flat, xx_flat].astype(np.int8),
            'depth': h[ee_flat, xx_flat].astype(np.float32),
        },
        geometry=geom,
        crs='EPSG:4326',
    )

    print(f'  Points built in {time.time() - t0:.1f}s — writing shapefile...')
    t1 = time.time()
    gdf.to_file(shp_file)
    print(
        f'Nodes shapefile: {shp_file}  '
        f'({n:,} points, {time.time() - t1:.1f}s write)'
    )


def main():
    parser = argparse.ArgumentParser(
        description='Convert a ROMS grid NetCDF file to ESRI Shapefile.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('nc_file', help='Input ROMS grid NetCDF file')
    parser.add_argument('shp_file', help='Output shapefile path (.shp)')

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--elements',
        action='store_true',
        help='Write each grid cell as a polygon (default: boundary only)',
    )
    mode.add_argument(
        '--nodes',
        action='store_true',
        help='Write each rho-point as a point feature',
    )

    parser.add_argument(
        '--water-only',
        action='store_true',
        default=False,
        help='Only include water cells (mask_rho=1) for --elements/--nodes',
    )

    args = parser.parse_args()
    grid = _read_grid(args.nc_file)

    if args.elements:
        write_elements(grid, args.shp_file, water_only=args.water_only)
    elif args.nodes:
        write_nodes(grid, args.shp_file, water_only=args.water_only)
    else:
        write_boundary(grid, args.shp_file)


if __name__ == '__main__':
    main()
