"""
Script Name: get_hf_radar.py

Author: RA

This script checks for available HF radar data for a user-provided OFS and
generates mag/dir ASCII files to be used with the frontend. HF radar sources
and OFSes are not hard-coded and just quickly checked to account for any
future HF radar or OFS additions.

    1) Check which HF radar source areas intersect with the provided OFS
        by computing the geographic bounds of both.
    2) Get the u/v data.
        2.5) Average the u/v data for the daily average mode.
    3) Convert it to mag/dir data.
    4) Output ASCII files from NetCDF.
        - For 25-hr daily average, expected file output looks like this:
            {ofs_name}_hfradar_dir_YYYYMMDD.asc
            {ofs_name}_hfradar_dir_YYYYMMDD.prj
            {ofs_name}_hfradar_mag_YYYYMMDD.asc
            {ofs_name}_hfradar_mag_YYYYMMDD.prj
        - For hourly, expected file output looks like this per hour:
            {ofs_name}_hfradar_dir_YYYYMMDD_HH00.asc
            {ofs_name}_hfradar_dir_YYYYMMDD_HH00.prj
            {ofs_name}_hfradar_mag_YYYYMMDD_HH00.asc
            {ofs_name}_hfradar_mag_YYYYMMDD_HH00.prj

HF Radar sources and times available per source (as of March 11, 2026):
    USEGC (US East Coast and Gulf of America)
    USWC (US West Coast)
    GLNA (Great Lakes North America)
    GAK (Gulf of Alaska)

HF Radar sources and corresponding OFSes:
    USEGC -> CBOFS, DBOFS, GOMOFS, NGOFS2, NYOFS, TBOFS
    USWC -> SFBOFS, SSCOFS, WCOFS
    GLNA -> LMHOFS
    GAK -> CIOFS

Example daily average call:
    python ./bin/obs_retrieval/get_hf_radar.py -d 20260310 \\
        -C ./data/observations/ -b ./ofs_extents/sfbofs.shp

Example hourly call:
    python ./bin/obs_retrieval/get_hf_radar.py -d 20260310 \\
        -C ./data/observations/ -b ./ofs_extents/sfbofs.shp \\
        -m hourly -s 2026030900 -e 2026031023
"""
from __future__ import annotations

import argparse
import logging
import logging.config
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio.transform import from_origin
from shapely.geometry import Point, Polygon

from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization.plotting_2d import plot_2d_current_quiver_map
from ofs_skill.visualization.processing_2d import (
    write_2d_array_to_ascii_grid,
    write_2d_arrays_to_json,
)

NRT_DELAY = timedelta(hours=1)
THREDDS_BASE_URL = 'https://dods.ndbc.noaa.gov/thredds/dodsC/hfradar'
NODATA = -9999

logger = None


# -- Intersection detection logic --

def polygons_intersect_2d(poly1, poly2):
    """Check whether two 2D polygons intersect using Shapely."""
    p1 = Polygon(poly1)
    p2 = Polygon(poly2)
    return p1.intersects(p2)


def ensure_clockwise(poly):
    """Return polygon vertices in clockwise winding order."""
    ring = Polygon(poly)
    if not ring.exterior.is_ccw:
        return poly
    return poly[::-1]


def parse_wkt_polygon(wkt_str):
    """Parse a WKT POLYGON string into a list of (lon, lat) tuples."""
    pattern = r'POLYGON\s*\(\(\s*(.+?)\s*\)\)'
    match = re.search(pattern, wkt_str)
    if not match:
        raise ValueError('Invalid WKT POLYGON format.')
    coord_pairs = match.group(1).split(',')
    polygon = []
    for pair in coord_pairs:
        lon, lat = map(float, pair.strip().split())
        polygon.append((lon, lat))

    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])

    return polygon


def get_geospatial_bounds(nc_file):
    """Extract geospatial bounds from a NetCDF dataset as a WKT polygon string."""
    try:
        if isinstance(nc_file, str):
            nc_file = xr.open_dataset(nc_file)

        if 'geospatial_bounds' in nc_file.attrs:
            return nc_file.attrs['geospatial_bounds']

        elif 'bbox' in nc_file.attrs:
            bbox = nc_file.attrs['bbox']
            lon_min, lat_min, lon_max, lat_max = bbox

        else:
            lat_min = nc_file.attrs.get('geospatial_lat_min')
            lat_max = nc_file.attrs.get('geospatial_lat_max')
            lon_min = nc_file.attrs.get('geospatial_lon_min')
            lon_max = nc_file.attrs.get('geospatial_lon_max')

            if None in [lat_min, lat_max, lon_min, lon_max]:
                if 'lat' in nc_file.coords and 'lon' in nc_file.coords:
                    lat_vals = nc_file['lat'].values
                    lon_vals = nc_file['lon'].values
                elif 'latitude' in nc_file.coords and 'longitude' in nc_file.coords:
                    lat_vals = nc_file['latitude'].values
                    lon_vals = nc_file['longitude'].values
                else:
                    raise ValueError(
                        'Cannot determine geospatial bounds: '
                        'no recognized coordinate variables found.'
                    )

                lat_min = float(lat_vals.min())
                lat_max = float(lat_vals.max())
                lon_min = float(lon_vals.min())
                lon_max = float(lon_vals.max())

        wkt_poly = (
            f'POLYGON (({lon_min} {lat_min}, {lon_min} {lat_max}, '
            f'{lon_max} {lat_max}, {lon_max} {lat_min}, {lon_min} {lat_min}))'
        )
        return wkt_poly

    except Exception as e:
        logger.error('Error reading bounds: %s', e)
        return None


def parse_utc_timestamp(timestr):
    """Parse a UTC timestamp string in YYYYMMDDHH format to a datetime object."""
    try:
        return datetime.strptime(timestr, '%Y%m%d%H')
    except Exception as e:
        logger.error("Error parsing timestamp '%s': %s", timestr, e)
        return None


def export_ascii(data_da, outfile):
    """Write a 2D xarray DataArray to an ESRI ASCII Grid (.asc) file.

    Parameters
    ----------
    data_da : xr.DataArray
        2D data array with 'lat' and 'lon' coordinates.
    outfile : Path or str
        Output file path for the .asc file.
    """
    data = data_da.values.copy()

    data = np.where(np.isnan(data), NODATA, data)

    data = data.astype(np.float32)

    lats = data_da['lat'].values
    lons = data_da['lon'].values

    if lats[0] < lats[-1]:
        data = np.flipud(data)
        lats = lats[::-1]

    west = float(lons.min())
    east = float(lons.max())
    south = float(lats.min())
    north = float(lats.max())

    nrows, ncols = data.shape

    dx = (east - west) / (ncols - 1)
    dy = (north - south) / (nrows - 1)

    transform = from_origin(
        lons.min() - dx/2,
        lats.max() + dy/2,
        dx,
        dy
    )

    with rasterio.open(
        outfile,
        'w',
        driver='AAIGrid',
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=rasterio.float32,
        crs='EPSG:4326',
        transform=transform,
        nodata=NODATA
    ) as dst:
        dst.write(data, 1)


def _build_hfradar_output_paths(data_dir, ofs, date_str, timestamp=None):
    """Build output file paths for JSON and ASCII grid txt formats.

    Parameters
    ----------
    data_dir : Path
        Base output directory (e.g. data/observations/).
    ofs : str
        OFS name.
    date_str : str
        Date string in YYYYMMDD format.
    timestamp : str or None
        Hourly timestamp (e.g. '20260315_0000'). If None, builds daily paths.

    Returns
    -------
    dict
        Keys: 'ssu_json', 'ssv_json', 'mag_txt', 'dir_txt'.
        Values: Path objects for each output file.
    """
    tag = timestamp if timestamp else date_str
    obs_2d_dir = Path(data_dir) / '2d'
    obs_2d_dir.mkdir(parents=True, exist_ok=True)
    return {
        'ssu_json': obs_2d_dir / f'{ofs}_{tag}_ssu_hfradar.json',
        'ssv_json': obs_2d_dir / f'{ofs}_{tag}_ssv_hfradar.json',
        'mag_txt': obs_2d_dir / f'{ofs}_mag_{tag}_hfradar.txt',
        'dir_txt': obs_2d_dir / f'{ofs}_dir_{tag}_hfradar.txt',
    }


def _write_leaflet_outputs(u_array, v_array, paths, log,
                           static_plots=False, ofs='', title_tag=''):
    """Write JSON and ASCII grid txt files from u/v xarray DataArrays.

    Parameters
    ----------
    u_array : xr.DataArray
        2D u-component data with lat/lon coordinates.
    v_array : xr.DataArray
        2D v-component data with lat/lon coordinates.
    paths : dict
        Output paths from _build_hfradar_output_paths().
    log : logging.Logger
        Logger instance.
    static_plots : bool
        If True, also generate a static cartopy PNG quiver map.
    ofs : str
        OFS name (used for plot titles).
    title_tag : str
        Date/time label for the plot title.
    """
    lats = u_array['lat'].values
    lons = u_array['lon'].values
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    u_values = np.where(np.isfinite(u_array.values), u_array.values, np.nan)
    v_values = np.where(np.isfinite(v_array.values), v_array.values, np.nan)

    # Write u/v JSON files for Leaflet vector display
    log.info('Writing HF radar u JSON to: %s', paths['ssu_json'])
    write_2d_arrays_to_json(
        lat_grid, lon_grid,
        np.round(u_values, decimals=4),
        str(paths['ssu_json']),
    )

    log.info('Writing HF radar v JSON to: %s', paths['ssv_json'])
    write_2d_arrays_to_json(
        lat_grid, lon_grid,
        np.round(v_values, decimals=4),
        str(paths['ssv_json']),
    )

    # Compute magnitude and direction (oceanographic: CW from north)
    magnitude = np.sqrt(u_values**2 + v_values**2)
    direction = np.degrees(np.arctan2(u_values, v_values)) % 360

    # Write ASCII grid txt files for mag/dir
    log.info('Writing HF radar magnitude txt to: %s', paths['mag_txt'])
    write_2d_array_to_ascii_grid(
        np.round(magnitude, decimals=4),
        lon_grid, lat_grid,
        str(paths['mag_txt']),
    )

    log.info('Writing HF radar direction txt to: %s', paths['dir_txt'])
    write_2d_array_to_ascii_grid(
        np.round(direction, decimals=1),
        lon_grid, lat_grid,
        str(paths['dir_txt']),
    )

    # Generate static cartopy quiver map if enabled
    if static_plots:
        png_path = paths['ssu_json'].parent / (
            paths['ssu_json'].stem.replace('_ssu_hfradar', '_hfradar_currents')
            + '.png'
        )
        title = f'{ofs.upper()} HF Radar Currents - {title_tag}'
        plot_2d_current_quiver_map(
            lon_grid, lat_grid, u_values, v_values,
            title, str(png_path), log,
        )


def clip_by_ofs(ds, gdf):
    """Mask a dataset to the OFS shapefile boundary using geopandas spatial join.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset with lat/lon coordinates.
    gdf : gpd.GeoDataFrame
        GeoDataFrame representing the OFS boundary.

    Returns
    -------
    xr.DataArray
        Boolean mask where True indicates points within the OFS boundary.
    """
    lat_name = 'lat' if 'lat' in ds.coords else 'latitude'
    lon_name = 'lon' if 'lon' in ds.coords else 'longitude'

    if lon_name == 'lon':
        lon = ds['lon'].values
        lat = ds['lat'].values
    else:
        lon = ds['longitude'].values
        lat = ds['latitude'].values

    lon2d, lat2d = np.meshgrid(lon, lat)

    points = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(lon2d.flatten(), lat2d.flatten())],
        crs='EPSG:4326'
    )

    points_in = gpd.sjoin(points, gdf, predicate='within', how='inner')

    mask = np.zeros(lon2d.size, dtype=bool)
    mask[points_in.index] = True
    mask = mask.reshape(lon2d.shape)

    mask_da = xr.DataArray(
        mask,
        coords=[ds[lat_name], ds[lon_name]],
        dims=[lat_name, lon_name]
    )

    return mask_da


def wrap_lon(lon):
    """Normalize longitude to the range [-180, 180)."""
    return ((lon + 180) % 360) - 180


def check_for_overlap(
    date_obj,
    data_dir,
    gdf,
    ofs,
    mode,
    start_time=None,
    end_time=None,
    static_plots=False,
):
    """Check which HF radar sources intersect with an OFS boundary and process them.

    HF radar source abbreviations:
        usegc: US East Coast and Gulf of America
        uswc:  US West Coast
        glna:  Great Lakes North America
        ushi:  US Hawaii
        gak:   Gulf of Alaska
        prvi:  Puerto Rico/Virgin Islands

    Parameters
    ----------
    date_obj : datetime
        Target date for data collection.
    data_dir : Path
        Output directory for ASCII files.
    gdf : gpd.GeoDataFrame
        OFS boundary geometry.
    ofs : str
        OFS name (e.g. 'sfbofs').
    mode : str
        'daily' or 'hourly'.
    start_time : datetime, optional
        Start time for hourly mode.
    end_time : datetime, optional
        End time for hourly mode.
    """
    hf_datasets = ['usegc', 'uswc', 'glna', 'ushi', 'gak', 'prvi']

    lon_min, lat_min, lon_max, lat_max = gdf.total_bounds
    study_area = [
        (lon_min, lat_min),
        (lon_min, lat_max),
        (lon_max, lat_max),
        (lon_max, lat_min),
        (lon_min, lat_min),
    ]
    study_area = ensure_clockwise(study_area)
    logger.info('Made study area')

    matching_files = {}

    logger.info('Starting check for overlap')
    for hf_source in hf_datasets:
        url = f'{THREDDS_BASE_URL}_{hf_source}_6km'

        try:
            ds = xr.open_dataset(url, decode_cf=True, mask_and_scale=True)
        except Exception as e:
            logger.error('Failed to open %s: %s', url, e)
            continue

        logger.info('Checking for and normalizing formatting')
        geospatial_bounds = get_geospatial_bounds(ds)

        if not geospatial_bounds:
            continue

        file_polygon = parse_wkt_polygon(geospatial_bounds)
        file_polygon = [(wrap_lon(lon), lat) for lon, lat in file_polygon]
        file_polygon = ensure_clockwise(file_polygon)

        if polygons_intersect_2d(study_area, file_polygon):
            matching_files[hf_source] = ds
            logger.info("Added HF radar source '%s' with overlap of study area", hf_source)

    process_files(matching_files, date_obj, data_dir, gdf, ofs, mode,
                  start_time, end_time, static_plots=static_plots)


def process_files(
    matching_files,
    date_obj,
    data_dir,
    gdf,
    ofs,
    mode,
    start_time=None,
    end_time=None,
    static_plots=False,
):
    """Process HF radar data producing both hourly and daily-averaged outputs.

    For each HF radar source, produces:
    - Hourly .asc, .json, and .txt files for each timestep
    - Daily-averaged .asc, .json, and .txt files (requires >=13 valid hours)
    - Static cartopy PNG quiver maps (if static_plots is True)

    Parameters
    ----------
    matching_files : dict
        Mapping of HF radar source name to opened xarray Dataset.
    date_obj : datetime
        Target date.
    data_dir : Path
        Output directory.
    gdf : gpd.GeoDataFrame
        OFS boundary geometry.
    ofs : str
        OFS name.
    mode : str
        Kept for backward compatibility; both hourly and daily are always produced.
    start_time : datetime, optional
        If provided with end_time, overrides the default 24-hour time window.
    end_time : datetime, optional
        If provided with start_time, overrides the default 24-hour time window.
    static_plots : bool
        If True, generate static cartopy PNG quiver maps alongside JSON/txt.
    """
    today = datetime.now(timezone.utc).date()

    if date_obj.date() == today:
        end_dt = (datetime.now(timezone.utc) - NRT_DELAY).replace(
            minute=0, second=0, microsecond=0
        )
        start_dt = end_dt - timedelta(hours=24)
    else:
        start_dt = datetime.combine(date_obj.date(), datetime.min.time())
        end_dt = start_dt + timedelta(hours=24)

    # Override time window if explicit start/end provided
    if start_time is not None and end_time is not None:
        start_dt = start_time
        end_dt = end_time

    logger.info('Got start time and end time')

    for _, ds in matching_files.items():
        ds_period = ds.sel(time=slice(start_dt, end_dt))

        u_var = 'u' if 'u' in ds_period.variables else 'ssu'
        v_var = 'v' if 'v' in ds_period.variables else 'ssv'

        dtp_u = ds_period[u_var].astype('float64')
        dtp_v = ds_period[v_var].astype('float64')

        mask_dtp = clip_by_ofs(ds_period, gdf)

        u_data = dtp_u.where(mask_dtp)
        v_data = dtp_v.where(mask_dtp)

        if np.isfinite(u_data).sum() == 0:
            continue

        logger.info('Clipped u/v data to study area')

        date_str = date_obj.strftime('%Y%m%d')

        # --- Hourly outputs ---
        logger.info('Starting hourly u/v -> mag/dir file creation')

        for t in range(len(u_data.time)):
            u_hour = u_data.isel(time=t)
            v_hour = v_data.isel(time=t)

            mag = np.sqrt(u_hour**2 + v_hour**2)
            dir_rad = np.arctan2(u_hour, v_hour)
            direction = (np.degrees(dir_rad) + 360) % 360

            timestamp = pd.to_datetime(u_hour.time.values).strftime('%Y%m%d_%H%M')
            mag_outfile = data_dir / f'{ofs}_hfradar_mag_{timestamp}.asc'
            dir_outfile = data_dir / f'{ofs}_hfradar_dir_{timestamp}.asc'

            export_ascii(mag, mag_outfile)
            export_ascii(direction, dir_outfile)

            # Write hourly JSON + ASCII grid txt
            hourly_paths = _build_hfradar_output_paths(
                data_dir, ofs, date_str, timestamp,
            )
            _write_leaflet_outputs(
                u_hour, v_hour, hourly_paths, logger,
                static_plots=static_plots, ofs=ofs, title_tag=timestamp,
            )

            logger.info('Finished writing hourly files for %s', timestamp)

        # --- Daily averaged outputs ---
        # Require at least 13 of 25 hourly observations for a valid
        # daily average (tidal filtering needs sufficient coverage).
        valid_count = u_data.count(dim='time')
        u_avg = u_data.mean(dim='time', skipna=True)
        v_avg = v_data.mean(dim='time', skipna=True)

        u_avg = u_avg.where(valid_count >= 13)
        v_avg = v_avg.where(valid_count >= 13)

        u_avg = u_avg.astype('float64')
        v_avg = v_avg.astype('float64')

        logger.info('Created u and v averages for daily average')

        mag = np.sqrt(u_avg**2 + v_avg**2)
        dir_rad = np.arctan2(u_avg, v_avg)
        direction = (np.degrees(dir_rad) + 360) % 360

        direction = direction.where(np.isfinite(direction))

        mag_outfile = data_dir / f'{ofs}_hfradar_mag_{date_str}.asc'
        dir_outfile = data_dir / f'{ofs}_hfradar_dir_{date_str}.asc'

        logger.info('Translated u/v into mag/dir data')

        export_ascii(mag, mag_outfile)
        export_ascii(direction, dir_outfile)

        # Write daily JSON + ASCII grid txt
        daily_paths = _build_hfradar_output_paths(data_dir, ofs, date_str)
        _write_leaflet_outputs(
            u_avg, v_avg, daily_paths, logger,
            static_plots=static_plots, ofs=ofs,
            title_tag=f'{date_str} Daily Avg',
        )

        logger.info('Finished writing daily average files')

    logger.info('Finished get_hf_radar.py!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='get_hf_radar.py',
        usage='%(prog)s',
        description="Create ASCII output for a given OFS's HF radar data"
    )

    parser.add_argument(
        '-d', '--date',
        required=True,
        help='Date for daily data collection (YYYYMMDD)'
    )

    parser.add_argument(
        '-C', '--catalogue',
        required=True,
        help='File directory to write the output files to'
    )

    parser.add_argument(
        '-b', '--bounds',
        help='Bounds (shapefile path)'
    )

    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file (default: conf/ofs_dps.conf)'
    )

    parser.add_argument(
        '-o', '--ofs',
        required=False,
        help='OFS of interest (used to derive shapefile path if -b not given)'
    )

    parser.add_argument(
        '-m', '--mode',
        choices=['daily', 'hourly'],
        default='daily',
        help='Choose daily or hourly period'
    )

    parser.add_argument(
        '-s', '--start',
        help='Start time for hourly period in format YYYYMMDDHH (UTC)'
    )

    parser.add_argument(
        '-e', '--end',
        help='End time for hourly period in format YYYYMMDDHH (UTC)'
    )

    args = parser.parse_args()

    # Validate that -b or -o is provided
    if args.bounds is None and args.ofs is None:
        parser.error('Either -b/--bounds or -o/--ofs must be provided.')

    # Validate hourly mode requires start and end
    if args.mode == 'hourly' and (args.start is None or args.end is None):
        parser.error('Hourly mode requires both -s/--start and -e/--end.')

    # Set up logging
    config_file = utils.Utils().get_config_file()
    log_config_file = os.path.join(os.getcwd(), 'conf', 'logging.conf')

    if not os.path.isfile(log_config_file):
        print(f'Log config file not found: {log_config_file}', file=sys.stderr)
        sys.exit(-1)
    if not os.path.isfile(config_file):
        print(f'Config file not found: {config_file}', file=sys.stderr)
        sys.exit(-1)

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger('root')
    logger.info('Using config %s', config_file)
    logger.info('Using log config %s', log_config_file)

    date_obj = datetime.strptime(args.date, '%Y%m%d')

    data_dir = Path(args.catalogue)
    data_dir.mkdir(parents=True, exist_ok=True)

    mode = args.mode

    start_time = None
    end_time = None

    if args.start:
        start_time = parse_utc_timestamp(args.start)

    if args.end:
        end_time = parse_utc_timestamp(args.end)

    # Determine OFS name and shapefile path
    if args.bounds is not None:
        ofs = Path(args.bounds).stem
        shapefile_path = Path(args.bounds)
    else:
        ofs = args.ofs
        shapefile_path = Path(f'./ofs_extents/{ofs}.shp')

    if not shapefile_path.exists():
        logger.error('Shapefile not found: %s', shapefile_path)
        sys.exit(-1)

    gdf = gpd.read_file(shapefile_path)

    # Read static_plots setting from config
    conf_settings = utils.Utils().read_config_section('settings', logger)
    static_plots = conf_settings.get(
        'static_plots', 'False',
    ).lower() in ('true', '1', 'yes')

    check_for_overlap(date_obj, data_dir, gdf, ofs, mode, start_time, end_time,
                      static_plots=static_plots)
