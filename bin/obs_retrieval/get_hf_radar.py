from datetime import datetime, timedelta
from pathlib import Path
import argparse
import os
import re
import numpy as np
import rasterio
from rasterio.transform import from_origin
import xarray as xr
import pandas as pd
import geopandas as gpd
import shapely
import shapely.geometry
import shapely.wkt
from shapely.geometry import Polygon, Point


NRT_DELAY = timedelta(hours=1)


'''
Intersection detection logic
'''
def polygons_intersect_2d(poly1, poly2):
    p1 = Polygon(poly1)
    p2 = Polygon(poly2)
    return p1.intersects(p2)


def ensure_clockwise(poly):
    ring = Polygon(poly)
    if not ring.exterior.is_ccw:
        return poly
    return poly[::-1]


def spherical_point_in_poly(p, poly_xyz, tol=1e-4):
    angle_sum = 0
    for i in range(len(poly_xyz)):
        a = poly_xyz[i]
        b = poly_xyz[(i + 1) % len(poly_xyz)]
        va = normalize(a - p)
        vb = normalize(b - p)
        cross = np.cross(va, vb)
        sin_theta = np.linalg.norm(cross)
        cos_theta = np.dot(va, vb)
        angle = np.arctan2(sin_theta, cos_theta)
        orientation = np.sign(np.dot(p, cross))
        angle_sum += orientation * angle
    return abs(abs(angle_sum) - 2 * np.pi) < tol


def is_point_on_arc(p, a, b, tol=1e-10):
    angle_ab = np.arccos(np.clip(np.dot(a, b), -1, 1))
    angle_ap = np.arccos(np.clip(np.dot(a, p), -1, 1))
    angle_pb = np.arccos(np.clip(np.dot(p, b), -1, 1))
    return abs((angle_ap + angle_pb) - angle_ab) < tol


def normalize(v, eps=1e-12):
    norm = np.linalg.norm(v)
    return v if norm < eps else v / norm


def segments_intersect_gc(a1, a2, b1, b2, tol=1e-10):
    n1 = normalize(np.cross(a1, a2))
    n2 = normalize(np.cross(b1, b2))
    cross = np.cross(n1, n2)
    if np.linalg.norm(cross) < tol:
        return False
    intersect_pts = [normalize(cross), normalize(-cross)]
    for p in intersect_pts:
        if is_point_on_arc(p, a1, a2) and is_point_on_arc(p, b1, b2):
            return True
    return False


def latlon_to_xyz(lat_deg, lon_deg):
    lat = np.radians(lat_deg)
    lon = np.radians(lon_deg)
    x = np.cos(lat) * np.cos(lon)
    y = np.cos(lat) * np.sin(lon)
    z = np.sin(lat)
    return np.array([x, y, z])


def get_geospatial_bounds(nc_file):
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

                lat_min = float(lat_vals.min())
                lat_max = float(lat_vals.max())
                lon_min = float(lon_vals.min())
                lon_max = float(lon_vals.max())

        wkt_poly = (
            f"POLYGON (({lon_min} {lat_min}, {lon_min} {lat_max}, "
            f"{lon_max} {lat_max}, {lon_max} {lat_min}, {lon_min} {lat_min}))"
        )
        return wkt_poly

    except Exception as e:
        print(f"Error reading bounds: {e}")
        return None


def polygons_intersect_spherical(poly1_latlon, poly2_latlon):
    poly1 = [latlon_to_xyz(lat, lon) for lon, lat in poly1_latlon]
    poly2 = [latlon_to_xyz(lat, lon) for lon, lat in poly2_latlon]

    for i in range(len(poly1)):
        a1 = poly1[i]
        a2 = poly1[(i + 1) % len(poly1)]
        for j in range(len(poly2)):
            b1 = poly2[j]
            b2 = poly2[(j + 1) % len(poly2)]
            if segments_intersect_gc(a1, a2, b1, b2):
                return True

    if any(spherical_point_in_poly(p, poly2) for p in poly1) or \
       any(spherical_point_in_poly(p, poly1) for p in poly2):
        return True

    return False


def parse_wkt_polygon(wkt_str):
    pattern = r'POLYGON\s*\(\(\s*(.+?)\s*\)\)'
    match = re.search(pattern, wkt_str)
    if not match:
        raise ValueError("Invalid WKT POLYGON format.")
    coord_pairs = match.group(1).split(',')
    polygon = []
    for pair in coord_pairs:
        lon, lat = map(float, pair.strip().split())
        polygon.append((lon, lat))

    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])

    return polygon


'''
Ensure timestamp is formatted
'''
def parse_utc_timestamp(timestr):
    try:
        return datetime.strptime(timestr, "%Y%m%d%H")
    except Exception as e:
        print(e)


'''
Write data in ASCII format
'''
def export_ascii(data_da, outfile):
    data = data_da.values.copy()

    data = np.where(np.isnan(data), -9999, data)

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
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999
    ) as dst:
        dst.write(data, 1)


'''
Mask by OFS shapefile boundary 
'''
def clip_by_ofs(ds, gdf):
    lat_name = "lat" if "lat" in ds.coords else "latitude"
    lon_name = "lon" if "lon" in ds.coords else "longitude"
    
    if lon_name == "lon":
        lon = ds["lon"].values
        lat = ds["lat"].values
    else:
        lon = ds["longitude"].values
        lat = ds["latitude"].values

    lon2d, lat2d = np.meshgrid(lon, lat)
   
    points = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(lon2d.flatten(), lat2d.flatten())],
        crs="EPSG:4326"
    )
    
    points_in = gpd.sjoin(points, gdf, predicate="within", how="inner")
    
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
    return ((lon + 180) % 360) - 180


'''
Check which HF radar sources intersect or are contained within an OFS boundary
'''
def check_for_overlap(
    date_obj, 
    data_dir, 
    gdf, 
    ofs, 
    mode, 
    start_time=None, 
    end_time=None
):
    """
    usegc: US East Coast and Gulf of America
    uswc: US West Coast
    glna: Great Lakes North America
    ushiL US Hawaii
    akns: Alaska North Slope
    gak: Gulf of Alaska
    prvi: Puerto Rico/Virgin Islands
    """

    #akns never seems to have data
    #hf_datasets = ["usegc", "uswc", "glna", "ushi", "akns", "gak", "prvi"]
    hf_datasets = ["usegc", "uswc", "glna", "ushi", "gak", "prvi"]

    lon_min, lat_min, lon_max, lat_max = gdf.total_bounds
    study_area = [
        (lon_min, lat_min),
        (lon_min, lat_max),
        (lon_max, lat_max),
        (lon_max, lat_min),
        (lon_min, lat_min),
    ]
    study_area = ensure_clockwise(study_area)

    matching_files = {}

    for hfd in hf_datasets:
        url = f"https://dods.ndbc.noaa.gov/thredds/dodsC/hfradar_{hfd}_6km"

        try:
            ds = xr.open_dataset(url)
        except Exception as e:
            print(e)
            continue

        geospatial_bounds = get_geospatial_bounds(ds)

        if not geospatial_bounds:
            continue

        file_polygon = parse_wkt_polygon(geospatial_bounds)
        file_polygon = [(wrap_lon(lon), lat) for lon, lat in file_polygon]
        file_polygon = ensure_clockwise(file_polygon)

        if polygons_intersect_2d(study_area, file_polygon):
            try:
                ds = xr.open_dataset(
                    url,
                    decode_cf=True,
                    mask_and_scale=True
                )

                matching_files[url] = ds

            except Exception as e:
                print(e)

    process_files(matching_files, date_obj, data_dir, gdf, ofs, mode, start_time, end_time)


'''
Set up time extent and process based on daily average or hourly
'''
def process_files(
    matching_files, 
    date_obj, 
    data_dir,
    gdf, 
    ofs, 
    mode, 
    start_time=None, 
    end_time=None
):

    if mode == "daily":
        today = datetime.utcnow().date()

        if date_obj.date() == today:
            et = (datetime.utcnow() - NRT_DELAY).replace(minute=0, second=0, microsecond=0)
            st = et - timedelta(hours=24)

        else:
            st = datetime.combine(date_obj.date(), datetime.min.time())
            et = st + timedelta(hours=24)

    elif mode == "hourly":
        if start_time is not None and end_time is not None:
            et = end_time
            st = start_time


    for url, ds in matching_files.items():
        #print("TIME RANGE IN FILE:", ds.time.min().values, ds.time.max().values)
        #print("REQUESTED:", st, et)
        
        dtp = ds.sel(time=slice(st, et))

        u_var = "u" if "u" in dtp.variables else "ssu"
        v_var = "v" if "v" in dtp.variables else "ssv"

        dtp_u = dtp[u_var].astype("float64")
        dtp_v = dtp[v_var].astype("float64")

        mask_dtp = clip_by_ofs(dtp, gdf)

        u_data = dtp_u.where(mask_dtp)
        v_data = dtp_v.where(mask_dtp)

        if np.isfinite(u_data).sum() == 0:
            continue

        if mode == "daily":
            u_avg = u_data.mean(dim="time", skipna=True)
            v_avg = v_data.mean(dim="time", skipna=True)

            u_avg = u_avg.where(np.isfinite(u_avg))
            v_avg = v_avg.where(np.isfinite(v_avg))

            u_avg = u_avg.astype("float64")
            v_avg = v_avg.astype("float64")

            mag = np.sqrt(u_avg**2 + v_avg**2)
            dir_rad = np.arctan2(u_avg, v_avg)
            direction = (np.degrees(dir_rad) + 360) % 360

            direction = direction.where(np.isfinite(direction))

            mag_outfile = data_dir / f"{ofs}_hfradar_mag_{date_obj.strftime('%Y%m%d')}.asc"
            dir_outfile = data_dir / f"{ofs}_hfradar_dir_{date_obj.strftime('%Y%m%d')}.asc"

            export_ascii(mag, mag_outfile)
            export_ascii(direction, dir_outfile)

        elif mode == "hourly":
            for t in range(len(u_data.time)):
                u_hour = u_data.isel(time=t)
                v_hour = v_data.isel(time=t)

                mag = np.sqrt(u_hour**2 + v_hour**2)
                dir_rad = np.arctan2(u_hour, v_hour)
                direction = (np.degrees(dir_rad) + 360) % 360

                timestamp = pd.to_datetime(u_hour.time.values).strftime("%Y%m%d_%H%M")
                mag_outfile = data_dir / f"{ofs}_hfradar_mag_{timestamp}.asc"
                dir_outfile = data_dir / f"{ofs}_hfradar_dir_{timestamp}.asc"

                export_ascii(mag, mag_outfile)
                export_ascii(direction, dir_outfile)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("my_date", help="Date for data collection")
    parser.add_argument("catalogue", help="File directory to write the files to")
    parser.add_argument("my_box", help="Bounding box (shapefile)")
    parser.add_argument("ofs", help="OFS of interest")

    parser.add_argument("--mode", choices=["daily", "hourly"], default="daily")
    parser.add_argument("--start", help="Start time YYYYMMDDHH (UTC)")
    parser.add_argument("--end", help="End time YYYYMMDDHH (UTC)")


    args = parser.parse_args()

    date_obj = datetime.strptime(args.my_date, "%Y%m%d")

    data_dir = Path(args.catalogue)

    mode = args.mode

    start_time = None
    end_time = None

    if args.start:
        start_time = parse_utc_timestamp(args.start)

    if args.end:
        end_time = parse_utc_timestamp(args.end)

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    if os.path.exists(Path(args.my_box)):
        gdf = gpd.read_file(Path(args.my_box))
        
        #print("OFS bounds:", gdf.total_bounds)
        
        bounds = gdf.total_bounds
        lon_min, lat_min, lon_max, lat_max = bounds

        if mode == "daily":
            check_for_overlap(date_obj, data_dir, gdf, args.ofs, mode)

        elif mode == "hourly":
            check_for_overlap(date_obj, data_dir, gdf, args.ofs, mode, start_time, end_time)


