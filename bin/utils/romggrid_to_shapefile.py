import netCDF4 as nc  # Import NetCDF4 for handling NetCDF files
import numpy as np  # Import NumPy for numerical operations
import shapefile  # Import shapefile for creating and writing Shapefiles
from shapely.geometry import Polygon, MultiPoint, MultiLineString  # Import geometric objects from Shapely
from scipy.spatial import ConvexHull  # Import ConvexHull for computing convex hulls

def roms_grid_to_shapefile(nc_file, shp_file):
    dataset = nc.Dataset(nc_file, 'r')  # Open the NetCDF file in read mode
   
    lon_rho = dataset.variables['lon_rho'][:]  # Extract longitude values
    lat_rho = dataset.variables['lat_rho'][:]  # Extract latitude values
    dataset.close()  # Close the dataset
   
    lon_rho = np.ma.filled(lon_rho, np.nan)  # Replace masked values with NaN
    lat_rho = np.ma.filled(lat_rho, np.nan)  # Replace masked values with NaN
   
    mask = ~np.isnan(lon_rho) & ~np.isnan(lat_rho)  # Create a mask for valid values
    points = np.column_stack((lon_rho[mask], lat_rho[mask]))  # Stack valid longitude and latitude values
   
    if points.shape[0] < 3:
        raise ValueError("Not enough valid points to form a convex hull.")  # Ensure sufficient points for convex hull computation
   
    hull = ConvexHull(points)  # Compute convex hull of valid points
    boundary = [tuple(points[i]) for i in hull.vertices]  # Extract boundary points
    boundary.append(boundary[0])  # Close the polygon by repeating the first point
   
    with shapefile.Writer(shp_file, shapefile.POLYGON) as shp:
        shp.field('ID', 'N')  # Define a numeric ID field
        shp.poly([list(boundary)])  # Write polygon geometry to the shapefile
        shp.record(1)  # Assign an ID to the polygon
   
    prj_content = """GEOGCS["WGS 84",
    DATUM["WGS_1984",
        SPHEROID["WGS 84",6378137,298.257223563,
            AUTHORITY["EPSG","7030"]],
        AUTHORITY["EPSG","6326"]],
    PRIMEM["Greenwich",0,
        AUTHORITY["EPSG","8901"]],
    UNIT["degree",0.0174532925199433,
        AUTHORITY["EPSG","9122"]],
    AUTHORITY["EPSG","4326"]]"""
    
    with open(shp_file.replace('.shp', '.prj'), 'w') as prj:
        prj.write(prj_content)  # Write projection information to a PRJ file
   
    print(f"Shapefile {shp_file} created successfully.")

nc_file = "grid_eccofs_3km_08_b7_wtype.nc"  # Define the input NetCDF file
shp_file = "roms_grid_7.shp"  # Define the output Shapefile name
roms_grid_to_shapefile(nc_file, shp_file)  # Execute the conversion function












