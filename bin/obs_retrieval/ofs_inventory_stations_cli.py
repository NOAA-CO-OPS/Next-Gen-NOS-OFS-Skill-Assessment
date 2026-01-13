"""
-*- coding: utf-8 -*-

Documentation for Scripts ofs_inventory_stations.py

Script Name: ofs_inventory_stations.py

Technical Contact(s): Name:  FC

Abstract:

   This script is used to create a final inventory file, by combining
   all individual inventory dataframes
   (T_C, NDBC, USGS...), and removing duplicates.
   Duplicates are removed based on location (lat, and long).
   Stations with the same lat and long
   (2 decimal degree precision). Precedent is given to Tides
   and Currents stations over NDBC, and NDBC over USGS.
   The final inventory is saved as a .csv file under /Control_Files

Language:  Python 3.8

Estimated Execution Time: < 4min

Scripts/Programs Called:
 ofs_geometry(ofs,path)
 --- This is called to create the inputs for the following scripts

 inventory_T_C(lat1,lat2,lon1,lon2)
 --- This is to create the Tides and Currents inventory

 inventory_NDBC(lat1,lat2,lon1,lon2)
 --- This is to create the NDBC inventory

 inventory_USGS(lat1,lat2,lon1,lon2,start_date,end_date)
 --- This is to create the USGS inventory

Usage: python ofs_inventory.py

OFS Inventory

Arguments:
 -h, --help            show this help message and exit
 -o ofs, --ofs OFS     Choose from the list on the ofs_extents/ folder, you
                       can also create your own shapefile, add it top the
                       ofs_extents/ folder and call it here
 -p PATH, --path PATH  Inventary File Path
 -s STARTDATE, --StartDate STARTDATE
                       Start Date
 -e ENDDATE, --EndDate ENDDATE
                       End Date
Output:
Name                 Description
inventory_all_{}.csv This is a simple .csv file that has all stations
                     available (ID, X, Y, Source, Name)
dataset_final        Pandas Dataframe with ID, X, Y, Source, and
                      Name info for all stations withing lat and lon 1 and 2

Author Name:  FC       Creation Date:  06/23/2023

Revisions:
Date          Author     Description
07-20-2023    MK   Modified the scripts to add config, logging,
                         try/except and argparse features

08-10-2023    MK   Modified the scripts to match the PEP-8
                         standard and code best practices
02-28-2024    AJK        Added inventory filter function

Remarks:
      The output from this script is used by for retrieving data.
      Only the data found in the
      dataset_final/inventory_all_{}.csv will be considered for download
      inventory_all_{}.csv can be edited manually if the user wants to
      include extra stations

"""
# Libraries:
import argparse
from pathlib import Path


from ofs_skill.obs_retrieval.ofs_inventory_stations import ofs_inventory_stations


# Execution:
if __name__ == '__main__':
    # Arguments:
    # Parse (optional and required) command line arguments
    parser = argparse.ArgumentParser(
        prog='python ofs_inventory_station.py',
        usage='%(prog)s',
        description='OFS Inventory Station',
    )

    parser.add_argument(
        '-o',
        '--OFS',
        required=True,
        help="""Choose from the list on the ofs_extents/ folder,
        you can also create your own shapefile, add it at the
        ofs_extents/ folder and call it here""",
    )
    parser.add_argument(
        '-p',
        '--Path',
        required=True,
        help='Inventary File path where ofs_extents/ folder is located',
    )
    parser.add_argument(
        '-s',
        '--StartDate',
        required=True,
        help="Start Date: YYYYMMDD e.g. '20230701'",
    )
    parser.add_argument(
        '-e',
        '--EndDate',
        required=True,
        help="End Date: YYYYMMDD e.g. '20230722'",
    )
    parser.add_argument(
        '-so',
        '--Station_Owner',
        required=False,
        default = 'co-ops,ndbc,usgs',
        help="'CO-OPS','NDBC','USGS',", )


    args = parser.parse_args()
    ofs_inventory_stations(
        args.OFS.lower(),
        args.StartDate,
        args.EndDate,
        args.Path,
        args.Station_Owner.lower(),
        None)
