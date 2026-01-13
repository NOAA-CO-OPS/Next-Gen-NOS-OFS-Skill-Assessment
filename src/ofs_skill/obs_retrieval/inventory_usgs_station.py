"""
Create inventory of USGS stream gauge stations.

This module queries the USGS NWIS (National Water Information System)
inventory service to retrieve all water and atmospheric stations within
specified geographic bounds. To handle large areas efficiently, the
region is divided into 1° x 1° grid cells for separate queries.
"""

import math
import urllib.error
import urllib.request
from datetime import datetime
from logging import Logger

import pandas as pd

from ofs_skill.obs_retrieval import utils


def get_inventory(data: list[str], start_dt: datetime) -> pd.DataFrame:
    """
    Parse USGS station inventory from XML data.

    Filters stations to include only those that are:
    - Currently real-time (rt_bol > 0), OR
    - Have been visited more than 3 times, OR
    - Have data after the start year

    Args:
        data: List of XML station elements split from response
        start_dt: Start datetime for filtering stations

    Returns:
        DataFrame with station inventory information
    """
    id_list, lon_list, lat_list, name_list = [], [], [], []

    for i in range(len(data) - 1):
        try:
            line = data[i + 1].split('\n')

            value_line = []
            for i in line:
                value = find_between(i, '>', '<')
                value_line.append(value)

            if (
                int(value_line[9]) > 0
                or int(value_line[10]) > 3
                or int(value_line[11][:4]) > start_dt.year
                or int(value_line[12][:4]) > start_dt.year
            ):
                if (
                    value_line[12] == '--'
                    or int(value_line[12][:4]) > start_dt.year
                ):
                    id_list.append(value_line[2])
                    name_list.append(value_line[3])
                    lat_list.append(value_line[5])
                    lon_list.append(value_line[6])

        except ValueError:
            pass

    inventory_usgs_final = pd.DataFrame(
        {
            'ID': id_list,
            'X': pd.to_numeric(lon_list),
            'Y': pd.to_numeric(lat_list),
            'Source': 'USGS',
            'Name': name_list,
        }
    )
    return inventory_usgs_final


def find_between(data_str: str, first: str, last: str) -> str:
    """
    Extract substring between two delimiter strings.

    This helper function is used to parse USGS XML data.

    Args:
        data_str: String to search
        first: Start delimiter
        last: End delimiter

    Returns:
        Substring between delimiters, or empty string if not found
    """
    try:
        start = data_str.index(first) + len(first)
        end = data_str.index(last, start)
        return data_str[start:end]
    except ValueError:
        return ''


def inventory_usgs_station(
    argu_list: list[float],
    start_date: str,
    end_date: str,
    logger: Logger
) -> pd.DataFrame:
    """
    Create inventory of USGS stations within geographic bounds.

    This function creates an inventory of all atmospheric and water-related
    USGS stations by looping through 1° x 1° grid cells. This approach
    avoids selecting an unnecessary number of stations by filtering to only
    those that are either real-time or have been visited more than 3 times.

    Args:
        argu_list: List of [lat_1, lat_2, lon_1, lon_2]
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format (currently unused)
        logger: Logger instance for logging messages

    Returns:
        DataFrame with columns:
            - ID: Station ID
            - X: Longitude
            - Y: Latitude
            - Source: Data source ('USGS')
            - Name: Station name

    Note:
        The inputs for this function can either be entered manually or
        obtained from ofs_geometry output. This output is used by
        ofs_inventory.py to create the final data inventory.
    """
    lat_1, lat_2, lon_1, lon_2, start_date, end_date = (
        str(argu_list[0]),
        str(argu_list[1]),
        str(argu_list[2]),
        str(argu_list[3]),
        str(start_date),
        str(end_date),
    )

    start_dt = datetime.strptime(start_date, '%Y%m%d')

    url_params = utils.Utils().read_config_section('urls', logger)

    all_data = []
    logger.info('Calling USGS service for inventory...')
    for lat in range(math.floor(float(lat_1)), math.ceil(float(lat_2))):
        lat_min = lat
        lat_max = lat + 1
        for lon in range(math.floor(float(lon_1)), math.ceil(float(lon_2))):
            lon_min = lon
            lon_max = lon + 1

            url = (
                url_params['usgs_nwls_inventory_url']
                + '/inventory?'
                + 'site_tp_cd=OC&site_tp_cd=OC-CO&site_tp_cd=ES&site_tp_cd=LK'
                '&site_tp_cd=ST&'
                + 'site_tp_cd=ST-CA&site_tp_cd=ST-DCH&site_tp_cd=ST-TS&'
                'site_tp_cd=WE&'
                + f'nw_longitude_va={lon_min}&nw_latitude_va={lat_max}'
                + f'&se_longitude_va={lon_max}&se_latitude_va={lat_min}'
                + '&coordinate_format=decimal_degrees&site_md=1&'
                'group_key=NONE&'
                + 'format=sitefile_output&sitefile_output_format=xml&'
                + 'column_name=agency_cd&column_name=site_no&'
                'column_name=station_nm&'
                + 'column_name=site_tp_cd&column_name=dec_lat_va&'
                'column_name=dec_long_va&'
                + 'column_name=rt_bol&column_name=sv_count_nu&'
                + 'list_of_search_criteria=lat_long_bounding_box%2C'
                'site_tp_cd%2Csite_md&'
                + 'column_name=sv_begin_date&column_name=sv_end_date'
            )

            try:
                with urllib.request.urlopen(url) as response:
                    data = response.read().decode('utf-8')
                data = data.split(' <site>')
                inventory_df = get_inventory(data, start_dt)
                all_data.append(inventory_df)
            except urllib.error.HTTPError as ex:
                logger.error(
                    'USGS data download failed at %s -- %s.', url, str(ex)
                )
                continue
            except Exception as ex:
                logger.error(
                    'Error during USGS inventory data retrieval: %s', str(ex)
                )
                continue

    if all_data:
        logger.info('inventory_USGS_station.py run successfully')
        return pd.concat(all_data, ignore_index=True)
    else:
        logger.warning('No USGS station data retrieved.')
        return pd.DataFrame(columns=['ID', 'X', 'Y', 'Source', 'Name'])
