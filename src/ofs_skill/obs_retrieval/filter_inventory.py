"""
Filter station inventory to remove duplicates and apply station list filter.

This module filters the final inventory file to remove duplicate stations
(developed to handle stations duplicated between NDBC and CO-OPS). Precedence
is given to CO-OPS stations. Also filters by user-provided station ID list.
"""

import sys
from logging import Logger

import numpy as np
import pandas as pd


def filter_inventory(
    dataset: pd.DataFrame,
    station_list: list[str],
    logger: Logger
) -> pd.DataFrame:
    """
    Filter station inventory for duplicates and by station list.

    This function removes duplicate stations based on ID matches in the Name
    field (handles cases where NDBC and CO-OPS share station IDs). If a
    station_list is provided, only those stations are retained.

    Args:
        dataset: DataFrame with columns ID, X, Y, Source, Name
        station_list: List of station IDs to filter by (empty list = no filter)
        logger: Logger instance

    Returns:
        Filtered DataFrame with duplicate stations removed

    Raises:
        SystemExit: If station_list provided but no matches found

    Note:
        The Name column is searched for duplicate IDs. When found, the first
        occurrence is kept (CO-OPS has precedence if ordered correctly).
    """
    droplist = []

    for i, id in enumerate(dataset['ID']):
        indx = dataset['Name'].str.find(id)
        if np.max(indx) == 0:
            droplist.append(*np.argwhere(indx == 0)[0])
    dataset_dropped = dataset.drop(dataset.index[droplist])

    # Filter dataset based on input list
    if len(station_list) > 0:
        dataset_dropped = dataset_dropped[
            dataset_dropped['ID'].isin(station_list)
        ]
        # Handle case where either the filtered df is empty
        if dataset_dropped.empty:
            logger.error(
                'No stations from the ofs_dps.conf list were found! '
                'Please update the station ID list and try again, '
                'or re-run without using the "list" option. '
                'Exiting...'
            )
            sys.exit(-1)

        # Report back how many stations in the list matched the DataFrame!
        # Get list of unique IDs from the filtered DataFrame
        ids_in_df = list(dataset_dropped['ID'].unique())
        if len(ids_in_df) == len(station_list):
            logger.info(
                'All station IDs provided in the ofs_dps.conf list '
                'were inventoried! Proceeding to check if '
                'observations are available.'
            )
        else:
            # Find the difference with the original station_list
            list_diff = list(set(station_list) - set(ids_in_df))
            logger.warning(
                'Only some of the station IDs provided in the '
                'ofs_dps.conf list were inventoried. Here are '
                'the stations that were not found/inventoried: %s',
                list_diff
            )
            logger.info(
                'Proceeding with the subset of stations that were '
                'inventoried!'
            )

    return dataset_dropped
