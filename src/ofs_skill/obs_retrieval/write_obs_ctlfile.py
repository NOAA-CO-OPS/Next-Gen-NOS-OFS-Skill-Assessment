"""
-*- coding: utf-8 -*-

Documentation for Scripts write_obs_ctlfile.py

Script Name: write_obs_ctlfile.py

Technical Contact(s):
Name:  FC

Language:  Python 3.8

Estimated Execution Time: >5min, <10min

Author Name:  FC       Creation Date:  06/29/2023

Revisions:
Date          Author             Description
07-20-2023    MK           Modified the scripts to add config,
logging,
                                 try/except and argparse features
08-01-2023    FC   Modified this script to be write control
                                 file ONLY
08-16-2023    MK           Modified the code to match PEP-8 standard.

"""

import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
from coastalmodeling_vdatum import vdatum

from ofs_skill.obs_retrieval import retrieve_properties, utils
from ofs_skill.obs_retrieval.currents_bins_override import (
    BinSpec,
    bin_spec_lookup,
    load_currents_bins_csv,
)
from ofs_skill.obs_retrieval.ofs_inventory_stations import ofs_inventory_stations
from ofs_skill.obs_retrieval.retrieve_chs_station import retrieve_chs_station
from ofs_skill.obs_retrieval.retrieve_ndbc_station import retrieve_ndbc_station
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import (
    retrieve_t_and_c_station,
)
from ofs_skill.obs_retrieval.retrieve_usgs_station import retrieve_usgs_station

_COOPS_MAX_WORKERS = 6
# Currents retrieval now fans out to many per-bin HTTP calls per station;
# keep station-level parallelism low to avoid CO-OPS per-IP rate limiting
# (403/429) that can otherwise drop bins.
_COOPS_CURRENTS_MAX_WORKERS = 2
_NDBC_MAX_WORKERS = 6
_CHS_MAX_WORKERS = 1
_USGS_MAX_WORKERS_WITH_KEY = 4
_USGS_MAX_WORKERS_NO_KEY = 2


def _process_coops_station(id_number, name, x_value, y_value,
                           start_date, end_date, variable, name_var,
                           datum, datum_list, ofs, logger,
                           bin_overrides=None):
    """Process a single CO-OPS station.

    Returns a list of CTL entry strings. For most variables the list
    contains at most one entry; for ``currents`` (ADCPs) one entry is
    emitted per bin using the virtual-ID format ``{parent}_b{NN}``.
    An empty list is returned on failure.

    ``bin_overrides`` is an optional ``dict[int, BinSpec]`` keyed by
    bin number for this parent station. When provided (currents only),
    the function filters the retrieved bins to only those listed and
    applies any per-row overrides (depth / orientation / name). The
    bin set is also pushed into ``retrieve_t_and_c_station`` via
    ``only_bins`` so the CO-OPS datagetter is called only for pinned
    bins.
    """
    try:
        retrieve_input = retrieve_properties.RetrieveProperties()
        retrieve_input.station = str( id_number )
        retrieve_input.start_date = start_date
        retrieve_input.end_date = end_date
        retrieve_input.variable = variable
        retrieve_input.datum = datum
        only_bins = (
            set(bin_overrides.keys())
            if (variable == 'currents' and bin_overrides)
            else None
        )
        timeseries = retrieve_t_and_c_station(
            retrieve_input, logger, only_bins=only_bins)
        if variable == 'water_level':
            if (isinstance(timeseries, pd.DataFrame)
                is False):
                all_datums = ['NAVD','MSL','MLLW',
                              'IGLD','LWD','MHHW',
                              'MHW','MTL','DTL',
                              'MLW', 'STND']
                accepted_datums = datum_list
                for data in range(0, len(all_datums)):
                    logger.info(
                        'Water level data not '
                        'found for station '
                        '%s for %s. '
                        'Trying %s...',
                        str(id_number), datum, all_datums [data]
                        )
                    try:
                        retrieve_input.station = \
                            str(id_number)
                        retrieve_input.start_date =\
                            start_date
                        retrieve_input.end_date =\
                            end_date
                        retrieve_input.variable =\
                            variable
                        retrieve_input.datum =\
                            all_datums [data]
                        timeseries = \
                            retrieve_t_and_c_station(
                                retrieve_input, logger)
                        if ((isinstance(timeseries, pd.DataFrame) is \
                            True) and
                            (all_datums[data] in accepted_datums)):
                            datum_found = \
                                all_datums [data]
                            if str(datum_found) == 'NAVD':
                                datum_found = 'NAVD88'
                            # if str(datum_found) == 'IGLD':
                            #     datum_found = 'IGLD85'
                            logger.info(
                                'Water level data '
                                'found for datum '
                                '%s and '
                                'station '
                                '%s',  all_datums [data],
                                str(id_number)
                                )
                            break
                    except Exception as ex:
                        logger.info(
                            'After trying multiple '
                            'datums, no water '
                            'level data found for '
                            'station %s.',
                            str(id_number)
                            )
                        raise Exception(
                            'Error at water level '
                            'data!'
                            ) from ex
            else:
                datum_found = datum
            if ofs not in [
                    'leofs',
                    'lmhofs',
                    'loofs',
                    'lsofs'
                    ]:
                if (str(datum_found).upper() == datum):
                    zdiff = 0
                elif (str(datum_found).upper() != datum and
                      str(datum_found).upper() in
                      datum_list):
                    ldatum = datum.lower()
                    dummyval = 10
                    _,_,z = vdatum.convert(
                        str(datum_found).lower(),
                        ldatum,
                        y_value,
                        x_value,
                        dummyval, #use dummy value
                        online=True,
                        epoch=None)
                    if math.isinf(z):
                        zdiff = 'RANGE'
                    else:
                        zdiff = round(z-dummyval,2) # datum offset
                else:
                    zdiff = 'UNKNOWN'
            else:
                if datum == 'LWD' and str(datum_found).upper() ==\
                    'IGLD':
                    if ofs == 'leofs':
                        zdiff = -173.5
                    elif ofs == 'lmhofs':
                        zdiff = -176.0
                    elif ofs == 'lsofs':
                        zdiff = -183.2
                    elif ofs == 'loofs':
                        zdiff = -74.2
                elif datum == 'IGLD' and str(datum_found).upper() ==\
                    'LWD':
                    if ofs == 'leofs':
                        zdiff = 173.5
                    elif ofs == 'lmhofs':
                        zdiff = 176.0
                    elif ofs == 'lsofs':
                        zdiff = 183.2
                    elif ofs == 'loofs':
                        zdiff = 74.2
                elif datum == str(datum_found).upper():
                    zdiff = 0 # No correction needed
                else:
                    zdiff = 'UNKNOWN'

        if (variable == 'water_level' and isinstance(
                timeseries, pd.DataFrame
                ) is True):
            logger.info(
                'CO-OPS %s data found '
                'for station %s.', variable,
                str(id_number)
                )
            return [(
                f'{str( id_number )} {str( id_number )}_'
                f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} {zdiff}  0.0  {datum_found}\n'
                )]
        elif (variable in {'water_temperature',
                          'salinity'} and isinstance(
                timeseries, pd.DataFrame) is True
            ):
            logger.info(
                'CO-OPS %s data found for '
                'station %s.', variable,
                str(id_number)
                )
            return [(
                f'{str( id_number )} {str( id_number )}_'
                f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} 0.0  '
                f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                )]
        elif variable == 'currents' and isinstance(timeseries, dict):
            # ``retrieve_t_and_c_station`` returns dict[int, DataFrame]
            # for currents — one DataFrame per ADCP bin. Emit one CTL
            # entry per bin using virtual-ID {parent}_b{NN}.
            #
            # For side-looking (PICS) ADCPs the MDAPI leaves ``depth``
            # null on every bin. We emit a 6th field on the coord line
            # carrying ``height_from_bottom`` so that the model-CTL
            # writer can later resolve an accurate obs depth from the
            # model bathymetry (depth = h - height_from_bottom).
            #
            # When ``bin_overrides`` is provided, only bins listed in
            # the user CSV are emitted; any per-row depth/orientation/
            # name values replace the MDAPI-derived ones. Bins the CSV
            # names but the datagetter did not return are logged.
            if bin_overrides is not None:
                requested = set(bin_overrides.keys())
                available = set(timeseries.keys())
                missing = sorted(requested - available)
                if missing:
                    logger.warning(
                        'Currents bins CSV lists bin(s) %s for station '
                        '%s but the CO-OPS datagetter did not return '
                        'them; skipping those rows.', missing,
                        str(id_number))
                selected_bins = sorted(requested & available)
            else:
                selected_bins = sorted(timeseries.keys())

            entries = []
            for bin_num in selected_bins:
                bin_df = timeseries[bin_num]
                depth = float(bin_df.attrs.get('depth', 0.0) or 0.0)
                hfb_raw = bin_df.attrs.get('height_from_bottom')
                try:
                    hfb = float(hfb_raw) if hfb_raw is not None else 0.0
                except (TypeError, ValueError):
                    hfb = 0.0
                suffix = f'bin {int(bin_num):02d}'

                # Apply CSV overrides for this bin.
                override = (
                    bin_overrides.get(bin_num)
                    if bin_overrides is not None else None
                )
                if override is not None:
                    if override.depth is not None:
                        depth = float(override.depth)
                        # User-specified depth supersedes the
                        # height_from_bottom side-looking path.
                        hfb = 0.0
                    if override.name:
                        suffix = f'bin {int(bin_num):02d} / {override.name}'

                virt_id = f'{str(id_number)}_b{int(bin_num):02d}'
                entries.append(
                    f'{virt_id} {virt_id}_'
                    f'{name_var}_{ofs}_CO-OPS '
                    f'"{name} ({suffix})"\n  '
                    f'{y_value:.3f} {x_value:.3f} 0.0  '
                    f'{depth:.2f}  0.0  {hfb:.2f}\n'
                )
            if bin_overrides is not None:
                logger.info(
                    'CO-OPS currents data found for station %s: '
                    '%d of %d CSV-requested bin(s) emitted.',
                    str(id_number), len(entries), len(bin_overrides))
            else:
                logger.info(
                    'CO-OPS currents data found for station %s: '
                    '%d bin(s) emitted.', str(id_number), len(entries)
                )
            return entries
    except Exception as ex:
        logger.info(
            'CO-OPS %s data not found for '
            'station %s. Exception: %s', variable,
            str(id_number), ex
            )
    return []


def _process_usgs_station(id_number, name, x_value, y_value,
                          start_date, end_date, variable, name_var,
                          datum, ofs, logger):
    """Process a single USGS station.

    Returns a list of CTL entry strings (at most one for USGS). Empty
    list on failure, preserving a uniform return shape with
    ``_process_coops_station``.
    """
    try:
        retrieve_input = retrieve_properties.RetrieveProperties()
        retrieve_input.station = str(id_number)
        retrieve_input.start_date = start_date
        retrieve_input.end_date = end_date
        retrieve_input.variable = variable
        timeseries = retrieve_usgs_station(
            retrieve_input, logger
            )
        if isinstance(timeseries, pd.DataFrame) \
            is False:
            logger.info(
                'USGS %s data not found for '
                'station %s.', variable,
                str(id_number)
                )
        else:
            logger.info(
                'USGS %s data found for '
                'station %s.', variable,
                str(id_number)
                )

            if variable == 'water_level':
                if ofs not in [
                        'leofs',
                        'lmhofs',
                        'loofs',
                        'lsofs'
                        ]:
                    if (str(
                            timeseries['Datum'][1]
                            ).upper() == datum):
                        zdiff = 0
                    elif (str(
                            timeseries ['Datum'][1]
                            ) == 'NAVD88' and
                            datum != 'NAVD88'):
                        ldatum = datum.lower()
                        dummyval = 10
                        _,_,z = vdatum.convert(
                            timeseries['Datum'][1].lower(),
                            ldatum,
                            y_value,
                            x_value,
                            dummyval, #use dummy value
                            online=True,
                            epoch=None)
                        if math.isinf(z):
                            zdiff = 'RANGE'
                        else:
                            zdiff = round(z-dummyval,2) # datum offset
                    elif (str(
                            timeseries['Datum'][1]
                            ) != 'NAVD88'):
                        zdiff = 'UNKNOWN'
                else:
                    if datum == 'LWD':
                        if ofs == 'leofs':
                            zdiff = -173.5
                        elif ofs == 'lmhofs':
                            zdiff = -176.0
                        elif ofs == 'lsofs':
                            zdiff = -183.2
                        elif ofs == 'loofs':
                            zdiff = -74.2
                    elif datum == 'IGLD':
                        zdiff = 0 # No correction needed
                    else:
                        zdiff = 'UNKNOWN'
                logger.info(
                    'There is a datum mismatch between this '
                    'water Level USGS station (%s) and the '
                    'user-specified datum (%s), '
                    'please check control file',timeseries['Datum'][1],
                    datum
                    )
                return [(
                    f'{str( id_number )} '
                    f'{str( id_number )}_{name_var}_'
                    f'{ofs}_USGS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} '
                    f'{zdiff}  0.0  {str(timeseries["Datum"][1])}\n'
                    )]

            elif variable in ['water_temperature' , 'salinity']:
                return [(
                    f'{str( id_number )} {str( id_number )}_'
                    f'{name_var}_{ofs}_USGS "{name}"\n  '
                    f'{y_value:.3f} {x_value:.3f} 0.0  '
                    f'{timeseries ["DEP01"] [1]:.2f}  0.0\n'
                    )]
            elif variable == 'currents':
                return [(
                    f'{str( id_number )} {str( id_number )}_'
                    f'{name_var}_{ofs}_USGS "{name}"\n  '
                    f'{y_value:.3f} {x_value:.3f} 0.0  '
                    f'{timeseries ["DEP01"] [1]:.2f}  0.0  0.00\n'
                    )]
    except Exception as ex:
        logger.info(
            'USGS %s data not found for '
            'station %s. Exception: %s', variable,
            str(id_number), ex
            )
    return []


def _process_ndbc_station(id_number, name, x_value, y_value,
                          start_date, end_date, variable, name_var,
                          datum, ofs, logger):
    """Process a single NDBC station.

    Returns a list of CTL entry strings (at most one). Empty list on
    failure.
    """
    try:
        data_station = retrieve_ndbc_station(
            start_date,
            end_date,
            id_number,
            variable,
            logger
            )

        if data_station is None:
            return []

        logger.info(
            'NDBC %s data found for '
            'station %s.', variable, str(id_number)
            )
        if variable == 'water_level':
            if (str(
                    data_station['Datum'][1]
                    ).upper() == datum):
                zdiff = 0
            elif (str(
                    data_station['Datum'][1]
                    ) == 'MLLW' and
                    datum != 'MLLW'):
                ldatum = datum.lower()
                dummyval = 10
                _,_,z = vdatum.convert(
                    data_station['Datum'][1].lower(),
                    ldatum,
                    y_value,
                    x_value,
                    dummyval, #use dummy value
                    online=True,
                    epoch=None)
                if math.isinf(z):
                    zdiff = 'RANGE'
                else:
                    zdiff = round(z-dummyval,2) # datum offset
            elif (str(
                    data_station['Datum'][1]
                    ) != 'MLLW'):
                zdiff = 'UNKNOWN'

            logger.info(
                'There is a datum mismatch between this '
                'water Level NDBC station (%s) and the '
                'user-specified datum (%s), '
                'please check control file',data_station['Datum'][1],
                datum
                )
            return [(
                f'{str( id_number )} '
                f'{str( id_number )}_{name_var}_'
                f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} '
                f'{zdiff}  0.0  {data_station["Datum"][1]}\n'
                )]

        elif variable in {'water_temperature','salinity'}:
            data_station ['DEP01'] = data_station [
                'DEP01'].astype( float )
            return [(
                f'{str( id_number )} {str( id_number )}_{name_var}_'
                f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} 0.0  '
                f'{data_station ["DEP01"].mean():.2f}  '
                f'0.0\n'
                )]
        elif variable == 'currents':
            data_station ['DEP01'] = data_station[
                'DEP01'].astype(float)
            return [(
                f'{str( id_number )} {str( id_number )}_{name_var}_'
                f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} 0.0  '
                f'{data_station ["DEP01"].mean():.2f}  '
                f'0.0  0.00\n'
                )]
    except Exception as ex:
        logger.info(
            'NDBC %s data not found for '
            'station %s. Exception: %s', variable,
            str(id_number), ex
            )
    return []

def _process_chs_station(id_number, name, x_value, y_value,
                          start_date, end_date, variable, name_var,
                          datum, ofs, logger):
    """Process a single CHS station.

    Returns a list of CTL entry strings (at most one). Empty list on
    failure, preserving a uniform return shape with
    ``_process_coops_station``.
    """
    try:
        data_station = retrieve_chs_station(
            start_date,
            end_date,
            id_number,
            variable,
            logger
            )

        if data_station is None:
            return []

        logger.info(
            'CHS %s data found for '
            'station %s.', variable, str(id_number)
            )

        if variable == 'water_level':
            # CHS IWLS API returns water levels relative to chart datum.
            # For Great Lakes, chart datum IS the Low Water Datum (LWD).
            # The Datum column is labeled 'IGLD' for consistency with the
            # pipeline, but the GL offsets below convert from LWD to IGLD
            # when the user requests IGLD.
            if ofs not in [
                    'leofs', 'lmhofs', 'loofs',
                    'loofs2', 'lsofs']:
                if (str(
                        data_station['Datum'][1]
                        ).upper() == datum):
                    zdiff = 0
                else:
                    ldatum = datum.lower()
                    dummyval = 10
                    _,_,z = vdatum.convert(
                        data_station['Datum'][1].lower(),
                        ldatum,
                        y_value,
                        x_value,
                        dummyval, #use dummy value
                        online=True,
                        epoch=None)
                    if math.isinf(z):
                        zdiff = 'RANGE'
                    else:
                        zdiff = round(z-dummyval,2) # datum offset
            else:
                if datum == 'IGLD':
                    if ofs == 'leofs':
                        zdiff = 173.5
                    elif ofs == 'lmhofs':
                        zdiff = 176.0
                    elif ofs == 'lsofs':
                        zdiff = 183.2
                    elif ofs == 'loofs' or ofs == 'loofs2':
                        zdiff = 74.2
                elif datum == 'LWD':
                    zdiff = 0 # No correction needed
                else:
                    zdiff = 'UNKNOWN'
            return [(
                f'{str( id_number )} '
                f'{str( id_number )}_{name_var}_'
                f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} '
                f'{zdiff}  0.0  {data_station["Datum"][1]}\n'
                )]

        else:
            data_station['DEP01'] = data_station[
                'DEP01'].astype(float)
            # Currents carry an extra 6th field (height_from_bottom) for
            # uniformity with the CO-OPS ADCP per-bin CTL lines. Other
            # variables use the legacy 5-field layout.
            if variable == 'currents':
                return [(
                    f'{str( id_number )} {str( id_number )}_{name_var}_'
                    f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} 0.0  '
                    f'{data_station["DEP01"].mean():.2f}  '
                    f'0.0  0.00\n'
                    )]
            return [(
                f'{str( id_number )} {str( id_number )}_{name_var}_'
                f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                f'{x_value:.3f} 0.0  '
                f'{data_station["DEP01"].mean():.2f}  '
                f'0.0\n'
                )]
    except Exception as ex:
        logger.info(
            'CHS %s data not found for '
            'station %s. Exception: %s', variable,
            str(id_number), ex
            )
    return []

def _process_variable(variable, inventory, var_to_col, start_date, end_date,
                      datum, datum_list, ofs, usgs_max_workers,
                      control_files_path, logger,
                      currents_bins_overrides=None):
    """Process all stations for a single variable. Writes .ctl file.

    ``currents_bins_overrides`` is a ``dict[str, list[BinSpec]]`` from
    :func:`~ofs_skill.obs_retrieval.currents_bins_override.load_currents_bins_csv`.
    Only consulted when ``variable == 'currents'``.
    """
    var_name_map = {
        'water_level': 'wl',
        'water_temperature': 'temp',
        'salinity': 'salt',
        'currents': 'cu',
    }
    name_var = var_name_map[variable]
    logger.info('Making %s station ctl file.', variable)

    # Filter inventory to only stations that have this variable
    var_col = var_to_col.get(variable, None)
    if var_col and var_col in inventory.columns:
        stations_with_var = inventory[inventory[var_col]]
        logger.info(
            'Filtered to %d stations with %s data',
            len(stations_with_var), variable
        )
    else:
        stations_with_var = inventory

    ctl_file = []

    # --- CO-OPS stations (parallel) ---
    coops_stations = stations_with_var.loc[
        stations_with_var['Source'] == 'CO-OPS']
    if not coops_stations.empty:
        coops_workers = (
            _COOPS_CURRENTS_MAX_WORKERS if variable == 'currents'
            else _COOPS_MAX_WORKERS
        )
        futures = []
        with ThreadPoolExecutor(max_workers=coops_workers) as executor:
            for _, row in coops_stations.iterrows():
                # Pull per-station override only for currents; the
                # override map only contains CO-OPS ADCP parent IDs.
                station_overrides = None
                if (variable == 'currents'
                        and currents_bins_overrides):
                    station_overrides = bin_spec_lookup(
                        currents_bins_overrides, str(row['ID']))
                futures.append(executor.submit(
                    _process_coops_station,
                    row['ID'], row['Name'], row['X'], row['Y'],
                    start_date, end_date, variable, name_var,
                    datum, datum_list, ofs, logger,
                    station_overrides,
                ))
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # --- USGS stations (parallel) ---
    usgs_stations = stations_with_var.loc[
        stations_with_var['Source'] == 'USGS']
    if not usgs_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=usgs_max_workers) as executor:
            for _, row in usgs_stations.iterrows():
                futures.append(executor.submit(
                    _process_usgs_station,
                    row['ID'], row['Name'], row['X'], row['Y'],
                    start_date, end_date, variable, name_var,
                    datum, ofs, logger
                ))
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # --- NDBC stations (parallel) ---
    ndbc_stations = stations_with_var.loc[
        stations_with_var['Source'] == 'NDBC']
    if not ndbc_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=_NDBC_MAX_WORKERS) as executor:
            for _, row in ndbc_stations.iterrows():
                futures.append(executor.submit(
                    _process_ndbc_station,
                    row['ID'], row['Name'], row['X'], row['Y'],
                    start_date, end_date, variable, name_var,
                    datum, ofs, logger
                ))
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)
    # --- CHS stations (parallel) ---
    chs_stations = stations_with_var.loc[
        stations_with_var['Source'] == 'CHS']
    if not chs_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=_CHS_MAX_WORKERS) as executor:
            for _, row in chs_stations.iterrows():
                futures.append(executor.submit(
                    _process_chs_station,
                    row['ID'], row['Name'], row['X'], row['Y'],
                    start_date, end_date, variable, name_var,
                    datum, ofs, logger
                ))
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # Write the .ctl file
    try:
        with open(
                r'' + f'{control_files_path}/{ofs}_'
                      f'{name_var}_station.ctl',
                'w', encoding='utf-8'
                ) as output:
            for i in ctl_file:
                output.write(str(i))
            logger.info(
                '%s_%s_station.ctl created '
                'successfully!', ofs, name_var)
    except Exception as ex:
        logger.error(
            'Saving station failed: {ex}. '
            'Please check the directory path: '
            '%s.', control_files_path
            )
        raise Exception('Saving station failed.') from ex


def write_obs_ctlfile(start_date , end_date , datum , path , ofs, stationowner,
                      var_list, logger, currents_bins_csv=None):
    """
    This function calls the Tid_numberes and Currents, NDBC, and USGS
    retrieval
    function in loop for all stations found for the
    ofs_inventory(ofs, start_date, end_date, path) and variables
    ['water_level', 'water_temperature', 'salinity', 'currents'].
    The output is a .ctl file for each variable with all stations that
    have data.

    ``currents_bins_csv`` is an optional path to a user-supplied CSV
    that pins which CO-OPS ADCP bins are processed and/or overrides
    their depth/orientation/name. Schema + behaviour are documented on
    the repo wiki under *CO-OPS ADCP current processing*:
    https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki/CO%E2%80%90OPS-ADCP-current-processing
    """

    start_dt = datetime.strptime( start_date , '%Y%m%d' )
    end_dt = datetime.strptime( end_date , '%Y%m%d' )

    dir_params = utils.Utils().read_config_section( 'directories' , logger )

    # Load the user currents-bins override CSV once (empty dict when no
    # path given or file missing). Passed down to the currents branch
    # of _process_coops_station.
    currents_bins_overrides = load_currents_bins_csv(
        currents_bins_csv, logger)
    datum_list = (utils.Utils().read_config_section('datums', logger)\
                       ['datum_list']).split(' ')

    control_files_path = os.path.join(
        path , dir_params ['control_files_dir']
        )
    os.makedirs( control_files_path , exist_ok=True )

    data_observations_1d_station_path = os.path.join(
        path , dir_params ['data_dir'] , dir_params ['observations_dir'] ,
        dir_params ['1d_station_dir'] , )
    os.makedirs( data_observations_1d_station_path , exist_ok=True )

    # This part of the script will load the inventory file, if the
    # inventory
    # file is not found it will then create a new one by running the
    # ofs_inventory function
    try:
        dtypes = {
            'ID': 'object',
            'X': 'float64',
            'Y': 'float64',
            'Source': 'object',
            'Name': 'object',
            'has_wl': 'bool',
            'has_temp': 'bool',
            'has_salt': 'bool',
            'has_cu': 'bool',
        }
        inventory = pd.read_csv(
            r'' +\
            f'{control_files_path}/inventory_all_{ofs}.csv',
            dtype=dtypes
            )
        # Add default variable columns if not present (backwards compatibility)
        for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
            if col not in inventory.columns:
                inventory[col] = True
        logger.info('Inventory (inventory_all_%s.csv) '
                    'found in %s. '
                    'If you instead want to create a new '
                    'inventory file, change the name or '
                    'delete the current file.', ofs, control_files_path)
    except FileNotFoundError:
        try:
            logger.info(
                'Inventory file not found. '
                'Creating Inventory file!. '
                'This might take a couple of minutes'
                )
            ofs_inventory_stations(
                ofs , start_date , end_date , path, stationowner, logger
                )
            dtypes = {
                'ID': 'object',
                'X': 'float64',
                'Y': 'float64',
                'Source': 'object',
                'Name': 'object',
            }
            inventory = pd.read_csv(
                r'' + f'{control_files_path}/inventory_all_{ofs}.csv',
                dtype=dtypes)
            # Add default variable columns if not present (backwards compatibility)
            for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
                if col not in inventory.columns:
                    inventory[col] = True
                else:
                    if inventory[col].dtype == object:
                        inventory[col] = inventory[col].map(
                            {'True': True, 'False': False, True: True, False: False}
                        ).fillna(True).astype(bool)
                    else:
                        inventory[col] = inventory[col].astype(bool)
            logger.info( 'Inventory file created successfully' )
        except Exception as ex:
            logger.error(
                f'Error when creating inventory files: {ex}'
                )
            raise Exception(
                'Error when creating inventory files'
                ) from ex

    logger.info('Downloading data from the Inventory file!')

    # This outer loop is used to download all data for all variables
    # Insid_numbere this loop there is another loop that will go over each
    # line in the inventory file and will try to download the data
    # from TandC, USGS, and NDBC based on the station ID

    if datum.lower() == 'igld85':
        datum = 'IGLD'
    if datum.lower() == 'navd88':
        datum = 'NAVD'

    # Map variable names to their availability column
    var_to_col = {
        'water_level': 'has_wl',
        'water_temperature': 'has_temp',
        'salinity': 'has_salt',
        'currents': 'has_cu',
    }

    # Determine USGS worker count based on API key availability
    usgs_max_workers = (
        _USGS_MAX_WORKERS_WITH_KEY
        if os.environ.get('API_USGS_PAT')
        else _USGS_MAX_WORKERS_NO_KEY
    )

    with ThreadPoolExecutor(max_workers=len(var_list)) as executor:
        futures = []
        for variable in var_list:
            futures.append(executor.submit(
                _process_variable,
                variable, inventory, var_to_col, start_date, end_date,
                datum, datum_list, ofs, usgs_max_workers,
                control_files_path, logger,
                currents_bins_overrides,
            ))
        # Wait for all variables to complete; re-raise any exceptions
        for future in futures:
            future.result()
