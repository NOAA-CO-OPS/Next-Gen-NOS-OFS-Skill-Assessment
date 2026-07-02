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
07-20-2023    MK           Modified the scripts to add config, logging,
                                 try/except and argparse features
08-01-2023    FC           Modified this script to be write control file ONLY
08-16-2023    MK           Modified the code to match PEP-8 standard.
"""

import math
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd

from ofs_skill.obs_retrieval import retrieve_properties, utils, vdatum_resilient
from ofs_skill.obs_retrieval.chs_utils import (
    CHS_IWLS_BASE_URL,
    CHS_SINE_BASE_URL,
    chs_get,
    is_chs_uuid,
)
from ofs_skill.obs_retrieval.currents_bins_override import (
    bin_spec_lookup,
    load_currents_bins_csv,
)
from ofs_skill.obs_retrieval.ofs_inventory_stations import (
    ofs_inventory_stations,
)
from ofs_skill.obs_retrieval.retrieve_chs_station import retrieve_chs_station
from ofs_skill.obs_retrieval.retrieve_ndbc_station import retrieve_ndbc_station
from ofs_skill.obs_retrieval.retrieve_t_and_c_station import (
    canonicalize_mounting_symbol,
    emit_adcp_mounting_summary,
    reset_run_counters,
    retrieve_t_and_c_station,
)
from ofs_skill.obs_retrieval.retrieve_usgs_station import retrieve_usgs_station

_COOPS_MAX_WORKERS = 6
_COOPS_CURRENTS_MAX_WORKERS = 2
_NDBC_MAX_WORKERS = 6
_CHS_MAX_WORKERS = 1
_USGS_MAX_WORKERS_WITH_KEY = 4
_USGS_MAX_WORKERS_NO_KEY = 2


def _get_chs_code(identifier: str, logger) -> str:
    """Helper to extract the CHS code if given a UUID."""
    if not is_chs_uuid(identifier):
        return identifier  # Already looks like a code

    url = f'{CHS_IWLS_BASE_URL}/stations/{identifier}'
    try:
        response = chs_get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get('code', identifier)
    except Exception as e:
        logger.error('Failed to map CHS UUID %s to code: %s', identifier, e)
    return identifier


def _get_chs_uuid(identifier: str, logger) -> str:
    """Helper to resolve a CHS station code to its UUID."""
    if is_chs_uuid(identifier):
        return identifier

    url = f'{CHS_IWLS_BASE_URL}/stations?code={identifier}'
    try:
        response = chs_get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list):
                return data[0].get('id', identifier)
    except Exception as e:
        logger.error('Failed to map CHS code %s to UUID: %s', identifier, e)
    return identifier


def _extract_chs_metadata(
    station_id: str, logger, target_datum: Optional[str] = None
) -> dict:
    """Retrieves and parses CHS station metadata via explicit schema paths."""
    url = f'{CHS_SINE_BASE_URL}/stations/{station_id}/metadata'
    extracted_data: dict = {'datum': None, 'offset': None}

    try:
        response = chs_get(url, timeout=10)
        response.raise_for_status()
        metadata = response.json()

        if not isinstance(metadata, dict):
            return extracted_data

        # Explicit schema paths where CHS SINE/IWLS exposes datum offsets
        datum_lists = [
            metadata.get('datums', []),
            metadata.get('verticalDatums', []),
            metadata.get('heightTypes', []),
        ]

        target_canonical = (
            _normalize_vdatum_name(target_datum).upper() if target_datum else None
        )

        for d_list in datum_lists:
            if not isinstance(d_list, list):
                continue
            for item in d_list:
                if not isinstance(item, dict):
                    continue

                d_name = (
                    item.get('code')
                    or item.get('datum')
                    or item.get('name')
                    or item.get('datumName')
                )
                if not d_name:
                    continue

                d_norm = _normalize_vdatum_name(str(d_name)).upper()

                # If target_datum specified, match target; otherwise grab first valid offset
                if (
                    target_canonical
                    and target_canonical not in d_norm
                    and d_norm not in target_canonical
                ):
                    continue

                offset_val = item.get('offset')
                if offset_val is None:
                    offset_val = item.get('value') or item.get('conversionOffset')

                if offset_val is not None:
                    try:
                        extracted_data['datum'] = str(d_name)
                        extracted_data['offset'] = float(offset_val)
                        return extracted_data
                    except (ValueError, TypeError):
                        continue

    except Exception as e:
        logger.debug(
            'Failed to retrieve metadata for CHS station %s: %s', station_id, e
        )

    return extracted_data


def _normalize_vdatum_name(name: str) -> str:
    """Canonicalize short datum aliases to the full names expected downstream."""
    if name is None:
        return name
    if str(name).upper() == 'NAVD':
        return 'NAVD88'
    if str(name).upper() == 'IGLD':
        return 'IGLD85'
    return name


def _emit_coops_currents_entries(
    id_number,
    name,
    x_value,
    y_value,
    ofs,
    name_var,
    timeseries,
    bin_overrides,
    logger,
):
    """Emit CTL entries for each ADCP bin returned by retrieve_t_and_c_station."""
    if bin_overrides is not None:
        requested = set(bin_overrides.keys())
        available = set(timeseries.keys())
        missing = sorted(requested - available)
        if missing:
            logger.warning(
                'Currents bins CSV lists bin(s) %s for station %s but the CO-OPS datagetter did not return them; skipping those rows.',
                missing,
                str(id_number),
            )
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
        mounting_type = canonicalize_mounting_symbol(
            bin_df.attrs.get('mounting_type')
        )
        suffix = f'bin {int(bin_num):02d}'
        depth_unknown = bool(bin_df.attrs.get('depth_unknown', False))

        override = (
            bin_overrides.get(bin_num) if bin_overrides is not None else None
        )
        if override is not None:
            if override.depth is not None:
                depth = float(override.depth)
                hfb = 0.0
                depth_unknown = False
            if override.name:
                suffix = f'bin {int(bin_num):02d} / {override.name}'
            if override.orientation:
                mounting_type = canonicalize_mounting_symbol(
                    override.orientation
                )
        if depth_unknown:
            suffix = f'{suffix}, depth unknown'

        virt_id = f'{str(id_number)}_b{int(bin_num):02d}'
        entries.append(
            f'{virt_id} {virt_id}_'
            f'{name_var}_{ofs}_CO-OPS '
            f'"{name} ({suffix})"\n  '
            f'{y_value:.3f} {x_value:.3f} 0.0  '
            f'{depth:.2f}  0.0  {hfb:.2f}  {mounting_type}\n'
        )
    if bin_overrides is not None:
        logger.info(
            'CO-OPS currents data found for station %s: %d of %d CSV-requested bin(s) emitted.',
            str(id_number),
            len(entries),
            len(bin_overrides),
        )
    else:
        logger.info(
            'CO-OPS currents data found for station %s: %d bin(s) emitted.',
            str(id_number),
            len(entries),
        )
    return entries


def _process_coops_station(
    id_number,
    name,
    x_value,
    y_value,
    start_date,
    end_date,
    variable,
    name_var,
    datum,
    datum_list,
    ofs,
    logger,
    bin_overrides=None,
    config_file=None,
    control_files_path=None,
):
    """Process a single CO-OPS station."""
    try:
        retrieve_input = retrieve_properties.RetrieveProperties()
        retrieve_input.station = str(id_number)
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
            retrieve_input,
            logger,
            only_bins=only_bins,
            config_file=config_file,
            control_files_path=control_files_path,
        )
        if variable == 'water_level':
            datum_found = None
            if isinstance(timeseries, pd.DataFrame) is False:
                all_datums = [
                    'NAVD',
                    'MSL',
                    'MLLW',
                    'IGLD',
                    'LWD',
                    'MHHW',
                    'MHW',
                    'MTL',
                    'DTL',
                    'MLW',
                    'STND',
                ]
                accepted_datums = datum_list
                for data in range(0, len(all_datums)):
                    logger.info(
                        'Water level data not found for station %s for %s. Trying %s...',
                        str(id_number),
                        datum,
                        all_datums[data],
                    )
                    try:
                        retrieve_input.station = str(id_number)
                        retrieve_input.start_date = start_date
                        retrieve_input.end_date = end_date
                        retrieve_input.variable = variable
                        retrieve_input.datum = all_datums[data]
                        timeseries = retrieve_t_and_c_station(
                            retrieve_input, logger, config_file=config_file
                        )
                        if (
                            isinstance(timeseries, pd.DataFrame) is True
                        ) and (all_datums[data] in accepted_datums):
                            datum_found = _normalize_vdatum_name(
                                all_datums[data]
                            )
                            logger.info(
                                'Water level data found for datum %s and station %s',
                                all_datums[data],
                                str(id_number),
                            )
                            break
                    except Exception as ex:
                        logger.info(
                            'After trying multiple datums, no water level data found for station %s.',
                            str(id_number),
                        )
                        raise Exception('Error at water level data!') from ex
            else:
                datum_found = _normalize_vdatum_name(datum)
            if not datum_found:
                logger.info(
                    'After trying multiple datums, no water level data found for station %s.',
                    str(id_number),
                )
                return None
            datum_canonical = _normalize_vdatum_name(datum).upper()
            if ofs not in ['leofs', 'lmhofs', 'loofs', 'lsofs', 'loofs2']:
                if str(datum_found).upper() == datum_canonical:
                    zdiff = 0
                elif (
                    str(datum_found).upper() != datum_canonical
                    and str(datum_found).upper() in datum_list
                ):
                    ldatum = _normalize_vdatum_name(datum).lower()
                    dummyval = 10
                    _, _, z = vdatum_resilient.convert(
                        str(datum_found).lower(),
                        ldatum,
                        y_value,
                        x_value,
                        dummyval,
                        epoch=None,
                        station_id=str(id_number),
                        logger=logger,
                    )
                    if math.isinf(z):
                        zdiff = 'RANGE'
                    else:
                        zdiff = round(z - dummyval, 2)
                else:
                    zdiff = 'UNKNOWN'
            else:
                if 'LWD' in datum.upper() and 'IGLD' in str(datum_found).upper():
                    if ofs == 'leofs':
                        zdiff = -173.5
                    elif ofs == 'lmhofs':
                        zdiff = -176.0
                    elif ofs == 'lsofs':
                        zdiff = -183.2
                    elif 'loofs' in ofs:
                        zdiff = -74.2
                elif 'IGLD' in datum.upper() and 'LWD' in str(datum_found).upper():
                    if ofs == 'leofs':
                        zdiff = 173.5
                    elif ofs == 'lmhofs':
                        zdiff = 176.0
                    elif ofs == 'lsofs':
                        zdiff = 183.2
                    elif 'loofs' in ofs:
                        zdiff = 74.2
                elif datum_canonical == str(datum_found).upper():
                    zdiff = 0
                else:
                    zdiff = 'UNKNOWN'

        if (
            variable == 'water_level'
            and isinstance(timeseries, pd.DataFrame) is True
        ):
            logger.info(
                'CO-OPS %s data found for station %s.', variable, str(id_number)
            )
            return [
                (
                    f'{str(id_number)} {str(id_number)}_'
                    f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} {zdiff}  0.0  {datum_found}\n'
                )
            ]
        elif (
            variable in {'water_temperature', 'salinity'}
            and isinstance(timeseries, pd.DataFrame) is True
        ):
            logger.info(
                'CO-OPS %s data found for station %s.', variable, str(id_number)
            )
            return [
                (
                    f'{str(id_number)} {str(id_number)}_'
                    f'{name_var}_{ofs}_CO-OPS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} 0.0  '
                    f'{timeseries["DEP01"][1]:.2f}  0.0\n'
                )
            ]
        elif variable == 'currents' and isinstance(timeseries, dict):
            return _emit_coops_currents_entries(
                id_number,
                name,
                x_value,
                y_value,
                ofs,
                name_var,
                timeseries,
                bin_overrides,
                logger,
            )
    except Exception as ex:
        logger.info(
            'CO-OPS %s data not found for station %s. Exception: %s',
            variable,
            str(id_number),
            ex,
        )
    return []


def _process_usgs_station(
    id_number,
    name,
    x_value,
    y_value,
    start_date,
    end_date,
    variable,
    name_var,
    datum,
    ofs,
    logger,
):
    """Process a single USGS station."""
    try:
        retrieve_input = retrieve_properties.RetrieveProperties()
        retrieve_input.station = str(id_number)
        retrieve_input.start_date = start_date
        retrieve_input.end_date = end_date
        retrieve_input.variable = variable
        timeseries = retrieve_usgs_station(retrieve_input, logger)
        if isinstance(timeseries, pd.DataFrame) is False:
            logger.info(
                'USGS %s data not found for station %s.', variable, str(id_number)
            )
        else:
            logger.info(
                'USGS %s data found for station %s.', variable, str(id_number)
            )

            if variable == 'water_level':
                ts_datum = str(timeseries['Datum'][1])
                ts_datum_upper = ts_datum.upper()

                ofs_base_offsets = {
                    'leofs': 173.5,
                    'lmhofs': 176.0,
                    'lsofs': 183.2,
                    'loofs': 74.2,
                    'loofs2': 74.2,
                }

                if ts_datum_upper == datum or (
                    datum == 'IGLD' and 'IGLD' in ts_datum_upper
                ) or (datum == 'LWD' and 'LWD' in ts_datum_upper):
                    zdiff = 0
                elif datum == 'LWD' and 'IGLD' in ts_datum_upper:
                    offset = ofs_base_offsets.get(ofs)
                    zdiff = -offset if offset is not None else 'UNKNOWN'
                elif datum == 'IGLD' and 'LWD' in ts_datum_upper:
                    zdiff = ofs_base_offsets.get(ofs, 'UNKNOWN')
                elif ts_datum == 'NAVD88' and datum != 'NAVD88':
                    ldatum = _normalize_vdatum_name(datum).lower()
                    dummyval = 10

                    _, _, z = vdatum_resilient.convert(
                        ts_datum.lower(),
                        ldatum,
                        y_value,
                        x_value,
                        dummyval,
                        epoch=None,
                        station_id=str(id_number),
                        logger=logger,
                    )
                    zdiff = 'RANGE' if math.isinf(z) else round((z - dummyval), 2)
                    if 'igld' in datum.lower() and zdiff != 'RANGE':
                        base_offset = ofs_base_offsets.get(ofs)
                        zdiff = (
                            round(zdiff + base_offset, 2)
                            if base_offset is not None
                            else 'UNKNOWN'
                        )
                else:
                    zdiff = 'UNKNOWN'

                if isinstance(zdiff, str):
                    logger.info(
                        'There is a datum mismatch between this water level USGS station (%s) and the user-specified datum (%s), please check control file',
                        ts_datum,
                        datum,
                    )
                return [
                    (
                        f'{str(id_number)} '
                        f'{str(id_number)}_{name_var}_'
                        f'{ofs}_USGS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} '
                        f'{zdiff}  0.0  {str(timeseries["Datum"][1])}\n'
                    )
                ]

            elif variable in ['water_temperature', 'salinity']:
                return [
                    (
                        f'{str(id_number)} {str(id_number)}_'
                        f'{name_var}_{ofs}_USGS "{name}"\n  '
                        f'{y_value:.3f} {x_value:.3f} 0.0  '
                        f'{timeseries["DEP01"][1]:.2f}  0.0\n'
                    )
                ]
            elif variable == 'currents':
                return [
                    (
                        f'{str(id_number)} {str(id_number)}_'
                        f'{name_var}_{ofs}_USGS "{name}"\n  '
                        f'{y_value:.3f} {x_value:.3f} 0.0  '
                        f'{timeseries["DEP01"][1]:.2f}  0.0  0.00\n'
                    )
                ]
    except Exception as ex:
        logger.info(
            'USGS %s data not found for station %s. Exception: %s',
            variable,
            str(id_number),
            ex,
        )
    return []


def _process_ndbc_station(
    id_number,
    name,
    x_value,
    y_value,
    start_date,
    end_date,
    variable,
    name_var,
    datum,
    ofs,
    logger,
):
    """Process a single NDBC station."""
    try:
        data_station = retrieve_ndbc_station(
            start_date, end_date, id_number, variable, logger
        )

        if data_station is None:
            return []

        logger.info('NDBC %s data found for station %s.', variable, str(id_number))
        if variable == 'water_level':
            if str(data_station['Datum'][1]).upper() == datum:
                zdiff = 0
            elif str(data_station['Datum'][1]) == 'MLLW' and datum != 'MLLW':
                ldatum = _normalize_vdatum_name(datum).lower()
                dummyval = 10
                _, _, z = vdatum_resilient.convert(
                    data_station['Datum'][1].lower(),
                    ldatum,
                    y_value,
                    x_value,
                    dummyval,
                    epoch=None,
                    station_id=str(id_number),
                    logger=logger,
                )
                if math.isinf(z):
                    zdiff = 'RANGE'
                else:
                    zdiff = round(z - dummyval, 2)
            elif str(data_station['Datum'][1]) != 'MLLW':
                zdiff = 'UNKNOWN'

            logger.info(
                'There is a datum mismatch between this water Level NDBC station (%s) and the user-specified datum (%s), please check control file',
                data_station['Datum'][1],
                datum,
            )
            return [
                (
                    f'{str(id_number)} '
                    f'{str(id_number)}_{name_var}_'
                    f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} '
                    f'{zdiff}  0.0  {data_station["Datum"][1]}\n'
                )
            ]

        elif variable in {'water_temperature', 'salinity'}:
            data_station['DEP01'] = data_station['DEP01'].astype(float)
            return [
                (
                    f'{str(id_number)} {str(id_number)}_{name_var}_'
                    f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} 0.0  '
                    f'{data_station["DEP01"].mean():.2f}  '
                    f'0.0\n'
                )
            ]
        elif variable == 'currents':
            data_station['DEP01'] = data_station['DEP01'].astype(float)
            return [
                (
                    f'{str(id_number)} {str(id_number)}_{name_var}_'
                    f'{ofs}_NDBC "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} 0.0  '
                    f'{data_station["DEP01"].mean():.2f}  '
                    f'0.0  0.00\n'
                )
            ]
    except Exception as ex:
        logger.info(
            'NDBC %s data not found for station %s. Exception: %s',
            variable,
            str(id_number),
            ex,
        )
    return []


def _process_chs_station(
    id_number,
    name,
    x_value,
    y_value,
    start_date,
    end_date,
    variable,
    name_var,
    datum,
    ofs,
    logger,
):
    """Process a single CHS station."""
    station_code = _get_chs_code(str(id_number), logger)

    try:
        data_station = retrieve_chs_station(
            start_date, end_date, str(id_number), variable, logger
        )

        if data_station is None or data_station.empty:
            return []

        logger.info(
            'CHS %s data found for station %s (Code: %s).',
            variable,
            str(id_number),
            station_code,
        )

        if variable == 'water_level':
            station_uuid = _get_chs_uuid(str(id_number), logger)
            metadata = _extract_chs_metadata(station_uuid, logger, target_datum=datum)
            meta_offset = metadata.get('offset')
            station_datum = str(data_station['Datum'].iloc[0])

            glofs_datums = {
                'leofs': 173.5,
                'lmhofs': 176.0,
                'lsofs': 183.2,
                'loofs': 74.2,
                'loofs2': 74.2,
            }

            if meta_offset is not None:
                expected_offset = glofs_datums.get(ofs)
                if expected_offset is not None:
                    if abs(float(meta_offset) - expected_offset) > 0.05:
                        logger.warning(
                            'Station %s metadata offset (%.2fm) diverges from %s chart datum (%.2fm)',
                            str(id_number),
                            float(meta_offset),
                            ofs,
                            expected_offset,
                        )

            if ofs in glofs_datums:
                if 'IGLD' in datum and meta_offset is not None:
                    zdiff = float(meta_offset)
                elif 'IGLD' in datum:
                    zdiff = glofs_datums[ofs]
                elif datum == 'LWD':
                    zdiff = 0
                else:
                    zdiff = 'UNKNOWN'
            else:
                if station_datum.upper() == datum:
                    zdiff = 0
                else:
                    ldatum = _normalize_vdatum_name(datum).lower()
                    dummyval = 10
                    try:
                        _, _, z = vdatum_resilient.convert(
                            station_datum.lower(),
                            ldatum,
                            y_value,
                            x_value,
                            dummyval,
                            epoch=None,
                            station_id=str(id_number),
                            logger=logger,
                        )
                    except ValueError as exc:
                        # CHS observed water level is referenced to chart
                        # datum (labeled 'IGLD' here). For non-Great-Lakes
                        # OFS there is no datum-conversion path from that
                        # datum to a tidal datum such as MLLW, so the
                        # station cannot be aligned to the model. Skip it
                        # cleanly rather than crashing or mislabeling the
                        # failure as missing data.
                        logger.warning(
                            'CHS station %s skipped: no datum conversion '
                            'path from %s to %s. CHS water level is in chart '
                            'datum and cannot be aligned to the requested '
                            'datum (%s) for this OFS. %s',
                            str(id_number), station_datum.lower(), ldatum,
                            datum, exc)
                        return []
                    zdiff = 'RANGE' if math.isinf(z) else round(z - dummyval, 2)

            return [
                (
                    f'{station_code} '
                    f'{station_code}_{name_var}_'
                    f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} '
                    f'{zdiff}  0.0  {station_datum}\n'
                )
            ]

        else:
            data_station['DEP01'] = data_station['DEP01'].astype(float)

            if variable == 'currents':
                return [
                    (
                        f'{station_code} {station_code}_{name_var}_'
                        f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                        f'{x_value:.3f} 0.0  '
                        f'{data_station["DEP01"].mean():.2f}  '
                        f'0.0  0.00\n'
                    )
                ]
            return [
                (
                    f'{station_code} {station_code}_{name_var}_'
                    f'{ofs}_CHS "{name}"\n  {y_value:.3f} '
                    f'{x_value:.3f} 0.0  '
                    f'{data_station["DEP01"].mean():.2f}  '
                    f'0.0\n'
                )
            ]

    except Exception as ex:
        logger.error(
            'CHS %s data not processed for station %s. Exception: %s',
            variable,
            str(id_number),
            ex,
        )

    return []


def _process_variable(
    variable,
    inventory,
    var_to_col,
    start_date,
    end_date,
    datum,
    datum_list,
    ofs,
    usgs_max_workers,
    control_files_path,
    logger,
    currents_bins_overrides=None,
    config_file=None,
):
    """Process all stations for a single variable. Writes .ctl file."""
    var_name_map = {
        'water_level': 'wl',
        'water_temperature': 'temp',
        'salinity': 'salt',
        'currents': 'cu',
    }
    name_var = var_name_map[variable]
    logger.info('Making %s station ctl file.', variable)

    var_col = var_to_col.get(variable, None)
    if var_col and var_col in inventory.columns:
        stations_with_var = inventory[inventory[var_col]]
        logger.info(
            'Filtered to %d stations with %s data',
            len(stations_with_var),
            variable,
        )
    else:
        stations_with_var = inventory

    ctl_file = []

    # --- CO-OPS stations (parallel) ---
    coops_stations = stations_with_var.loc[
        stations_with_var['Source'] == 'CO-OPS'
    ]
    if not coops_stations.empty:
        coops_workers = (
            _COOPS_CURRENTS_MAX_WORKERS
            if variable == 'currents'
            else _COOPS_MAX_WORKERS
        )
        futures = []
        with ThreadPoolExecutor(max_workers=coops_workers) as executor:
            for _, row in coops_stations.iterrows():
                station_overrides = None
                if variable == 'currents' and currents_bins_overrides:
                    station_overrides = bin_spec_lookup(
                        currents_bins_overrides, str(row['ID'])
                    )
                futures.append(
                    executor.submit(
                        _process_coops_station,
                        row['ID'],
                        row['Name'],
                        row['X'],
                        row['Y'],
                        start_date,
                        end_date,
                        variable,
                        name_var,
                        datum,
                        datum_list,
                        ofs,
                        logger,
                        station_overrides,
                        config_file=config_file,
                        control_files_path=control_files_path,
                    )
                )

            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # --- USGS stations (parallel) ---
    usgs_stations = stations_with_var.loc[stations_with_var['Source'] == 'USGS']
    if not usgs_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=usgs_max_workers) as executor:
            for _, row in usgs_stations.iterrows():
                futures.append(
                    executor.submit(
                        _process_usgs_station,
                        row['ID'],
                        row['Name'],
                        row['X'],
                        row['Y'],
                        start_date,
                        end_date,
                        variable,
                        name_var,
                        datum,
                        ofs,
                        logger,
                    )
                )
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # --- NDBC stations (parallel) ---
    ndbc_stations = stations_with_var.loc[stations_with_var['Source'] == 'NDBC']
    if not ndbc_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=_NDBC_MAX_WORKERS) as executor:
            for _, row in ndbc_stations.iterrows():
                futures.append(
                    executor.submit(
                        _process_ndbc_station,
                        row['ID'],
                        row['Name'],
                        row['X'],
                        row['Y'],
                        start_date,
                        end_date,
                        variable,
                        name_var,
                        datum,
                        ofs,
                        logger,
                    )
                )
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    # --- CHS stations (parallel) ---
    chs_stations = stations_with_var.loc[stations_with_var['Source'] == 'CHS']
    if not chs_stations.empty:
        futures = []
        with ThreadPoolExecutor(max_workers=_CHS_MAX_WORKERS) as executor:
            for _, row in chs_stations.iterrows():
                futures.append(
                    executor.submit(
                        _process_chs_station,
                        row['ID'],
                        row['Name'],
                        row['X'],
                        row['Y'],
                        start_date,
                        end_date,
                        variable,
                        name_var,
                        datum,
                        ofs,
                        logger,
                    )
                )
            for future in futures:
                result = future.result()
                if result:
                    ctl_file.extend(result)

    try:
        with open(
            r'' + f'{control_files_path}/{ofs}_{name_var}_station.ctl',
            'w',
            encoding='utf-8',
        ) as output:
            for i in ctl_file:
                output.write(str(i))
            logger.info(
                '%s_%s_station.ctl created successfully!', ofs, name_var
            )
    except Exception as ex:
        logger.error(
            'Saving station failed: {ex}. Please check the directory path: %s.',
            control_files_path,
        )
        raise Exception('Saving station failed.') from ex


def write_obs_ctlfile(
    start_date,
    end_date,
    datum,
    path,
    ofs,
    stationowner,
    var_list,
    logger,
    currents_bins_csv=None,
    config_file=None,
):
    """Main entry point to loop over inventories and write observation CTL files."""
    dir_params = utils.Utils(config_file).read_config_section('directories', logger)

    currents_bins_overrides = load_currents_bins_csv(currents_bins_csv, logger)
    datum_list = (
        utils.Utils(config_file)
        .read_config_section('datums', logger)['datum_list']
        .split(' ')
    )

    control_files_path = os.path.join(path, dir_params['control_files_dir'])
    os.makedirs(control_files_path, exist_ok=True)

    data_observations_1d_station_path = os.path.join(
        path,
        dir_params['data_dir'],
        dir_params['observations_dir'],
        dir_params['1d_station_dir'],
    )
    os.makedirs(data_observations_1d_station_path, exist_ok=True)

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
            r'' + f'{control_files_path}/inventory_all_{ofs}.csv', dtype=dtypes
        )
        for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
            if col not in inventory.columns:
                inventory[col] = True
        logger.info(
            'Inventory (inventory_all_%s.csv) found in %s.',
            ofs,
            control_files_path,
        )
    except FileNotFoundError:
        try:
            logger.info('Inventory file not found. Creating Inventory file!.')
            ofs_inventory_stations(
                ofs,
                start_date,
                end_date,
                path,
                stationowner,
                logger,
                config_file=config_file,
            )
            dtypes = {
                'ID': 'object',
                'X': 'float64',
                'Y': 'float64',
                'Source': 'object',
                'Name': 'object',
            }
            inventory = pd.read_csv(
                r'' + f'{control_files_path}/inventory_all_{ofs}.csv', dtype=dtypes
            )
            for col in ['has_wl', 'has_temp', 'has_salt', 'has_cu']:
                if col not in inventory.columns:
                    inventory[col] = True
                else:
                    if inventory[col].dtype == object:
                        inventory[col] = (
                            inventory[col]
                            .map({'True': True, 'False': False, True: True, False: False})
                            .fillna(True)
                            .astype(bool)
                        )
                    else:
                        inventory[col] = inventory[col].astype(bool)
            logger.info('Inventory file created successfully')
        except Exception as ex:
            logger.error(f'Error when creating inventory files: {ex}')
            raise Exception('Error when creating inventory files') from ex

    logger.info('Downloading data from the Inventory file!')

    if datum.lower() == 'igld85':
        datum = 'IGLD'
    if datum.lower() == 'navd88':
        datum = 'NAVD'

    var_to_col = {
        'water_level': 'has_wl',
        'water_temperature': 'has_temp',
        'salinity': 'has_salt',
        'currents': 'has_cu',
    }

    usgs_max_workers = (
        _USGS_MAX_WORKERS_WITH_KEY
        if os.environ.get('API_USGS_PAT')
        else _USGS_MAX_WORKERS_NO_KEY
    )

    reset_run_counters()

    with ThreadPoolExecutor(max_workers=len(var_list)) as executor:
        futures = []
        for variable in var_list:
            futures.append(
                executor.submit(
                    _process_variable,
                    variable,
                    inventory,
                    var_to_col,
                    start_date,
                    end_date,
                    datum,
                    datum_list,
                    ofs,
                    usgs_max_workers,
                    control_files_path,
                    logger,
                    currents_bins_overrides,
                    config_file,
                )
            )

        for future in futures:
            future.result()

    emit_adcp_mounting_summary(logger)
