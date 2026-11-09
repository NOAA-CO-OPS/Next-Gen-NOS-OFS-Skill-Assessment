from __future__ import annotations

import configparser
import math
import os

from ofs_skill.obs_retrieval import (
    find_nearest_tidal_stations,
    retrieve_tidal_predictions,
    vdatum_resilient,
)
from ofs_skill.obs_retrieval.retrieve_properties import RetrieveProperties
from ofs_skill.obs_retrieval.utils import Utils

# Datums the CO-OPS predictions product can serve. Model-only / GL-only
# references are excluded from the API call and converted after a native
# fetch when needed.
_COOPS_PREDICTION_DATUMS = frozenset({
    'MHHW', 'MHW', 'MTL', 'MSL', 'MLW', 'MLLW', 'NAVD88', 'STND',
})


def _convert_tidal_datum(tidal_data, used_datum, requested_datum, lat, lon, logger):
    """
    Re-references a tidal prediction series from its retrieved datum to the
    requested datum.

    CO-OPS serves no xgeoid20b, so tides are pulled at a native datum and
    converted here. ``vdatum_resilient.convert`` returns ``z' = z + (from_zero -
    to_zero)``, so the value offset ``(z' - dummy_val)`` is ADDED to the tide to
    match the model-side conversion in get_datum_offset.py / get_node_ofs.py.

    Returns:
        tuple: (tidal_data, used_datum). ``tidal_data`` is None if the
        conversion offset is non-finite.
    """
    logger.info('Retrieved datum (%s) for tidal predictions is different '
                'than the requested datum (%s)! Converting...',
                used_datum, requested_datum)
    dummy_val = 10
    used_datum_temp = used_datum
    if used_datum_temp.upper() == 'MSL':
        used_datum_temp = 'lmsl'
    _, _, z = vdatum_resilient.convert(
        used_datum_temp.lower(),
        requested_datum.lower(),
        lat,
        lon,
        dummy_val,  # use dummy value
        epoch=None,
        logger=logger,
        )
    if math.isinf(z):
        return None, used_datum
    tidal_data['TIDE'] = tidal_data['TIDE'] + (float(round(z - dummy_val, 2)))
    return tidal_data, requested_datum


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ('true', '1', 'yes')


def _allow_nearby_tidal_stations(logger) -> bool:
    """Read ``[tides] allow_nearby_tidal_stations``; default False if unset."""
    try:
        config_path = Utils().get_config_file()
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option('tides', 'allow_nearby_tidal_stations'):
            return _parse_bool(
                config.get('tides', 'allow_nearby_tidal_stations'), False)
    except Exception as ex:
        logger.debug(
            'Could not read allow_nearby_tidal_stations; defaulting to false: %s',
            ex)
    return False


def _api_datum_for_request(requested_datum: str) -> str:
    """Return a CO-OPS-servable datum for the predictions API call."""
    datum = str(requested_datum).upper()
    if datum in _COOPS_PREDICTION_DATUMS:
        return datum
    return 'MLLW'


def _station_coords(control_files_path, ofs, obs_station_id, logger):
    """Read lat/lon for ``obs_station_id`` from the water-level ctl file."""
    try:
        ctl_file = os.path.join(
            control_files_path, f'{ofs}_wl_station.ctl')
        with open(ctl_file) as f:
            for line in f:
                if line.strip().startswith(obs_station_id):
                    coords = next(f).split()
                    return float(coords[0]), float(coords[1])
    except Exception as ex:
        logger.warning(
            'Could not find coordinates for station %s: %s',
            obs_station_id, ex)
    return None, None


def get_station_tidal_data(start_dt, end_dt, prop, station_id, logger):
    """
    Retrieve tidal prediction data for a station, optionally from neighbors.

    Tries the observation station first when it is a CO-OPS ID. Each station
    is queried once with a CO-OPS-servable datum (the requested datum when
    supported, otherwise MLLW with conversion). If the API reports no
    predictions, remaining datums for that station are not tried.

    Nearby CO-OPS tide stations are searched only when
    ``[tides] allow_nearby_tidal_stations`` is true in ``ofs_dps.conf``
    (default false).

    Args:
        start_dt: Start of the prediction window.
        end_dt: End of the prediction window.
        prop: Properties with ``datum``, ``ofs``, and ``control_files_path``.
        station_id: Observation station ID string.
        logger: Logger instance.

    Returns:
        tuple: ``(tidal_data, tidal_info)`` where ``tidal_info`` has
        ``tidal_station_id``, ``tidal_station_name``,
        ``tidal_station_distance``, and ``used_datum``.
    """
    retrieve_input = RetrieveProperties()
    obs_station_id = str(station_id)
    station_source = 'CO-OPS' if len(obs_station_id) == 7 else 'USGS/NDBC/CHS'

    retrieve_input.start_date = start_dt.strftime('%Y%m%d%H%M%S')
    retrieve_input.end_date = end_dt.strftime('%Y%m%d%H%M%S')

    requested_datum = prop.datum
    api_datum = _api_datum_for_request(requested_datum)
    allow_nearby = _allow_nearby_tidal_stations(logger)

    def try_get_tidal_data(station_id_to_try):
        # One prediction request per station. A False return means the
        # station rejected the query (no predictions / bad datum); do not
        # walk the rest of datum_list for this ID.
        retrieve_input.station = station_id_to_try
        retrieve_input.datum = api_datum
        data = retrieve_tidal_predictions(retrieve_input, logger)
        if data is False:
            return None, None
        if data is not None and len(data) > 0:
            return data, api_datum
        return None, None

    lat, lon = _station_coords(
        prop.control_files_path, prop.ofs, obs_station_id, logger)

    tidal_data = None
    used_datum = None
    tidal_station_id = None
    tidal_station_name = None
    tidal_station_distance = None

    if station_source.upper() in ['CO-OPS', 'COOPS', 'TC', 'TAC']:
        tidal_data, used_datum = try_get_tidal_data(obs_station_id)
        if tidal_data is not None:
            if used_datum != requested_datum:
                tidal_data, used_datum = _convert_tidal_datum(
                    tidal_data, used_datum, requested_datum, lat, lon, logger)
            if tidal_data is not None:
                tidal_station_id = obs_station_id
                tidal_station_distance = 0.0

    if tidal_data is None and allow_nearby and lat is not None and lon is not None:
        logger.info(
            'Finding nearby tidal stations for %s station %s...',
            station_source, obs_station_id)
        nearby_stations = find_nearest_tidal_stations(
            lat, lon, logger, max_stations=10)

        for candidate_id, candidate_name, candidate_dist in nearby_stations:
            if candidate_id == obs_station_id:
                continue

            logger.info(
                'Trying tidal station %s (%s) at %.1f km...',
                candidate_id, candidate_name, candidate_dist)
            tidal_data, used_datum = try_get_tidal_data(candidate_id)

            if tidal_data is not None:
                if used_datum != requested_datum:
                    tidal_data, used_datum = _convert_tidal_datum(
                        tidal_data, used_datum, requested_datum, lat, lon,
                        logger)
                if tidal_data is None:
                    continue
                tidal_station_id = candidate_id
                tidal_station_name = candidate_name
                tidal_station_distance = candidate_dist
                break
    elif tidal_data is None and not allow_nearby:
        logger.info(
            'Skipping nearby tidal search for %s station %s '
            '(allow_nearby_tidal_stations=false).',
            station_source, obs_station_id)

    tidal_info = {
        'tidal_station_id': tidal_station_id,
        'tidal_station_name': tidal_station_name,
        'tidal_station_distance': tidal_station_distance,
        'used_datum': used_datum,
    }
    return tidal_data, tidal_info
