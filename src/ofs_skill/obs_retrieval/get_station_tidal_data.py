from __future__ import annotations

import configparser
from pathlib import Path

import os

from ofs_skill.obs_retrieval import (
    find_nearest_tidal_stations,
    retrieve_tidal_predictions,
)
from ofs_skill.obs_retrieval.retrieve_properties import RetrieveProperties


def get_station_tidal_data(start_dt, end_dt, prop, station_id, logger):
    """
    Retrieves tidal prediction data for a station or its nearest neighbors.

    The function first attempts to pull data for the primary station using the
    requested datum. If that fails, it iterates through a list of fallback datums.
    If data is still not found (or if the station is not a CO-OPS station),
    it identifies the 10 nearest tidal stations via coordinates and repeats the
    search until valid data is retrieved.

    Args:
        obs_df (pandas.DataFrame): Dataframe containing observation data.
            Must include a 'DateTime' column to determine the search window.
        prop (object): Properties object containing configuration settings such as
            'datum', 'ofs', 'control_files_path', and model metadata.
        station_id (list/tuple): A collection where index 0 is the station ID string
            and index 2 is the station source (e.g., 'CO-OPS').
        logger (logging.Logger): Logger instance for tracking errors and info.

    Returns:
        tuple: A five-element tuple containing:
            - tidal_data (pd.DataFrame or None): The retrieved tidal predictions.
            - used_datum (str or None): The specific datum that successfully returned data.
            - tidal_station_id (str or None): ID of the station that provided the data.
            - tidal_station_name (str or None): Name of the fallback station used.
            - tidal_station_distance (float or None): Distance in km from the original station.
    """
    # 1. Initialize variables and inputs
    retrieve_input = RetrieveProperties()
    obs_station_id = str(station_id)
    station_source = 'CO-OPS' if len(station_id) == 7 else 'USGS/NDBC/CHS'

    # 2. Set date range from observation data
    retrieve_input.start_date = start_dt.strftime('%Y%m%d%H%M%S')
    retrieve_input.end_date = end_dt.strftime('%Y%m%d%H%M%S')

    # 3. Handle Datum logic (Requested + Fallbacks)
    requested_datum = prop.datum
    fallback_datums = ['MLLW', 'MHHW', 'MHW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B']

    config_file = Path(__file__).resolve().parent.parent.parent.parent / 'conf' / 'ofs_dps.conf'
    if config_file.exists():
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            if config.has_option('datums', 'datum_list'):
                datum_list_str = config.get('datums', 'datum_list')
                fallback_datums = [d.strip() for d in datum_list_str.split()]
        except Exception as ex:
            logger.warning('Could not read datum_list from config, using defaults: %s', ex)

    datums_to_try = [requested_datum] + [d for d in fallback_datums if d != requested_datum]

    # 4. Define helper for data retrieval attempts
    def try_get_tidal_data(station_id_to_try):
        retrieve_input.station = station_id_to_try
        for datum in datums_to_try:
            retrieve_input.datum = datum
            data = retrieve_tidal_predictions(retrieve_input, logger)
            if data is False:
                return None, None
            if data is not None and len(data) > 0:
                return data, datum
        return None, None

    # 5. Extract coordinates for proximity search
    lat, lon = None, None
    try:
        ctl_file = os.path.join(prop.control_files_path, f'{prop.ofs}_wl_station.ctl')
        with open(ctl_file) as f:
            for line in f:
                if line.strip().startswith(obs_station_id):
                    coords = next(f).split()
                    lat, lon = float(coords[0]), float(coords[1])
                    break
    except Exception as ex:
        logger.warning('Could not find coordinates for station %s: %s', obs_station_id, ex)

    # 6. Attempt Primary Retrieval
    tidal_data = None
    used_datum = None
    tidal_station_id = None
    tidal_station_name = None
    tidal_station_distance = None

    if station_source.upper() in ['CO-OPS', 'COOPS', 'TC', 'TAC']:
        tidal_data, used_datum = try_get_tidal_data(obs_station_id)
        if tidal_data is not None:
            tidal_station_id = obs_station_id
            tidal_station_distance = 0.0

    # 7. Attempt Secondary Retrieval (Nearby Stations)
    if tidal_data is None and lat is not None and lon is not None:
        logger.info('Finding nearby tidal stations for %s station %s...', station_source, obs_station_id)
        nearby_stations = find_nearest_tidal_stations(lat, lon, logger, max_stations=10)

        for candidate_id, candidate_name, candidate_dist in nearby_stations:
            if candidate_id == obs_station_id:
                continue

            logger.info('Trying tidal station %s (%s) at %.1f km...', candidate_id, candidate_name, candidate_dist)
            tidal_data, used_datum = try_get_tidal_data(candidate_id)

            if tidal_data is not None:
                tidal_station_id = candidate_id
                tidal_station_name = candidate_name
                tidal_station_distance = candidate_dist
                break

    # # # 8. Write tidal_data to file if it exists
    # if tidal_data is not None:
    #     try:
    #         # Construct filename: e.g., output_path/8518750_tidal_predictions_MLLW.csv
    #         output_dir = Path(prop.output_path)
    #         output_dir.mkdir(parents=True, exist_ok=True)

    #         filename = f"{tidal_station_id}_tidal_predictions_{used_datum}.csv"
    #         file_path = output_dir / filename

    #         tidal_data.to_csv(file_path, index=False)
    #         logger.info('Successfully saved tidal data to %s', file_path)
    #     except Exception as ex:
    #         logger.error('Failed to write tidal data to file: %s', ex)

    tidal_info = {"tidal_station_id": tidal_station_id,
                  "tidal_station_name": tidal_station_name,
                  "tidal_station_distance": tidal_station_distance,
                  "used_datum": used_datum,
        }
    return tidal_data, tidal_info
