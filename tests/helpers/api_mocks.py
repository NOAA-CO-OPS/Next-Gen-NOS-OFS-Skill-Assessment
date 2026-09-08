"""Shared HTTP / provider mock helpers for offline tests.

These helpers keep PR CI deterministic: no live USGS / CO-OPS / NDBC calls.
Live coverage stays on ``@pytest.mark.network`` / ``@pytest.mark.manual`` jobs.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

FIXTURES_ROOT = Path(__file__).resolve().parent.parent / 'fixtures'
PIPELINE_FIXTURES = FIXTURES_ROOT / 'pipeline'


def make_usgs_searvey_raw(
    *,
    start: str = '2024-01-01T00:00:00',
    periods: int = 24,
    freq: str = 'h',
    code: str = '00010',
    value: float = 12.5,
) -> pd.DataFrame:
    """Build a DataFrame shaped like searvey ``get_usgs_station_data`` output.

    Flat columns are fine: ``retrieve_usgs_station`` calls ``reset_index()``
    then filters on ``code`` / ``datetime`` / ``value``.
    """
    idx = pd.date_range(start=start, periods=periods, freq=freq, tz='UTC')
    return pd.DataFrame({
        'datetime': idx,
        'code': [code] * periods,
        'value': [value] * periods,
        'time_series_id': ['ts1'] * periods,
    })


def make_usgs_inventory_stations() -> pd.DataFrame:
    """Build a DataFrame shaped like searvey ``get_usgs_stations`` output."""
    return pd.DataFrame({
        'site_no': ['01646500'],
        'station_nm': ['Potomac River at Washington, DC'],
        'dec_long_va': [-77.04],
        'dec_lat_va': [38.95],
        'site_type_code': ['ST'],
    })


def make_usgs_obs_dataframe(
    *,
    start: str = '2024-01-01T00:00:00',
    periods: int = 24,
    freq: str = 'h',
    value: float = 12.5,
    datum: str | None = None,
) -> pd.DataFrame:
    """Build a DataFrame shaped like ``retrieve_usgs_station`` return value."""
    idx = pd.date_range(start=start, periods=periods, freq=freq, tz='UTC')
    frame = pd.DataFrame({
        'DateTime': idx,
        'OBS': [value] * periods,
        'DEP01': [0.0] * periods,
    })
    if datum is not None:
        frame['Datum'] = datum
    return frame


@contextmanager
def mock_usgs_searvey(
    timeseries: pd.DataFrame | None = None,
    inventory: pd.DataFrame | None = None,
    param_codes: dict | None = None,
) -> Iterator[dict[str, Any]]:
    """Patch searvey entry points used by USGS retrieve/inventory modules."""
    ts = timeseries if timeseries is not None else make_usgs_searvey_raw()
    inv = inventory if inventory is not None else make_usgs_inventory_stations()
    codes = param_codes if param_codes is not None else {'01646500': ['00010', '00065']}

    with patch(
        'ofs_skill.obs_retrieval.retrieve_usgs_station.get_usgs_station_data',
        return_value=ts,
    ) as retrieve_patch, patch(
        'ofs_skill.obs_retrieval.inventory_usgs_station.get_usgs_stations',
        return_value=inv,
    ) as inventory_patch, patch(
        'ofs_skill.obs_retrieval.inventory_usgs_station.get_station_parameter_availability',
        return_value=pd.DataFrame({
            'site_no': list(codes.keys()),
            'has_water_level': [True] * len(codes),
            'has_temperature': [True] * len(codes),
            'has_salinity': [False] * len(codes),
            'has_currents': [False] * len(codes),
        }),
    ):
        yield {
            'retrieve': retrieve_patch,
            'inventory': inventory_patch,
            'timeseries': ts,
            'inventory_df': inv,
        }


@contextmanager
def mock_coops_http(
    json_payload: dict | list | None = None,
    text_payload: str = '',
) -> Iterator[dict[str, Any]]:
    """Patch CO-OPS urllib + requests helpers used by inventory/retrieve."""
    payload = json_payload if json_payload is not None else {'stations': []}

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = payload
    fake_resp.text = text_payload
    fake_resp.raise_for_status = MagicMock()

    with patch(
        'ofs_skill.obs_retrieval.inventory_t_c_station.urllib.request.urlopen',
    ) as urlopen_patch, patch(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station._rate_limited_get',
        return_value=fake_resp,
    ) as get_patch:
        mock_cm = MagicMock()
        mock_cm.read.return_value = b'{"count":0,"stations":[]}'
        mock_cm.__enter__.return_value = mock_cm
        mock_cm.__exit__.return_value = False
        urlopen_patch.return_value = mock_cm
        yield {'urlopen': urlopen_patch, 'get': get_patch, 'response': fake_resp}


@contextmanager
def mock_ndbc_http(activestations_xml: str | None = None) -> Iterator[dict[str, Any]]:
    """Patch NDBC activestations.xml fetch (urllib)."""
    xml = activestations_xml or (
        '<?xml version="1.0"?>'
        '<stations>'
        '<station id="44025" lat="40.25" lon="-73.16" name="Long Island" '
        'met="y" currents="n" waterquality="y"/>'
        '</stations>'
    )
    mock_cm = MagicMock()
    mock_cm.read.return_value = xml.encode('utf-8')
    mock_cm.__enter__.return_value = mock_cm
    mock_cm.__exit__.return_value = False

    with patch(
        'ofs_skill.obs_retrieval.inventory_ndbc_station.urllib.request.urlopen',
        return_value=mock_cm,
    ) as urlopen_patch:
        yield {'urlopen': urlopen_patch, 'xml': xml}


def julian_timeseries(
    start: str,
    periods: int,
    values: list[float] | float,
    *,
    freq: str = 'h',
) -> pd.DataFrame:
    """Build the 0–6 column julian frame expected by ``paired_scalar``."""
    stamps = pd.date_range(start=start, periods=periods, freq=freq)
    if isinstance(values, int | float):
        values = [float(values)] * periods
    # Approximate day-of-year julian used only as a passthrough column.
    julian = stamps.to_julian_date() - pd.Timestamp('2000-01-01').to_julian_date()
    return pd.DataFrame({
        0: julian,
        1: stamps.year,
        2: stamps.month,
        3: stamps.day,
        4: stamps.hour,
        5: stamps.minute,
        6: values,
    })


def load_julian_disk_series(path: Path | str, *, n_value_cols: int = 1) -> pd.DataFrame:
    """Load a production ``.obs`` / ``.prd`` file into a 0–N column DataFrame.

    On-disk layout is space-delimited::

        julian  YYYY  M  D  H  MIN  value[ ...]

    Scalar wl/temp/salt use one value column; ADCP currents use four
    (speed, direction, u, v).
    """
    frame = pd.read_csv(path, sep=r'\s+', header=None)
    expected = 6 + n_value_cols
    assert frame.shape[1] >= expected, (
        f'{path}: expected >= {expected} columns, got {frame.shape[1]}'
    )
    return frame.iloc[:, :expected]


def write_minimal_ofs_config(tmp_path: Path) -> Path:
    """Write a minimal ``ofs_dps.conf`` under ``tmp_path`` for ctl writers."""
    cfg_path = tmp_path / 'ofs_dps.conf'
    cfg_path.write_text(
        '[directories]\n'
        f'home={tmp_path.as_posix()}\n'
        'data_dir=data\n'
        'ofs_extents_dir=ofs_extents\n'
        'control_files_dir=control_files\n'
        'observations_dir=observations\n'
        '1d_station_dir=1d_station\n'
        'model_dir=model\n'
        'skill_dir=skill\n'
        'model_historical_dir=%(home)s/example_data\n'
        'netcdf_dir=netcdf\n'
        '1d_node_dir=1d_node\n'
        '1d_pair_dir=1d_pair\n'
        'stats_dir=stats\n'
        'visual_dir=visual\n'
        'visual_maps=plotly_maps\n'
        'om_dir=om_files\n'
        '\n'
        '[datums]\n'
        'datum_list=MHHW MHW MLLW MLW NAVD88 IGLD85 LWD\n'
        '\n'
        '[urls]\n'
        'ndbc_noaa_url=https://www.ndbc.noaa.gov/\n'
        'co_ops_mdapi_base_url=https://api.tidesandcurrents.noaa.gov/mdapi/prod/\n'
        'co_ops_api_base_url=https://api.tidesandcurrents.noaa.gov/api/prod/\n',
        encoding='utf-8',
    )
    return cfg_path
