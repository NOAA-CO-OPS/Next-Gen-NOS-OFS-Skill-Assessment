"""
Writer -> reader round-trip tests for the descriptive file headers.

The header constants in ofs_skill.utils.file_headers are written by the
ctl/.obs/.prd writers and must be stripped by every reader without
losing the first data row. These tests pin the writer/reader agreement
for each file type, for headered (new), headerless (legacy), and
single-entry files.
"""

import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from ofs_skill.model_processing.get_node_ofs import (
    ofs_ctlfile_extract as node_ctlfile_extract,
)
from ofs_skill.model_processing.parse_ofs_ctlfile import parse_ofs_ctlfile
from ofs_skill.obs_retrieval.station_ctl_file_extract import (
    station_ctl_file_extract,
)
from ofs_skill.skill_assessment.get_skill import (
    ofs_ctlfile_extract as skill_ctlfile_extract,
)
from ofs_skill.utils.file_headers import (
    MODEL_CTL_HEADER,
    OBS_CTL_HEADER,
    series_header,
    series_rows_to_skip,
    strip_model_ctl_header,
    strip_obs_ctl_header,
)

logger = logging.getLogger(__name__)

MODEL_CTL_ROWS = [
    '14 0 38.985  -76.475  8575512  0.0\n',
    '32 0 37.999  -76.448  8635750  0.0\n',
    '41 6 41.807  -71.401  8638901  3.8\n',
]

OBS_CTL_ROWS = [
    '8575512 8575512_wl_cbofs_CO-OPS "Annapolis"\n'
    '  38.983 -76.4816 0.0  0.0  0.0\n',
    '8635750 8635750_wl_cbofs_CO-OPS "Lewisetta"\n'
    '  37.9946 -76.4674 0.0  0.0  0.0\n',
]


def _write_model_ctl(tmp_path, rows, header=True):
    ctl_dir = tmp_path / 'control_files'
    ctl_dir.mkdir(exist_ok=True)
    path = ctl_dir / 'cbofs_wl_model_station.ctl'
    path.write_text(
        (MODEL_CTL_HEADER if header else '') + ''.join(rows),
        encoding='utf-8',
    )
    return path


def _model_prop(tmp_path):
    return SimpleNamespace(
        ofs='cbofs',
        ofsfiletype='stations',
        control_files_path=str(tmp_path / 'control_files'),
        ctl_flag=1,
    )


@pytest.mark.parametrize('header', [True, False])
def test_parse_ofs_ctlfile_keeps_all_stations(tmp_path, header):
    """parse_ofs_ctlfile must return every station, headered or not."""
    path = _write_model_ctl(tmp_path, MODEL_CTL_ROWS, header=header)
    _, nodes, depths, shifts, ids = parse_ofs_ctlfile(str(path))
    assert nodes == [14, 32, 41]
    assert depths == [0, 0, 6]
    assert ids == ['8575512', '8635750', '8638901']
    assert shifts == [0.0, 0.0, 3.8]


@pytest.mark.parametrize('header', [True, False])
def test_skill_ctlfile_extract_keeps_all_stations(tmp_path, header):
    """get_skill's ctl extractor must return every station."""
    _write_model_ctl(tmp_path, MODEL_CTL_ROWS, header=header)
    result = skill_ctlfile_extract(_model_prop(tmp_path), 'wl', logger)
    assert result is not None
    _, nodes, _, _, ids = result
    assert nodes == [14, 32, 41]
    assert ids == ['8575512', '8635750', '8638901']


@pytest.mark.parametrize('header', [True, False])
def test_node_ctlfile_extract_keeps_all_stations(tmp_path, header):
    """get_node_ofs's ctl extractor must return every station."""
    _write_model_ctl(tmp_path, MODEL_CTL_ROWS, header=header)
    result = node_ctlfile_extract(
        _model_prop(tmp_path), 'wl', None, logger)
    assert result is not None
    _, nodes, _, _, ids = result
    assert nodes == [14, 32, 41]
    assert ids == ['8575512', '8635750', '8638901']


def test_single_station_model_ctl_survives_header(tmp_path):
    """A one-station file must not come back empty (regression: the
    readers used to skip two lines for a one-line header)."""
    path = _write_model_ctl(tmp_path, MODEL_CTL_ROWS[:1])
    _, nodes, _, _, ids = parse_ofs_ctlfile(str(path))
    assert nodes == [14]
    assert ids == ['8575512']
    result = skill_ctlfile_extract(_model_prop(tmp_path), 'wl', logger)
    assert result is not None and result[1] == [14]


@pytest.mark.parametrize('header', [True, False])
def test_station_ctl_file_extract_keeps_all_stations(tmp_path, header):
    """The obs-ctl extractor must keep station/coord line pairing."""
    path = tmp_path / 'cbofs_wl_station.ctl'
    path.write_text(
        (OBS_CTL_HEADER if header else '') + ''.join(OBS_CTL_ROWS),
        encoding='utf-8',
    )
    result = station_ctl_file_extract(str(path))
    assert result is not None
    info_rows, coord_rows = result
    assert [row[0] for row in info_rows] == ['8575512', '8635750']
    assert coord_rows[0][0] == '38.983'


def test_series_header_roundtrip(tmp_path):
    """.obs/.prd written with a header must read back with the first
    data row intact; legacy files must read identically."""
    rows = [
        '2461247.50000000 2026  7 26  0  0    0.9630',
        '2461247.50416667 2026  7 26  0  6    0.9410',
    ]
    headered = tmp_path / 'new_station.obs'
    headered.write_text(
        series_header('wl') + '\n'.join(rows) + '\n', encoding='utf-8')
    legacy = tmp_path / 'legacy_station.obs'
    legacy.write_text('\n'.join(rows) + '\n', encoding='utf-8')

    assert series_rows_to_skip(str(headered)) == 1
    assert not series_rows_to_skip(str(legacy))

    frames = [
        pd.read_csv(str(p), sep=r'\s+', header=None,
                    skiprows=series_rows_to_skip(str(p)))
        for p in (headered, legacy)
    ]
    assert frames[0].equals(frames[1])
    assert len(frames[0]) == len(rows)
    assert frames[0].iloc[0, 6] == pytest.approx(0.9630)


def test_series_header_labels():
    """Header labels come from the shared map; unknowns fail soft."""
    assert series_header('wl').startswith('Julian days')
    assert 'Water level (m)' in series_header('wl')
    assert 'u, v' in series_header('cu')
    # Unmapped variables fall back to the variable name, never 'None'.
    assert 'None' not in series_header('mystery_var')
    assert 'mystery_var' in series_header('mystery_var')


def test_strip_helpers_are_noops_on_data():
    """Legacy data lines and empty inputs pass through untouched."""
    data = MODEL_CTL_ROWS[0].split('\n')
    assert strip_model_ctl_header(data) == data
    obs = OBS_CTL_ROWS[0].split('\n')
    assert strip_obs_ctl_header(obs) == obs
    assert not strip_model_ctl_header([])
    assert not strip_obs_ctl_header([])
