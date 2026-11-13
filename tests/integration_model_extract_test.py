"""Integration tests: model extract / ctl write boundary (no live NODD)."""

from __future__ import annotations

import logging
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile
from ofs_skill.skill_assessment.get_skill import (
    _get_valid_cached_model,
    _set_cached_model,
    ofs_ctlfile_extract,
)
from tests.helpers.fvcom_minimal import write_minimal_config, write_obs_station_ctl


@pytest.mark.integration
def test_write_ofs_ctlfile_from_synthetic_netcdf(tmp_path, fvcom_minimal_dataset):
    """Model extract boundary: synthetic NetCDF → model station.ctl."""
    combined, lon_1d, lat_1d = fvcom_minimal_dataset

    cfg_path = write_minimal_config(tmp_path)
    control_dir = tmp_path / 'control_files'
    control_dir.mkdir()

    chosen_nodes = [1, 3]
    stations = [
        (f'9000{ni:02d}', float(lat_1d[ni]), float(lon_1d[ni]), 0.0)
        for ni in chosen_nodes
    ]
    write_obs_station_ctl(control_dir, 'tbofs', 'wl', stations)

    prop = SimpleNamespace(
        config_file=str(cfg_path),
        ofs='tbofs',
        var_list=['water_level'],
        ofsfiletype='stations',
        user_input_location=False,
        model_source='fvcom',
        control_files_path=str(control_dir),
        datum='MLLW',
    )
    logger = logging.getLogger('integration_model_extract')
    write_ofs_ctlfile(prop, combined, logger)

    out_path = control_dir / 'tbofs_wl_model_station.ctl'
    assert out_path.exists()
    assert os.path.getsize(out_path) > 0
    rows = [ln for ln in out_path.read_text().splitlines() if ln.strip()]
    # Model ctl now includes a descriptive header row (file_headers).
    if rows and rows[0].startswith('Node/station index'):
        rows = rows[1:]
    assert len(rows) == len(chosen_nodes)


@pytest.mark.integration
@patch('ofs_skill.skill_assessment.get_skill.os.path.getsize', return_value=0)
@patch('ofs_skill.skill_assessment.get_skill.os.path.isfile', return_value=False)
@patch('ofs_skill.skill_assessment.get_skill.get_node_ofs')
def test_ofs_ctlfile_extract_calls_get_node(
    mock_get_node_ofs, _mock_isfile, _mock_getsize,
):
    """get_skill → get_node_ofs boundary with intake mocked at get_node."""
    ds = MagicMock(name='dataset-nowcast')
    mock_get_node_ofs.return_value = ds

    prop = SimpleNamespace(
        ofs='cbofs',
        whichcast='nowcast',
        ofsfiletype='stations',
        forecast_hr=None,
        start_date_full='2024-01-01T00:00:00Z',
        end_date_full='2024-01-01T23:00:00Z',
        control_files_path='/fake/control_files',
    )
    logger = logging.getLogger('integration_get_node_boundary')

    cached = _get_valid_cached_model(prop)
    assert cached is None
    ofs_ctlfile_extract(prop, 'wl', logger, model_dataset=cached)

    assert mock_get_node_ofs.call_count == 1
    assert mock_get_node_ofs.call_args.kwargs['model_dataset'] is None
    assert _get_valid_cached_model(prop) is ds
    _set_cached_model(prop, ds)  # idempotent stamp
    assert _get_valid_cached_model(prop) is ds
