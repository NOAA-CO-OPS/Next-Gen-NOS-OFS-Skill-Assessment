"""Integration tests: inventory CSV + observation/model CTL parsing."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ofs_skill.obs_retrieval.station_ctl_file_extract import station_ctl_file_extract
from ofs_skill.obs_retrieval.write_obs_ctlfile import write_obs_ctlfile
from tests.helpers.api_mocks import (
    make_usgs_obs_dataframe,
    mock_ndbc_http,
    mock_usgs_searvey,
    write_minimal_ofs_config,
)

FIXTURES = Path(__file__).resolve().parent / 'fixtures' / 'pipeline'


@pytest.fixture
def logger():
    return logging.getLogger('integration_inventory_ctl')


@pytest.mark.integration
def test_station_ctl_file_extract_from_fixture():
    """Obs CTL fixture parses into station + coordinate rows."""
    result = station_ctl_file_extract(str(FIXTURES / 'cbofs_wl_station.ctl'))
    assert result is not None
    station_info, coord_info = result
    assert len(station_info) == 2
    assert station_info[0][0] == '8637689'
    assert station_info[0][3] == 'CO-OPS'
    assert station_info[1][0] == '8575512'
    assert float(coord_info[0][0]) == pytest.approx(37.227, abs=1e-3)
    assert float(coord_info[0][1]) == pytest.approx(-76.479, abs=1e-3)


@pytest.mark.integration
def test_model_ctl_fixture_shape():
    """Model CTL fixture is whitespace-delimited node rows."""
    lines = [
        ln for ln in (FIXTURES / 'cbofs_wl_model_station.ctl').read_text().splitlines()
        if ln.strip()
    ]
    assert len(lines) == 2
    tokens = lines[0].split()
    assert len(tokens) >= 6
    assert tokens[0] == '45'
    assert tokens[4] == '8637689'


@pytest.mark.integration
def test_inventory_fixture_loads():
    inv = pd.read_csv(FIXTURES / 'inventory_all_cbofs.csv')
    assert set(inv['Source']) == {'CO-OPS'}
    assert 'has_wl' in inv.columns
    assert '8637689' in set(inv['ID'].astype(str))


@pytest.mark.integration
def test_write_obs_ctlfile_usgs_temp_with_mocks(tmp_path, logger):
    """USGS temperature CTL write path without live APIs (synthetic inventory)."""
    cfg = write_minimal_ofs_config(tmp_path)
    control = tmp_path / 'control_files'
    control.mkdir()
    # Mentor inventory is CO-OPS-only; USGS path uses a one-row synthetic inventory.
    inv_path = control / 'inventory_all_cbofs.csv'
    pd.DataFrame({
        'ID': ['01646500'],
        'X': [-77.04],
        'Y': [38.95],
        'Source': ['USGS'],
        'Name': ['Potomac River at Washington DC'],
        'has_wl': [True],
        'has_temp': [True],
        'has_salt': [False],
        'has_cu': [False],
    }).to_csv(inv_path, index=False)

    obs = make_usgs_obs_dataframe(periods=8, value=14.0)

    # The inventory gate deletes pre-manifest files as stale, so stamp the
    # synthetic inventory with the signature this call will compute. Mocking
    # ensure_fresh instead would switch off the very gate under test -- that
    # is what previously hid an empty rebuild being recorded as fresh.
    from ofs_skill.utils import cache_manifest
    cache_manifest.record_artifact(
        str(inv_path),
        cache_manifest.inventory_signature(
            'cbofs',
            '2023-12-29T00:00:00Z', # Updated to match start_date_full
            '2024-01-05T00:00:00Z', # Updated to match end_date_full
            'usgs',
            None                    # Added currents_bins_csv matching write_obs_ctlfile
        ),
        str(control),
    )

    with patch(
        'ofs_skill.obs_retrieval.write_obs_ctlfile.retrieve_usgs_station',
        return_value=obs,
    ), mock_usgs_searvey():
        write_obs_ctlfile(
            start_date='20240101',
            end_date='20240102',
            start_date_full='2023-12-29T00:00:00Z',
            end_date_full='2024-01-05T00:00:00Z',
            datum='MLLW',
            path=str(tmp_path),
            ofs='cbofs',
            stationowner='usgs',
            var_list=['water_temperature'],
            logger=logger,
            config_file=str(cfg),
        )

    ctl = control / 'cbofs_temp_station.ctl'
    assert ctl.exists()
    assert ctl.stat().st_size > 0
    text = ctl.read_text()
    assert '01646500' in text
    assert 'USGS' in text


@pytest.mark.integration
def test_ndbc_inventory_uses_mock_http(tmp_path, logger):
    """NDBC inventory parses activestations.xml from the mock layer."""
    cfg = write_minimal_ofs_config(tmp_path)
    with mock_ndbc_http():
        from ofs_skill.obs_retrieval.inventory_ndbc_station import inventory_ndbc_station

        inv = inventory_ndbc_station(
            40.0, 41.0, -74.0, -72.0, logger, config_file=str(cfg),
        )

    assert inv is not None
    assert not inv.empty
    assert inv.iloc[0]['ID'] == '44025'
    assert inv.iloc[0]['Source'] == 'NDBC'
