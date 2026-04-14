"""
Tests for per-bin CO-OPS currents retrieval and CTL emission (Issue #87).

Covers the Phase 1 + Phase 2 behavior defined in
``issue_87_currents_bins_plan.md``:

- ``_get_station_depth`` caches and parses the bins endpoint.
- ``retrieve_t_and_c_station`` returns one DataFrame per ADCP bin for
  ``variable='currents'``, with bin/depth/orientation stamped onto
  ``df.attrs``.
- ``_process_coops_station`` emits one virtual-ID CTL entry per bin
  (``{parent}_b{NN}``) with the bin's depth.
- ``station_ctl_file_extract`` preserves the virtual ID verbatim and
  still parses ``source = 'CO-OPS'``.
- Fallback: when the bins endpoint returns ``None``/empty, one legacy
  CTL entry is emitted and a WARN is logged.
- ``get_title`` (visualization) appends a "Bin NN" line for virtual IDs.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def logger():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('coops_currents_bins_test')


@pytest.fixture
def bins_payload():
    """Mock CO-OPS bins endpoint for a 3-bin bottom-mounted ADCP."""
    return {
        'nbr_of_bins': 3,
        'bin_size': 1.0,
        'center_bin_1_dist': 2.0,
        'units': 'metric',
        'real_time_bin': 1,
        'bins': [
            {'num': 1, 'depth': -2.0, 'distance': 2.0,
             'qc_flag': 0, 'orientation': 'up', 'ping_int': 6},
            {'num': 2, 'depth': -4.0, 'distance': 4.0,
             'qc_flag': 0, 'orientation': 'up', 'ping_int': 6},
            {'num': 3, 'depth': -6.0, 'distance': 6.0,
             'qc_flag': 0, 'orientation': 'up', 'ping_int': 6},
        ],
    }


@pytest.fixture
def currents_payloads_by_bin():
    """Per-bin datagetter responses keyed by the ``&bin=N`` param.

    The new retrieval issues a separate datagetter call per bin, so the
    mock HTTP layer returns a different payload per bin.
    """
    return {
        1: {'data': [
            {'t': '2025-01-01 00:00', 's': '50', 'd': '90', 'b': '1'},
            {'t': '2025-01-01 00:06', 's': '52', 'd': '91', 'b': '1'},
        ]},
        2: {'data': [
            {'t': '2025-01-01 00:00', 's': '40', 'd': '92', 'b': '2'},
            {'t': '2025-01-01 00:06', 's': '41', 'd': '93', 'b': '2'},
        ]},
        3: {'data': [
            {'t': '2025-01-01 00:00', 's': '30', 'd': '94', 'b': '3'},
            {'t': '2025-01-01 00:06', 's': '31', 'd': '95', 'b': '3'},
        ]},
    }


@pytest.fixture
def retrieve_input():
    return SimpleNamespace(
        station='8454000',
        start_date='20250101',
        end_date='20250102',
        variable='currents',
        datum='MLLW',
    )


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    def json(self):
        return self._payload


# ---------------------------------------------------------------------------
# _get_station_depth
# ---------------------------------------------------------------------------

def test_get_station_depth_parses_bins(bins_payload, logger):
    import importlib
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    # Clear module cache so the test is deterministic.
    rtc._depth_cache.clear()
    fake = _FakeResponse(bins_payload)
    with patch.object(rtc, '_get_session') as mock_session:
        mock_session.return_value.get.return_value = fake
        result = rtc._get_station_depth(
            '8454000', 'https://api.example/mdapi/prod/', logger)

    assert result == bins_payload
    assert len(result['bins']) == 3
    # Cache hit: no second HTTP call.
    with patch.object(rtc, '_get_session') as mock_session:
        rtc._get_station_depth(
            '8454000', 'https://api.example/mdapi/prod/', logger)
        mock_session.assert_not_called()


# ---------------------------------------------------------------------------
# retrieve_t_and_c_station — per-bin return
# ---------------------------------------------------------------------------

def test_retrieve_currents_returns_per_bin(
    retrieve_input, bins_payload, currents_payloads_by_bin, logger
):
    import importlib
    import re
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    rtc._depth_cache.clear()
    rtc._depth_cache['8454000'] = bins_payload

    fake_urls = {
        'co_ops_mdapi_base_url': 'https://api.example/mdapi/prod',
        'co_ops_api_base_url': 'https://api.example/api/prod',
    }

    class _StubUtils:
        def read_config_section(self, section, _logger):
            assert section == 'urls'
            return fake_urls

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        if not m:
            return _FakeResponse({'data': []})
        bn = int(m.group(1))
        return _FakeResponse(currents_payloads_by_bin.get(bn, {'data': []}))

    with patch.object(rtc.utils, 'Utils', return_value=_StubUtils()), \
            patch.object(rtc, '_get_session') as mock_session:
        mock_session.return_value.get.side_effect = _router
        result = rtc.retrieve_t_and_c_station(retrieve_input, logger)

    assert isinstance(result, dict)
    assert set(result.keys()) == {1, 2, 3}
    # Depths come from bins_payload, looked up per bin record.
    assert result[1].attrs['depth'] == -2.0
    assert result[2].attrs['depth'] == -4.0
    assert result[3].attrs['depth'] == -6.0
    assert result[1].attrs['orientation'] == 'up'
    assert result[1].attrs['bin'] == 1
    # Speed converted cm/s -> m/s
    assert result[1]['OBS'].iloc[0] == pytest.approx(0.50)
    assert result[2]['OBS'].iloc[0] == pytest.approx(0.40)
    # Each bin frame has its own rows (2 timestamps each here).
    for frame in result.values():
        assert len(frame) == 2


def test_retrieve_currents_legacy_fallback_when_bins_missing(
    retrieve_input, logger
):
    import importlib
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    rtc._depth_cache.clear()
    # Simulate bins endpoint returning None → single unfiltered datagetter.
    rtc._depth_cache['8454000'] = None

    fake_urls = {
        'co_ops_mdapi_base_url': 'https://api.example/mdapi/prod',
        'co_ops_api_base_url': 'https://api.example/api/prod',
    }

    class _StubUtils:
        def read_config_section(self, section, _logger):
            return fake_urls

    # Legacy-path datagetter returns the real-time bin rows only.
    legacy_payload = {
        'data': [
            {'t': '2025-01-01 00:00', 's': '50', 'd': '90', 'b': '7'},
            {'t': '2025-01-01 00:06', 's': '52', 'd': '91', 'b': '7'},
        ],
    }

    with patch.object(rtc.utils, 'Utils', return_value=_StubUtils()), \
            patch.object(rtc, '_get_session') as mock_session:
        mock_session.return_value.get.return_value = _FakeResponse(
            legacy_payload)
        result = rtc.retrieve_t_and_c_station(retrieve_input, logger)

    assert isinstance(result, dict)
    assert list(result.keys()) == [1]  # synthetic key for legacy fallback
    assert result[1].attrs['depth'] == 0.0


# ---------------------------------------------------------------------------
# write_obs_ctlfile._process_coops_station — N CTL entries per ADCP
# ---------------------------------------------------------------------------

def test_process_coops_station_emits_n_entries_for_currents(
    bins_payload, logger
):
    import importlib
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')

    # Build a dict[int, DataFrame] as if ``retrieve_t_and_c_station``
    # had returned it for currents.
    def _df(depth, bin_num, orientation='up'):
        df = pd.DataFrame({
            'DateTime': pd.to_datetime(['2025-01-01 00:00']),
            'DEP01': [depth],
            'DIR': [90.0],
            'OBS': [0.5],
        })
        df.attrs['depth'] = depth
        df.attrs['bin'] = bin_num
        df.attrs['orientation'] = orientation
        return df

    fake_bin_frames = {
        1: _df(-2.0, 1),
        2: _df(-4.0, 2),
        3: _df(-6.0, 3),
    }

    with patch.object(
            woc, 'retrieve_t_and_c_station', return_value=fake_bin_frames):
        entries = woc._process_coops_station(
            id_number='8454000',
            name='Providence',
            x_value=-71.401,
            y_value=41.807,
            start_date='20250101',
            end_date='20250102',
            variable='currents',
            name_var='cu',
            datum='MLLW',
            datum_list=['NAVD', 'MLLW'],
            ofs='cbofs',
            logger=logger,
        )

    assert isinstance(entries, list)
    assert len(entries) == 3
    # Ordering must match bin number.
    assert entries[0].startswith('8454000_b01 ')
    assert entries[1].startswith('8454000_b02 ')
    assert entries[2].startswith('8454000_b03 ')
    # Human-readable bin tag is part of the quoted station name.
    assert '"Providence (bin 01)"' in entries[0]
    # Depth formatting — 2 decimals, and depth is the bin's depth.
    assert '-2.00' in entries[0]
    assert '-4.00' in entries[1]
    assert '-6.00' in entries[2]


def test_process_coops_station_empty_dict_returns_empty_list(logger):
    import importlib
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')

    with patch.object(woc, 'retrieve_t_and_c_station', return_value={}):
        entries = woc._process_coops_station(
            id_number='8454000',
            name='Providence',
            x_value=-71.401,
            y_value=41.807,
            start_date='20250101',
            end_date='20250102',
            variable='currents',
            name_var='cu',
            datum='MLLW',
            datum_list=['NAVD', 'MLLW'],
            ofs='cbofs',
            logger=logger,
        )
    assert entries == []


# ---------------------------------------------------------------------------
# station_ctl_file_extract — virtual IDs survive the parser
# ---------------------------------------------------------------------------

def test_station_ctl_file_extract_preserves_virtual_id(tmp_path):
    from ofs_skill.obs_retrieval.station_ctl_file_extract import (
        station_ctl_file_extract,
    )

    ctl = tmp_path / 'cbofs_cu_station.ctl'
    ctl.write_text(
        '8454000_b05 8454000_b05_cu_cbofs_CO-OPS "Providence (bin 05)"\n'
        '  41.807 -71.401 0.0  -10.00  0.0\n'
    )
    info, coords = station_ctl_file_extract(str(ctl))
    assert info[0] == [
        '8454000_b05',
        '8454000_b05_cu_cbofs_CO-OPS',
        'Providence (bin 05)',
        'CO-OPS',
    ]
    # First field of the coord row is the latitude string.
    assert coords[0][0] == '41.807'


# ---------------------------------------------------------------------------
# _split_virtual_currents_id
# ---------------------------------------------------------------------------

def test_split_virtual_currents_id_roundtrips():
    from ofs_skill.obs_retrieval.get_station_observations import (
        _split_virtual_currents_id,
    )
    assert _split_virtual_currents_id('8454000_b05') == ('8454000', 5)
    assert _split_virtual_currents_id('8454000') == ('8454000', None)
    assert _split_virtual_currents_id('cb0101') == ('cb0101', None)


# ---------------------------------------------------------------------------
# get_title — bin-aware line
# ---------------------------------------------------------------------------

def test_get_title_includes_bin_line_for_virtual_id(bins_payload, logger):
    import importlib

    from ofs_skill.visualization import plotting_functions as pf
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    rtc._depth_cache.clear()
    rtc._depth_cache['8454000'] = bins_payload

    prop = SimpleNamespace(
        start_date_full='20250101-00:00:00',
        end_date_full='20250102-00:00:00',
        ofs='cbofs',
        control_files_path='/does/not/exist',  # no model ctl → obs-only
    )
    # station_id per get_title: (station_number, station_name, source)
    station_id = (
        '8454000_b02',
        'Providence (bin 02)',
        'CO-OPS',
    )
    title = pf.get_title(prop, '12345', station_id, 'cu', logger)
    assert 'Bin&nbsp;02' in title
    assert 'Obs&nbsp;depth&nbsp;-4.0' in title
    assert '(up)' in title
    # No model ctl file -> model depth should not appear
    assert 'Model&nbsp;depth' not in title


def test_get_title_skips_bin_line_when_metadata_missing(logger):
    import importlib

    from ofs_skill.visualization import plotting_functions as pf
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    rtc._depth_cache.clear()
    rtc._depth_cache['9999999'] = None

    prop = SimpleNamespace(
        start_date_full='20250101-00:00:00',
        end_date_full='20250102-00:00:00',
        ofs='cbofs',
        control_files_path='/does/not/exist',
    )
    station_id = (
        '9999999_b01',
        'Unknown (bin 01)',
        'CO-OPS',
    )
    title = pf.get_title(prop, '1', station_id, 'cu', logger)
    assert 'Bin&nbsp;' not in title
    assert 'Obs&nbsp;depth' not in title


def test_get_title_includes_model_depth_from_ctl(
    bins_payload, tmp_path, logger
):
    """Model depth should appear when the model_station.ctl file exists."""
    import importlib

    from ofs_skill.visualization import plotting_functions as pf
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    rtc._depth_cache.clear()
    rtc._depth_cache['8454000'] = bins_payload
    pf._MODEL_CTL_CACHE.clear()

    # Drop a minimal model_station.ctl file with our target station.
    ctl_dir = tmp_path / 'control_files'
    ctl_dir.mkdir()
    (ctl_dir / 'cbofs_cu_model_station.ctl').write_text(
        '41 6 41.807 -71.401 8454000_b02 3.8\n'
    )

    prop = SimpleNamespace(
        start_date_full='20250101-00:00:00',
        end_date_full='20250102-00:00:00',
        ofs='cbofs',
        control_files_path=str(ctl_dir),
    )
    station_id = ('8454000_b02', 'Providence (bin 02)', 'CO-OPS')
    title = pf.get_title(prop, '41', station_id, 'cu', logger)
    assert 'Bin&nbsp;02' in title
    assert 'Obs&nbsp;depth&nbsp;-4.0' in title
    assert 'Model&nbsp;depth&nbsp;-3.8' in title
