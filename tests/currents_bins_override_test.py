"""
Tests for the optional currents-bins override CSV (Phase 3 of issue #87).

Covers:

- ``load_currents_bins_csv`` parsing (valid + degraded rows, missing file,
  header validation).
- ``bin_spec_lookup`` mapping.
- ``_process_coops_station`` currents branch: filter-only, depth override,
  name override, and a bin listed in the CSV that the datagetter did
  not return.
"""

from __future__ import annotations

import logging
from textwrap import dedent
from unittest.mock import patch

import pandas as pd
import pytest


@pytest.fixture
def logger():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('currents_bins_override_test')


# ---------------------------------------------------------------------------
# load_currents_bins_csv
# ---------------------------------------------------------------------------

def _write_csv(tmp_path, body: str):
    p = tmp_path / 'overrides.csv'
    p.write_text(dedent(body).lstrip())
    return str(p)


def test_load_csv_valid(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        load_currents_bins_csv,
    )
    path = _write_csv(tmp_path, '''
        station_id,bin,depth,orientation,name
        cb1001,5,6.0,up,
        cb1001,15,9.0,,
        cb1301,10,,,mid-channel
    ''')
    overrides = load_currents_bins_csv(path, logger)
    assert set(overrides.keys()) == {'cb1001', 'cb1301'}
    cb1001 = {s.bin: s for s in overrides['cb1001']}
    assert cb1001[5].depth == 6.0
    assert cb1001[5].orientation == 'up'
    assert cb1001[15].depth == 9.0
    assert cb1001[15].orientation is None
    cb1301 = overrides['cb1301'][0]
    assert cb1301.bin == 10
    assert cb1301.depth is None
    assert cb1301.name == 'mid-channel'


def test_load_csv_missing_required_column(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        load_currents_bins_csv,
    )
    path = _write_csv(tmp_path, '''
        station_id,depth
        cb1001,6.0
    ''')
    assert load_currents_bins_csv(path, logger) == {}


def test_load_csv_skips_malformed_rows(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        load_currents_bins_csv,
    )
    path = _write_csv(tmp_path, '''
        station_id,bin,depth
        ,5,6.0
        cb1001,,6.0
        cb1001,not_a_number,6.0
        cb1001,5,not_a_number
        cb1001,7,7.0
    ''')
    overrides = load_currents_bins_csv(path, logger)
    # Row 6 bin=7 valid; row 5 (bin=5, bad depth) silently drops depth
    # but keeps the bin spec.
    assert set(overrides.keys()) == {'cb1001'}
    by_bin = {s.bin: s for s in overrides['cb1001']}
    assert 7 in by_bin and by_bin[7].depth == 7.0
    assert 5 in by_bin and by_bin[5].depth is None


def test_load_csv_missing_file(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        load_currents_bins_csv,
    )
    assert load_currents_bins_csv(str(tmp_path / 'nope.csv'), logger) == {}
    assert load_currents_bins_csv(None, logger) == {}
    assert load_currents_bins_csv('', logger) == {}


def test_load_csv_duplicate_bin_last_wins(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        load_currents_bins_csv,
    )
    path = _write_csv(tmp_path, '''
        station_id,bin,depth
        cb1001,5,3.0
        cb1001,5,9.9
    ''')
    overrides = load_currents_bins_csv(path, logger)
    assert overrides['cb1001'] == [
        overrides['cb1001'][0]  # single entry
    ]
    assert overrides['cb1001'][0].depth == 9.9


def test_bin_spec_lookup(tmp_path, logger):
    from ofs_skill.obs_retrieval.currents_bins_override import (
        bin_spec_lookup, load_currents_bins_csv,
    )
    path = _write_csv(tmp_path, '''
        station_id,bin,depth
        cb1001,5,6.0
        cb1001,15,9.0
    ''')
    overrides = load_currents_bins_csv(path, logger)
    result = bin_spec_lookup(overrides, 'cb1001')
    assert isinstance(result, dict)
    assert set(result.keys()) == {5, 15}
    # Missing station returns None (distinct from empty dict).
    assert bin_spec_lookup(overrides, 'cb0402') is None


# ---------------------------------------------------------------------------
# _process_coops_station with bin_overrides
# ---------------------------------------------------------------------------

def _fake_bin_frames(bins):
    """Build a dict[int, DataFrame] that mimics retrieve_t_and_c_station
    output for currents."""
    frames = {}
    for bn, depth, orient, hfb in bins:
        df = pd.DataFrame({
            'DateTime': pd.to_datetime(['2025-01-01 00:00']),
            'DEP01': [depth],
            'DIR': [90.0],
            'OBS': [0.5],
        })
        df.attrs['bin'] = bn
        df.attrs['depth'] = depth
        df.attrs['orientation'] = orient
        df.attrs['height_from_bottom'] = hfb
        frames[bn] = df
    return frames


def test_process_coops_station_filters_to_csv_bins(logger):
    import importlib
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')
    from ofs_skill.obs_retrieval.currents_bins_override import BinSpec

    # 4 bins available; CSV only names 2 of them.
    frames = _fake_bin_frames([
        (1, 2.0, 'up', None),
        (2, 4.0, 'up', None),
        (3, 6.0, 'up', None),
        (4, 8.0, 'up', None),
    ])
    overrides = {
        2: BinSpec(bin=2),
        4: BinSpec(bin=4),
    }
    with patch.object(woc, 'retrieve_t_and_c_station', return_value=frames):
        entries = woc._process_coops_station(
            id_number='cb1001', name='Cove Point',
            x_value=-76.384, y_value=38.403,
            start_date='20250101', end_date='20250102',
            variable='currents', name_var='cu',
            datum='MLLW', datum_list=['MLLW'], ofs='cbofs',
            logger=logger, bin_overrides=overrides,
        )
    assert len(entries) == 2
    assert entries[0].startswith('cb1001_b02 ')
    assert entries[1].startswith('cb1001_b04 ')


def test_process_coops_station_applies_depth_override(logger):
    import importlib
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')
    from ofs_skill.obs_retrieval.currents_bins_override import BinSpec

    # Bin 5 comes from MDAPI with depth=6.0 + hfb=0.0; user overrides to 9.5.
    frames = _fake_bin_frames([(5, 6.0, 'up', None)])
    overrides = {5: BinSpec(bin=5, depth=9.5, name='mid-column')}
    with patch.object(woc, 'retrieve_t_and_c_station', return_value=frames):
        entries = woc._process_coops_station(
            id_number='cb1001', name='Cove Point',
            x_value=-76.384, y_value=38.403,
            start_date='20250101', end_date='20250102',
            variable='currents', name_var='cu',
            datum='MLLW', datum_list=['MLLW'], ofs='cbofs',
            logger=logger, bin_overrides=overrides,
        )
    assert len(entries) == 1
    entry = entries[0]
    # New depth value present, old 6.00 is not.
    assert '9.50' in entry
    assert '6.00' not in entry
    # Custom name appended in the quoted display label.
    assert 'bin 05 / mid-column' in entry


def test_process_coops_station_warns_on_missing_requested_bin(
    logger, caplog
):
    import importlib
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')
    from ofs_skill.obs_retrieval.currents_bins_override import BinSpec

    # Only bin 2 available; CSV asks for 2 and 99.
    frames = _fake_bin_frames([(2, 4.0, 'up', None)])
    overrides = {
        2: BinSpec(bin=2),
        99: BinSpec(bin=99),
    }
    with patch.object(woc, 'retrieve_t_and_c_station', return_value=frames), \
            caplog.at_level(logging.WARNING):
        entries = woc._process_coops_station(
            id_number='cb1001', name='Cove Point',
            x_value=-76.384, y_value=38.403,
            start_date='20250101', end_date='20250102',
            variable='currents', name_var='cu',
            datum='MLLW', datum_list=['MLLW'], ofs='cbofs',
            logger=logger, bin_overrides=overrides,
        )
    assert len(entries) == 1
    assert '[99]' in caplog.text or 'bin(s) [99]' in caplog.text
