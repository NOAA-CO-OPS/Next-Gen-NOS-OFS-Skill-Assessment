"""
Tests for the aggregated skill-table helpers added for issue #148.

Covers the three helpers in ``bin/visualization/create_1dplot.py``:
``get_variable_from_filename``, ``get_forecast_type_from_filename``, and
``combine_files_by_pattern``. The latter must:
  * tag each row with source_file / variable / cast type,
  * scope by OFS substring and (optionally) by the current run's casts,
  * stay idempotent on re-runs (never re-ingest its own output),
  * drop stale duplicate rows, and
  * survive glob metacharacters in the search string.
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_1DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_1dplot.py'


@pytest.fixture(scope='module')
def mod():
    spec = importlib.util.spec_from_file_location(
        'create_1dplot_combine_under_test', CREATE_1DPLOT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('filename, expected', [
    ('skill_cbofs_water_level_hw_nowcast_stations.csv', 'Water Level high tide'),
    ('skill_cbofs_water_level_lw_nowcast_stations.csv', 'Water Level low tide'),
    ('skill_cbofs_water_level_nowcast_stations.csv', 'Water Level'),
    ('skill_cbofs_water_temperature_nowcast_stations.csv', 'Temperature'),
    ('skill_cbofs_currents_dir_nowcast_stations.csv', 'Current direction'),
    ('skill_cbofs_currents_nowcast_stations.csv', 'Current speed'),
    ('skill_cbofs_salinity_nowcast_stations.csv', 'Salinity'),
    ('skill_cbofs_mystery_nowcast_stations.csv', 'Other'),
])
def test_get_variable_from_filename(mod, filename, expected):
    assert mod.get_variable_from_filename(filename) == expected


@pytest.mark.parametrize('filename, expected', [
    ('skill_cbofs_water_level_nowcast_stations.csv', 'Nowcast'),
    ('skill_cbofs_water_level_forecast_a_stations.csv', 'Forecast (A)'),
    ('skill_cbofs_water_level_forecast_b_stations.csv', 'Forecast (B)'),
    ('skill_cbofs_water_level_forecast_stations.csv', 'Forecast'),
    ('skill_cbofs_water_level_hindcast_stations.csv', 'Hindcast'),
    ('skill_cbofs_water_level_stations.csv', 'Unknown'),
])
def test_get_forecast_type_from_filename(mod, filename, expected):
    assert mod.get_forecast_type_from_filename(filename) == expected


def _write_csv(path, ids):
    pd.DataFrame({'ID': ids, 'rmse': [0.1] * len(ids)}).to_csv(path, index=False)


def test_combine_basic_and_metadata(mod, tmp_path):
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a', 'b'])
    _write_csv(tmp_path / 'skill_cbofs_currents_nowcast_stations.csv',
               ['c', 'd', 'e'])

    out = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='cbofs')

    assert out is not None and len(out) == 5
    assert {'source_file', 'variable', 'type'}.issubset(out.columns)
    assert set(out['variable']) == {'Water Level', 'Current speed'}
    assert set(out['type']) == {'Nowcast'}
    assert (tmp_path / 'skill_cbofs_all_stations.csv').is_file()


def test_combine_is_idempotent_on_rerun(mod, tmp_path):
    """The aggregate output must never be folded back into itself."""
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a', 'b'])

    first = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='cbofs')
    second = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='cbofs')

    assert len(first) == len(second) == 2


def test_combine_scopes_by_whichcast(mod, tmp_path):
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a', 'b'])
    _write_csv(tmp_path / 'skill_cbofs_water_level_forecast_b_stations.csv',
               ['c'])

    out = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv',
        search_string='cbofs', whichcasts=['nowcast'])

    assert len(out) == 2
    assert set(out['type']) == {'Nowcast'}


def test_combine_scopes_by_ofs(mod, tmp_path):
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a', 'b'])
    _write_csv(tmp_path / 'skill_sfbofs_water_level_nowcast_stations.csv',
               ['x', 'y', 'z'])

    out = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='cbofs')

    assert len(out) == 2
    assert all('cbofs' in s for s in out['source_file'])


def test_combine_drops_stale_duplicates(mod, tmp_path):
    """Same station/variable/cast in two files collapses to one row."""
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a', 'b'])
    # A leftover/renamed file describing the same variable+cast for station 'a'.
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations_old.csv',
               ['a'])

    out = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='cbofs')

    assert sorted(out['ID']) == ['a', 'b']


def test_combine_no_match_returns_none(mod, tmp_path):
    assert mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv',
        search_string='cbofs') is None


def test_combine_handles_glob_metacharacters(mod, tmp_path):
    """A search string with glob metachars must not raise or mis-match."""
    _write_csv(tmp_path / 'skill_cbofs_water_level_nowcast_stations.csv',
               ['a'])

    out = mod.combine_files_by_pattern(
        str(tmp_path), 'skill_cbofs_all_stations.csv', search_string='c*b[o]')

    assert out is None  # literal 'c*b[o]' is not a substring of any file
