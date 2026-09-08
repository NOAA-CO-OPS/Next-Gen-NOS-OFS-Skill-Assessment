"""Integration tests: pair → skill metrics (+ fixture layout, plot smoke)."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from ofs_skill.skill_assessment.format_paired_one_d import paired_scalar
from ofs_skill.skill_assessment.metrics_paired_one_d import skill_scalar
from ofs_skill.visualization import summary_barplots
from tests.helpers.api_mocks import PIPELINE_FIXTURES, load_julian_disk_series

# Mentor CBOFS wl window for station 8637689 (native 6-min cadence).
_OBS_PATH = PIPELINE_FIXTURES / '8637689_cbofs_wl_station.obs'
_PRD_PATH = PIPELINE_FIXTURES / '8637689_cbofs_wl_45_nowcast_stations_model.prd'
_PAIR_PATH = PIPELINE_FIXTURES / 'cbofs_wl_8637689_45_nowcast_stations_pair.int'
_SKILL_PATH = PIPELINE_FIXTURES / 'skill_cbofs_water_level_nowcast_stations.csv'
_WINDOW_START = '20260327-18:00:00'
_WINDOW_END = '20260327-22:42:00'


@pytest.fixture
def logger():
    return logging.getLogger('integration_pair_skill')


@pytest.mark.integration
def test_pair_skill_metrics_pipeline(tmp_path, logger):
    """Core 1D boundary: mentor .obs/.prd → paired_scalar → skill_scalar."""
    conf_dir = tmp_path / 'conf'
    conf_dir.mkdir()
    shutil.copy(PIPELINE_FIXTURES / 'error_ranges.csv', conf_dir / 'error_ranges.csv')

    obs_df = load_julian_disk_series(_OBS_PATH)
    ofs_df = load_julian_disk_series(_PRD_PATH)
    result = paired_scalar(
        obs_df,
        ofs_df,
        _WINDOW_START,
        _WINDOW_END,
        logger,
        lookback_hours=0,
    )
    assert isinstance(result, tuple)
    _formatted, paired = result
    assert {'OBS', 'OFS', 'BIAS'}.issubset(paired.columns)
    # Mirror get_skill: restore DateTime from julian columns before metrics.
    paired = paired.copy()
    paired['DateTime'] = pd.to_datetime({
        'year': paired[1],
        'month': paired[2],
        'day': paired[3],
        'hour': paired[4],
        'minute': paired[5],
    })
    assert len(paired.dropna(subset=['OBS', 'OFS'])) >= 5

    prop = SimpleNamespace(path=str(tmp_path), ofs='cbofs', datum='MLLW')
    # Avoid live CO-OPS tidal fetch used by wl WOF on non-lake OFS.
    with patch(
        'ofs_skill.skill_assessment.metrics_paired_one_d.get_station_tidal_data',
        return_value=(None, {}),
    ):
        metrics = skill_scalar(paired, 'wl', '8637689', prop, logger)

    assert isinstance(metrics, list)
    assert len(metrics) >= 18
    assert isinstance(metrics[0], float)
    # Mentor pair window has a clear positive model-minus-obs bias.
    assert 0.05 < float(metrics[2]) < 0.30


@pytest.mark.integration
def test_fixture_obs_prd_pair_aligned():
    """Mentor .obs / .prd / .pair.int stay length-aligned and node-consistent."""
    obs = load_julian_disk_series(_OBS_PATH)
    model = load_julian_disk_series(_PRD_PATH)
    assert len(obs) == len(model) == 48

    pair = pd.read_csv(_PAIR_PATH, sep=r'\s+')
    assert len(pair) == 48
    assert {'VAL_OB', 'VAL_MODEL', 'BIAS'}.issubset(pair.columns)
    # Non-trivial positive bias (mentor note: ~0.1 m order of magnitude).
    assert 0.05 < float(pair['BIAS'].mean()) < 0.30

    # Model CTL node 45 is embedded in the .prd / .pair filenames.
    assert '45' in _PRD_PATH.name
    assert '45' in _PAIR_PATH.name
    assert '8637689' in _OBS_PATH.name


@pytest.mark.integration
def test_currents_obs_fixture_layout():
    """ADCP currents .obs has four value columns (speed dir u v)."""
    cu = load_julian_disk_series(
        PIPELINE_FIXTURES / 'cb0201_b01_stofs_3d_atl_cu_station.obs',
        n_value_cols=4,
    )
    assert len(cu) == 48
    assert cu.shape[1] == 10


@pytest.mark.integration
def test_summary_barplot_smoke_from_skill_csv(tmp_path, logger):
    """Light plot entry-point: make_summary_bars from mentor skill CSV."""
    conf_dir = tmp_path / 'conf'
    conf_dir.mkdir()
    shutil.copy(PIPELINE_FIXTURES / 'error_ranges.csv', conf_dir / 'error_ranges.csv')

    prop = SimpleNamespace(
        path=str(tmp_path),
        ofs='cbofs',
        whichcast='nowcast',
        whichcasts=['nowcast'],
        ofsfiletype='stations',
        start_date_full='2026-03-28T00:00:00Z',
        end_date_full='2026-03-28T23:00:00Z',
        forecast_hr=None,
        static_plots=False,
        data_skill_stats_path=str(tmp_path / 'data' / 'skill' / 'stats'),
        visuals_1d_station_path=str(tmp_path / 'data' / 'visual'),
        om_files=str(tmp_path / 'data' / 'visual' / '1d' / 'om'),
    )
    Path(prop.data_skill_stats_path).mkdir(parents=True)
    Path(prop.visuals_1d_station_path).mkdir(parents=True)
    Path(prop.om_files).mkdir(parents=True)

    shutil.copy(_SKILL_PATH, Path(prop.data_skill_stats_path) / _SKILL_PATH.name)

    summary_barplots.make_summary_bars(
        prop, ['water_level', 'wl', []], logger,
    )

    html_hits = list(Path(prop.visuals_1d_station_path).rglob('*.html'))
    assert html_hits, 'expected at least one summary bar HTML plot'
