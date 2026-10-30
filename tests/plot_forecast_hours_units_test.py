"""Issue #217: RMSE labels on the forecast-horizon bar plots carry units."""
from __future__ import annotations

import logging
import os
import types

import numpy as np
import pandas as pd
import pytest

from ofs_skill.visualization import (
    make_static_plots,
    plot_forecast_hours,
    plotting_functions,
)
from tests.conftest import decode_plotly_escapes


def _write_error_ranges(root) -> None:
    conf_dir = root / 'conf'
    conf_dir.mkdir(parents=True, exist_ok=True)
    (conf_dir / 'error_ranges.csv').write_text(
        'name_var,X1,X2\n'
        'salt,3.5,0.5\n'
        'temp,3,0.5\n'
        'wl,0.15,0.5\n'
        'cu,0.26,0.5\n'
        'cu_dir,22.5,0.5\n'
        'ice_conc,10,0.5\n',
        encoding='utf-8')


def _make_prop(tmp_path, static=False):
    prop = types.SimpleNamespace()
    prop.path = str(tmp_path)
    prop.ofs = 'cbofs'
    prop.whichcast = 'forecast_b'
    prop.ofsfiletype = 'stations'
    prop.static_plots = static
    prop.start_date_full = '2026-03-28T00:00:00Z'
    prop.end_date_full = '2026-03-30T00:00:00Z'
    prop.visuals_horizon_path = str(tmp_path / 'horizon')
    prop.om_files = str(tmp_path / 'om')
    os.makedirs(prop.visuals_horizon_path, exist_ok=True)
    os.makedirs(prop.om_files, exist_ok=True)
    return prop


def _df_all(n=120):
    rng = np.random.default_rng(0)
    hours = np.tile([6, 12, 18, 24], n // 4)
    cycles = np.repeat(['20260328-00:00:00', '20260328-06:00:00',
                        '20260328-12:00:00', '20260328-18:00:00'],
                       n // 4)
    error = rng.normal(0.0, 0.1, size=n)
    return pd.DataFrame({
        'DateTime': pd.date_range('2026-03-28', periods=n, freq='h'),
        'hour_bins': hours,
        'model_cycle': cycles,
        'error': error,
        'square_error': error ** 2,
        'OBS': rng.normal(0.0, 0.5, size=n),
        'OFS': rng.normal(0.0, 0.5, size=n),
    })


def _info(name_var, variable):
    return [name_var, '123', '8638901', 'Test Station', 'CO-OPS',
            ['8638901'], False, variable]


@pytest.fixture
def _no_network(monkeypatch):
    monkeypatch.setattr(plotting_functions, 'get_title',
                        lambda *a, **k: 'Test title')
    monkeypatch.setattr(make_static_plots, 'get_title_static',
                        lambda *a, **k: 'Test title')


def _run(tmp_path, prop, name_var, variable):
    plot_forecast_hours.make_horizonbin_plots(
        _df_all(), _info(name_var, variable), prop,
        logging.getLogger('test'))
    html_path = os.path.join(
        prop.visuals_horizon_path,
        f'{prop.ofs}_8638901_{variable}_rmse_bars.html')
    assert os.path.isfile(html_path), html_path
    with open(html_path, encoding='utf-8') as fh:
        content = fh.read()
    # plotly escapes characters inside the embedded JSON, which would
    # otherwise hide 'm/s', the '<i>' unit markup and any non-ASCII unit
    # from a substring search.
    return decode_plotly_escapes(content)


@pytest.mark.usefixtures('_no_network')
def test_horizonbin_water_level_labels_carry_units(tmp_path):
    _write_error_ranges(tmp_path)
    prop = _make_prop(tmp_path)
    html = _run(tmp_path, prop, 'wl', 'water_level')
    assert 'RMSE (<i>meters</i>)' in html
    assert 'Mean error (<i>meters</i>)' in html
    assert 'Target error range (+0.15 meters)' in html
    assert 'Target error range (-0.15 meters)' in html
    # The y-axis title already carried its unit -- do not regress it, and
    # keep the well-formed closing tag.
    assert 'RMSE or error (<i>meters</i>)' in html
    assert '<i>meters<i>' not in html


@pytest.mark.usefixtures('_no_network')
def test_horizonbin_currents_direction_uses_degrees(tmp_path):
    """info[7] is the long variable name -- the only key that separates
    current speed from current direction."""
    _write_error_ranges(tmp_path)
    prop = _make_prop(tmp_path)
    html = _run(tmp_path, prop, 'cu', 'currents_dir')
    assert 'RMSE (<i>degrees</i>)' in html
    assert 'Mean error (<i>degrees</i>)' in html
    assert 'RMSE (<i>m/s</i>)' not in html
    # The y-axis label must follow the same key, not info[0].
    assert 'Current direction<br>RMSE or error (<i>degrees</i>)' in html
    assert 'Current speed' not in html


@pytest.mark.usefixtures('_no_network')
def test_horizonbin_threshold_and_unit_share_one_key(tmp_path):
    """The number in the target-error annotation must be the threshold
    for the variable whose unit is printed next to it: 22.5 degrees for
    current direction, not the 0.26 m/s current-speed threshold."""
    _write_error_ranges(tmp_path)
    prop = _make_prop(tmp_path)
    html = _run(tmp_path, prop, 'cu', 'currents_dir')
    assert 'Target error range (+22.5 degrees)' in html
    assert 'Target error range (-22.5 degrees)' in html
    assert '0.26 degrees' not in html


@pytest.mark.usefixtures('_no_network')
def test_static_bar_plot_axis_label_is_plain_text(tmp_path, monkeypatch):
    """bar_plots de-HTMLs the plotly axis title and dispatches on the
    literal 'RMSE' token; the label must stay tag-free."""
    _write_error_ranges(tmp_path)
    prop = _make_prop(tmp_path, static=True)

    seen: dict[str, list] = {'ylabels': [], 'legend': []}
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    original_ylabel = plt.Axes.set_ylabel
    original_axhline = plt.Axes.axhline

    def _ylabel(self, label, *a, **k):
        seen['ylabels'].append(label)
        return original_ylabel(self, label, *a, **k)

    def _axhline(self, *a, **k):
        if 'label' in k:
            seen['legend'].append(k['label'])
        return original_axhline(self, *a, **k)

    monkeypatch.setattr(plt.Axes, 'set_ylabel', _ylabel)
    monkeypatch.setattr(plt.Axes, 'axhline', _axhline)

    _run(tmp_path, prop, 'wl', 'water_level')

    png = os.path.join(prop.om_files,
                       f'{prop.ofs}_8638901_water_level_rmse_bars.png')
    assert os.path.isfile(png), 'static RMSE bar PNG was not written'
    assert seen['ylabels'], 'no y-axis label was set'
    assert all(lbl == 'Water level\nRMSE (meters)'
               for lbl in seen['ylabels']), seen['ylabels']
    assert all('<' not in lbl for lbl in seen['ylabels']), seen['ylabels']
    assert 'Target error range (0.15 meters)' in seen['legend'], \
        seen['legend']


@pytest.mark.usefixtures('_no_network')
def test_timeseries_target_error_annotations_carry_value_and_unit(tmp_path):
    """The error subplot's two mirror hlines used to render the same
    bare 'Target error range' with no sign, value or unit, while the
    sibling bar plot from the same run spelled all three out."""
    _write_error_ranges(tmp_path)
    prop = _make_prop(tmp_path)
    info = _info('wl', 'water_level')
    cycles = ['20260328-00:00:00', '20260328-06:00:00',
              '20260328-12:00:00', '20260328-18:00:00']
    plot_forecast_hours.make_timeseries_plots(
        _df_all(), cycles, info, prop, logging.getLogger('test'))

    html_path = os.path.join(
        prop.visuals_horizon_path,
        f'{prop.ofs}_8638901_water_level_cycle_series.html')
    assert os.path.isfile(html_path), html_path
    with open(html_path, encoding='utf-8') as fh:
        content = fh.read()
    assert 'Target error range (+0.15 meters)' in content
    assert 'Target error range (-0.15 meters)' in content
    assert '"text":"Target error range"' not in content
