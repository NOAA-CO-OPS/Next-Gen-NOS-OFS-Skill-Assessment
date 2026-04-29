"""Regression tests for summary_barplots: station-by-station RMSE and CF bars."""
from __future__ import annotations

import logging
import os
import types

import pandas as pd

from ofs_skill.visualization import summary_barplots


def _write_error_ranges(root: str) -> None:
    conf_dir = os.path.join(root, 'conf')
    os.makedirs(conf_dir, exist_ok=True)
    with open(os.path.join(conf_dir, 'error_ranges.csv'), 'w',
              encoding='utf-8') as fh:
        fh.write('name_var,X1,X2\n')
        fh.write('wl,0.15,0.5\n')
        fh.write('temp,3.0,0.5\n')
        fh.write('salt,3.5,0.5\n')
        fh.write('cu,0.26,0.5\n')


def _make_prop(tmp_path, ofs='cbofs', whichcast='nowcast', static=False):
    prop = types.SimpleNamespace()
    prop.path = str(tmp_path)
    prop.ofs = ofs
    prop.whichcast = whichcast
    prop.whichcasts = [whichcast]
    prop.ofsfiletype = 'stations'
    prop.start_date_full = '2026-03-28T00:00:00Z'
    prop.end_date_full = '2026-03-29T00:00:00Z'
    prop.forecast_hr = None
    prop.static_plots = static
    prop.data_skill_stats_path = str(tmp_path / 'data' / 'skill' / 'stats')
    prop.visuals_1d_station_path = str(tmp_path / 'data' / 'visual')
    prop.om_files = str(tmp_path / 'data' / 'visual' / '1d' / 'om')
    os.makedirs(prop.data_skill_stats_path, exist_ok=True)
    os.makedirs(prop.visuals_1d_station_path, exist_ok=True)
    os.makedirs(prop.om_files, exist_ok=True)
    return prop


def _write_wl_csv(prop, n=5):
    rows = []
    for i in range(n):
        rmse = 0.05 + 0.04 * i  # mix pass / fail against X1=0.15
        cf = 95.0 - 12.0 * i    # mix pass / fail against 90% threshold
        rows.append({
            'ID': f'8{i:06d}',
            'NODE': i,
            'obs_water_depth': 0.0,
            'mod_water_depth': 0.5,
            'rmse': rmse,
            'r': 0.9,
            'bias': 0.0,
            'bias_perc': 10.0,
            'bias_dir': '',
            'central_freq': cf,
            'central_freq_pass_fail': 'pass' if cf >= 90 else 'fail',
            'pos_outlier_freq': 0.0,
            'pos_outlier_freq_pass_fail': 'pass',
            'neg_outlier_freq': 0.0,
            'neg_outlier_freq_pass_fail': 'pass',
            'max_duration_pos_outlier': 0.0,
            'max_duration_pos_outlier_pass_fail': 'pass',
            'max_duration_neg_outlier': 0.0,
            'max_duration_neg_outlier_pass_fail': 'pass',
            'worst_case_outlier_freq': 0.0,
            'worst_case_outlier_freq_pass_fail': 'pass',
            'bias_standard_dev': 0.05,
            'target_error_range': 0.15,
            'datum': 'MLLW',
            'Y': 38.0 + 0.1 * i,
            'X': -76.5 + 0.05 * i,
            'start_date': '2026-03-28T00:00:00Z',
            'end_date': '2026-03-29T00:00:00Z',
        })
    df = pd.DataFrame(rows)
    csv_path = os.path.join(
        prop.data_skill_stats_path,
        f'skill_{prop.ofs}_water_level_{prop.whichcast}_'
        f'{prop.ofsfiletype}.csv')
    df.to_csv(csv_path)
    return csv_path


def test_make_summary_bars_writes_html(tmp_path):
    _write_error_ranges(str(tmp_path))
    prop = _make_prop(tmp_path)
    _write_wl_csv(prop)

    summary_barplots.make_summary_bars(
        prop, ['water_level', 'wl', []], logging.getLogger('test'))

    html = os.path.join(
        prop.visuals_1d_station_path,
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations.html')
    png = os.path.join(
        prop.om_files,
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations.png')
    assert os.path.isfile(html), 'HTML output missing'
    assert os.path.getsize(html) > 1000
    assert not os.path.isfile(png), \
        'PNG should be skipped when prop.static_plots is False'


def test_make_summary_bars_writes_png_when_static_plots(tmp_path):
    _write_error_ranges(str(tmp_path))
    prop = _make_prop(tmp_path, static=True)
    _write_wl_csv(prop)

    summary_barplots.make_summary_bars(
        prop, ['water_level', 'wl', []], logging.getLogger('test'))

    png = os.path.join(
        prop.om_files,
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations.png')
    assert os.path.isfile(png), 'PNG output missing'
    assert os.path.getsize(png) > 5000


def test_make_summary_bars_missing_csv_warns(tmp_path, caplog):
    _write_error_ranges(str(tmp_path))
    prop = _make_prop(tmp_path)
    # No CSV written.
    with caplog.at_level(logging.WARNING):
        summary_barplots.make_summary_bars(
            prop, ['water_level', 'wl', []], logging.getLogger('test'))
    msgs = [rec.message for rec in caplog.records]
    assert any('not found' in m for m in msgs), msgs
    # Output dirs exist but are empty (apart from any test artifacts).
    html_files = [f for f in os.listdir(prop.visuals_1d_station_path)
                  if f.endswith('.html')]
    assert html_files == []


def test_currents_xlabel_includes_depth(tmp_path):
    _write_error_ranges(str(tmp_path))
    df = pd.DataFrame({
        'ID': ['cb1401', 'cb1401', 'cb1401'],
        'obs_water_depth': [2.5, 5.5, 9.0],
    })
    labels = summary_barplots._x_labels(df, 'cu')
    assert labels == ['cb1401@2.5m', 'cb1401@5.5m', 'cb1401@9.0m']


def test_water_level_xlabel_is_plain_id(tmp_path):
    df = pd.DataFrame({
        'ID': ['8637689', '8575512'],
        'obs_water_depth': [0.0, 0.0],
    })
    assert summary_barplots._x_labels(df, 'wl') == ['8637689', '8575512']


def test_rmse_color_palette():
    colors = summary_barplots._rmse_colors([0.05, 0.20, -0.10, float('nan')],
                                           x1=0.15)
    assert colors[0] == 'palegreen'   # within ±0.15
    assert colors[1] == 'lightcoral'  # above 0.15
    assert colors[2] == 'palegreen'   # within ±0.15
    assert colors[3] == 'lightcoral'  # nan -> fail


def test_cf_color_palette():
    colors = summary_barplots._cf_colors([95.0, 89.9, 100.0, float('nan')])
    assert colors == ['palegreen', 'lightcoral', 'palegreen', 'lightcoral']


def test_html_embeds_export_filename(tmp_path):
    """The toImage button in plotly should export with the same stem as
    the HTML file, not the default ``newplot``."""
    _write_error_ranges(str(tmp_path))
    prop = _make_prop(tmp_path)
    _write_wl_csv(prop)

    summary_barplots.make_summary_bars(
        prop, ['water_level', 'wl', []], logging.getLogger('test'))

    html_path = os.path.join(
        prop.visuals_1d_station_path,
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations.html')
    with open(html_path, encoding='utf-8') as fh:
        content = fh.read()
    expected_stem = (
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations')
    assert f'"filename": "{expected_stem}"' in content, (
        'plotly toImage button is missing the matching filename config')


def test_make_summary_bars_emits_hw_lw_for_water_level(tmp_path):
    _write_error_ranges(str(tmp_path))
    prop = _make_prop(tmp_path)
    _write_wl_csv(prop)
    # Also create the hw / lw extrema CSVs.
    saved_path = os.path.join(prop.data_skill_stats_path,
                              f'skill_{prop.ofs}_water_level_'
                              f'{prop.whichcast}_{prop.ofsfiletype}.csv')
    df = pd.read_csv(saved_path)
    for suffix in ('hw', 'lw'):
        df.to_csv(os.path.join(
            prop.data_skill_stats_path,
            f'skill_{prop.ofs}_water_level_{suffix}_{prop.whichcast}_'
            f'{prop.ofsfiletype}.csv'))

    summary_barplots.make_summary_bars(
        prop, ['water_level', 'wl', []], logging.getLogger('test'))

    expected = [
        f'{prop.ofs}_summary_barplot_water_level_nowcast_stations.html',
        f'{prop.ofs}_summary_barplot_water_level_hw_nowcast_stations.html',
        f'{prop.ofs}_summary_barplot_water_level_lw_nowcast_stations.html',
    ]
    for name in expected:
        assert os.path.isfile(
            os.path.join(prop.visuals_1d_station_path, name)), name
