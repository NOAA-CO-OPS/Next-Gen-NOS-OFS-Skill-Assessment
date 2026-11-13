"""
Tests for the station-to-node distance annotation in 1D plot titles.

The distance shown next to the node ID in ``get_title`` /
``get_title_static`` is resolved at plot time from the obs station.ctl
(station coordinates) and the model ctl (matched node/point
coordinates) via ``plotting_functions.get_station_node_distance_km``.
These tests cover the haversine math, both ctl coordinate parsers, the
end-to-end fragment builder, and its graceful degradation when the ctl
files are missing or the station is unknown.
"""
import logging
from types import SimpleNamespace

import pytest

from ofs_skill.visualization import make_static_plots
from ofs_skill.visualization import plotting_functions as pf

LOGGER = logging.getLogger(__name__)


def _write_obs_ctl(path, entries):
    """Write a minimal 2-line-per-station obs station.ctl."""
    lines = []
    for sid, lat, lon in entries:
        lines.append(f'{sid} {sid}_wl_test_CO-OPS "Station {sid}"')
        lines.append(f'  {lat:.3f} {lon:.3f} 0  0.0  MLLW')
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def _write_model_ctl(path, entries):
    """Write a minimal <node> <layer> <lat> <lon> <id> <shift> model ctl."""
    lines = [
        f'{node} 0 {lat:.3f}  {lon:.3f}  {sid}  0.0'
        for sid, node, lat, lon in entries
    ]
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def _prop(ctl_dir, ofs='cbofs'):
    return SimpleNamespace(ofs=ofs, control_files_path=str(ctl_dir))


class TestHaversine:
    def test_one_degree_latitude(self):
        # 1 degree of latitude on a 6371 km sphere is ~111.19 km.
        assert pf._haversine_km(37.0, -76.0, 38.0, -76.0) == \
            pytest.approx(111.19, abs=0.05)

    def test_zero_distance(self):
        assert pf._haversine_km(45.0, -120.0, 45.0, -120.0) == 0.0

    def test_longitude_convention_mix(self):
        # -76.0 and 284.0 are the same meridian; distance must be ~0
        # even when the two ctl files use different lon conventions.
        assert pf._haversine_km(37.0, -76.0, 37.0, 284.0) == \
            pytest.approx(0.0, abs=1e-6)

    def test_symmetry(self):
        d_ab = pf._haversine_km(43.6, -70.2, 43.7, -70.1)
        d_ba = pf._haversine_km(43.7, -70.1, 43.6, -70.2)
        assert d_ab == pytest.approx(d_ba)


class TestCtlCoordParsers:
    def test_model_ctl_parse(self, tmp_path):
        ctl = tmp_path / 'cbofs_wl_model_station.ctl'
        _write_model_ctl(ctl, [('8637689', 45, 37.229, -76.478)])
        coords = pf._load_model_node_coords(str(ctl))
        assert coords['8637689'] == pytest.approx((37.229, -76.478))

    def test_obs_ctl_parse(self, tmp_path):
        ctl = tmp_path / 'cbofs_wl_station.ctl'
        _write_obs_ctl(ctl, [('8637689', 37.227, -76.480)])
        coords = pf._load_obs_station_coords(str(ctl))
        assert coords['8637689'] == pytest.approx((37.227, -76.480))

    def test_missing_files_yield_empty(self, tmp_path):
        assert pf._load_model_node_coords(
            str(tmp_path / 'nope_model.ctl')) == {}
        assert pf._load_obs_station_coords(
            str(tmp_path / 'nope_station.ctl')) == {}

    def test_malformed_lines_skipped(self, tmp_path):
        ctl = tmp_path / 'bad_wl_model_station.ctl'
        ctl.write_text(
            'garbage\n'
            '12 0 not_a_lat not_a_lon 999 0.0\n'
            '45 0 37.229  -76.478  8637689  0.4\n',
            encoding='utf-8')
        coords = pf._load_model_node_coords(str(ctl))
        assert list(coords) == ['8637689']


class TestDistanceResolution:
    def test_known_distance(self, tmp_path):
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.0, -76.0)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 38.0, -76.0)])
        dist = pf.get_station_node_distance_km(
            _prop(tmp_path), '8637689', 'wl')
        assert dist == pytest.approx(111.19, abs=0.05)

    def test_fields_ctl_fallback(self, tmp_path):
        # Only the fields-file naming ({ofs}_{var}_model.ctl) exists.
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.0, -76.0)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model.ctl',
            [('8637689', 45, 37.0, -76.1)])
        dist = pf.get_station_node_distance_km(
            _prop(tmp_path), '8637689', 'wl')
        assert dist == pytest.approx(8.88, abs=0.05)

    def test_unknown_station_returns_none(self, tmp_path):
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.0, -76.0)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.0, -76.1)])
        assert pf.get_station_node_distance_km(
            _prop(tmp_path), '0000000', 'wl') is None

    def test_missing_ctl_dir_returns_none(self):
        prop = SimpleNamespace(ofs='cbofs')
        assert pf.get_station_node_distance_km(
            prop, '8637689', 'wl') is None


class TestTitleFragment:
    def test_fragment_format(self, tmp_path):
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.227, -76.480)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.229, -76.478)])
        frag = pf.build_node_dist_fragment(
            _prop(tmp_path), ['8637689', 'Name', 'CO-OPS'], 'wl', LOGGER)
        assert frag == '&nbsp;(0.3&nbsp;km&nbsp;from&nbsp;station)'

    def test_sub_100m_reads_less_than(self, tmp_path):
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.229, -76.478)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.229, -76.478)])
        frag = pf.build_node_dist_fragment(
            _prop(tmp_path), ['8637689', 'Name', 'CO-OPS'], 'wl', LOGGER)
        assert frag == '&nbsp;(&lt;0.1&nbsp;km&nbsp;from&nbsp;station)'

    def test_unresolvable_yields_empty(self, tmp_path):
        frag = pf.build_node_dist_fragment(
            _prop(tmp_path), ['8637689', 'Name', 'CO-OPS'], 'wl', LOGGER)
        assert frag == ''

    def test_get_title_carries_distance(self, tmp_path):
        # NDBC source skips the CO-OPS NWS-ID network lookup, keeping
        # the test offline.
        _write_obs_ctl(
            tmp_path / 'cbofs_temp_station.ctl',
            [('44042', 38.0, -76.4)])
        _write_model_ctl(
            tmp_path / 'cbofs_temp_model_station.ctl',
            [('44042', 12, 38.01, -76.4)])
        prop = SimpleNamespace(
            ofs='cbofs',
            control_files_path=str(tmp_path),
            start_date_full='2026-07-01T00:00:00Z',
            end_date_full='2026-07-02T00:00:00Z',
        )
        title = pf.get_title(
            prop, '12', ('44042', 'Buoy', 'NDBC'), 'temp', LOGGER)
        assert 'Node ID:&nbsp;12&nbsp;(1.1&nbsp;km&nbsp;from&nbsp;station)' \
            in title

    def test_get_title_static_carries_distance(self, tmp_path):
        _write_obs_ctl(
            tmp_path / 'cbofs_temp_station.ctl',
            [('44042', 38.0, -76.4)])
        _write_model_ctl(
            tmp_path / 'cbofs_temp_model_station.ctl',
            [('44042', 12, 38.01, -76.4)])
        prop = SimpleNamespace(
            ofs='cbofs',
            control_files_path=str(tmp_path),
            start_date_full='2026-07-01T00:00:00Z',
            end_date_full='2026-07-02T00:00:00Z',
        )
        title = make_static_plots.get_title_static(
            prop, '12', ('44042', 'Buoy', 'NDBC'), 'temp', LOGGER)
        assert 'Node ID: 12 (1.1 km from station)' in title


class TestFiletypeAwareLookup:
    def _both_ctls(self, tmp_path):
        # Same station, deliberately different model coordinates in the
        # stations-run ctl (~0.22 km away) vs the fields-run ctl
        # (~2.22 km away), so the resolved distance reveals which file
        # was consulted.
        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.000, -76.000)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.002, -76.000)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model.ctl',
            [('8637689', 45, 37.020, -76.000)])

    def test_stations_run_prefers_model_station_ctl(self, tmp_path):
        self._both_ctls(tmp_path)
        prop = SimpleNamespace(
            ofs='cbofs', control_files_path=str(tmp_path),
            ofsfiletype='stations')
        dist = pf.get_station_node_distance_km(prop, '8637689', 'wl')
        assert dist == pytest.approx(0.22, abs=0.02)

    def test_fields_run_prefers_model_ctl(self, tmp_path):
        # A fields run in a directory where a stations run also left its
        # ctl file must resolve against the fields ctl, not the stale
        # stations one.
        self._both_ctls(tmp_path)
        prop = SimpleNamespace(
            ofs='cbofs', control_files_path=str(tmp_path),
            ofsfiletype='fields')
        dist = pf.get_station_node_distance_km(prop, '8637689', 'wl')
        assert dist == pytest.approx(2.22, abs=0.02)


class TestCoordCacheLifecycle:
    def test_invalidation_hook_refreshes_obs_coords(self, tmp_path):
        # The obs station.ctl can be rewritten mid-pipeline (side-looking
        # ADCP depth back-patch); the invalidation hook must refresh the
        # coordinate cache along with the depth cache.
        obs_path = tmp_path / 'cbofs_wl_station.ctl'
        _write_obs_ctl(obs_path, [('8637689', 37.000, -76.000)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.000, -76.000)])
        prop = _prop(tmp_path)

        assert pf.get_station_node_distance_km(
            prop, '8637689', 'wl') == pytest.approx(0.0, abs=1e-6)

        _write_obs_ctl(obs_path, [('8637689', 37.010, -76.000)])
        pf._invalidate_obs_station_depths(str(obs_path))
        assert pf.get_station_node_distance_km(
            prop, '8637689', 'wl') == pytest.approx(1.11, abs=0.02)

    def test_missing_file_is_not_cached(self, tmp_path):
        # A parse attempted before the ctl file exists must not pin an
        # empty result: once the file appears, the distance resolves.
        prop = _prop(tmp_path)
        assert pf.get_station_node_distance_km(prop, '8637689', 'wl') is None

        _write_obs_ctl(
            tmp_path / 'cbofs_wl_station.ctl',
            [('8637689', 37.000, -76.000)])
        _write_model_ctl(
            tmp_path / 'cbofs_wl_model_station.ctl',
            [('8637689', 45, 37.000, -76.000)])
        assert pf.get_station_node_distance_km(
            prop, '8637689', 'wl') == pytest.approx(0.0, abs=1e-6)
