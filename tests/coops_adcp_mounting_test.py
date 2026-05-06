"""
Tests for CO-OPS ADCP mounting-type classification + depth assignment
(issues #140, #141).

Driven by canned MDAPI fixtures captured from production
``stations.json`` / ``deployments.json`` / ``bins.json`` for one
station of each mounting type. Covers:

* ``resolve_mounting_type`` against canonical + edge-case orientation
  strings.
* ``_resolve_side_real_time_bin`` priority (bins.json over
  deployments[0]).
* ``_resolve_side_sensor_depth`` fallback chain.
* ``_retrieve_currents_all_bins`` end-to-end for side / up / down /
  unknown / null-depth-anomaly cases.
* CTL emission carries the canonical 7th-token mounting symbol.
* Plotting label formatter normalises to human-readable strings and
  silently omits unknown / empty mounting types (the issue #141 fix —
  no more silent default-to-'up' for downward stations).
* End-of-run mounting summary log fires once and counts each type.
"""
from __future__ import annotations

import importlib
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

_FIXTURES = Path(__file__).parent / 'fixtures' / 'coops_mdapi'


def _load_fixture(stem: str) -> dict:
    path = _FIXTURES / stem
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


@pytest.fixture
def logger():
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger('coops_adcp_mounting_test')


@pytest.fixture(autouse=True)
def _reset_caches():
    """Clear module-level caches between tests."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    rtc._depth_cache.clear()
    rtc._station_info_cache.clear()
    rtc._station_deployment_cache.clear()
    rtc.reset_run_counters()
    yield
    rtc._depth_cache.clear()
    rtc._station_info_cache.clear()
    rtc._station_deployment_cache.clear()


# ---------------------------------------------------------------------------
# resolve_mounting_type
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('raw,expected', [
    ('side', 'side'),
    ('SIDE', 'side'),
    ('horizontal', 'side'),
    ('h', 'side'),
    ('up', 'up'),
    ('Upward', 'up'),
    ('U', 'up'),
    ('down', 'down'),
    ('DOWN', 'down'),
    ('Downward', 'down'),
    ('d', 'down'),
    ('vertical', 'unknown'),
    ('', 'unknown'),
    ('  ', 'unknown'),
    ('spaceship', 'unknown'),
])
def test_resolve_mounting_type(raw, expected):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    assert rtc.resolve_mounting_type({'orientation': raw}) == expected


def test_resolve_mounting_type_none_or_missing():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    assert rtc.resolve_mounting_type(None) == 'unknown'
    assert rtc.resolve_mounting_type({}) == 'unknown'
    assert rtc.resolve_mounting_type({'orientation': None}) == 'unknown'


# ---------------------------------------------------------------------------
# Real-time-bin + sensor-depth helpers (issue #140)
# ---------------------------------------------------------------------------

def test_real_time_bin_prefers_bins_endpoint_over_deployments_zero():
    """``deployments[0].real_time_bin`` is null on cb1401 / ca0101 but the
    top-level ``bins.json`` carries the correct value (30 / 14). The
    helper must prefer the bins-endpoint source."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    bins = _load_fixture('cb1401_bins.json')
    dep = _load_fixture('cb1401_deployments.json')
    assert dep['deployments'][0]['real_time_bin'] is None
    assert bins['real_time_bin'] == 30
    assert rtc._resolve_side_real_time_bin(bins, dep) == 30

    bins_ca = _load_fixture('ca0101_bins.json')
    dep_ca = _load_fixture('ca0101_deployments.json')
    assert rtc._resolve_side_real_time_bin(bins_ca, dep_ca) == 14


def test_real_time_bin_falls_back_to_deployments_zero():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    bins = {'real_time_bin': None}
    dep = {'deployments': [{'real_time_bin': 7}]}
    assert rtc._resolve_side_real_time_bin(bins, dep) == 7


def test_real_time_bin_returns_none_when_unavailable():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    assert rtc._resolve_side_real_time_bin({}, {'deployments': []}) is None
    assert rtc._resolve_side_real_time_bin(None, None) is None


def test_sensor_depth_prefers_explicit_field():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    dep = _load_fixture('ca0101_deployments.json')
    assert rtc._resolve_side_sensor_depth(dep) == 4.0


def test_sensor_depth_falls_back_to_measured_depth_minus_hfb():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    dep = {
        'sensor_depth': None,
        'measured_depth': 17.0,
        'height_from_bottom': 10.3,
    }
    assert rtc._resolve_side_sensor_depth(dep) == pytest.approx(6.7)


def test_sensor_depth_returns_none_when_all_fields_missing():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    assert rtc._resolve_side_sensor_depth(None) is None
    assert rtc._resolve_side_sensor_depth({}) is None


def test_sensor_depth_rejects_negative_explicit_value():
    """``hb0401`` / ``nl0101`` carry negative ``measured_depth`` in
    MDAPI; the helper must NOT propagate a negative depth into
    ``DEP01`` because ``index_nearest_depth`` would silently flip the
    pairing to the shallowest model layer."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    # Explicit negative sensor_depth is rejected; pipeline routes to
    # the bathymetry-based fallback in _resolve_side_looking_depths.
    assert rtc._resolve_side_sensor_depth(
        {'sensor_depth': -3.0}) is None


def test_sensor_depth_rejects_negative_fallback():
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    # measured_depth < 0: physically impossible (depth-below-surface
    # is positive), reject and route to bathymetry-based fallback.
    assert rtc._resolve_side_sensor_depth({
        'sensor_depth': None,
        'measured_depth': -7.2,
        'height_from_bottom': 2.4,
    }) is None
    # measured_depth < height_from_bottom: sensor would be above the
    # surface, reject.
    assert rtc._resolve_side_sensor_depth({
        'sensor_depth': None,
        'measured_depth': 2.0,
        'height_from_bottom': 5.0,
    }) is None
    # height_from_bottom < 0: nonsensical, reject.
    assert rtc._resolve_side_sensor_depth({
        'sensor_depth': None,
        'measured_depth': 10.0,
        'height_from_bottom': -1.0,
    }) is None


def test_real_time_bin_prefers_active_deployment_over_index_zero():
    """When ``deployments`` carries multiple historical entries with
    ``retrieved`` timestamps, prefer the active one (``retrieved ==
    ''``) — otherwise we'd be reading a 20-year-old install's bin
    number from the chronologically-first entry."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    deployment_info = {
        'orientation': 'side',
        'sensor_depth': 5.0,
        'deployments': [
            # Old, retrieved.
            {'real_time_bin': 5, 'retrieved': '2010-01-01 00:00:00'},
            # Active.
            {'real_time_bin': 12, 'retrieved': ''},
            # Even older, retrieved.
            {'real_time_bin': 7, 'retrieved': '2005-01-01 00:00:00'},
        ],
    }
    # bins.json has no rtb -> deployments path runs.
    assert rtc._resolve_side_real_time_bin(
        {'real_time_bin': None}, deployment_info) == 12


# ---------------------------------------------------------------------------
# End-to-end retrieval per mounting type
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    def json(self):
        return self._payload


def _retrieve_input(station_id):
    return SimpleNamespace(
        station=station_id,
        start_date='20250101',
        end_date='20250102',
        variable='currents',
        datum='MLLW',
    )


def _build_currents_payload(rows):
    return {'data': [
        {'t': row[0], 's': row[1], 'd': row[2], 'b': str(row[3])}
        for row in rows
    ]}


def _wire_caches(rtc, station_id, deployment, bins_payload, station_info):
    rtc._depth_cache[station_id] = bins_payload
    rtc._station_deployment_cache[station_id] = deployment
    rtc._station_info_cache[station_id] = station_info


def _run_currents(rtc, station_id, datagetter_router, logger):
    fake_urls = {
        'co_ops_mdapi_base_url': 'https://api.example/mdapi/prod',
        'co_ops_api_base_url': 'https://api.example/api/prod',
    }

    class _StubUtils:
        def read_config_section(self, section, _logger):
            return fake_urls

    with patch.object(rtc.utils, 'Utils', return_value=_StubUtils()), \
            patch.object(rtc, '_get_session') as mock_session:
        mock_session.return_value.get.side_effect = datagetter_router
        return rtc.retrieve_t_and_c_station(
            _retrieve_input(station_id), logger)


def test_side_looking_collapses_to_real_time_bin(logger):
    """cb1401: 48 bin records, all depth=null. Expected: 1 frame at
    real_time_bin=30 with depth=sensor_depth=6.7 m."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = _load_fixture('cb1401_deployments.json')
    bins_payload = _load_fixture('cb1401_bins.json')
    station_info = _load_fixture('cb1401_station.json')['stations'][0]
    _wire_caches(
        rtc, 'cb1401', deployment, bins_payload, station_info)

    rows = _build_currents_payload([
        ('2025-01-01 00:00', '50', '90', '30'),
        ('2025-01-01 00:06', '52', '91', '30'),
    ])

    def _router(url, timeout=120):
        return _FakeResponse(rows)

    result = _run_currents(rtc, 'cb1401', _router, logger)
    assert isinstance(result, dict)
    assert list(result.keys()) == [30]
    df = result[30]
    assert df.attrs['mounting_type'] == 'side'
    assert df.attrs['orientation'] == 'side'
    assert df.attrs['bin'] == 30
    assert df.attrs['depth'] == pytest.approx(6.7)
    assert df.attrs['depth_unknown'] is False
    assert df.attrs['height_from_bottom'] == pytest.approx(10.3)
    assert df['DEP01'].tolist() == pytest.approx([6.7] * len(df))


def test_side_looking_falls_back_to_measured_minus_hfb(logger):
    """When sensor_depth is missing, depth resolves to
    measured_depth - height_from_bottom."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = {
        'orientation': 'side',
        'sensor_depth': None,
        'measured_depth': 17.0,
        'height_from_bottom': 10.3,
        'deployments': [{'real_time_bin': None}],
    }
    bins_payload = {'real_time_bin': 30, 'bins': [
        {'num': i, 'depth': None, 'distance': 5 + i}
        for i in range(1, 49)
    ]}
    _wire_caches(rtc, 'cb1401', deployment, bins_payload, {})

    rows = _build_currents_payload([
        ('2025-01-01 00:00', '50', '90', '30'),
    ])

    def _router(url, timeout=120):
        return _FakeResponse(rows)

    result = _run_currents(rtc, 'cb1401', _router, logger)
    assert list(result.keys()) == [30]
    assert result[30].attrs['depth'] == pytest.approx(6.7)
    assert result[30].attrs['depth_unknown'] is False


def test_side_looking_no_real_time_bin_defaults_to_one(logger, caplog):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = {
        'orientation': 'side',
        'sensor_depth': 5.0,
        'measured_depth': None,
        'height_from_bottom': None,
        'deployments': [{'real_time_bin': None}],
    }
    bins_payload = {'real_time_bin': None, 'bins': [
        {'num': 1, 'depth': None, 'distance': 5.0},
    ]}
    _wire_caches(rtc, 'sx0001', deployment, bins_payload, {})

    rows = _build_currents_payload([
        ('2025-01-01 00:00', '50', '90', '1'),
    ])

    def _router(url, timeout=120):
        return _FakeResponse(rows)

    with caplog.at_level(logging.WARNING):
        result = _run_currents(rtc, 'sx0001', _router, logger)
    assert list(result.keys()) == [1]
    assert any('no real_time_bin' in r.message
               for r in caplog.records)


def test_side_looking_depth_unknown_when_all_fields_missing(logger):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = {
        'orientation': 'side',
        'sensor_depth': None,
        'measured_depth': None,
        'height_from_bottom': None,
        'deployments': [{'real_time_bin': None}],
    }
    bins_payload = {'real_time_bin': 5, 'bins': [
        {'num': i, 'depth': None, 'distance': i}
        for i in range(1, 11)
    ]}
    _wire_caches(rtc, 'sx0002', deployment, bins_payload, {})

    rows = _build_currents_payload([
        ('2025-01-01 00:00', '50', '90', '5'),
    ])

    def _router(url, timeout=120):
        return _FakeResponse(rows)

    result = _run_currents(rtc, 'sx0002', _router, logger)
    assert result[5].attrs['depth'] == 0.0
    assert result[5].attrs['depth_unknown'] is True


def test_upward_looking_keeps_per_bin_fanout(logger):
    """n03020: 9 bin records with descending depths (bin 1 deepest).
    Expected: 9 frames; orientation propagates from deployment."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = _load_fixture('n03020_deployments.json')
    bins_payload = _load_fixture('n03020_bins.json')
    station_info = _load_fixture('n03020_station.json')['stations'][0]
    _wire_caches(rtc, 'n03020', deployment, bins_payload, station_info)

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        bn = int(m.group(1)) if m else 0
        return _FakeResponse(_build_currents_payload([
            ('2025-01-01 00:00', '20', '90', str(bn)),
        ]))

    result = _run_currents(rtc, 'n03020', _router, logger)
    assert isinstance(result, dict)
    assert len(result) == 9
    for bn, df in result.items():
        assert df.attrs['mounting_type'] == 'up'
        assert df.attrs['orientation'] == 'up'
        # Depth from MDAPI per-bin record (issue #141 root cause: this
        # was already correct; what was broken was the orientation).
        assert df.attrs['depth'] is not None
    # Bin 1 is deepest (sensor near bottom looking up); bin 9 nearest
    # surface. depths from fixture: 9.94 ... 1.92.
    assert result[1].attrs['depth'] == pytest.approx(9.94)
    assert result[9].attrs['depth'] == pytest.approx(1.92)


def test_downward_looking_keeps_per_bin_fanout_with_correct_orientation(
    logger,
):
    """cb0102: 15 bin records with ascending depths. Expected: 15
    frames stamped with mounting_type='down' (NOT 'up' as before)."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = _load_fixture('cb0102_deployments.json')
    bins_payload = _load_fixture('cb0102_bins.json')
    station_info = _load_fixture('cb0102_station.json')['stations'][0]
    _wire_caches(rtc, 'cb0102', deployment, bins_payload, station_info)

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        bn = int(m.group(1)) if m else 0
        return _FakeResponse(_build_currents_payload([
            ('2025-01-01 00:00', '15', '90', str(bn)),
        ]))

    result = _run_currents(rtc, 'cb0102', _router, logger)
    assert len(result) == 15
    for df in result.values():
        assert df.attrs['mounting_type'] == 'down'
        assert df.attrs['orientation'] == 'down'
    # Bin 1 nearest surface; bin 15 deepest.
    assert result[1].attrs['depth'] == pytest.approx(3.51)
    assert result[15].attrs['depth'] == pytest.approx(17.53)


def test_deployment_outage_uses_bins_signature_to_detect_side(
    logger, caplog,
):
    """When the deployment endpoint is unreachable but the bins
    endpoint reports null per-bin depths, treat as side-looking — the
    null-depth pattern is the side-looking signature confirmed across
    all 40 surveyed side stations. Without this safety net a
    transient deployment-endpoint outage silently re-introduces the
    48-bin zero-depth fan-out that issue #140 was filed to fix."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    # No deployment_info on purpose — simulates the outage.
    bins_payload = {
        'real_time_bin': 30,
        'bins': [
            {'num': i, 'depth': None, 'distance': 5.0 + 4 * i}
            for i in range(1, 11)
        ],
    }
    rtc._depth_cache['cb1401'] = bins_payload
    rtc._station_info_cache['cb1401'] = {}
    # Explicitly simulate the outage by NOT pre-populating the
    # deployment cache; get_station_deployment falls through to
    # _get_with_retry which returns None when the endpoint is down.

    rows = _build_currents_payload([
        ('2025-01-01 00:00', '50', '90', '30'),
    ])

    def _router(url, timeout=120):
        if 'deployments.json' in url:
            # Simulate a 5xx that exhausts the retry budget.
            return _FakeResponse({'error': 'gateway timeout'},
                                 status_code=503)
        return _FakeResponse(rows)

    # Short-circuit retries so the test runs fast.
    with patch.object(rtc, '_RETRY_MAX_ATTEMPTS', 1), \
            patch.object(rtc, '_RETRY_BASE_DELAY', 0), \
            caplog.at_level(logging.WARNING):
        result = _run_currents(rtc, 'cb1401', _router, logger)
    assert result is not None
    assert list(result.keys()) == [30], (
        'Side-looking signature must collapse to one bin even when '
        'deployment endpoint is down.'
    )
    assert result[30].attrs['mounting_type'] == 'side'
    assert any(
        'bins-endpoint signature' in r.message for r in caplog.records
    )


def test_side_looking_multi_bin_override_emits_each(logger, caplog):
    """User CSV pinning multiple bins on a side-looking station emits
    each as a separate virtual station and logs WARNING that all
    share the same sensor_depth (since side ADCPs sample one depth)."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = _load_fixture('cb1401_deployments.json')
    bins_payload = _load_fixture('cb1401_bins.json')
    station_info = _load_fixture('cb1401_station.json')['stations'][0]
    _wire_caches(
        rtc, 'cb1401', deployment, bins_payload, station_info)

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        bn = int(m.group(1)) if m else 30
        return _FakeResponse(_build_currents_payload([
            ('2025-01-01 00:00', '50', '90', str(bn)),
        ]))

    only_bins = {30, 31, 32}

    fake_urls = {
        'co_ops_mdapi_base_url': 'https://api.example/mdapi/prod',
        'co_ops_api_base_url': 'https://api.example/api/prod',
    }

    class _StubUtils:
        def read_config_section(self, section, _logger):
            return fake_urls

    with patch.object(rtc.utils, 'Utils', return_value=_StubUtils()), \
            patch.object(rtc, '_get_session') as mock_session, \
            caplog.at_level(logging.WARNING):
        mock_session.return_value.get.side_effect = _router
        result = rtc.retrieve_t_and_c_station(
            _retrieve_input('cb1401'), logger, only_bins=only_bins)

    assert isinstance(result, dict)
    assert set(result.keys()) == only_bins
    # All bins share the same sensor_depth.
    for df in result.values():
        assert df.attrs['depth'] == pytest.approx(6.7)
        assert df.attrs['mounting_type'] == 'side'
    # WARNING about same-depth multi-bin output fires.
    assert any(
        'samples a single depth' in r.message
        for r in caplog.records
    )


def test_unknown_orientation_logs_warning_and_continues(logger, caplog):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = {
        'orientation': 'spaceship',
        'sensor_depth': 5.0,
        'measured_depth': 10.0,
        'height_from_bottom': 0.0,
        'deployments': [{'real_time_bin': None}],
    }
    bins_payload = {
        'real_time_bin': 1,
        'bins': [
            {'num': 1, 'depth': 3.0, 'distance': None},
            {'num': 2, 'depth': 5.0, 'distance': None},
        ],
    }
    _wire_caches(rtc, 'qq0001', deployment, bins_payload, {})

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        bn = int(m.group(1)) if m else 1
        return _FakeResponse(_build_currents_payload([
            ('2025-01-01 00:00', '20', '90', str(bn)),
        ]))

    with caplog.at_level(logging.WARNING):
        result = _run_currents(rtc, 'qq0001', _router, logger)
    assert any('unrecognized orientation' in r.message
               for r in caplog.records)
    assert len(result) == 2
    for df in result.values():
        assert df.attrs['mounting_type'] == 'unknown'


def test_down_with_null_bin_depth_logs_error(logger, caplog):
    """A null per-bin depth on a non-side ADCP is anomalous (every
    surveyed up/down station has populated depths). Code logs ERROR
    and emits the bin with depth=0 — distinct from the side-looking
    happy path."""
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

    deployment = {
        'orientation': 'down',
        'sensor_depth': 2.0,
        'measured_depth': 10.0,
        'height_from_bottom': 0.0,
        'deployments': [{'real_time_bin': None}],
    }
    bins_payload = {'real_time_bin': 1, 'bins': [
        {'num': 1, 'depth': 3.0, 'distance': None},
        {'num': 2, 'depth': None, 'distance': None},
    ]}
    _wire_caches(rtc, 'dn0001', deployment, bins_payload, {})

    def _router(url, timeout=120):
        m = re.search(r'[?&]bin=(\d+)', url)
        bn = int(m.group(1)) if m else 1
        return _FakeResponse(_build_currents_payload([
            ('2025-01-01 00:00', '20', '90', str(bn)),
        ]))

    with caplog.at_level(logging.ERROR):
        result = _run_currents(rtc, 'dn0001', _router, logger)

    err_records = [r for r in caplog.records if r.levelname == 'ERROR']
    assert any('returned no depth' in r.message for r in err_records)
    assert result[2].attrs['depth'] == 0.0


# ---------------------------------------------------------------------------
# CTL emission carries the mounting symbol
# ---------------------------------------------------------------------------

def _build_df(depth, bin_num, mounting_type, orientation, hfb):
    df = pd.DataFrame({
        'DateTime': pd.to_datetime(['2025-01-01 00:00']),
        'DEP01': [depth],
        'DIR': [90.0],
        'OBS': [0.5],
    })
    df.attrs['depth'] = depth
    df.attrs['bin'] = bin_num
    df.attrs['orientation'] = orientation
    df.attrs['mounting_type'] = mounting_type
    df.attrs['height_from_bottom'] = hfb
    return df


def test_ctl_orientation_override_canonicalises_to_mounting_token(
    logger,
):
    """BinSpec.orientation from the user CSV is the canonical user
    intent for the mounting symbol; emit it through
    canonicalize_mounting_symbol so a free-form value like
    ``Side-Looking`` still writes a clean ``side`` token."""
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')
    cbo = importlib.import_module(
        'ofs_skill.obs_retrieval.currents_bins_override')

    df = _build_df(6.5, 5, 'up', 'up', 0.0)
    overrides = {5: cbo.BinSpec(
        bin=5, depth=None,
        orientation='Side-Looking',  # legacy free-form
        name=None,
    )}
    with patch.object(
            woc, 'retrieve_t_and_c_station', return_value={5: df}):
        entries = woc._process_coops_station(
            id_number='cb0102',
            name='Cape Henry',
            x_value=-76.013,
            y_value=36.959,
            start_date='20250101',
            end_date='20250102',
            variable='currents',
            name_var='cu',
            datum='MLLW',
            datum_list=['NAVD', 'MLLW'],
            ofs='cbofs',
            logger=logger,
            bin_overrides=overrides,
        )
    assert len(entries) == 1
    assert entries[0].rstrip().endswith('side')
    # Garbage value still funnels to 'unknown'.
    overrides_garbage = {5: cbo.BinSpec(
        bin=5, depth=None,
        orientation='spaceship',
        name=None,
    )}
    df = _build_df(6.5, 5, 'up', 'up', 0.0)
    with patch.object(
            woc, 'retrieve_t_and_c_station', return_value={5: df}):
        entries = woc._process_coops_station(
            id_number='cb0102',
            name='Cape Henry',
            x_value=-76.013,
            y_value=36.959,
            start_date='20250101',
            end_date='20250102',
            variable='currents',
            name_var='cu',
            datum='MLLW',
            datum_list=['NAVD', 'MLLW'],
            ofs='cbofs',
            logger=logger,
            bin_overrides=overrides_garbage,
        )
    assert entries[0].rstrip().endswith('unknown')


def test_ctl_emits_mounting_token_for_each_type(logger):
    woc = importlib.import_module(
        'ofs_skill.obs_retrieval.write_obs_ctlfile')

    cases = [
        ('side', 'side', 6.7, 30, 10.3, ' 6.70  0.0  10.30  side'),
        ('up', 'up', 9.94, 1, 0.9, ' 9.94  0.0  0.90  up'),
        ('down', 'down', 3.51, 1, 0.0, ' 3.51  0.0  0.00  down'),
        ('unknown', '', 5.0, 1, 0.0, ' 5.00  0.0  0.00  unknown'),
    ]
    for mounting, orient, depth, bin_num, hfb, expected_substr in cases:
        df = _build_df(depth, bin_num, mounting, orient, hfb)
        with patch.object(
                woc, 'retrieve_t_and_c_station', return_value={bin_num: df}):
            entries = woc._process_coops_station(
                id_number='cb0102',
                name='Cape Henry',
                x_value=-76.013,
                y_value=36.959,
                start_date='20250101',
                end_date='20250102',
                variable='currents',
                name_var='cu',
                datum='MLLW',
                datum_list=['NAVD', 'MLLW'],
                ofs='cbofs',
                logger=logger,
            )
        assert len(entries) == 1, f'mounting={mounting}'
        assert expected_substr in entries[0], (
            f'mounting={mounting}: expected {expected_substr!r} in '
            f'{entries[0]!r}'
        )


# ---------------------------------------------------------------------------
# Plot-title formatter (issue #141 regression — no silent default-to-up)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('mounting,expected_label', [
    ('side', 'Side-Looking ADCP'),
    ('up', 'Upward-Looking ADCP (bottom-mounted)'),
    ('down', 'Downward-Looking ADCP (top-mounted)'),
    ('unknown', ''),
    ('', ''),
])
def test_orientation_label_formatter(mounting, expected_label):
    pf = importlib.import_module(
        'ofs_skill.visualization.plotting_functions')
    assert pf._format_adcp_orientation_label(mounting) == expected_label


def test_obs_ctl_round_trip_with_mounting_token(tmp_path):
    """Write an obs station.ctl with the new 7th token and confirm the
    parser surfaces it on read."""
    pf = importlib.import_module(
        'ofs_skill.visualization.plotting_functions')
    pf._OBS_CTL_CACHE.clear()
    ctl = tmp_path / 'cbofs_cu_station.ctl'
    ctl.write_text(
        'cb0102_b04 cb0102_b04_cu_cbofs_CO-OPS "Cape Henry (bin 04)"\n'
        '  36.959 -76.013 0.0  6.51  0.0  0.00  down\n'
        'cb1401_b30 cb1401_b30_cu_cbofs_CO-OPS '
        '"Newport News (bin 30)"\n'
        '  36.984 -76.444 0.0  6.70  0.0  10.30  side\n'
    )
    table = pf._load_obs_station_depths(str(ctl))
    assert table['cb0102_b04'] == (6.51, 0.0, 'down')
    assert table['cb1401_b30'] == (6.70, 10.3, 'side')


def test_obs_ctl_legacy_no_seventh_token(tmp_path):
    """CTL files written before this change have no 7th token; the
    parser must default mounting to '' so legacy plots gracefully omit
    the ADCP-type line rather than crash or mislabel."""
    pf = importlib.import_module(
        'ofs_skill.visualization.plotting_functions')
    pf._OBS_CTL_CACHE.clear()
    ctl = tmp_path / 'legacy_cu_station.ctl'
    ctl.write_text(
        '8454000_b02 8454000_b02_cu_cbofs_CO-OPS "Providence (bin 02)"\n'
        '  41.807 -71.401 0.0  -4.00  0.0  0.50\n'
    )
    table = pf._load_obs_station_depths(str(ctl))
    assert table['8454000_b02'] == (-4.0, 0.5, '')


# ---------------------------------------------------------------------------
# End-of-run mounting summary
# ---------------------------------------------------------------------------

def test_emit_mounting_summary_counts_each_type(logger, caplog):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    rtc.reset_run_counters()
    rtc._record_mounting('side')
    rtc._record_mounting('side')
    rtc._record_mounting('up')
    rtc._record_mounting('down')
    rtc._record_mounting('down')
    rtc._record_mounting('down')
    with caplog.at_level(logging.INFO):
        rtc.emit_adcp_mounting_summary(logger)
    msgs = [r.message for r in caplog.records]
    assert any('ADCP mounting summary: 6 total (2 side, 1 up, 3 down, '
               '0 unknown)' in m for m in msgs)


def test_emit_mounting_summary_promotes_unknown_to_warning(
    logger, caplog,
):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    rtc.reset_run_counters()
    rtc._record_mounting('up')
    rtc._record_mounting('unknown')
    rtc._record_mounting('not-a-type')  # also funnels to unknown
    with caplog.at_level(logging.WARNING):
        rtc.emit_adcp_mounting_summary(logger)
    warns = [r for r in caplog.records if r.levelname == 'WARNING']
    assert any('2 unknown' in r.message for r in warns)


def test_emit_mounting_summary_silent_when_no_currents(logger, caplog):
    rtc = importlib.import_module(
        'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
    rtc.reset_run_counters()
    with caplog.at_level(logging.INFO):
        rtc.emit_adcp_mounting_summary(logger)
    assert not any(
        'ADCP mounting summary' in r.message for r in caplog.records)
