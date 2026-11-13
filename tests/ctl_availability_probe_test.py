"""
Tests for the ctl-stage CO-OPS currents availability probe (issue #239).

At ctl-file creation the only download-derived fact the emitter uses is
per-bin data presence, so ``retrieve_t_and_c_station(...,
availability_probe=True)`` replaces the per-bin full-window downloads
with one no-bin full-window scan (30-day chunks, early stop) plus one
short-range per-bin confirmation anchored at the known-data timestamp.

Covers:
- probe emits exactly the bins whose confirmation returns data;
- dead station rejected after C(W) chunk requests and nothing else
  (the request-count collapse is the point of the feature);
- "Wrong Bin Number" rejection (null real_time_bin) retried with an
  explicit metadata bin;
- station whose data starts mid-window is found by a later chunk
  (the bh0101 scenario — head-only sampling would false-reject);
- bin-depth-shift stations scan/anchor inside the most recent
  deployment period only;
- ``only_bins`` restricts the confirmation requests;
- ``availability_probe=False`` keeps the full-download path;
- side-looking stations run the single pinned-bin scan, no confirms;
- the ``[settings] ctl_availability_probe`` escape hatch and its
  threading through ``_process_coops_station``.

All HTTP is mocked at the ``_get_with_retry`` seam; no test touches the
real CO-OPS API.
"""

from __future__ import annotations

import importlib
import logging
import re
from unittest.mock import patch

import pandas as pd
import pytest

rtc = importlib.import_module(
    'ofs_skill.obs_retrieval.retrieve_t_and_c_station')
woc = importlib.import_module(
    'ofs_skill.obs_retrieval.write_obs_ctlfile')

API = 'https://api.example/api/prod'
MDAPI = 'https://api.example/mdapi/prod'
STATION = 'cb9999'

NO_DATA = {'error': {'message': (
    'No data was found. This product may not be offered at this '
    'station at the requested time.')}}
WRONG_BIN = {'error': {'message': (
    ' Wrong Bin Number: The Bin Number cannot be null or empty for '
    'the currents survey')}}


@pytest.fixture
def logger():
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger('ctl_availability_probe_test')


def _rows(timestamps, bin_num):
    return [
        {'t': t, 's': '50', 'd': '90', 'b': str(bin_num)}
        for t in timestamps
    ]


def _updown_bins_payload(real_time_bin=2):
    return {
        'nbr_of_bins': 3,
        'units': 'metric',
        'real_time_bin': real_time_bin,
        'bins': [
            {'num': 1, 'depth': 2.0, 'distance': 2.0, 'qc_flag': 1},
            {'num': 2, 'depth': 4.0, 'distance': 4.0, 'qc_flag': 1},
            {'num': 3, 'depth': 6.0, 'distance': 6.0, 'qc_flag': 1},
        ],
    }


def _deployment_info(orientation='up', deployments=None):
    if deployments is None:
        deployments = [
            {'deployed': '2026-01-01 00:00:00', 'retrieved': ''},
        ]
    return {
        'orientation': orientation,
        'sensor_depth': 8.5,
        'measured_depth': 9.0,
        'height_from_bottom': 0.5,
        'deployments': deployments,
    }


def _prime_caches(bins_payload, deployment_info, has_depth_shifts=False):
    """Pin all MDAPI metadata in the module caches so the only HTTP
    traffic the test sees is datagetter traffic through _get_with_retry."""
    rtc._depth_cache.clear()
    rtc._depth_cache[STATION] = bins_payload
    rtc._station_info_cache.clear()
    rtc._station_info_cache[STATION] = {'height_from_bottom': 0.5}
    rtc._station_deployment_cache.clear()
    rtc._station_deployment_cache[STATION] = deployment_info
    with rtc._bin_depth_change_cache_lock:
        rtc._bin_depth_change_cache.clear()
        rtc._bin_depth_change_cache[STATION] = has_depth_shifts


class _FakeDatagetter:
    """Recording stand-in for ``_get_with_retry``."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []  # (url, return_error_body)

    def __call__(self, url, station_id, context, logger,
                 return_error_body=False):
        self.calls.append((url, return_error_body))
        return self.handler(url)

    @property
    def urls(self):
        return [url for url, _ in self.calls]

    def scan_urls(self):
        return [u for u in self.urls if 'begin_date=' in u]

    def confirm_urls(self):
        return [u for u in self.urls if 'range=24' in u]


def _bin_of(url):
    m = re.search(r'[?&]bin=(\d+)', url)
    return int(m.group(1)) if m else None


def _run_probe(fake, logger, start_date='20260201', end_date='20260210',
               only_bins=None, availability_probe=True):
    with patch.object(rtc, '_get_with_retry', fake):
        return rtc._retrieve_currents_all_bins(
            station_id=STATION,
            start_date=start_date,
            end_date=end_date,
            api_url=API,
            mdapi_url=MDAPI,
            logger=logger,
            only_bins=only_bins,
            control_files_path=None,
            availability_probe=availability_probe,
        )


# ---------------------------------------------------------------------------
# Up/down probe: emit exactly the confirmed bins
# ---------------------------------------------------------------------------

def test_probe_emits_exactly_confirmed_bins(logger):
    _prime_caches(_updown_bins_payload(), _deployment_info())

    def handler(url):
        if 'range=24' in url:
            bin_num = _bin_of(url)
            if bin_num == 3:
                return NO_DATA
            return {'data': _rows(['2026-02-03 10:00'], bin_num)}
        # no-bin full-window scan chunk
        return {'data': _rows(['2026-02-03 10:00', '2026-02-03 10:06'], 2)}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger)

    assert isinstance(result, dict)
    assert set(result.keys()) == {1, 2}
    # attrs stamped from metadata, same as the full path
    assert result[1].attrs['depth'] == 2.0
    assert result[2].attrs['depth'] == 4.0
    assert result[1].attrs['mounting_type'] == 'up'
    assert result[1].attrs['height_from_bottom'] == 0.5
    assert result[1].attrs['bin'] == 1
    assert 'depth_unknown' not in result[1].attrs
    # 10-day window = 1 scan chunk; then one confirm per candidate bin
    assert len(fake.scan_urls()) == 1
    assert _bin_of(fake.scan_urls()[0]) is None
    assert sorted(_bin_of(u) for u in fake.confirm_urls()) == [1, 2, 3]
    # confirm anchored at the day AFTER the known-data timestamp
    assert all('end_date=20260204&range=24' in u
               for u in fake.confirm_urls())
    assert len(fake.calls) == 4


# ---------------------------------------------------------------------------
# Dead station: rejected after C(W) chunk requests, nothing else
# ---------------------------------------------------------------------------

def test_probe_dead_station_costs_only_window_chunks(logger):
    _prime_caches(_updown_bins_payload(), _deployment_info())

    fake = _FakeDatagetter(lambda url: NO_DATA)
    # 20260101-20260401 in 30-day chunks: 0101, 0131, 0302, 0401 = 4
    result = _run_probe(fake, logger, start_date='20260101',
                        end_date='20260401')

    assert result is None
    assert len(fake.calls) == 4
    assert all('begin_date=' in u for u in fake.urls)
    # never fanned out per bin, never confirmed anything
    assert all(_bin_of(u) is None for u in fake.urls)
    assert fake.confirm_urls() == []


# ---------------------------------------------------------------------------
# "Wrong Bin Number" (null real_time_bin): explicit-bin retry of the chunk
# ---------------------------------------------------------------------------

def test_probe_wrong_bin_number_falls_back_to_explicit_bin(logger):
    _prime_caches(_updown_bins_payload(real_time_bin=None),
                  _deployment_info())

    def handler(url):
        if 'range=24' in url:
            return {'data': _rows(['2026-02-02 05:00'], _bin_of(url))}
        if _bin_of(url) is None:
            return WRONG_BIN
        return {'data': _rows(['2026-02-02 05:00'], _bin_of(url))}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger)

    assert set(result.keys()) == {1, 2, 3}
    scans = fake.scan_urls()
    assert len(scans) == 2
    # first attempt is no-bin; the SAME chunk is retried with the first
    # metadata bin pinned
    assert _bin_of(scans[0]) is None
    assert _bin_of(scans[1]) == 1
    assert 'begin_date=20260201' in scans[0]
    assert 'begin_date=20260201' in scans[1]
    # scan requests opt in to seeing the API error body
    assert all(flag for url, flag in fake.calls if 'begin_date=' in url)


# ---------------------------------------------------------------------------
# Mid-window data onset (bh0101 scenario): later chunk finds the data
# ---------------------------------------------------------------------------

def test_probe_finds_mid_window_start_station(logger):
    _prime_caches(_updown_bins_payload(), _deployment_info())

    def handler(url):
        if 'range=24' in url:
            return {'data': _rows(['2026-03-05 12:00'], _bin_of(url))}
        if 'begin_date=20260302' in url:
            return {'data': _rows(['2026-03-05 12:00'], 2)}
        return NO_DATA

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger, start_date='20260101',
                        end_date='20260401')

    assert set(result.keys()) == {1, 2, 3}
    # chunks 20260101 and 20260131 were empty; scan stopped at 20260302
    # without requesting the 4th chunk
    assert [u.split('begin_date=')[1][:8] for u in fake.scan_urls()] == \
        ['20260101', '20260131', '20260302']
    assert all('end_date=20260306&range=24' in u
               for u in fake.confirm_urls())


# ---------------------------------------------------------------------------
# Bin-depth shifts: scan and confirms restricted to the latest deployment
# ---------------------------------------------------------------------------

def test_probe_restricts_to_most_recent_deployment_period(logger):
    deployments = [
        {'deployed': '2025-06-01 00:00:00',
         'retrieved': '2026-02-01 12:00:00'},
        {'deployed': '2026-03-01 00:00:00', 'retrieved': ''},
    ]
    _prime_caches(_updown_bins_payload(),
                  _deployment_info(deployments=deployments),
                  has_depth_shifts=True)

    def handler(url):
        if 'range=24' in url:
            if _bin_of(url) == 3:
                # rows exist but only OUTSIDE the most recent deployment
                # period — must not confirm the bin
                return {'data': _rows(['2026-01-15 00:00'], 3)}
            return {'data': _rows(['2026-03-02 06:00'], _bin_of(url))}
        return {'data': _rows(['2026-03-02 06:00'], 2)}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger, start_date='20260101',
                        end_date='20260401')

    assert set(result.keys()) == {1, 2}
    # the scan starts at the most recent deployment period, not the
    # window start — data in the earlier period cannot flip the station
    # to emit under the full path either
    assert 'begin_date=20260301' in fake.scan_urls()[0]
    assert all('end_date=20260303&range=24' in u
               for u in fake.confirm_urls())


# ---------------------------------------------------------------------------
# only_bins (currents-bins CSV override) restricts the confirms
# ---------------------------------------------------------------------------

def test_probe_only_bins_restricts_confirms(logger):
    _prime_caches(_updown_bins_payload(), _deployment_info())

    def handler(url):
        if 'range=24' in url:
            return {'data': _rows(['2026-02-03 10:00'], _bin_of(url))}
        return {'data': _rows(['2026-02-03 10:00'], 2)}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger, only_bins={2})

    assert set(result.keys()) == {2}
    assert [_bin_of(u) for u in fake.confirm_urls()] == [2]


# ---------------------------------------------------------------------------
# availability_probe=False keeps the full-download path
# ---------------------------------------------------------------------------

def test_probe_disabled_uses_full_per_bin_downloads(logger):
    _prime_caches(_updown_bins_payload(), _deployment_info())

    def handler(url):
        bin_num = _bin_of(url)
        # full coverage so the gap-patching machinery stays quiet
        return {'data': _rows(
            ['2026-02-01 00:00', '2026-02-09 23:54'], bin_num)}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger, availability_probe=False)

    assert set(result.keys()) == {1, 2, 3}
    # one full-window download per bin — the pre-probe behavior
    assert sorted(_bin_of(u) for u in fake.urls) == [1, 2, 3]
    assert all('begin_date=20260201' in u for u in fake.urls)
    assert fake.confirm_urls() == []
    assert len(result[1]) == 2
    assert result[1].attrs['depth'] == 2.0


# ---------------------------------------------------------------------------
# Side-looking stations: single pinned-bin scan, no confirmation pass
# ---------------------------------------------------------------------------

def _side_bins_payload(real_time_bin=17):
    return {
        'nbr_of_bins': 20,
        'units': 'metric',
        'real_time_bin': real_time_bin,
        'bins': [
            {'num': i, 'depth': None, 'distance': 5.0 + 4.0 * i,
             'qc_flag': 1}
            for i in range(1, 21)
        ],
    }


def test_probe_side_looking_scans_pinned_bin_only(logger):
    _prime_caches(_side_bins_payload(),
                  _deployment_info(orientation='side'))

    def handler(url):
        assert _bin_of(url) == 17  # never no-bin, never another bin
        return {'data': _rows(['2026-02-05 00:00'], 17)}

    fake = _FakeDatagetter(handler)
    result = _run_probe(fake, logger)

    assert set(result.keys()) == {17}
    assert result[17].attrs['depth'] == 8.5  # sensor_depth
    assert result[17].attrs['depth_unknown'] is False
    assert result[17].attrs['mounting_type'] == 'side'
    assert len(fake.calls) == 1
    assert fake.confirm_urls() == []


def test_probe_side_looking_dead_station_rejected(logger):
    _prime_caches(_side_bins_payload(),
                  _deployment_info(orientation='side'))

    fake = _FakeDatagetter(lambda url: NO_DATA)
    result = _run_probe(fake, logger, start_date='20260101',
                        end_date='20260401')

    assert result is None
    assert len(fake.calls) == 4
    assert all(_bin_of(u) == 17 for u in fake.urls)


# ---------------------------------------------------------------------------
# Config escape hatch: [settings] ctl_availability_probe
# ---------------------------------------------------------------------------

def _write_conf(tmp_path, body):
    conf = tmp_path / 'ofs_dps.conf'
    conf.write_text(body, encoding='utf-8')
    return conf


def test_ctl_availability_probe_defaults_true(tmp_path, logger):
    conf = _write_conf(tmp_path, '[settings]\nstatic_plots=False\n')
    assert woc._ctl_availability_probe_enabled(conf, logger) is True


def test_ctl_availability_probe_false_disables(tmp_path, logger):
    conf = _write_conf(
        tmp_path, '[settings]\nctl_availability_probe=False\n')
    assert woc._ctl_availability_probe_enabled(conf, logger) is False


def test_ctl_availability_probe_missing_section_defaults_true(
        tmp_path, logger):
    conf = _write_conf(tmp_path, '[directories]\nhome=.\n')
    assert woc._ctl_availability_probe_enabled(conf, logger) is True


# ---------------------------------------------------------------------------
# Threading: only the ctl-stage currents call sets availability_probe
# ---------------------------------------------------------------------------

def _probe_result_frame():
    df = pd.DataFrame({
        'DateTime': pd.to_datetime(['2026-02-03 10:00']),
        'DEP01': [4.0],
        'DIR': [90.0],
        'OBS': [0.5],
    })
    df.attrs.update({
        'bin': 2, 'depth': 4.0, 'mounting_type': 'up',
        'height_from_bottom': 0.5,
    })
    return df


def test_process_coops_station_passes_probe_flag_for_currents(logger):
    with patch.object(woc, 'retrieve_t_and_c_station',
                      return_value={2: _probe_result_frame()}) as mock_ret:
        entries = woc._process_coops_station(
            STATION, 'Test Station', -70.0, 42.0,
            '20260201', '20260210', 'currents', 'cu',
            'MLLW', ['MLLW'], 'necofs', logger,
            availability_probe=True,
        )
    assert mock_ret.call_args.kwargs['availability_probe'] is True
    assert len(entries) == 1
    assert f'{STATION}_b02' in entries[0]


def test_process_coops_station_no_probe_for_other_variables(logger):
    with patch.object(woc, 'retrieve_t_and_c_station',
                      return_value=None) as mock_ret:
        woc._process_coops_station(
            STATION, 'Test Station', -70.0, 42.0,
            '20260201', '20260210', 'water_temperature', 'temp',
            'MLLW', ['MLLW'], 'necofs', logger,
            availability_probe=True,
        )
    assert mock_ret.call_args.kwargs['availability_probe'] is False
