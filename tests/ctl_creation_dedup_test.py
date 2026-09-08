"""
Tests for cross-thread dedup of station ctl file creation and the
per-run cache of the bin-depth-change MDAPI audit (issue #239).

``write_obs_ctlfile`` builds ctl files for ALL variables in one pass,
so under parallel_variables every variable thread that finds its own
ctl file missing would launch the same full build concurrently.
``_create_station_ctl_file`` serializes the create path behind
``_ctl_create_lock`` and re-checks existence after acquiring it.

``check_bin_depth_changes`` fires an expanded ``deployments,bins``
MDAPI request from two call sites per station scan; its result is now
cached per station for the run and cleared by ``reset_run_counters``.
"""

import importlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

# The package __init__ re-exports functions under the same names as
# their modules, so attribute-style imports resolve to the functions.
gso = importlib.import_module(
    'ofs_skill.obs_retrieval.get_station_observations')
rtc = importlib.import_module(
    'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

logger = logging.getLogger(__name__)


class TestCtlCreationDedup:
    N_THREADS = 4

    def _run_concurrent_create(self, tmp_path, monkeypatch):
        """Race N_THREADS variable threads into the ctl create path."""
        ctl_file_path = tmp_path / 'cbofs_cu_station.ctl'
        barrier = threading.Barrier(self.N_THREADS)
        build_calls = []
        parsed_sentinel = (['station'], ['metadata'])

        def fake_write_obs_ctlfile(start_date, end_date, datum, path, ofs,
                                   stationowner, var_list, logger_arg,
                                   **kwargs):
            build_calls.append(threading.get_ident())
            # Widen the window a real multi-minute build would occupy so
            # a missing lock reliably produces duplicate builds.
            time.sleep(0.1)
            ctl_file_path.write_text('marker', encoding='utf-8')

        def fake_extract(filepath):
            return parsed_sentinel if os.path.isfile(filepath) else None

        monkeypatch.setattr(gso, 'write_obs_ctlfile',
                            fake_write_obs_ctlfile)
        monkeypatch.setattr(gso, 'station_ctl_file_extract', fake_extract)

        def worker():
            barrier.wait()
            return gso._create_station_ctl_file(
                str(ctl_file_path),
                SimpleNamespace(currents_bins_csv=None),
                'MLLW', '20260301', '20260308', str(tmp_path), 'cbofs',
                ['co-ops'],
                ['water_level', 'water_temperature', 'salinity',
                 'currents'],
                logger,
            )

        with ThreadPoolExecutor(max_workers=self.N_THREADS) as executor:
            futures = [executor.submit(worker)
                       for _ in range(self.N_THREADS)]
            results = [f.result() for f in futures]
        return build_calls, results, parsed_sentinel

    def test_concurrent_threads_build_exactly_once(
            self, tmp_path, monkeypatch):
        build_calls, results, parsed = self._run_concurrent_create(
            tmp_path, monkeypatch)
        assert len(build_calls) == 1
        assert results == [parsed] * self.N_THREADS

    def test_existing_file_skips_build(self, tmp_path, monkeypatch):
        ctl_file_path = tmp_path / 'cbofs_wl_station.ctl'
        ctl_file_path.write_text('marker', encoding='utf-8')
        parsed_sentinel = (['station'], ['metadata'])
        build_calls = []

        monkeypatch.setattr(
            gso, 'write_obs_ctlfile',
            lambda *args, **kwargs: build_calls.append(args))
        monkeypatch.setattr(
            gso, 'station_ctl_file_extract',
            lambda filepath: parsed_sentinel
            if os.path.isfile(filepath) else None)

        result = gso._create_station_ctl_file(
            str(ctl_file_path), SimpleNamespace(currents_bins_csv=None),
            'MLLW', '20260301', '20260308', str(tmp_path), 'cbofs',
            ['co-ops'], ['water_level'], logger,
        )
        assert result == parsed_sentinel
        assert build_calls == []


class TestBinDepthChangeCache:
    SHIFTING_PAYLOAD = {'stations': [{'bins': [
        {'deploymentId': 1, 'num': 1, 'distance': 2.0},
        {'deploymentId': 2, 'num': 1, 'distance': 5.0},
    ]}]}
    STABLE_PAYLOAD = {'stations': [{'bins': [
        {'deploymentId': 1, 'num': 1, 'distance': 2.0},
        {'deploymentId': 2, 'num': 1, 'distance': 2.0},
    ]}]}

    def _patch_fetch(self, monkeypatch, payload):
        calls = []

        def fake_get_with_retry(url, *args, **kwargs):
            calls.append(url)
            return payload

        monkeypatch.setattr(rtc, '_get_with_retry', fake_get_with_retry)
        return calls

    def test_second_call_served_from_cache(self, monkeypatch):
        rtc.reset_run_counters()
        calls = self._patch_fetch(monkeypatch, self.SHIFTING_PAYLOAD)
        first = rtc.check_bin_depth_changes(
            'cb0102', 'https://mdapi.invalid', logger)
        second = rtc.check_bin_depth_changes(
            'cb0102', 'https://mdapi.invalid', logger)
        assert first is True
        assert second is True
        assert len(calls) == 1

    def test_false_result_is_cached_too(self, monkeypatch):
        rtc.reset_run_counters()
        calls = self._patch_fetch(monkeypatch, self.STABLE_PAYLOAD)
        assert rtc.check_bin_depth_changes(
            'n03020', 'https://mdapi.invalid', logger) is False
        assert rtc.check_bin_depth_changes(
            'n03020', 'https://mdapi.invalid', logger) is False
        assert len(calls) == 1

    def test_stations_cached_independently(self, monkeypatch):
        rtc.reset_run_counters()
        calls = self._patch_fetch(monkeypatch, self.SHIFTING_PAYLOAD)
        rtc.check_bin_depth_changes(
            'cb0102', 'https://mdapi.invalid', logger)
        rtc.check_bin_depth_changes(
            'cb0201', 'https://mdapi.invalid', logger)
        assert len(calls) == 2

    def test_reset_run_counters_clears_cache(self, monkeypatch):
        rtc.reset_run_counters()
        calls = self._patch_fetch(monkeypatch, self.SHIFTING_PAYLOAD)
        rtc.check_bin_depth_changes(
            'cb0102', 'https://mdapi.invalid', logger)
        rtc.reset_run_counters()
        rtc.check_bin_depth_changes(
            'cb0102', 'https://mdapi.invalid', logger)
        assert len(calls) == 2
