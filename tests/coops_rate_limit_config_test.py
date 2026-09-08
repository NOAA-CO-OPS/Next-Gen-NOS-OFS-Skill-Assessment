"""
Tests for the configurable CO-OPS rate limiter and the parsed-config
cache.

``coops_request_concurrency`` / ``coops_request_gap_sec`` in the
[parallelization] section feed ``configure_coops_rate_limit`` in
``retrieve_t_and_c_station``. The config parse cache in ``utils`` keeps
repeated section reads in memory, invalidated by file mtime/size.
"""

import importlib
import logging

from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.utils import get_parallel_config

# The package __init__ re-exports the function under the same name as
# its module, so attribute-style imports resolve to the function.
rtc = importlib.import_module(
    'ofs_skill.obs_retrieval.retrieve_t_and_c_station')

logger = logging.getLogger(__name__)


def _write_conf(path, parallel_lines):
    body = '[parallelization]\n' + '\n'.join(parallel_lines) + '\n'
    path.write_text(body, encoding='utf-8')
    return path


class TestParallelConfigKeys:
    def test_defaults_present(self):
        cfg = get_parallel_config(logger)
        assert cfg['coops_request_concurrency'] >= 1
        assert cfg['coops_request_gap_sec'] >= 0.0

    def test_overrides_parsed(self, tmp_path):
        conf = _write_conf(tmp_path / 'ofs_dps.conf', [
            'coops_request_concurrency=4',
            'coops_request_gap_sec=0.25',
        ])
        cfg = get_parallel_config(logger, config_file=conf)
        assert cfg['coops_request_concurrency'] == 4
        assert cfg['coops_request_gap_sec'] == 0.25

    def test_overrides_clamped(self, tmp_path):
        conf = _write_conf(tmp_path / 'ofs_dps.conf', [
            'coops_request_concurrency=99',
            'coops_request_gap_sec=-3',
        ])
        cfg = get_parallel_config(logger, config_file=conf)
        assert cfg['coops_request_concurrency'] == 8
        assert cfg['coops_request_gap_sec'] == 0.0

    def test_invalid_values_keep_defaults(self, tmp_path):
        conf = _write_conf(tmp_path / 'ofs_dps.conf', [
            'coops_request_concurrency=fast',
            'coops_request_gap_sec=snail',
        ])
        cfg = get_parallel_config(logger, config_file=conf)
        assert cfg['coops_request_concurrency'] == 2
        assert cfg['coops_request_gap_sec'] == 0.5

    def test_parallel_disabled_forces_serial_coops(self, tmp_path):
        conf = _write_conf(tmp_path / 'ofs_dps.conf', [
            'parallel_enabled=False',
            'coops_request_concurrency=4',
        ])
        cfg = get_parallel_config(logger, config_file=conf)
        assert cfg['coops_request_concurrency'] == 1


class TestConfigureCoopsRateLimit:
    def setup_method(self):
        self._orig = (rtc._COOPS_CONCURRENCY_LIMIT,
                      rtc._COOPS_MIN_REQUEST_GAP_SEC,
                      rtc._coops_request_semaphore)

    def teardown_method(self):
        (rtc._COOPS_CONCURRENCY_LIMIT,
         rtc._COOPS_MIN_REQUEST_GAP_SEC,
         rtc._coops_request_semaphore) = self._orig

    def test_defaults_are_noop(self):
        semaphore_before = rtc._coops_request_semaphore
        rtc.configure_coops_rate_limit(concurrency=2, gap_sec=0.5)
        assert rtc._coops_request_semaphore is semaphore_before
        assert rtc._COOPS_CONCURRENCY_LIMIT == 2
        assert rtc._COOPS_MIN_REQUEST_GAP_SEC == 0.5

    def test_none_is_noop(self):
        semaphore_before = rtc._coops_request_semaphore
        rtc.configure_coops_rate_limit(concurrency=None, gap_sec=None)
        assert rtc._coops_request_semaphore is semaphore_before

    def test_override_swaps_semaphore_and_gap(self):
        semaphore_before = rtc._coops_request_semaphore
        rtc.configure_coops_rate_limit(concurrency=3, gap_sec=0.25,
                                       logger=logger)
        assert rtc._coops_request_semaphore is not semaphore_before
        assert rtc._COOPS_CONCURRENCY_LIMIT == 3
        assert rtc._COOPS_MIN_REQUEST_GAP_SEC == 0.25

    def test_floor_of_one(self):
        rtc.configure_coops_rate_limit(concurrency=0, gap_sec=-1.0)
        assert rtc._COOPS_CONCURRENCY_LIMIT == 1
        assert rtc._COOPS_MIN_REQUEST_GAP_SEC == 0.0


class TestConfigParseCache:
    def test_cache_hit_returns_same_parser(self, tmp_path):
        conf = _write_conf(tmp_path / 'a.conf', ['obs_coops_workers=3'])
        first = utils._read_config_cached(conf)
        second = utils._read_config_cached(conf)
        assert first is second

    def test_modified_file_reparses(self, tmp_path):
        import os

        conf = _write_conf(tmp_path / 'a.conf', ['obs_coops_workers=3'])
        first = utils._read_config_cached(conf)
        _write_conf(conf, ['obs_coops_workers=5'])
        # Force a distinct mtime even on coarse-resolution filesystems.
        stat = os.stat(conf)
        os.utime(conf, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

        second = utils._read_config_cached(conf)
        assert second is not first
        assert second.get('parallelization', 'obs_coops_workers') == '5'

    def test_read_config_section_uses_fresh_values(self, tmp_path):
        import os

        conf = _write_conf(tmp_path / 'b.conf', ['obs_coops_workers=3'])
        params = utils.Utils(conf).read_config_section(
            'parallelization', logger)
        assert params['obs_coops_workers'] == '3'

        _write_conf(conf, ['obs_coops_workers=7'])
        stat = os.stat(conf)
        os.utime(conf, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

        params = utils.Utils(conf).read_config_section(
            'parallelization', logger)
        assert params['obs_coops_workers'] == '7'
