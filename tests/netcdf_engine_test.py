"""
Tests for netCDF engine detection (netcdf_engine.py) and the
engine-aware extraction guard (get_node_ofs._extract_guard).

Covers:
- magic-byte format sniffing for local files (all NetCDF magics, HDF5
  superblock at nonzero offsets, unreadable/garbage files)
- engine resolution under engine_strategy=auto/netcdf4/h5netcdf,
  including the historical scipy special cases (stofs_2d_glo, custom
  free-run station files)
- the extraction guard: thread-unsafe engines fully serialized,
  h5netcdf bounded by parallel_extract_slots
- parallel_extract_slots config parsing
"""

import threading
from types import SimpleNamespace

import pytest

from ofs_skill.model_processing import netcdf_engine
from ofs_skill.model_processing.get_node_ofs import _extract_guard
from ofs_skill.model_processing.netcdf_engine import (
    FORMAT_HDF5,
    FORMAT_NETCDF3,
    FORMAT_UNKNOWN,
    get_engine_strategy,
    resolve_engine,
    sniff_netcdf_format,
)
from ofs_skill.obs_retrieval.utils import get_parallel_config

HDF5_MAGIC = b'\x89HDF\r\n\x1a\n'


@pytest.fixture(autouse=True)
def _clear_sniff_cache():
    with netcdf_engine._SNIFF_CACHE_LOCK:
        netcdf_engine._SNIFF_CACHE.clear()
    yield
    with netcdf_engine._SNIFF_CACHE_LOCK:
        netcdf_engine._SNIFF_CACHE.clear()


def _write(tmp_path, name, payload):
    path = tmp_path / name
    path.write_bytes(payload)
    return str(path)


# ---------------------------------------------------------------------
# sniff_netcdf_format
# ---------------------------------------------------------------------

def test_sniff_hdf5(tmp_path):
    path = _write(tmp_path, 'a.nc', HDF5_MAGIC + b'\x00' * 16)
    assert sniff_netcdf_format(path) == FORMAT_HDF5


@pytest.mark.parametrize('magic', [b'CDF\x01', b'CDF\x02', b'CDF\x05'])
def test_sniff_netcdf3_variants(tmp_path, magic):
    path = _write(tmp_path, 'a.nc', magic + b'\x00' * 16)
    assert sniff_netcdf_format(path) == FORMAT_NETCDF3


def test_sniff_hdf5_superblock_at_offset(tmp_path):
    # The HDF5 spec allows the superblock at offsets 512 << n.
    path = _write(tmp_path, 'a.nc', b'\x00' * 512 + HDF5_MAGIC)
    assert sniff_netcdf_format(path) == FORMAT_HDF5


def test_sniff_garbage_is_unknown(tmp_path):
    path = _write(tmp_path, 'a.nc', b'not a netcdf file at all')
    assert sniff_netcdf_format(path) == FORMAT_UNKNOWN


def test_sniff_missing_file_is_unknown(tmp_path):
    assert sniff_netcdf_format(str(tmp_path / 'nope.nc')) == FORMAT_UNKNOWN


def test_sniff_non_string_is_unknown():
    assert sniff_netcdf_format(None) == FORMAT_UNKNOWN
    assert sniff_netcdf_format(42) == FORMAT_UNKNOWN


def test_sniff_strips_fsspec_protocol_chain(tmp_path):
    path = _write(tmp_path, 'a.nc', HDF5_MAGIC + b'\x00' * 16)
    assert sniff_netcdf_format(f'simplecache::{path}') == FORMAT_HDF5


def test_sniff_result_is_cached(tmp_path):
    path = _write(tmp_path, 'a.nc', HDF5_MAGIC + b'\x00' * 16)
    assert sniff_netcdf_format(path) == FORMAT_HDF5
    # Rewrite the file; the cached classification must win (one sniff
    # per path per run).
    (tmp_path / 'a.nc').write_bytes(b'CDF\x01' + b'\x00' * 16)
    assert sniff_netcdf_format(path) == FORMAT_HDF5


def test_sniff_failure_is_not_cached(tmp_path):
    path = str(tmp_path / 'late.nc')
    assert sniff_netcdf_format(path) == FORMAT_UNKNOWN
    (tmp_path / 'late.nc').write_bytes(HDF5_MAGIC + b'\x00' * 16)
    assert sniff_netcdf_format(path) == FORMAT_HDF5


# ---------------------------------------------------------------------
# get_engine_strategy / resolve_engine
# ---------------------------------------------------------------------

def _conf_with_strategy(tmp_path, strategy=None):
    conf = tmp_path / 'ofs_dps.conf'
    lines = ['[settings]']
    if strategy is not None:
        lines.append(f'engine_strategy={strategy}')
    conf.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return str(conf)


def _prop(tmp_path, ofs='cbofs', strategy=None):
    return SimpleNamespace(
        ofs=ofs, config_file=_conf_with_strategy(tmp_path, strategy))


def test_engine_strategy_default_is_auto(tmp_path):
    assert get_engine_strategy(_conf_with_strategy(tmp_path)) == 'auto'


@pytest.mark.parametrize('strategy', ['auto', 'netcdf4', 'h5netcdf'])
def test_engine_strategy_reads_config(tmp_path, strategy):
    conf = _conf_with_strategy(tmp_path, strategy)
    assert get_engine_strategy(conf) == strategy


def test_engine_strategy_invalid_falls_back_to_auto(tmp_path):
    conf = _conf_with_strategy(tmp_path, 'pydap')
    assert get_engine_strategy(conf) == 'auto'


def test_resolve_all_hdf5_uses_h5netcdf(tmp_path, caplog):
    import logging
    files = [_write(tmp_path, f'f{i}.nc', HDF5_MAGIC + b'\x00' * 16)
             for i in range(3)]
    engine = resolve_engine(files, _prop(tmp_path),
                            logging.getLogger('test'))
    assert engine == 'h5netcdf'


def test_resolve_netcdf3_uses_netcdf4(tmp_path):
    import logging
    files = [_write(tmp_path, f'f{i}.nc', b'CDF\x02' + b'\x00' * 16)
             for i in range(2)]
    engine = resolve_engine(files, _prop(tmp_path, ofs='necofs'),
                            logging.getLogger('test'))
    assert engine == 'netcdf4'


def test_resolve_mixed_formats_uses_netcdf4(tmp_path):
    import logging
    files = [
        _write(tmp_path, 'a.nc', HDF5_MAGIC + b'\x00' * 16),
        _write(tmp_path, 'b.nc', b'CDF\x01' + b'\x00' * 16),
    ]
    engine = resolve_engine(files, _prop(tmp_path),
                            logging.getLogger('test'))
    assert engine == 'netcdf4'


def test_resolve_unknown_format_uses_netcdf4(tmp_path):
    import logging
    files = [_write(tmp_path, 'a.nc', b'garbage bytes here!')]
    engine = resolve_engine(files, _prop(tmp_path),
                            logging.getLogger('test'))
    assert engine == 'netcdf4'


def test_resolve_stofs2d_netcdf3_uses_scipy(tmp_path):
    import logging
    files = [_write(tmp_path, 'points.cwl.nc', b'CDF\x01' + b'\x00' * 16)]
    engine = resolve_engine(files, _prop(tmp_path, ofs='stofs_2d_glo'),
                            logging.getLogger('test'))
    assert engine == 'scipy'


def test_resolve_stofs2d_hdf5_uses_h5netcdf(tmp_path):
    # stofs_2d_glo fields files are HDF5 — the scipy special case only
    # applies when the batch actually contains NetCDF-3 files.
    import logging
    files = [_write(tmp_path, 'fields.cwl.nc', HDF5_MAGIC + b'\x00' * 16)]
    engine = resolve_engine(files, _prop(tmp_path, ofs='stofs_2d_glo'),
                            logging.getLogger('test'))
    assert engine == 'h5netcdf'


def test_resolve_custom_marker_uses_scipy(tmp_path):
    import logging
    files = [_write(tmp_path, 'necofs_2017_free_run_station.nc',
                    b'CDF\x01' + b'\x00' * 16)]
    engine = resolve_engine(files, _prop(tmp_path, ofs='necofs'),
                            logging.getLogger('test'))
    assert engine == 'scipy'


@pytest.mark.parametrize('strategy', ['netcdf4', 'h5netcdf'])
def test_resolve_forced_strategy_skips_detection(tmp_path, strategy):
    import logging
    # Files deliberately do not exist: a forced strategy must not sniff.
    files = [str(tmp_path / 'missing.nc')]
    engine = resolve_engine(files, _prop(tmp_path, strategy=strategy),
                            logging.getLogger('test'))
    assert engine == strategy


def test_resolve_empty_batch_uses_netcdf4(tmp_path):
    import logging
    assert resolve_engine([], _prop(tmp_path),
                          logging.getLogger('test')) == 'netcdf4'


def test_resolve_samples_large_batches(tmp_path, monkeypatch):
    import logging
    sniffed = []
    real_sniff = netcdf_engine.sniff_netcdf_format

    def counting_sniff(path, logger=None):
        sniffed.append(path)
        return real_sniff(path, logger)

    monkeypatch.setattr(netcdf_engine, 'sniff_netcdf_format', counting_sniff)
    files = [_write(tmp_path, f'f{i:03d}.nc', HDF5_MAGIC + b'\x00' * 16)
             for i in range(100)]
    engine = resolve_engine(files, _prop(tmp_path),
                            logging.getLogger('test'))
    assert engine == 'h5netcdf'
    assert len(sniffed) <= netcdf_engine._MAX_SNIFFS_PER_BATCH
    # First and last files must always be sampled.
    assert files[0] in sniffed and files[-1] in sniffed


# ---------------------------------------------------------------------
# _extract_guard
# ---------------------------------------------------------------------

def _capacity(guard):
    """Count how many times a semaphore can be acquired right now."""
    acquired = 0
    while guard.acquire(blocking=False):
        acquired += 1
    for _ in range(acquired):
        guard.release()
    return acquired


@pytest.mark.parametrize('engine', [None, 'netcdf4', 'scipy'])
def test_guard_serializes_thread_unsafe_engines(engine):
    assert _capacity(_extract_guard(engine, slots=4)) == 1


def test_guard_h5netcdf_uses_configured_slots():
    assert _capacity(_extract_guard('h5netcdf', slots=3)) == 3


def test_guard_h5netcdf_invalid_slots_default_to_two():
    assert _capacity(_extract_guard('h5netcdf', slots=None)) == 2
    assert _capacity(_extract_guard('h5netcdf', slots='bogus')) == 2


def test_guard_is_shared_across_calls():
    # Same slot count must return the same semaphore instance, so all
    # variable threads contend on one guard.
    assert _extract_guard('h5netcdf', 2) is _extract_guard('h5netcdf', 2)
    assert _extract_guard('netcdf4', 5) is _extract_guard(None, 3)


def test_guard_is_a_context_manager():
    guard = _extract_guard('h5netcdf', 2)
    with guard:
        pass
    assert isinstance(guard, type(threading.BoundedSemaphore()))


# ---------------------------------------------------------------------
# parallel_extract_slots config parsing
# ---------------------------------------------------------------------

def _parallel_conf(tmp_path, body):
    conf = tmp_path / 'ofs_dps.conf'
    conf.write_text('[parallelization]\n' + body, encoding='utf-8')
    return str(conf)


def test_extract_slots_default():
    assert get_parallel_config()['parallel_extract_slots'] >= 1


def test_extract_slots_from_config(tmp_path):
    conf = _parallel_conf(tmp_path, 'parallel_extract_slots=4\n')
    assert get_parallel_config(config_file=conf)['parallel_extract_slots'] == 4


def test_extract_slots_clamped(tmp_path):
    conf = _parallel_conf(tmp_path, 'parallel_extract_slots=99\n')
    assert get_parallel_config(config_file=conf)['parallel_extract_slots'] == 8
    conf2 = _parallel_conf(tmp_path, 'parallel_extract_slots=0\n')
    assert get_parallel_config(
        config_file=conf2)['parallel_extract_slots'] == 1


def test_extract_slots_invalid_keeps_default(tmp_path):
    conf = _parallel_conf(tmp_path, 'parallel_extract_slots=lots\n')
    assert get_parallel_config(config_file=conf)['parallel_extract_slots'] == 2


def test_extract_slots_forced_serial_when_parallel_disabled(tmp_path):
    conf = _parallel_conf(
        tmp_path, 'parallel_enabled=False\nparallel_extract_slots=4\n')
    assert get_parallel_config(config_file=conf)['parallel_extract_slots'] == 1
