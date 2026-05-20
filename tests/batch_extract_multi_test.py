"""Regression tests for ``_batch_extract_multi``.

The fused multi-var path is the speed win for currents on long
multi-month runs: it lets Dask read each backing file once and share
that read between u and v selections, instead of re-reading every file
for v after finishing u.

These tests pin the invariants:

- Fused output for ``var_names=[u, v]`` matches running ``_batch_extract``
  separately for ``u`` and ``v`` — bit-for-bit, across 2-D (FVCOM zeta-
  shape), 3-D FVCOM order, and 3-D ROMS order layouts.
- Single-window fast path produces the same numbers as the chunked
  multi-window path.
- Eager (non-Dask) input is handled.
- Mismatched leading time dim across var_names raises.
- Empty idx_list returns empty arrays (one per var, no compute).
- Per-chunk progress log fires once per chunk with the var-name pair.
"""

import logging
import logging.handlers

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import (
    _batch_extract,
    _batch_extract_multi,
)


def _logger():
    return logging.getLogger('batch_extract_multi_test')


def _make_fvcom_dataset(n_time, n_siglay=10, n_station=12, seed=0):
    rng = np.random.default_rng(seed)
    u = rng.standard_normal((n_time, n_siglay, n_station)).astype(np.float32)
    v = rng.standard_normal((n_time, n_siglay, n_station)).astype(np.float32)
    zeta = rng.standard_normal((n_time, n_station)).astype(np.float32)
    ds = xr.Dataset(
        {
            'u': (('time', 'siglay', 'station'), u),
            'v': (('time', 'siglay', 'station'), v),
            'zeta': (('time', 'station'), zeta),
        },
        coords={
            'time': np.datetime64('2026-02-16T00')
                    + np.arange(n_time) * np.timedelta64(6, 'm'),
        },
    )
    return ds.chunk({'time': max(1, n_time // 4)})


def _make_roms_dataset(n_time, n_srho=10, n_station=12, seed=1):
    rng = np.random.default_rng(seed)
    u_east = rng.standard_normal((n_time, n_station, n_srho)).astype(np.float32)
    v_north = rng.standard_normal((n_time, n_station, n_srho)).astype(np.float32)
    ds = xr.Dataset(
        {
            'u_east': (('ocean_time', 'station', 's_rho'), u_east),
            'v_north': (('ocean_time', 'station', 's_rho'), v_north),
        },
        coords={
            'ocean_time': np.datetime64('2026-02-16T00')
                          + np.arange(n_time) * np.timedelta64(6, 'm'),
        },
    )
    return ds.chunk({'ocean_time': max(1, n_time // 4)})


# ---------------------------------------------------------------------------
# Fused output equals sequential single-var output
# ---------------------------------------------------------------------------


def test_fused_matches_sequential_3d_fvcom():
    ds = _make_fvcom_dataset(n_time=80)
    indices = [0, 3, 7, 11]
    depths = [0, 5, 9, 2]

    fused_u, fused_v = _batch_extract_multi(
        ds, ['u', 'v'], indices, depths, idx_first=False,
        logger=_logger(), time_chunk=20,
    )
    seq_u = _batch_extract(ds, 'u', indices, depths, idx_first=False,
                            logger=_logger(), time_chunk=20)
    seq_v = _batch_extract(ds, 'v', indices, depths, idx_first=False,
                            logger=_logger(), time_chunk=20)

    np.testing.assert_array_equal(fused_u, seq_u)
    np.testing.assert_array_equal(fused_v, seq_v)


def test_fused_matches_sequential_3d_roms():
    ds = _make_roms_dataset(n_time=60)
    indices = [0, 2, 7]
    depths = [3, 6, 9]

    fused_u, fused_v = _batch_extract_multi(
        ds, ['u_east', 'v_north'], indices, depths, idx_first=True,
        logger=_logger(), time_chunk=15,
    )
    seq_u = _batch_extract(ds, 'u_east', indices, depths, idx_first=True,
                            logger=_logger(), time_chunk=15)
    seq_v = _batch_extract(ds, 'v_north', indices, depths, idx_first=True,
                            logger=_logger(), time_chunk=15)

    np.testing.assert_array_equal(fused_u, seq_u)
    np.testing.assert_array_equal(fused_v, seq_v)


def test_fused_matches_sequential_2d():
    """Schism stofs path uses dep_list=None (2-D vars). Mock with zeta-shape u/v."""
    rng = np.random.default_rng(2)
    n_time, n_station = 60, 8
    u2 = rng.standard_normal((n_time, n_station)).astype(np.float32)
    v2 = rng.standard_normal((n_time, n_station)).astype(np.float32)
    ds = xr.Dataset(
        {'u': (('time', 'station'), u2), 'v': (('time', 'station'), v2)},
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(6, 'm')},
    ).chunk({'time': 15})
    indices = [0, 3, 7]

    fused_u, fused_v = _batch_extract_multi(
        ds, ['u', 'v'], indices, dep_list=None,
        logger=_logger(), time_chunk=15,
    )
    seq_u = _batch_extract(ds, 'u', indices, dep_list=None,
                           logger=_logger(), time_chunk=15)
    seq_v = _batch_extract(ds, 'v', indices, dep_list=None,
                           logger=_logger(), time_chunk=15)

    np.testing.assert_array_equal(fused_u, seq_u)
    np.testing.assert_array_equal(fused_v, seq_v)


# ---------------------------------------------------------------------------
# Single-window fast path matches chunked path
# ---------------------------------------------------------------------------


def test_single_window_path_correct():
    ds = _make_fvcom_dataset(n_time=12)  # smaller than default chunk
    indices = [0, 5]
    depths = [0, 4]

    fused_u, fused_v = _batch_extract_multi(
        ds, ['u', 'v'], indices, depths, idx_first=False,
        logger=_logger(), time_chunk=100,  # forces single-window
    )
    seq_u = _batch_extract(ds, 'u', indices, depths, idx_first=False,
                            logger=_logger(), time_chunk=100)
    seq_v = _batch_extract(ds, 'v', indices, depths, idx_first=False,
                            logger=_logger(), time_chunk=100)

    np.testing.assert_array_equal(fused_u, seq_u)
    np.testing.assert_array_equal(fused_v, seq_v)


# ---------------------------------------------------------------------------
# Defensive edge cases
# ---------------------------------------------------------------------------


def test_eager_input_handled():
    """Numpy-backed dataset (no Dask) — fused path still returns correct output."""
    rng = np.random.default_rng(3)
    n_time, n_station = 20, 5
    u = rng.standard_normal((n_time, n_station))
    v = rng.standard_normal((n_time, n_station))
    ds = xr.Dataset(
        {'u': (('time', 'station'), u), 'v': (('time', 'station'), v)},
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(6, 'm')},
    )
    assert not hasattr(ds['u'].data, 'dask')
    indices = [0, 2, 4]

    fused_u, fused_v = _batch_extract_multi(
        ds, ['u', 'v'], indices, dep_list=None,
        logger=_logger(), time_chunk=5,
    )
    np.testing.assert_array_equal(fused_u, np.stack([u[:, i] for i in indices], axis=1))
    np.testing.assert_array_equal(fused_v, np.stack([v[:, i] for i in indices], axis=1))


def test_mismatched_time_dim_raises():
    """If u and v don't share the leading time dim, raise loudly."""
    rng = np.random.default_rng(4)
    ds = xr.Dataset(
        {
            'u': (('time', 'station'), rng.standard_normal((10, 3))),
            'v': (('ocean_time', 'station'), rng.standard_normal((10, 3))),
        },
        coords={
            'time': np.datetime64('2026-02-16T00')
                    + np.arange(10) * np.timedelta64(6, 'm'),
            'ocean_time': np.datetime64('2026-02-16T00')
                          + np.arange(10) * np.timedelta64(6, 'm'),
        },
    ).chunk({'time': 5})

    import pytest
    with pytest.raises(ValueError, match='share the leading time dim'):
        _batch_extract_multi(ds, ['u', 'v'], [0, 1], dep_list=None,
                              logger=_logger(), time_chunk=5)


def test_empty_idx_list_returns_empty_per_var():
    ds = _make_fvcom_dataset(n_time=10)
    out = _batch_extract_multi(ds, ['u', 'v'], [], dep_list=None,
                                logger=_logger(), time_chunk=5)
    assert len(out) == 2
    for arr in out:
        assert arr.shape[1] == 0


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def test_chunk_log_uses_combined_var_name(caplog):
    ds = _make_fvcom_dataset(n_time=30)
    indices = [0, 1]
    depths = [0, 1]

    with caplog.at_level(logging.INFO, logger='batch_extract_multi_test'):
        _batch_extract_multi(ds, ['u', 'v'], indices, depths,
                              idx_first=False,
                              logger=_logger(), time_chunk=10)

    chunk_lines = [r for r in caplog.records
                   if 'chunk' in r.getMessage()
                   and 'done' in r.getMessage()]
    # 30 ts / chunk=10 = 3 chunks
    assert len(chunk_lines) == 3
    assert all('u+v' in line.getMessage() for line in chunk_lines), \
        'chunk log should show the combined var name'


def test_summary_log_says_fused_n_var():
    """Header line should report the number of variables being fused."""
    logger = logging.getLogger('batch_extract_multi_test_summary')

    ds = _make_fvcom_dataset(n_time=30)
    indices = [0]
    depths = [0]

    handler = logging.handlers.MemoryHandler(capacity=100)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        _batch_extract_multi(ds, ['u', 'v'], indices, depths,
                              idx_first=False, logger=logger, time_chunk=10)
        msgs = [r.getMessage() for r in handler.buffer]
        header = [m for m in msgs if 'split into' in m]
        assert len(header) == 1
        assert 'fused 2-var compute' in header[0], header[0]
    finally:
        logger.removeHandler(handler)
