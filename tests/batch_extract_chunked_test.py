"""Regression tests for time-chunked ``_batch_extract``.

The function used to do a single ``dask.compute`` over the full time axis
for all stations at once. On a 316-file NECOFS dataset the all-at-once
graph held intermediate chunks for every backing file in memory at once,
pushing the OS into paging and stretching the water-level stage to >16 h.

The chunked version splits the time axis into windows of
``_BATCH_EXTRACT_TIME_CHUNK`` steps, computes each window separately,
and concatenates. These tests verify:

- Bit-for-bit equality vs the un-chunked output, for 2-D (FVCOM zeta),
  3-D FVCOM order (``[:, dep, idx]``), and 3-D ROMS order
  (``[:, idx, dep]``) layouts.
- A single window (n_time <= chunk) still produces correct output.
- Cross-chunk boundaries don't drop or duplicate any timestep.
- The eager (non-Dask) input branch is left intact.
- Per-chunk progress log fires exactly once per chunk.
- Empty idx_list returns an empty array instead of erroring.
"""

import logging
from collections.abc import Sequence

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import (
    _BATCH_EXTRACT_TIME_CHUNK,
    _batch_extract,
)


def _logger():
    return logging.getLogger('batch_extract_chunked_test')


def _make_fvcom_dataset(n_time, n_siglay=10, n_station=12, seed=0):
    """FVCOM-shape stations dataset with Dask backing."""
    rng = np.random.default_rng(seed)
    zeta = rng.standard_normal((n_time, n_station)).astype(np.float32)
    temp = rng.standard_normal((n_time, n_siglay, n_station)).astype(np.float32)
    ds = xr.Dataset(
        {
            'zeta': (('time', 'station'), zeta),
            'temp': (('time', 'siglay', 'station'), temp),
        },
        coords={
            'time': np.arange(n_time) * np.timedelta64(6, 'm')
                    + np.datetime64('2026-02-16'),
        },
    )
    # Force Dask backing on the time axis with small chunks so the
    # chunk-window logic exercises real graph-pruning behaviour.
    return ds.chunk({'time': max(1, n_time // 4)})


def _make_roms_dataset(n_time, n_srho=10, n_station=12, seed=1):
    """ROMS-shape stations dataset (idx before depth) with Dask backing."""
    rng = np.random.default_rng(seed)
    salt = rng.standard_normal((n_time, n_station, n_srho)).astype(np.float32)
    ds = xr.Dataset(
        {'salt': (('ocean_time', 'station', 's_rho'), salt)},
        coords={
            'ocean_time': np.arange(n_time) * np.timedelta64(6, 'm')
                          + np.datetime64('2026-02-16'),
        },
    )
    return ds.chunk({'ocean_time': max(1, n_time // 4)})


def _expected_2d(ds, var, indices: Sequence[int]):
    """Reference single-shot output via simple per-station numpy slicing."""
    arr = ds[var].values  # forces materialization, full dataset
    return np.stack([arr[:, i] for i in indices], axis=1)


def _expected_3d_fvcom(ds, var, indices, depths):
    arr = ds[var].values
    return np.stack(
        [arr[:, d, i] for i, d in zip(indices, depths)],
        axis=1,
    )


def _expected_3d_roms(ds, var, indices, depths):
    arr = ds[var].values
    return np.stack(
        [arr[:, i, d] for i, d in zip(indices, depths)],
        axis=1,
    )


# ---------------------------------------------------------------------------
# Bit-for-bit equality across multiple time-chunks
# ---------------------------------------------------------------------------


def test_chunked_2d_matches_single_shot():
    n_time = 80  # > 20 ensures > 1 chunk at time_chunk=20
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [0, 3, 7, 11]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=20,
    )
    expected = _expected_2d(ds, 'zeta', indices)

    assert out.shape == (n_time, len(indices))
    np.testing.assert_array_equal(out, expected)


def test_chunked_3d_fvcom_matches_single_shot():
    n_time = 80
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [1, 4, 9]
    depths = [0, 5, 9]

    out = _batch_extract(
        ds, 'temp', indices, dep_list=depths, idx_first=False,
        logger=_logger(), time_chunk=20,
    )
    expected = _expected_3d_fvcom(ds, 'temp', indices, depths)

    assert out.shape == (n_time, len(indices))
    np.testing.assert_array_equal(out, expected)


def test_chunked_3d_roms_matches_single_shot():
    n_time = 60
    ds = _make_roms_dataset(n_time=n_time)
    indices = [0, 2, 7]
    depths = [3, 6, 9]

    out = _batch_extract(
        ds, 'salt', indices, dep_list=depths, idx_first=True,
        logger=_logger(), time_chunk=15,
    )
    expected = _expected_3d_roms(ds, 'salt', indices, depths)

    assert out.shape == (n_time, len(indices))
    np.testing.assert_array_equal(out, expected)


# ---------------------------------------------------------------------------
# Edge cases at the chunk boundary
# ---------------------------------------------------------------------------


def test_single_window_path_when_n_time_le_chunk():
    """If the dataset fits in one window we should not concatenate / log
    per-chunk. The output must still match exact reference values."""
    n_time = 12
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [0, 5]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=100,  # much larger than n_time
    )
    expected = _expected_2d(ds, 'zeta', indices)
    np.testing.assert_array_equal(out, expected)


def test_partial_final_chunk_is_handled():
    """Last chunk shorter than time_chunk must still contribute its
    timesteps exactly once, in the right place."""
    # 25 timesteps, chunk=10 → 3 chunks of 10, 10, 5
    n_time = 25
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [2, 6]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=10,
    )
    expected = _expected_2d(ds, 'zeta', indices)
    assert out.shape == (n_time, len(indices))
    np.testing.assert_array_equal(out, expected)


def test_chunk_size_one_still_correct():
    """Pathological tiny chunk size — every timestep its own compute.
    Output must still match exact reference values."""
    n_time = 8
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [0, 4]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=1,
    )
    expected = _expected_2d(ds, 'zeta', indices)
    np.testing.assert_array_equal(out, expected)


def test_chunk_size_zero_or_negative_clamped_to_one():
    """time_chunk <= 0 should not divide-by-zero; clamp to 1."""
    ds = _make_fvcom_dataset(n_time=5)
    indices = [0]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=0,
    )
    expected = _expected_2d(ds, 'zeta', indices)
    np.testing.assert_array_equal(out, expected)


# ---------------------------------------------------------------------------
# Eager (non-Dask) branch
# ---------------------------------------------------------------------------


def test_eager_input_skips_chunking():
    """A numpy-backed (non-Dask) dataset should be materialized directly
    without ever building a chunked compute graph."""
    n_time, n_station = 30, 5
    rng = np.random.default_rng(2)
    ds = xr.Dataset(
        {'zeta': (('time', 'station'),
                  rng.standard_normal((n_time, n_station)))},
        coords={'time': np.arange(n_time)
                + np.datetime64('2026-02-16').astype('datetime64[h]')},
    )
    # Confirm no Dask backing.
    assert not hasattr(ds['zeta'].data, 'dask')
    indices = [0, 2, 4]

    out = _batch_extract(
        ds, 'zeta', indices, dep_list=None,
        logger=_logger(), time_chunk=5,
    )
    expected = _expected_2d(ds, 'zeta', indices)
    np.testing.assert_array_equal(out, expected)


# ---------------------------------------------------------------------------
# Logging behaviour
# ---------------------------------------------------------------------------


def test_logs_one_line_per_chunk(caplog):
    n_time = 30
    ds = _make_fvcom_dataset(n_time=n_time)
    indices = [0, 1]

    with caplog.at_level(logging.INFO, logger='batch_extract_chunked_test'):
        _batch_extract(
            ds, 'zeta', indices, dep_list=None,
            logger=_logger(), time_chunk=10,
        )

    chunk_lines = [r for r in caplog.records
                   if 'chunk' in r.getMessage()
                   and 'done' in r.getMessage()]
    # 30 timesteps / chunk=10 = 3 chunks → 3 done-lines
    assert len(chunk_lines) == 3


def test_summary_log_emitted_when_split(caplog):
    """The 'time axis N steps split into K chunks' header should fire
    exactly once when chunking happens."""
    n_time = 30
    ds = _make_fvcom_dataset(n_time=n_time)

    with caplog.at_level(logging.INFO, logger='batch_extract_chunked_test'):
        _batch_extract(
            ds, 'zeta', [0], dep_list=None,
            logger=_logger(), time_chunk=10,
        )

    header_lines = [r for r in caplog.records
                    if 'split into' in r.getMessage()]
    assert len(header_lines) == 1


def test_no_chunk_log_when_dataset_fits_in_one_window(caplog):
    ds = _make_fvcom_dataset(n_time=8)

    with caplog.at_level(logging.INFO, logger='batch_extract_chunked_test'):
        _batch_extract(
            ds, 'zeta', [0], dep_list=None,
            logger=_logger(), time_chunk=100,
        )

    chunk_lines = [r for r in caplog.records
                   if 'chunk' in r.getMessage()]
    assert chunk_lines == []


def test_default_chunk_constant_is_sane():
    """Smoke-check: the module-level default is a positive integer in a
    range that still gives meaningful chunking on a 71-day run."""
    assert isinstance(_BATCH_EXTRACT_TIME_CHUNK, int)
    assert 100 <= _BATCH_EXTRACT_TIME_CHUNK <= 20000


# ---------------------------------------------------------------------------
# Defensive edge: empty indices
# ---------------------------------------------------------------------------


def test_empty_idx_list_returns_empty_without_compute():
    ds = _make_fvcom_dataset(n_time=10)
    out = _batch_extract(
        ds, 'zeta', [], dep_list=None,
        logger=_logger(), time_chunk=5,
    )
    # No stations → 0-column array, no crash.
    assert out.shape[1] == 0
