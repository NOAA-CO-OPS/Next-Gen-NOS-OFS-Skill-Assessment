"""Tests for batched extraction of ``fields`` model files (issue #297).

Before this change ``_precompute_stations_data`` ran only for
``ofsfiletype == 'stations'``, so every ``-t fields`` run fell back to
per-station extraction: ``np.array(model[var][:, node, depth])`` once per
station, each a separate Dask compute with no chunk reuse.

That is pathological for SCHISM fields files, where one HDF5 chunk is the
whole spatial domain for a single timestep (STOFS-3D-Atl:
``(1, 3052121, 49)`` gzip, ~600 MB inflated). Extracting one node forces a
full-domain inflate, so N stations cost N full-domain inflates.

Two things are asserted here:

* **Equivalence** — the batch path returns exactly what the sequential
  ``format_*`` indexing expressions return, for every supported
  (model_source, ofs-family, variable) cell. SCHISM carries three different
  axis orders under one ``model_source``, and every wrong permutation
  returns a correctly-shaped array of *wrong numbers* rather than raising,
  so the fixtures encode position-identifying values.
* **Chunk reuse** — the batch path reads each chunk once, not once per
  station. That is the actual fix; wall clock is only its symptom.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import (
    _fields_current_layout,
    _fields_scalar_layout,
    _precompute_current_data,
    _precompute_scalar_data,
)

N_TIME, N_NODE, N_LAYER = 4, 37, 11


def _logger():
    return logging.getLogger('fields_batch_extract_test')


def _ident(n_time, dim_a, dim_b):
    """Values that encode their own (a, b) position.

    A transposed read returns a correctly-shaped array of wrong numbers, so
    plain random data would let an axis-order flip pass. Encoding position
    makes any permutation a hard failure.
    """
    t = np.arange(n_time)[:, None, None] * 1e6
    a = np.arange(dim_a)[None, :, None] * 1e3
    b = np.arange(dim_b)[None, None, :]
    return (t + a + b).astype('float32')


def _times(n_time):
    return np.datetime64('2026-07-05') + np.arange(n_time) * np.timedelta64(1, 'h')


def _ctlfile(n_station, n_node=N_NODE, n_layer=N_LAYER, shift=0.0):
    rng = np.random.default_rng(11)
    nodes = rng.integers(0, n_node, n_station).tolist()
    depths = rng.integers(0, n_layer, n_station).tolist()
    return (
        [],
        nodes,
        depths,
        [shift] * n_station,
        [f'st{i}' for i in range(n_station)],
    )


def _props(model_source, ofs, ofsfiletype='fields'):
    return SimpleNamespace(
        model_source=model_source,
        ofs=ofs,
        ofsfiletype=ofsfiletype,
        whichcast='nowcast',
    )


def _ds(varmap, node_first):
    """Build a fields-like dataset. ``node_first`` picks the axis order."""
    dims = ('time', 'node', 'layer') if node_first else ('time', 'layer', 'node')
    shape = (N_NODE, N_LAYER) if node_first else (N_LAYER, N_NODE)
    return xr.Dataset(
        {name: (dims, _ident(N_TIME, *shape)) for name in varmap},
        coords={'time': _times(N_TIME)},
    )


# --------------------------------------------------------------------------
# Layout resolution
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('model_source', 'ofs', 'model_var', 'expected'),
    [
        ('schism', 'stofs_3d_atl', 'temp', ('temperature', True)),
        ('schism', 'stofs_3d_atl', 'salinity', ('salinity', True)),
        ('schism', 'stofs_3d_pac', 'temp', ('temperature', True)),
        ('schism', 'secofs', 'temp', ('temp', False)),
        ('schism', 'secofs', 'salinity', ('salinity', False)),
        ('fvcom', 'necofs', 'temp', ('temp', False)),
    ],
)
def test_scalar_layout_matches_sequential_order(model_source, ofs, model_var, expected):
    assert _fields_scalar_layout(_props(model_source, ofs), model_var) == expected


@pytest.mark.parametrize(
    ('model_source', 'ofs'),
    [
        ('roms', 'cbofs'),  # [:, layer, i, j] via roms_nodes() — 4-D, inexpressible
        ('schism', 'loofs2'),  # format_temp_salt has no fields branch at all
    ],
)
def test_unbatchable_sources_return_none(model_source, ofs):
    """Excluded by policy, never by exception.

    ROMS fields indexes ``[:, layer, i, j]``. Falling through to a 3-D
    selection would NOT raise for node numbers below ``len(s_rho)`` — it
    would silently read the wrong physical location.
    """
    assert _fields_scalar_layout(_props(model_source, ofs), 'temp') is None
    assert _fields_current_layout(_props(model_source, ofs)) is None


@pytest.mark.parametrize(
    ('ofs', 'expected'),
    [
        ('stofs_3d_atl', (['horizontalVelX', 'horizontalVelY'], True)),
        ('secofs', (['u', 'v'], False)),
    ],
)
def test_current_layout_matches_sequential_order(ofs, expected):
    assert _fields_current_layout(_props('schism', ofs)) == expected


# --------------------------------------------------------------------------
# Equivalence: batch == sequential
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('model_source', 'ofs', 'model_var', 'disk_var', 'node_first'),
    [
        ('schism', 'stofs_3d_atl', 'temp', 'temperature', True),
        ('schism', 'stofs_3d_atl', 'salinity', 'salinity', True),
        ('schism', 'secofs', 'temp', 'temp', False),
        ('fvcom', 'necofs', 'temp', 'temp', False),
    ],
)
def test_scalar_batch_matches_sequential(model_source, ofs, model_var, disk_var, node_first):
    ds = _ds([disk_var], node_first)
    ctl = _ctlfile(9)
    prop = _props(model_source, ofs)

    got = _precompute_scalar_data(prop, ds, ctl, model_var, _logger())['scalar_data']

    for i, (node, dep) in enumerate(zip(ctl[1], ctl[2])):
        if node_first:
            expected = np.array(ds[disk_var][:, int(node), int(dep)])
        else:
            expected = np.array(ds[disk_var][:, int(dep), int(node)])
        np.testing.assert_array_equal(got[:, i], expected)


def test_stofs_fields_values_decode_to_requested_node_and_layer():
    """Guard on the guard: decode the extracted values back to (node, layer).

    ``_ident`` encodes position as ``t*1e6 + node*1e3 + layer``, so this
    asserts the batch path read the exact cell asked for. Any axis-order flip
    that happens to stay in bounds produces a different node/layer pair and
    fails here rather than silently returning plausible numbers.
    """
    ds = _ds(['temperature'], node_first=True)
    ctl = _ctlfile(6)
    got = _precompute_scalar_data(
        _props('schism', 'stofs_3d_atl'), ds, ctl, 'temp', _logger()
    )['scalar_data']

    for i, (node, dep) in enumerate(zip(ctl[1], ctl[2])):
        for t in range(N_TIME):
            decoded = got[t, i] - t * 1e6
            assert int(decoded // 1e3) == node
            assert int(decoded % 1e3) == dep


def test_stofs_fields_water_level_uses_2d_elevation():
    """STOFS fields WL reads out2d ``elevation`` and is 2-D (no layer axis)."""
    ds = xr.Dataset(
        {'elevation': (('time', 'node'), _ident(N_TIME, N_NODE, 1)[:, :, 0])},
        coords={'time': _times(N_TIME)},
    )
    ctl = _ctlfile(7)
    got = _precompute_scalar_data(
        _props('schism', 'stofs_3d_atl'), ds, ctl, 'zeta', _logger()
    )['scalar_data']

    for i, node in enumerate(ctl[1]):
        np.testing.assert_array_equal(got[:, i], np.array(ds['elevation'][:, int(node)]))


@pytest.mark.parametrize(
    ('ofs', 'uvar', 'vvar', 'node_first'),
    [
        ('stofs_3d_atl', 'horizontalVelX', 'horizontalVelY', True),
        ('secofs', 'u', 'v', False),
    ],
)
def test_current_batch_matches_sequential(ofs, uvar, vvar, node_first):
    ds = _ds([uvar, vvar], node_first)
    ctl = _ctlfile(8)

    got = _precompute_current_data(_props('schism', ofs), ds, ctl, _logger())

    for i, (node, dep) in enumerate(zip(ctl[1], ctl[2])):
        if node_first:
            exp_u = np.array(ds[uvar][:, int(node), int(dep)])
            exp_v = np.array(ds[vvar][:, int(node), int(dep)])
        else:
            exp_u = np.array(ds[uvar][:, int(dep), int(node)])
            exp_v = np.array(ds[vvar][:, int(dep), int(node)])
        np.testing.assert_array_equal(got['u_data'][:, i], exp_u)
        np.testing.assert_array_equal(got['v_data'][:, i], exp_v)


def test_unbatchable_precompute_returns_none_not_raises():
    ds = _ds(['temp'], node_first=False)
    assert _precompute_scalar_data(
        _props('roms', 'cbofs'), ds, _ctlfile(4), 'temp', _logger()
    ) is None
    assert _precompute_current_data(_props('roms', 'cbofs'), ds, _ctlfile(4), _logger()) is None


def test_nan_values_survive_equivalence():
    """Real STOFS elevation carries NaN dry nodes.

    ``np.array_equal`` returns False for identical arrays containing NaN, so
    the suite uses ``assert_array_equal`` throughout; keep a NaN in the
    fixtures so that cannot silently regress.
    """
    data = _ident(N_TIME, N_NODE, 1)[:, :, 0]
    data[:, 3] = np.nan
    ds = xr.Dataset(
        {'elevation': (('time', 'node'), data)}, coords={'time': _times(N_TIME)}
    )
    ctl = ([], [3, 5], [0, 0], [0.0, 0.0], ['a', 'b'])
    got = _precompute_scalar_data(
        _props('schism', 'stofs_3d_atl'), ds, ctl, 'zeta', _logger()
    )['scalar_data']
    assert np.isnan(got[:, 0]).all()
    np.testing.assert_array_equal(got[:, 1], np.array(ds['elevation'][:, 5]))


# --------------------------------------------------------------------------
# Chunk reuse — the actual fix
# --------------------------------------------------------------------------

class _CountingArray:
    """numpy-like wrapper that counts chunk reads.

    ``dask.array.from_array`` calls ``__getitem__`` once per chunk it needs,
    so the counter is a direct, deterministic measure of "how many times was
    a chunk inflated" — the quantity issue #297 is about. Counting this
    rather than wall clock keeps the test meaningful in CI.
    """

    def __init__(self, values):
        self._values = values
        self.shape = values.shape
        self.dtype = values.dtype
        self.ndim = values.ndim
        self.reads = 0

    def __getitem__(self, key):
        self.reads += 1
        return self._values[key]


def _counting_fields_ds(n_time=6):
    """STOFS-fields-shaped dataset: one dask chunk per timestep."""
    import dask.array as dask_array

    values = _ident(n_time, N_NODE, N_LAYER)
    counter = _CountingArray(values)
    darr = dask_array.from_array(counter, chunks=(1, N_NODE, N_LAYER))
    # from_array probes the array once while building the graph — discount it
    # so the counter measures compute-time chunk reads only.
    counter.reads = 0
    ds = xr.Dataset(
        {'temperature': (('time', 'node', 'layer'), darr)},
        coords={'time': _times(n_time)},
    )
    return ds, counter


def test_batch_reads_each_chunk_once_regardless_of_station_count():
    """The fix: chunk reads are O(timesteps), not O(stations x timesteps)."""
    n_time = 6
    for n_stations in (1, 5, 25):
        ds, counter = _counting_fields_ds(n_time)
        _precompute_scalar_data(
            _props('schism', 'stofs_3d_atl'),
            ds,
            _ctlfile(n_stations),
            'temp',
            _logger(),
        )
        assert counter.reads == n_time, (
            f'{n_stations} stations caused {counter.reads} chunk reads; '
            f'expected {n_time} (one per timestep)'
        )


def test_per_station_path_rereads_every_chunk():
    """Characterises the old behaviour, so the win cannot silently regress."""
    n_time, n_stations = 6, 5
    ds, counter = _counting_fields_ds(n_time)
    ctl = _ctlfile(n_stations)
    for node, dep in zip(ctl[1], ctl[2]):
        np.array(ds['temperature'][:, int(node), int(dep)])
    assert counter.reads == n_time * n_stations


# --------------------------------------------------------------------------
# The no-op trap: consumers must actually USE precomputed on fields
# --------------------------------------------------------------------------

def test_format_temp_salt_consumes_precomputed_for_fields():
    """Widening the gate without widening the consumer guards costs MORE.

    If ``format_temp_salt`` still required ``ofsfiletype == 'stations'``, it
    would fall through and re-read per station — paying for the batch read
    AND the per-station read while every log line claimed batching was on.
    """
    from ofs_skill.model_processing.get_node_ofs import format_temp_salt

    ds = _ds(['temperature'], node_first=True)
    ctl = _ctlfile(3)
    prop = _props('schism', 'stofs_3d_atl')
    prop.start_date_full = '2026-07-05T00:00:00Z'
    prop.end_date_full = '2026-07-06T00:00:00Z'

    sentinel = np.full((N_TIME, len(ctl[1])), 77.75, dtype='float32')
    precomputed = {'model_time': np.array(ds['time']), 'scalar_data': sentinel}

    series = format_temp_salt(prop, ds, ctl, 'temp', 0, precomputed=precomputed)

    assert series, 'formatted series was empty'
    assert any('77.75' in str(row) for row in series), (
        'precomputed values were ignored — the consumer guard still requires '
        "ofsfiletype == 'stations' and the change is a no-op"
    )


# --------------------------------------------------------------------------
# Review follow-ups
# --------------------------------------------------------------------------

def test_adcirc_only_water_level_is_batchable():
    """STOFS-2D-Global has no temp/salt/currents — the sequential path raises.

    Returning a layout for them would batch a variable the sequential code
    deliberately refuses to produce.
    """
    prop = _props('adcirc', 'stofs_2d_glo')
    assert _fields_scalar_layout(prop, 'zeta') == ('zeta', False)
    assert _fields_scalar_layout(prop, 'temp') is None
    assert _fields_scalar_layout(prop, 'salinity') is None
    assert _fields_current_layout(prop) is None


def test_non_stofs_schism_fields_water_level_is_batchable():
    """format_waterlevel's SCHISM fields branch is not split on stofs/secofs.

    It handles every SCHISM OFS with a plain 2-D ``[:, node]`` read, so
    loofs2 fields water level batches even though its temp/salt do not.
    """
    prop = _props('schism', 'loofs2')
    assert _fields_scalar_layout(prop, 'zeta') == ('zeta', False)
    assert _fields_scalar_layout(prop, 'temp') is None
    assert _fields_current_layout(prop) is None


def test_current_layout_secofs_predicate_is_exact():
    """``_fields_current_layout`` mirrors format_currents' exact match.

    format_currents tests ``prop.ofs in ['secofs']`` while format_temp_salt
    tests ``'secofs' in prop.ofs``. The two are inconsistent upstream, so each
    resolver copies the predicate of the function it mirrors rather than
    silently adopting the looser one.
    """
    assert _fields_current_layout(_props('schism', 'secofs')) == (['u', 'v'], False)
    assert _fields_current_layout(_props('schism', 'secofs_v2')) is None
    # The scalar resolver keeps the substring form, matching format_temp_salt.
    assert _fields_scalar_layout(_props('schism', 'secofs_v2'), 'temp') == ('temp', False)


def test_num_workers_budget_accounts_for_slots_and_fused_vars():
    """Peak is workers x chunk x n_vars x slots — all three must divide in.

    The naive form (budget // chunk_bytes) computed 14 workers for STOFS-3D
    currents and then actually peaked near 33 GB: u and v are fused into one
    compute, and _extract_guard admits parallel_extract_slots of them.
    """
    import dask.array as dask_array

    from ofs_skill.model_processing.get_node_ofs import (
        _EXTRACT_MEM_BUDGET_BYTES,
        _EXTRACT_MIN_WORKERS,
        _extract_num_workers,
    )

    chunk_bytes = 600 * 1024**2
    n_node = chunk_bytes // (4 * 49)
    darr = dask_array.zeros((3, n_node, 49), chunks=(1, n_node, 49), dtype='float32')
    ds = xr.Dataset({'temperature': (('time', 'node', 'layer'), darr)})

    single = _extract_num_workers(ds, 'temperature', None, n_vars=1, slots=1)
    fused = _extract_num_workers(ds, 'temperature', None, n_vars=2, slots=2)

    assert fused <= single
    # Actual peak across every slot must respect the budget, unless the
    # MIN_WORKERS floor (deliberately) overrides it.
    peak = fused * chunk_bytes * 2 * 2
    assert peak <= _EXTRACT_MEM_BUDGET_BYTES or fused == _EXTRACT_MIN_WORKERS


def test_thin_chunks_keep_dask_defaults():
    """Stations-file behaviour must be untouched: thin chunks -> None."""
    import dask.array as dask_array

    from ofs_skill.model_processing.get_node_ofs import _extract_num_workers

    darr = dask_array.zeros((10, 500, 20), chunks=(10, 500, 20), dtype='float32')
    ds = xr.Dataset({'temp': (('time', 'station', 'layer'), darr)})
    assert _extract_num_workers(ds, 'temp', None) is None
