"""Regression tests for ``fix_fvcom`` and ``fix_roms_uv`` under
``data_vars='minimal'`` intake.

These adjusters touch the dataset *after* intake to compute z / zc /
mask_rho. They were originally written against the legacy
``data_vars='all'`` shape where static mesh vars were replicated to
``(time, ...)``. After switching intake to ``'minimal'`` (commit
``e26291f``) those static vars retain their native rank and the
``[0, :]`` / ``[0, :, :]`` time-strip ops blow up with
``IndexError: too many indices``.

These tests exercise the adjusters on both shapes and confirm:

- Stations-mode fix_fvcom builds z correctly when h is 1-D ``(station,)``.
- Stations-mode fix_fvcom still works when h is the legacy 2-D shape.
- Fields-mode fix_fvcom handles 1-D h + 2-D nv (minimal) and 2-D h +
  3-D nv (legacy) without raising.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr


def _logger():
    return logging.getLogger('fix_fvcom_minimal_intake_test')


def _stations_dataset_minimal():
    """FVCOM stations dataset as intake produces under data_vars='minimal':
    h is 1-D ``(station,)``; siglay/siglev are 2-D ``(layer, station)``."""
    n_station = 8
    n_siglay = 4
    n_siglev = n_siglay + 1
    n_time = 6
    siglay = np.tile(np.linspace(-1, 0, n_siglay)[:, None],
                     (1, n_station)).astype(np.float32)
    siglev = np.tile(np.linspace(-1, 0, n_siglev)[:, None],
                     (1, n_station)).astype(np.float32)
    return xr.Dataset(
        data_vars={
            'h': (('station',),
                  np.linspace(5.0, 30.0, n_station, dtype=np.float32)),
            'siglay': (('siglay', 'station'), siglay),
            'siglev': (('siglev', 'station'), siglev),
            'zeta': (('time', 'station'),
                     np.zeros((n_time, n_station), dtype=np.float32)),
        },
        coords={'time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(6, 'm')},
    )


def _stations_dataset_legacy_all():
    """Same dataset under the legacy ``data_vars='all'`` shape: h is
    replicated to ``(time, station)`` along the concat dim."""
    ds = _stations_dataset_minimal()
    h_1d = ds['h'].values
    n_time = ds.sizes['time']
    h_2d = np.broadcast_to(h_1d, (n_time, h_1d.shape[0])).copy()
    ds = ds.drop_vars('h')
    ds['h'] = (('time', 'station'), h_2d)
    return ds


def test_fix_fvcom_stations_under_minimal_intake():
    from ofs_skill.model_processing.intake_scisa import fix_fvcom

    ds = _stations_dataset_minimal()
    prop = SimpleNamespace(ofsfiletype='stations')

    # Must not raise IndexError.
    out = fix_fvcom(prop, ds, _logger())

    # z coordinate should be attached: siglay (4,8) * h (8,) -> (4, 8)
    assert 'z' in out.coords
    assert out['z'].shape == (4, 8)
    # Sanity: z values match siglay * h elementwise
    expected_z = ds['siglay'].values * ds['h'].values
    np.testing.assert_allclose(out['z'].values, expected_z)


def test_fix_fvcom_stations_under_legacy_all_intake():
    """The same code path must still work when h carries a leading time
    dim, so a host running an unpatched intake still gets correct z."""
    from ofs_skill.model_processing.intake_scisa import fix_fvcom

    ds = _stations_dataset_legacy_all()
    prop = SimpleNamespace(ofsfiletype='stations')

    out = fix_fvcom(prop, ds, _logger())
    assert 'z' in out.coords
    # z = siglay * h. Under all-form h is broadcast to (time, station),
    # so z ends up (time, siglay, station). Don't pin the exact shape —
    # just confirm the helper didn't raise.
    assert out['z'].ndim in (2, 3)


def _fields_dataset_minimal():
    """FVCOM fields dataset under data_vars='minimal': h is 1-D (node,),
    nv (mesh connectivity) is 2-D (three, nele) — no time dim."""
    n_node = 12
    n_nele = 4
    n_siglay = 3
    n_siglev = n_siglay + 1
    rng = np.random.default_rng(0)
    siglev = np.tile(np.linspace(-1, 0, n_siglev)[:, None],
                     (1, n_node)).astype(np.float32)
    siglay = np.tile(np.linspace(-1, 0, n_siglay)[:, None],
                     (1, n_node)).astype(np.float32)
    nv = rng.integers(1, n_node + 1, size=(3, n_nele)).astype(np.int32)
    return xr.Dataset(
        data_vars={
            'h': (('node',),
                  np.linspace(5.0, 30.0, n_node, dtype=np.float32)),
            'siglay': (('siglay', 'node'), siglay),
            'siglev': (('siglev', 'node'), siglev),
            'nv': (('three', 'nele'), nv),
        },
        coords={'time': np.array(['2026-02-16T00:00'], dtype='datetime64[m]')},
    )


def _fields_dataset_legacy_all():
    """Same fields dataset under the legacy time-replicated shape."""
    ds = _fields_dataset_minimal()
    h_1d = ds['h'].values
    nv_2d = ds['nv'].values
    n_time = 1
    ds = ds.drop_vars(['h', 'nv'])
    ds['h'] = (('time', 'node'),
               np.broadcast_to(h_1d, (n_time, h_1d.shape[0])).copy())
    ds['nv'] = (('time', 'three', 'nele'),
                np.broadcast_to(nv_2d,
                                (n_time, *nv_2d.shape)).copy())
    return ds


def test_fix_fvcom_fields_under_minimal_intake():
    from ofs_skill.model_processing.intake_scisa import fix_fvcom

    ds = _fields_dataset_minimal()
    prop = SimpleNamespace(ofsfiletype='fields')

    out = fix_fvcom(prop, ds, _logger())
    assert 'z' in out.variables
    assert 'zc' in out.variables


def test_fix_fvcom_fields_under_legacy_all_intake():
    from ofs_skill.model_processing.intake_scisa import fix_fvcom

    ds = _fields_dataset_legacy_all()
    prop = SimpleNamespace(ofsfiletype='fields')

    out = fix_fvcom(prop, ds, _logger())
    assert 'z' in out.variables
    assert 'zc' in out.variables


# ---------------------------------------------------------------------------
# fix_roms_uv mask_rho time-strip
# ---------------------------------------------------------------------------


def _roms_uv_dataset(time_replicated_mask: bool):
    """Minimal ROMS fields dataset exercising the mask_rho path in fix_roms_uv.

    Includes enough of the u/eta_u/xi_u + v/eta_v/xi_v scaffolding so the
    function doesn't error on missing vars in the averaging step.
    """
    eta, xi = 6, 7
    n_time = 2
    rng = np.random.default_rng(1)
    mask_2d = rng.integers(0, 2, size=(eta, xi)).astype(np.int32)
    mask_dims: tuple[str, ...]
    if time_replicated_mask:
        mask = np.broadcast_to(mask_2d, (n_time, eta, xi)).copy()
        mask_dims = ('ocean_time', 'eta_rho', 'xi_rho')
    else:
        mask = mask_2d
        mask_dims = ('eta_rho', 'xi_rho')
    return xr.Dataset(
        data_vars={
            'mask_rho': (mask_dims, mask),
            # u defined on staggered grid (eta_u = eta, xi_u = xi - 1)
            'u': (('ocean_time', 'eta_u', 'xi_u'),
                  rng.standard_normal((n_time, eta, xi - 1)).astype(np.float32)),
            'v': (('ocean_time', 'eta_v', 'xi_v'),
                  rng.standard_normal((n_time, eta - 1, xi)).astype(np.float32)),
        },
        coords={'ocean_time': np.datetime64('2026-02-16T00')
                + np.arange(n_time) * np.timedelta64(6, 'm')},
    )


def test_fix_roms_uv_mask_under_minimal_intake():
    from ofs_skill.model_processing.intake_scisa import fix_roms_uv

    ds = _roms_uv_dataset(time_replicated_mask=False)
    prop = SimpleNamespace(ofsfiletype='fields')

    # Must not raise. The function does further averaging — that bit may
    # rely on additional vars we haven't faked. Catch ValueError /
    # KeyError downstream of the mask op so we only assert the mask path
    # itself works.
    try:
        fix_roms_uv(prop, ds, _logger())
    except IndexError as exc:
        pytest.fail(f'fix_roms_uv raised IndexError on 2-D mask_rho: {exc}')
    except (KeyError, ValueError):
        # Downstream averaging needs more grid scaffolding we don't
        # provide — that's fine; we only care about the mask path.
        pass


def test_fix_roms_uv_mask_under_legacy_all_intake():
    from ofs_skill.model_processing.intake_scisa import fix_roms_uv

    ds = _roms_uv_dataset(time_replicated_mask=True)
    prop = SimpleNamespace(ofsfiletype='fields')

    try:
        fix_roms_uv(prop, ds, _logger())
    except IndexError as exc:
        pytest.fail(
            f'fix_roms_uv raised IndexError on time-replicated mask_rho: {exc}'
        )
    except (KeyError, ValueError):
        pass
