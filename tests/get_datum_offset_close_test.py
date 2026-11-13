"""Tests for vdatum file-handle cleanup in ``get_datum_offset.py``.

Issue #94: the vdatum grid datasets are opened with the h5netcdf/h5py
backend and, when left open, are finalized during interpreter shutdown
after h5py's globals are torn down, printing noisy
``TypeError: bad operand type for unary ~: 'NoneType'`` tracebacks.

These tests cover the ``_close_quietly`` helper directly and verify that
``get_datum_offset`` closes the vdatum dataset it opens once the offset
has been computed, even though the offset itself is returned normally.
"""

import importlib
import logging

import numpy as np
import xarray as xr

# NOTE: ``ofs_skill.model_processing`` re-exports a *function* named
# ``get_datum_offset``, which shadows the submodule of the same name for
# attribute-style access. Import the module object explicitly so ``gdo``
# is the module (with ``_close_quietly`` / ``read_vdatum_from_bucket`` on
# it), not the function.
gdo = importlib.import_module(
    'ofs_skill.model_processing.get_datum_offset')

logger = logging.getLogger('test')


class _Closable:
    """Minimal stand-in that records whether close() fired."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_close_quietly_closes_object():
    obj = _Closable()
    gdo._close_quietly(obj)
    assert obj.closed is True


def test_close_quietly_ignores_none():
    # Must not raise.
    gdo._close_quietly(None)


def test_close_quietly_ignores_object_without_close():
    # A plain object (or an int error code, or a DataFrame) has no dataset
    # close semantics; the helper is a no-op rather than raising.
    gdo._close_quietly(object())
    gdo._close_quietly(-9990)


def test_close_quietly_swallows_close_exception():
    class _Broken:
        def __init__(self):
            self.called = False

        def close(self):
            self.called = True
            raise RuntimeError('boom')

    obj = _Broken()
    gdo._close_quietly(obj)  # Must not raise.
    assert obj.called is True


class _MockProps:
    """FVCOM stations prop with a target datum that lives in the vdatum file."""

    def __init__(self):
        self.ofs = 'ngofs2'  # generic fvcom, not secofs/glofs/stofs
        self.ofsfiletype = 'stations'
        self.model_source = 'fvcom'
        self.datum = 'MLLW'
        self.path = '.'
        self.config_file = None
        self.start_date_full = '2026-03-28T00:00:00Z'


def _fvcom_model(n_station=4):
    return xr.Dataset(
        data_vars={
            'lon': (('station',),
                    np.linspace(288.0, 291.0, n_station, dtype=np.float64)),
            'lat': (('station',),
                    np.linspace(29.0, 30.0, n_station, dtype=np.float64)),
        }
    )


def _fvcom_vdatum(model):
    """Vdatum dataset whose lon/lat match the model nodes exactly so the
    nearest-node search is deterministic. Carries an mllwtomsl field."""
    lon = np.asarray(model['lon'].values)
    lat = np.asarray(model['lat'].values)
    return xr.Dataset(
        data_vars={
            'mllwtomsl': (('node',), np.full(lon.shape, 0.5, dtype=np.float64)),
        },
        coords={
            'longitude': (('node',), lon),
            'latitude': (('node',), lat),
        },
    )


def test_get_datum_offset_closes_vdatum_dataset(monkeypatch):
    """The vdatum dataset opened for a normal (successful) datum conversion
    is closed before get_datum_offset returns, so its file handle is not
    left for the shutdown-time GC (issue #94)."""
    model = _fvcom_model()
    base_vdatum = _fvcom_vdatum(model)

    class _TrackingDataset:
        """Wrap an xr.Dataset so the variable lookups still work but we can
        observe close()."""

        def __init__(self, ds):
            self._ds = ds
            self.closed = False

        def __getitem__(self, key):
            return self._ds[key]

        def close(self):
            self.closed = True

    tracker = _TrackingDataset(base_vdatum)

    # Stub the S3 read so the test is offline and returns our tracker.
    monkeypatch.setattr(gdo, 'read_vdatum_from_bucket',
                        lambda prop, log: tracker)

    prop = _MockProps()
    offset = gdo.get_datum_offset(prop, 0, model, '8000000', logger)

    # The offset for node 0: mllwtomsl=0.5 -> datum_offset=0.5 (no sign flip
    # for a generic fvcom ofs). Value correctness is secondary here; the
    # point is the handle got closed.
    assert isinstance(offset, float)
    assert tracker.closed is True, \
        'vdatum dataset should be closed before get_datum_offset returns'
