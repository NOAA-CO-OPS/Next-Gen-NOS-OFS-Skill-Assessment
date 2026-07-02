"""Regression tests for vdatum_resilient.convert (transient PROJ failures)."""
from __future__ import annotations

import logging
from unittest import mock

import pyproj.exceptions
import pytest

from ofs_skill.obs_retrieval import vdatum_resilient


def test_convert_passes_through_on_success(monkeypatch):
    """Happy path: underlying vdatum.convert returns once, wrapper returns same."""
    # Mark the pair as already primed so the wrapper skips the
    # single-threaded warm-up and just makes the real call.
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('mllw', 'navd88')})
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            return_value=(36.94, -76.33, 8.5)) as mock_convert:
        out = vdatum_resilient.convert('mllw', 'navd88',
                                       36.94, -76.33, 10.0,
                                       station_id='8638901')
    assert out == (36.94, -76.33, 8.5)
    assert mock_convert.call_count == 1
    # First call always uses online=True (the path the project's caller
    # used directly before the wrapper).
    assert mock_convert.call_args.kwargs['online'] is True


def test_convert_retries_on_proj_error_then_succeeds(monkeypatch):
    """Transient PROJ network failure -> retry -> succeed."""
    sleeps = []
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('mllw', 'navd88')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: sleeps.append(attempt))

    err = pyproj.exceptions.ProjError(
        'Invalid projection ... Error 1029 (File not found or invalid)')
    side_effects = [err, err, (36.0, -76.0, 9.0)]
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=side_effects) as mock_convert:
        out = vdatum_resilient.convert(
            'mllw', 'navd88', 36.0, -76.0, 10.0)
    assert out == (36.0, -76.0, 9.0)
    assert mock_convert.call_count == 3
    assert sleeps == [0, 1]  # backoff for first two retries only


def test_convert_falls_back_to_offline_when_online_exhausted(monkeypatch):
    """All online attempts fail -> single offline retry succeeds (cached grid)."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('mllw', 'navd88')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    err = pyproj.exceptions.ProjError('1029')
    online_results = [err] * vdatum_resilient._RETRY_ATTEMPTS

    def side_effect(*args, online, **kwargs):
        if online:
            raise online_results.pop(0)
        return (37.0, -76.0, 9.5)

    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=side_effect) as mock_convert:
        out = vdatum_resilient.convert(
            'mllw', 'navd88', 37.0, -76.0, 10.0)
    assert out == (37.0, -76.0, 9.5)
    # _RETRY_ATTEMPTS online tries, then 1 offline try.
    assert mock_convert.call_count == vdatum_resilient._RETRY_ATTEMPTS + 1


def test_convert_raises_when_all_attempts_fail(monkeypatch, caplog):
    """Permanent failure: all online + 1 offline raise -> ProjError bubbles up."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('mllw', 'navd88')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    err = pyproj.exceptions.ProjError('1029')
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert', side_effect=err):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(pyproj.exceptions.ProjError):
                vdatum_resilient.convert(
                    'mllw', 'navd88', 36.0, -76.0, 10.0,
                    station_id='8638901')
    msgs = [r.message for r in caplog.records]
    assert any('permanently failed' in m for m in msgs), msgs
    assert any('PROJ_NETWORK' in m for m in msgs), msgs


def test_convert_passes_station_id_in_warning(caplog, monkeypatch):
    """Station id should appear in the per-attempt warning so users can grep."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('mllw', 'navd88')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    err = pyproj.exceptions.ProjError('1029')

    def side_effect(*args, online, **kwargs):
        if online:
            raise err
        return (37.0, -76.0, 9.5)

    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert', side_effect=side_effect):
        with caplog.at_level(logging.WARNING):
            vdatum_resilient.convert(
                'mllw', 'navd88', 37.0, -76.0, 10.0,
                station_id='8638901')
    assert any('8638901' in r.message for r in caplog.records), \
        [r.message for r in caplog.records]


def test_prime_runs_only_once_per_pair(monkeypatch):
    """First call to convert() for a (vd_from, vd_to) pair should run a
    prime call; subsequent calls should not re-prime."""
    # Reset prime state so this test is order-independent.
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS', set())

    calls: list[tuple] = []

    def fake_convert(vd_from, vd_to, lat, lon, z, *, online, epoch=None):
        calls.append((vd_from, vd_to, float(lat), float(lon), float(z)))
        return (lat, lon, z + 0.5)

    monkeypatch.setattr(vdatum_resilient.vdatum, 'convert', fake_convert)
    vdatum_resilient.convert('mllw', 'navd88', 37.0, -76.0, 10.0)
    vdatum_resilient.convert('mllw', 'navd88', 38.0, -75.0, 11.0)

    # First convert: prime (uses _PRIME_LAT/_PRIME_LON, z=0.0) + real call.
    # Second convert: no prime, just real call.
    assert calls == [
        ('mllw', 'navd88',
         vdatum_resilient._PRIME_LAT,
         vdatum_resilient._PRIME_LON,
         0.0),
        ('mllw', 'navd88', 37.0, -76.0, 10.0),
        ('mllw', 'navd88', 38.0, -75.0, 11.0),
    ]


def test_prime_failure_releases_lock(monkeypatch, caplog):
    """If the prime call itself fails, the pair should still be marked
    primed so the lock is released and other callers proceed."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS', set())
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    err = pyproj.exceptions.ProjError('1029')

    def fake_convert(*args, online, **kwargs):
        # Prime fails, then the real retry loop also fails.
        raise err

    monkeypatch.setattr(vdatum_resilient.vdatum, 'convert', fake_convert)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(pyproj.exceptions.ProjError):
            vdatum_resilient.convert('mllw', 'navd88', 37.0, -76.0, 10.0)

    assert ('mllw', 'navd88') in vdatum_resilient._PRIMED_PAIRS
    assert any('PROJ grid prime failed' in r.message
               for r in caplog.records), \
        [r.message for r in caplog.records]


def test_convert_rejects_unknown_datum_without_calling_vdatum(monkeypatch):
    """An out-of-vocabulary datum (e.g. CHS 'igld', missing the '85') must
    raise a clear ValueError up front, never reaching vdatum.convert where
    the dependency's precedence bug would raise a cryptic UnboundLocalError."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS', set())
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert') as mock_convert:
        with pytest.raises(ValueError, match='Unsupported vertical datum'):
            vdatum_resilient.convert('igld', 'mllw', 45.0, -65.0, 10.0,
                                     station_id='5cebf1e0')
    mock_convert.assert_not_called()


def test_convert_raises_valueerror_for_inwocab_pair_with_no_path(monkeypatch):
    """An in-vocabulary pair with no conversion pipeline (Great-Lakes datum
    to a tidal datum) makes the dependency raise UnboundLocalError. The
    wrapper must translate that into a clean ValueError without retrying."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('igld85', 'mllw')})
    sleeps = []
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: sleeps.append(attempt))
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=UnboundLocalError(
                "cannot access local variable 'h_g'")) as mock_convert:
        with pytest.raises(ValueError, match='No vertical datum conversion'):
            vdatum_resilient.convert('igld85', 'mllw', 45.0, -65.0, 10.0)
    # Deterministic failure -> exactly one attempt, no backoff, no offline.
    assert mock_convert.call_count == 1
    assert sleeps == []


def test_prime_swallows_unbound_local_error(monkeypatch):
    """If the prime call hits the dependency's UnboundLocalError, the pair is
    still marked primed (lock released) and the real call surfaces the clean
    ValueError rather than the raw UnboundLocalError leaking from prime."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS', set())
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=UnboundLocalError(
                "cannot access local variable 'h_g'")):
        with pytest.raises(ValueError, match='No vertical datum conversion'):
            vdatum_resilient.convert('igld85', 'mllw', 45.0, -65.0, 10.0)
    assert ('igld85', 'mllw') in vdatum_resilient._PRIMED_PAIRS


def test_module_import_enables_pyproj_network():
    """Importing the module should leave pyproj network globally enabled so
    that worker threads spawned later inherit network=True."""
    # The import already happened at module scope; verify side-effect.
    import pyproj
    assert pyproj.network.is_network_enabled() is True
