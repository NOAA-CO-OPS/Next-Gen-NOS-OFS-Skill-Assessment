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


# ---------------------------------------------------------------------------
# GEOID18 grid discovery and remediation text (issues #127, #216, #295)
#
# PROJ reports an absent GEOID18 grid and an unreachable NOAA bucket with
# byte-identical text -- both are "Error 1029 (File not found or
# invalid)". Verified on pyproj 3.7.2 / PROJ 9.7.1 by running a real
# conversion twice: once with the grid removed from every PROJ data
# directory, and once with the grid on disk but outbound HTTPS pointed
# at a dead proxy. So the exception text cannot be used to classify the
# fault, and the retry policy must stay uniform. The filesystem is
# consulted only to choose what the operator is told.
# ---------------------------------------------------------------------------

REAL_PROJ_1029 = (
    'Invalid projection +proj=pipeline +step +proj=vgridshift '
    '+grids=us_noaa_g2018u0.tif: (Internal Proj Error: proj_create: '
    'Error 1029 (File not found or invalid): pipeline: Pipeline: Bad step '
    'definition: proj=vgridshift (File not found or invalid))')


def test_real_proj_1029_text_still_retries(monkeypatch):
    """The wording PROJ actually emits must not short-circuit the retries.

    A transient NOAA-bucket outage produces this exact text with the
    GEOID18 grid sitting on disk, so treating it as a permanent
    missing-grid fault would strip the #127 protection from the case it
    was written for.
    """
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('navd88', 'mllw')})
    sleeps = []
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: sleeps.append(attempt))

    err = pyproj.exceptions.ProjError(REAL_PROJ_1029)
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=[err, err, (36.0, -76.0, 9.0)]) as mock_convert:
        out = vdatum_resilient.convert('navd88', 'mllw', 36.94, -76.33, 10.0)

    assert out == (36.0, -76.0, 9.0)
    assert mock_convert.call_count == 3
    assert sleeps == [0, 1]


def test_real_proj_1029_still_reaches_the_offline_fallback(monkeypatch):
    """After the retries are exhausted the online=False fallback still runs.

    With a warm PROJ cache and a dead network that fallback succeeds, so
    skipping it would drop stations that are perfectly convertible.
    """
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('navd88', 'mllw')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)

    err = pyproj.exceptions.ProjError(REAL_PROJ_1029)
    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=[err, err, err, err,
                         (36.0, -76.0, 9.0)]) as mock_convert:
        out = vdatum_resilient.convert('navd88', 'mllw', 36.94, -76.33, 10.0)

    assert out == (36.0, -76.0, 9.0)
    assert mock_convert.call_args.kwargs['online'] is False


def test_find_geoid18_grid_searches_every_proj_data_dir(monkeypatch,
                                                        tmp_path):
    """The grid may sit in any PROJ data directory, not just the first."""
    empty = tmp_path / 'empty'
    empty.mkdir()
    holding = tmp_path / 'holding'
    holding.mkdir()
    grid = holding / vdatum_resilient.GEOID18_GRID
    grid.write_bytes(b'not a real grid')

    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [empty, holding])
    assert vdatum_resilient.find_geoid18_grid() == grid

    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [empty])
    assert vdatum_resilient.find_geoid18_grid() is None


def test_proj_data_dirs_splits_multi_directory_settings(monkeypatch,
                                                        tmp_path):
    """PROJ_DATA may hold several directories joined by os.pathsep."""
    import os

    first = tmp_path / 'a'
    second = tmp_path / 'b'
    monkeypatch.setattr(vdatum_resilient.pyproj.datadir, 'get_data_dir',
                        lambda: os.pathsep.join([str(first), str(second)]))
    monkeypatch.setattr(vdatum_resilient.pyproj.datadir,
                        'get_user_data_dir', lambda: '')

    assert vdatum_resilient._proj_data_dirs() == [first, second]


def test_remediation_for_a_missing_grid_says_download_it(monkeypatch,
                                                         tmp_path):
    """Grid absent -> point the operator at the downloader."""
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    text = vdatum_resilient.grid_remediation()
    assert 'make proj-grids' in text
    assert vdatum_resilient.GEOID18_GRID in text
    assert str(tmp_path) in text


def test_remediation_for_a_present_grid_blames_network_egress(monkeypatch,
                                                              tmp_path):
    """Grid present -> the download is not the problem, egress is."""
    grid = tmp_path / vdatum_resilient.GEOID18_GRID
    grid.write_bytes(b'not a real grid')
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])

    text = vdatum_resilient.grid_remediation()
    assert vdatum_resilient.VDATUM_GRID_HOST in text
    assert 'outbound' in text.lower()
    # Do not send the operator after a download that is already done.
    assert 'make proj-grids' not in text


def test_the_two_remedies_are_never_the_same_text(monkeypatch, tmp_path):
    """The whole point: identical PROJ text, two distinguishable remedies."""
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    missing = vdatum_resilient.grid_remediation()
    (tmp_path / vdatum_resilient.GEOID18_GRID).write_bytes(b'not a grid')
    present = vdatum_resilient.grid_remediation()
    assert missing != present


def test_permanent_failure_message_carries_the_remediation(monkeypatch,
                                                           caplog, tmp_path):
    """A dropped station must be told how to fix the environment."""
    monkeypatch.setattr(vdatum_resilient, '_PRIMED_PAIRS',
                        {('navd88', 'mllw')})
    monkeypatch.setattr(vdatum_resilient, '_sleep_with_backoff',
                        lambda attempt: None)
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])

    with mock.patch.object(
            vdatum_resilient.vdatum, 'convert',
            side_effect=pyproj.exceptions.ProjError(REAL_PROJ_1029)):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(pyproj.exceptions.ProjError):
                vdatum_resilient.convert('navd88', 'mllw', 36.94, -76.33,
                                         10.0, station_id='8638901')

    msgs = [r.message for r in caplog.records]
    assert any('make proj-grids' in m for m in msgs), msgs
    assert any(vdatum_resilient.GEOID18_GRID in m for m in msgs), msgs
    assert any('8638901' in m for m in msgs), msgs
