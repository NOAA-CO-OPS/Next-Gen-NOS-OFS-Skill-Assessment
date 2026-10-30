"""Startup preflight for the PROJ vertical datum grids (#127, #216, #295).

Every ``coastalmodeling_vdatum`` pipeline routes through the GEOID18
grid ``us_noaa_g2018u0.tif``, which PROJ resolves by bare filename. When
it cannot be resolved, every conversion fails and every station needing
one is dropped -- 192 USGS gauges per production run, for months, with
nothing louder than an INFO line to show for it.

The gate is the conversion itself, never the presence of a file on disk:
PROJ also serves grids out of its network cache (``cache.db``, which
never holds a ``.tif``), so a disk probe would abort hosts that convert
perfectly well. These tests pin that distinction down.

No test here touches the network or downloads a grid.
"""
from __future__ import annotations

import importlib
import logging

import pyproj.exceptions
import pytest

# Import the module itself rather than the name the package __init__
# re-exports, so monkeypatching lands on the object under test.
vdatum_resilient = importlib.import_module(
    'ofs_skill.obs_retrieval.vdatum_resilient')


class _Prop:
    """Minimal stand-in for the model properties object."""

    def __init__(self, ofs='cbofs', datum='MLLW', ofsfiletype='stations'):
        self.ofs = ofs
        self.datum = datum
        self.ofsfiletype = ofsfiletype


@pytest.fixture()
def logger(caplog):
    """A real logger whose records caplog can inspect."""
    caplog.set_level(logging.DEBUG)
    return logging.getLogger('proj_vdatum_preflight_test')


def _messages(caplog):
    return [r.message for r in caplog.records]


def _raiser(exc):
    def _boom(*args, **kwargs):
        raise exc
    return _boom


def test_preflight_passes_whenever_the_conversion_works(monkeypatch, caplog,
                                                        logger, tmp_path):
    """A working conversion is the only thing that matters.

    The GEOID18 grid is deliberately absent from every PROJ data
    directory here: that is the shape of a host serving the grid from
    PROJ's network cache, which is normal on developer laptops and CI.
    Such a run must not be blocked.
    """
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    monkeypatch.setattr(vdatum_resilient, 'convert',
                        lambda *a, **k: (36.94, -76.33, -0.5))

    vdatum_resilient.validate_proj_vdatum_grids(_Prop(), logger)

    assert vdatum_resilient.find_geoid18_grid() is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_preflight_aborts_the_default_production_run(monkeypatch, caplog,
                                                     logger, tmp_path):
    """cbofs at the default -d MLLW is exactly where the bug was reported.

    MLLW is both the CLI default and the cbofs native datum, so a gate
    that skips "no conversion needed" runs would have a hole precisely
    over the failure. The observation side converts on the datum each
    *station* reports: a NAVD88 USGS gauge is still converted to MLLW.
    """
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    monkeypatch.setattr(
        vdatum_resilient, 'convert',
        _raiser(pyproj.exceptions.ProjError('Error 1029')))

    with pytest.raises(SystemExit):
        vdatum_resilient.validate_proj_vdatum_grids(
            _Prop(ofs='cbofs', datum='MLLW'), logger)

    msgs = _messages(caplog)
    assert any('Abort!' in m for m in msgs), msgs
    assert any('make proj-grids' in m for m in msgs), msgs


@pytest.mark.parametrize(('ofs', 'datum'), [
    ('cbofs', 'MLLW'),
    ('cbofs', 'NAVD88'),
    ('sfbofs', 'MLLW'),
    ('necofs', 'MSL'),
    ('stofs_3d_atl', 'NAVD88'),
    ('stofs_2d_glo', 'MSL'),
])
def test_a_broken_host_aborts_regardless_of_the_requested_datum(
        monkeypatch, caplog, logger, tmp_path, ofs, datum):
    """No coastal run is exempt: they all convert NAVD88 observations."""
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    monkeypatch.setattr(
        vdatum_resilient, 'convert',
        _raiser(pyproj.exceptions.ProjError('Error 1029')))

    with pytest.raises(SystemExit):
        vdatum_resilient.validate_proj_vdatum_grids(
            _Prop(ofs=ofs, datum=datum), logger)


@pytest.mark.parametrize('ofs', vdatum_resilient.GREAT_LAKES_OFS)
def test_great_lakes_runs_are_warned_but_not_killed(monkeypatch, caplog,
                                                    logger, tmp_path, ofs):
    """Great Lakes model offsets are fixed arithmetic and need no PROJ.

    Those runs work today on a host with no grids at all, so aborting
    them would be a regression. They still get a loud ERROR, because a
    USGS gauge reporting NAVD88 will be missing from the assessment.
    """
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    monkeypatch.setattr(
        vdatum_resilient, 'convert',
        _raiser(pyproj.exceptions.ProjError('Error 1029')))

    vdatum_resilient.validate_proj_vdatum_grids(
        _Prop(ofs=ofs, datum='IGLD85'), logger)

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, _messages(caplog)
    assert any('USGS' in r.message for r in errors), _messages(caplog)


def test_the_probe_converts_the_pair_that_needs_the_geoid18_grid(
        monkeypatch, logger, tmp_path):
    """A probe on the wrong pair would validate nothing.

    Every pair ``coastalmodeling_vdatum`` supports is built as
    "GEOID18 grid -> ITRF2020 helmert chain -> one NOAA-bucket grid", so
    navd88 -> mllw exercises the resolution every other pair depends on.
    A probe silently changed to an identity or unrelated pair would keep
    every other test in this file green.
    """
    recorded = []

    def _record(*args, **kwargs):
        recorded.append(args)
        return (36.94, -76.33, -0.5)

    monkeypatch.setattr(vdatum_resilient, 'convert', _record)
    vdatum_resilient.validate_proj_vdatum_grids(_Prop(), logger)

    assert recorded, 'the preflight never attempted a conversion'
    assert recorded[0][:2] == ('navd88', 'mllw'), recorded[0]
    assert recorded[0][0] != recorded[0][1]


def test_missing_grid_and_unreachable_bucket_get_different_remedies(
        monkeypatch, caplog, logger, tmp_path):
    """Identical PROJ text, two faults, two remedies."""
    monkeypatch.setattr(vdatum_resilient, '_proj_data_dirs',
                        lambda: [tmp_path])
    monkeypatch.setattr(
        vdatum_resilient, 'convert',
        _raiser(pyproj.exceptions.ProjError('Error 1029')))

    with pytest.raises(SystemExit):
        vdatum_resilient.validate_proj_vdatum_grids(_Prop(), logger)
    missing = _messages(caplog)
    assert any('make proj-grids' in m for m in missing), missing

    caplog.clear()
    (tmp_path / vdatum_resilient.GEOID18_GRID).write_bytes(b'not a grid')
    with pytest.raises(SystemExit):
        vdatum_resilient.validate_proj_vdatum_grids(_Prop(), logger)
    present = _messages(caplog)

    assert missing != present
    assert any(vdatum_resilient.VDATUM_GRID_HOST in m for m in present), present
    assert not any('make proj-grids' in m for m in present), present
