"""SECOFS local vdatum data must be validated up front, and a missing
grid must not spam per-station HDF5 stacks.

Regression tests for a run where the config's ``local_vdatum`` still
held the example placeholder path: parameter validation reported the
datum available (it probes the *bucket* vdatum file, but SECOFS
conversion reads the *local* one), the missing file surfaced ~28 hours
later once per station — 147 ERROR lines each preceded by a multi-frame
HDF5-DIAG stack — and the run completed with ``-9994`` offsets, i.e.
unusable MLLW water levels.

Covers:
- ``validate_secofs_local_vdatum``: aborts when neither the corrections
  file nor ``secofs_vdatums.nc`` exists at the configured location, and
  passes when either does;
- ``_open_secofs_vdatums``: returns ``None`` without touching the HDF5
  layer when the grid is missing, reports the path once at ERROR and
  repeats at DEBUG, and still opens a real grid.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.get_datum_offset import (
    _MISSING_VDATUM_REPORTED,
    _open_secofs_vdatums,
    validate_secofs_local_vdatum,
)

LOGGER = logging.getLogger('secofs_vdatum_failfast_test')


@pytest.fixture(name='conf_with_local_vdatum')
def fixture_conf_with_local_vdatum(tmp_path):
    """Write a minimal conf whose local_vdatum points into tmp_path."""
    def _write(vdatum_dir_name='vdatum'):
        vdatum_dir = tmp_path / vdatum_dir_name
        vdatum_dir.mkdir(exist_ok=True)
        conf = tmp_path / 'ofs_dps.test.conf'
        corrections = vdatum_dir / 'secofs_mllw_corrections.txt'
        conf.write_text(
            '[directories]\n'
            'home = ./\n'
            f'local_vdatum = {corrections}\n',
            encoding='utf-8',
        )
        return conf, corrections
    return _write


def _prop(conf):
    return SimpleNamespace(config_file=str(conf))


# ---------------------------------------------------------------------------
# validate_secofs_local_vdatum
# ---------------------------------------------------------------------------


def test_validation_aborts_when_nothing_exists(conf_with_local_vdatum):
    """Neither corrections nor grid on disk must abort the run."""
    conf, _ = conf_with_local_vdatum()
    with pytest.raises(SystemExit):
        validate_secofs_local_vdatum(_prop(conf), LOGGER)


def test_validation_passes_with_corrections_file(conf_with_local_vdatum):
    """The corrections TSV alone satisfies validation."""
    conf, corrections = conf_with_local_vdatum()
    corrections.write_text('ID\tCorrection1\tCorrection2\n', encoding='utf-8')
    validate_secofs_local_vdatum(_prop(conf), LOGGER)


def test_validation_passes_with_vdatum_grid_only(conf_with_local_vdatum):
    """The fallback grid alone satisfies validation."""
    conf, corrections = conf_with_local_vdatum()
    (corrections.parent / 'secofs_vdatums.nc').write_bytes(b'\x89HDF\n')
    validate_secofs_local_vdatum(_prop(conf), LOGGER)


def test_validation_aborts_without_local_vdatum_setting(tmp_path):
    """A conf without local_vdatum must abort with a clear error."""
    conf = tmp_path / 'ofs_dps.test.conf'
    conf.write_text('[directories]\nhome = ./\n', encoding='utf-8')
    with pytest.raises(SystemExit):
        validate_secofs_local_vdatum(_prop(conf), LOGGER)


# ---------------------------------------------------------------------------
# _open_secofs_vdatums
# ---------------------------------------------------------------------------


def test_open_missing_grid_returns_none_and_logs_once(tmp_path, caplog):
    """Missing grid: None result, one ERROR, repeats at DEBUG."""
    _MISSING_VDATUM_REPORTED.clear()
    corrections = tmp_path / 'secofs_mllw_corrections.txt'

    with caplog.at_level(logging.DEBUG,
                         logger='secofs_vdatum_failfast_test'):
        assert _open_secofs_vdatums(str(corrections), LOGGER) is None
        assert _open_secofs_vdatums(str(corrections), LOGGER) is None

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(errors) == 1
    assert 'secofs_vdatums.nc' in errors[0].getMessage()
    assert len(debugs) == 1


def test_open_real_grid_succeeds(tmp_path):
    """A real netCDF grid still opens through the guarded path."""
    _MISSING_VDATUM_REPORTED.clear()
    grid = tmp_path / 'secofs_vdatums.nc'
    xr.Dataset(
        {'mllwtomsl': (('node',), np.array([0.5, 0.7]))}
    ).to_netcdf(grid)
    corrections = tmp_path / 'secofs_mllw_corrections.txt'

    opened = _open_secofs_vdatums(str(corrections), LOGGER)
    assert opened is not None
    assert 'mllwtomsl' in opened
    opened.close()
