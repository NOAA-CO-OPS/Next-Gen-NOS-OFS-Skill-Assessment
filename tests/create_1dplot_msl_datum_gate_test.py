"""
Tests for the MSL datum gate in ``create_1dplot``.

MSL is only meaningful for the STOFS components (they run natively
against (L)MSL and convert through the VDatum API). The ROMS/FVCOM and
non-STOFS SCHISM systems convert through per-grid vdatum netCDF files,
which have no MSL conversion field — a request for MSL on those systems
must abort with a clear message rather than silently switching datums.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_1DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_1dplot.py'


@pytest.fixture(scope='module')
def create_1dplot_mod():
    """Import the create_1dplot script as a module."""
    spec = importlib.util.spec_from_file_location(
        'create_1dplot_msl_gate_under_test', CREATE_1DPLOT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['create_1dplot_msl_gate_under_test'] = mod
    spec.loader.exec_module(mod)
    return mod


class _MockLogger:
    """Collects error messages so tests can assert on them."""

    def __init__(self):
        self.errors = []

    def error(self, msg, *args):
        """Record the formatted error message."""
        self.errors.append(msg % args if args else msg)

    def info(self, *args, **kwargs):
        """Ignore info messages."""

    def warning(self, *args, **kwargs):
        """Ignore warnings."""


@pytest.mark.parametrize('ofs', [
    'cbofs',    # ROMS
    'tbofs',    # ROMS
    'necofs',   # FVCOM
    'sfbofs',   # FVCOM
    'secofs',   # SCHISM, non-STOFS (vdatum grid file, no MSL field)
    'loofs2',   # SCHISM, Great Lakes
    'leofs',    # FVCOM, Great Lakes
])
def test_msl_rejected_for_non_stofs(create_1dplot_mod, ofs):
    """MSL on any non-STOFS OFS must abort with a clear error."""
    logger = _MockLogger()
    with pytest.raises(SystemExit):
        create_1dplot_mod.validate_msl_datum(ofs, 'MSL', logger)
    assert logger.errors
    assert 'MSL' in logger.errors[0]
    assert ofs in logger.errors[0]


@pytest.mark.parametrize('ofs', [
    'stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac',
])
def test_msl_allowed_for_stofs(create_1dplot_mod, ofs):
    """The STOFS components accept MSL without aborting."""
    logger = _MockLogger()
    create_1dplot_mod.validate_msl_datum(ofs, 'MSL', logger)
    assert not logger.errors


@pytest.mark.parametrize('datum', [
    'MLLW', 'MHHW', 'MHW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B',
])
def test_non_msl_datums_pass_through(create_1dplot_mod, datum):
    """The gate only concerns MSL; every other datum passes untouched."""
    logger = _MockLogger()
    create_1dplot_mod.validate_msl_datum('cbofs', datum, logger)
    assert not logger.errors


def test_gate_is_case_insensitive(create_1dplot_mod):
    """The gate holds regardless of the case of the inputs."""
    logger = _MockLogger()
    with pytest.raises(SystemExit):
        create_1dplot_mod.validate_msl_datum('CBOFS', 'msl', logger)
