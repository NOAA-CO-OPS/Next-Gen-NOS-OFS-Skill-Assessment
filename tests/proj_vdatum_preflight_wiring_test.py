"""The PROJ preflight has to be wired into the entry points, not just exist.

A gate nothing calls is worth nothing. Deleting the single call in
``create_1dplot`` -- the whole user-visible half of the fix for issues
#127/#216/#295 -- left the rest of the suite green, so these tests pin
the call sites down.

They read the entry points' own source rather than driving them, because
reaching the gate for real needs a populated conf file, an ofs_extents
shapefile and a live NODD bucket. What is asserted is exactly what the
mutation broke: the call exists in the right function, with the run's
properties and logger passed to it.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_1DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_1dplot.py'

GATE = 'validate_proj_vdatum_grids'


def _called_names(func):
    """Every plain function name called in ``func``'s body."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def _gate_call(func):
    """The single ``validate_proj_vdatum_grids`` call node in ``func``."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == GATE
    ]
    assert len(calls) == 1, f'expected exactly one {GATE} call, got {calls}'
    return calls[0]


@pytest.fixture(scope='module')
def create_1dplot_mod():
    """Import the create_1dplot script as a module."""
    spec = importlib.util.spec_from_file_location(
        'create_1dplot_proj_gate_under_test', CREATE_1DPLOT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['create_1dplot_proj_gate_under_test'] = mod
    spec.loader.exec_module(mod)
    return mod


def test_create_1dplot_runs_the_preflight(create_1dplot_mod):
    """The plotting entry point must gate on PROJ before it does any work."""
    assert GATE in _called_names(create_1dplot_mod.create_1dplot)

    call = _gate_call(create_1dplot_mod.create_1dplot)
    assert [a.id for a in call.args] == ['prop', 'logger'], ast.dump(call)


def test_create_1dplot_imports_the_real_gate(create_1dplot_mod):
    """The name it calls has to be the function under test, not a stub."""
    from ofs_skill.obs_retrieval import vdatum_resilient

    assert getattr(create_1dplot_mod, GATE) is (
        vdatum_resilient.validate_proj_vdatum_grids)


def test_get_station_observations_runs_the_preflight():
    """The observation entry point is where the 192 stations were dropped.

    ``bin/obs_retrieval/get_station_observations_cli.py`` and the obs GUI
    both funnel through this function and never touch ``create_1dplot``,
    so gating only the plotting script would leave them unprotected.
    """
    module = importlib.import_module(
        'ofs_skill.obs_retrieval.get_station_observations')

    assert GATE in _called_names(module.get_station_observations)

    call = _gate_call(module.get_station_observations)
    assert [a.id for a in call.args] == ['prop', 'logger'], ast.dump(call)

    from ofs_skill.obs_retrieval import vdatum_resilient
    assert getattr(module, GATE) is (
        vdatum_resilient.validate_proj_vdatum_grids)
