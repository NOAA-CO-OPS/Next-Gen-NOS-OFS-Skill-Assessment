"""Tests for the ``[settings] make_plotly_2d_maps`` config toggle.

The interactive plotly express 2D maps used to be gated by a hardcoded
``make_plotly_maps = False`` literal inside ``plotting_2d.plot_2d``. This
value is now driven by the ``[settings] make_plotly_2d_maps`` config key,
resolved onto ``prop1`` by ``create_2dplot._run_pipeline`` before ``plot_2d``
is called.

These tests cover:

1. The string-to-bool coercion applied to the config value (matching the
   ``static_plots`` truthy handling: ``true``/``1``/``yes`` case-insensitive).
   This exercises the real ``create_2dplot._coerce_bool`` helper, not a copy.
2. That ``plot_2d`` honors ``prop1.make_plotly_2d_maps`` when logging the
   toggle state, and defaults to ``False`` when the attribute is absent so
   existing configs are unaffected.
"""

import importlib.util
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_2DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_2dplot.py'


@pytest.fixture(scope='module')
def create_2dplot_mod():
    """Load ``bin/visualization/create_2dplot.py`` as a module.

    ``bin/`` is not importable as a package, so we load it by path — the same
    pattern the other ``bin/`` regression tests use.
    """
    spec = importlib.util.spec_from_file_location(
        'create_2dplot_under_test', CREATE_2DPLOT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['create_2dplot_under_test'] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ('True', True),
        ('true', True),
        ('TRUE', True),
        ('1', True),
        ('yes', True),
        ('  Yes  ', True),
        ('False', False),
        ('false', False),
        ('0', False),
        ('no', False),
        ('', False),
        ('maybe', False),
        (None, False),
    ],
)
def test_make_plotly_2d_maps_coercion(create_2dplot_mod, raw, expected):
    """The shipped ``_coerce_bool`` must map config strings as documented."""
    assert create_2dplot_mod._coerce_bool(raw) is expected


def test_plot_2d_defaults_to_false_when_attr_absent(monkeypatch, caplog):
    """A prop1 without make_plotly_2d_maps must not enable plotly maps."""
    from ofs_skill.visualization import plotting_2d

    class _Stop(Exception):
        pass

    def _boom(*_args, **_kwargs):
        raise _Stop

    # Short-circuit right after the toggle is logged.
    monkeypatch.setattr(plotting_2d, 'list_of_json_files', _boom)

    prop1 = SimpleNamespace(
        data_observations_2d_json_path='/nonexistent',
        data_model_2d_json_path='/nonexistent',
    )
    with caplog.at_level(logging.INFO):
        with pytest.raises(_Stop):
            plotting_2d.plot_2d(prop1, logger)

    assert 'Make plotly express maps? False.' in caplog.text


def test_plot_2d_honors_true_flag(monkeypatch, caplog):
    """prop1.make_plotly_2d_maps=True must be reflected in the toggle log."""
    from ofs_skill.visualization import plotting_2d

    class _Stop(Exception):
        pass

    def _boom(*_args, **_kwargs):
        raise _Stop

    monkeypatch.setattr(plotting_2d, 'list_of_json_files', _boom)

    prop1 = SimpleNamespace(
        make_plotly_2d_maps=True,
        data_observations_2d_json_path='/nonexistent',
        data_model_2d_json_path='/nonexistent',
    )
    with caplog.at_level(logging.INFO):
        with pytest.raises(_Stop):
            plotting_2d.plot_2d(prop1, logger)

    assert 'Make plotly express maps? True.' in caplog.text
