"""Regression tests for 2D JSON output honoring the configured data_dir
(issue #226).

The 2D pipeline previously hardcoded the literal ``data`` directory when
deriving the model / observation JSON output directories in
``processing_2d.param_val``. A user who set ``data_dir=%(home)s/work`` in the
config still had 2D JSON files written to ``<home>/data`` while downstream
stats / static-map steps looked under ``<home>/work`` (the resolved
``data_model_2d_json_path``), producing empty ``work/`` trees and a
``No files found in directory .../work/observations/2d`` error.

These tests exercise ``param_val`` directly (no model data required) and assert
that the output directories it creates match the paths resolved from a custom
``data_dir`` when the caller supplies them on ``prop1``.
"""

import logging
import logging.config
import os
from types import SimpleNamespace

import pytest

from ofs_skill.visualization import processing_2d

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _no_global_logging_reconfig(monkeypatch):
    """Stop ``param_val`` from reconfiguring the root logging system.

    ``param_val`` calls ``utils.init_root_logger``, which runs
    ``logging.config.fileConfig``. ``fileConfig`` mutates the *global*
    logging configuration (and by default disables existing loggers), which
    silently breaks ``caplog``-based assertions in unrelated tests that
    happen to run later in the same process. Patching the shared
    ``logging.config`` module keeps these unit tests hermetic and prevents
    cross-test pollution. The directory-derivation logic under test does not
    depend on the logging handlers themselves.
    """
    monkeypatch.setattr(logging.config, 'fileConfig', lambda *a, **k: None)


def _make_prop(base, data_dir_name):
    """Build a prop-like object mirroring what create_2dplot resolves.

    ``data_model_2d_json_path`` / ``data_observations_2d_json_path`` are the
    paths create_2dplot builds from ``dir_params['data_dir']`` and sets on the
    ModelProperties object before calling parse_leaflet_json/param_val.
    """
    model_2d = os.path.join(base, data_dir_name, 'model', '2d')
    obs_2d = os.path.join(base, data_dir_name, 'observations', '2d')
    return SimpleNamespace(
        path=base,
        config_file=None,
        data_model_2d_json_path=model_2d,
        data_observations_2d_json_path=obs_2d,
    )


def test_param_val_honors_custom_data_dir(tmp_path):
    """With a custom data_dir (e.g. 'work'), param_val writes 2D JSON dirs
    under work/ - NOT the hardcoded data/."""
    base = str(tmp_path)
    prop1 = _make_prop(base, 'work')

    _logger, outdir = processing_2d.param_val(None, prop1)

    expected_model = os.path.join(base, 'work', 'model', '2d')
    expected_obs = os.path.join(base, 'work', 'observations', '2d')

    assert outdir[0] == expected_model
    assert outdir[1] == expected_obs
    assert os.path.isdir(expected_model)
    assert os.path.isdir(expected_obs)

    # The buggy hardcoded 'data' location must NOT be created.
    assert not os.path.exists(os.path.join(base, 'data', 'model', '2d'))
    assert not os.path.exists(os.path.join(base, 'data', 'observations', '2d'))


def test_param_val_absolute_data_dir(tmp_path):
    """An absolute data_dir on another disk is used verbatim."""
    ext = tmp_path / 'bigdisk' / 'ofs_data'
    prop1 = SimpleNamespace(
        path=str(tmp_path / 'install'),
        config_file=None,
        data_model_2d_json_path=str(ext / 'model' / '2d'),
        data_observations_2d_json_path=str(ext / 'observations' / '2d'),
    )

    _logger, outdir = processing_2d.param_val(None, prop1)

    assert outdir[0] == str(ext / 'model' / '2d')
    assert outdir[1] == str(ext / 'observations' / '2d')
    assert os.path.isdir(ext / 'model' / '2d')
    assert os.path.isdir(ext / 'observations' / '2d')


def test_param_val_fallback_default_data_dir(tmp_path):
    """When the resolved 2D json paths are absent, param_val falls back to
    the legacy prop1.path/data/... layout (default data_dir=data)."""
    base = str(tmp_path)
    prop1 = SimpleNamespace(path=base, config_file=None)

    _logger, outdir = processing_2d.param_val(None, prop1)

    assert outdir[0] == os.path.join(base, 'data', 'model', '2d')
    assert outdir[1] == os.path.join(base, 'data', 'observations', '2d')
