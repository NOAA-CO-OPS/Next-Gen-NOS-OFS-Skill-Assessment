"""Tests for writing output outside the installation directory (issue #215).

The working directory (``-p`` / ``home=``) and the ``data_dir`` /
``control_files_dir`` settings may point anywhere, including an external
disk. Input assets that ship with the installation (``ofs_extents/``,
``conf/logging.conf``, ``conf/error_ranges.csv``, ``src/wcofs_msl.nc``)
must resolve from the installation root when they are not present under
the working directory, so users never have to copy or symlink them.

Covers:

- ``resolve_asset_path``: working-directory override wins when present;
  otherwise the installation-root copy is returned.
- ``read_config_section('directories')``: values are ``~``-expanded so
  home-relative external paths work.
- ``get_s3_cache_dir``: honors ``s3_cache_dir`` from the config and
  falls back to ``~/.ofs_cache/s3`` when unset.
"""

import logging
import os
from pathlib import Path

import pytest

from ofs_skill.obs_retrieval.utils import (
    Utils,
    get_project_root,
    get_s3_cache_dir,
    resolve_asset_path,
)

logger = logging.getLogger(__name__)


def test_project_root_contains_assets():
    root = get_project_root()
    assert (root / 'ofs_extents').is_dir()
    assert (root / 'conf' / 'logging.conf').is_file()
    assert (root / 'conf' / 'error_ranges.csv').is_file()


def test_resolve_asset_falls_back_to_installation_root(tmp_path):
    """An empty external working dir resolves assets from the install."""
    resolved = resolve_asset_path(tmp_path, 'ofs_extents')
    assert Path(resolved) == get_project_root() / 'ofs_extents'
    assert os.path.isdir(resolved)

    resolved = resolve_asset_path(tmp_path, 'conf', 'logging.conf')
    assert Path(resolved) == get_project_root() / 'conf' / 'logging.conf'
    assert os.path.isfile(resolved)


def test_resolve_asset_prefers_working_directory_copy(tmp_path):
    """A copy under the working directory overrides the installed asset."""
    local = tmp_path / 'conf'
    local.mkdir()
    (local / 'error_ranges.csv').write_text('name_var,X1,X2\nwl,0.2,0.5\n')

    resolved = resolve_asset_path(tmp_path, 'conf', 'error_ranges.csv')
    assert Path(resolved) == local / 'error_ranges.csv'


def test_resolve_asset_none_base_uses_installation_root():
    resolved = resolve_asset_path(None, 'conf', 'logging.conf')
    assert Path(resolved) == get_project_root() / 'conf' / 'logging.conf'


def test_resolve_asset_missing_everywhere_returns_install_candidate(tmp_path):
    """Nonexistent assets resolve to the install path so error messages
    point at the canonical location."""
    resolved = resolve_asset_path(tmp_path, 'ofs_extents', 'nofs.shp')
    assert Path(resolved) == get_project_root() / 'ofs_extents' / 'nofs.shp'
    assert not os.path.exists(resolved)


@pytest.fixture
def conf_with_dirs(tmp_path):
    def _write(extra_lines=''):
        conf = tmp_path / 'ofs_dps.test.conf'
        conf.write_text(
            '[directories]\n'
            'home = ./\n'
            'data_dir = ~/external_data\n'
            'control_files_dir = /mnt/big/control_files\n'
            f'{extra_lines}'
        )
        return conf
    return _write


def test_directories_section_expands_tilde(conf_with_dirs):
    conf = conf_with_dirs()
    params = Utils(conf).read_config_section('directories', logger)
    assert params['data_dir'] == os.path.expanduser('~/external_data')
    assert params['control_files_dir'] == '/mnt/big/control_files'


def test_absolute_dir_settings_pass_through_os_path_join(conf_with_dirs):
    """os.path.join must use an absolute setting as-is (drop the prefix)."""
    conf = conf_with_dirs()
    params = Utils(conf).read_config_section('directories', logger)
    joined = os.path.join('/install/dir', params['control_files_dir'])
    assert joined == '/mnt/big/control_files'


def test_s3_cache_dir_default_when_unset(conf_with_dirs):
    conf = conf_with_dirs()
    cache = get_s3_cache_dir(conf, logger)
    assert cache == os.path.join(os.path.expanduser('~'), '.ofs_cache', 's3')


def test_s3_cache_dir_from_config(conf_with_dirs, tmp_path):
    conf = conf_with_dirs(f's3_cache_dir = {tmp_path / "model_cache"}\n')
    cache = get_s3_cache_dir(conf, logger)
    assert cache == str(tmp_path / 'model_cache')


def test_s3_cache_dir_blank_falls_back_to_default(conf_with_dirs):
    conf = conf_with_dirs('s3_cache_dir =\n')
    cache = get_s3_cache_dir(conf, logger)
    assert cache == os.path.join(os.path.expanduser('~'), '.ofs_cache', 's3')
