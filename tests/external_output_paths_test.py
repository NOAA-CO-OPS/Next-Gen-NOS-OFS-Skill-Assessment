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
- ``init_root_logger``: the shared CLI logger bootstrap added for issue
  #225, which extends the same resolution (and a message naming the
  missing file) to every entry point outside the 1D plotting pipeline.
- ``Utils(config_file)``: a relative ``-c`` falls back to the
  installation root when it is not under the working directory.
"""

import logging
import logging.config
import os
from pathlib import Path

import pytest

from ofs_skill.obs_retrieval import utils
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


# ---------------------------------------------------------------------------
# init_root_logger: the shared CLI logger bootstrap (issue #225)
#
# Every CLI entry point used to hand-roll the conf/logging.conf lookup. Most
# resolved it relative to the installation only, and most exited with no
# diagnostic at all when it was missing. init_root_logger centralizes that on
# resolve_asset_path so an external working directory works everywhere the 1D
# pipeline already worked, and names the missing file on stderr.
# ---------------------------------------------------------------------------

@pytest.fixture
def record_file_config(monkeypatch):
    """Capture the path handed to ``logging.config.fileConfig``.

    ``fileConfig`` mutates the *global* logging configuration and by default
    disables existing loggers, which breaks ``caplog`` assertions in unrelated
    tests later in the same process. Patching the shared ``logging.config``
    module keeps these tests hermetic while still recording what would have
    been applied.
    """
    applied: list[str] = []
    monkeypatch.setattr(
        logging.config, 'fileConfig', lambda path, *a, **k: applied.append(str(path)),
    )
    return applied


def test_init_root_logger_uses_installed_log_config(tmp_path, record_file_config):
    """An external working directory with no conf/ still gets a logger."""
    result = utils.init_root_logger(tmp_path)

    assert record_file_config == [str(get_project_root() / 'conf' / 'logging.conf')]
    assert result.name == 'root'


def test_init_root_logger_prefers_working_directory_log_config(
        tmp_path, record_file_config):
    """A conf/logging.conf under the working directory overrides the install."""
    local_conf = tmp_path / 'conf'
    local_conf.mkdir()
    local_log_config = local_conf / 'logging.conf'
    local_log_config.write_text(
        '[loggers]\nkeys=root\n'
        '[handlers]\nkeys=screen\n'
        '[formatters]\nkeys=simple\n'
        '[logger_root]\nlevel=INFO\nhandlers=screen\n'
        '[handler_screen]\nclass=StreamHandler\nformatter=simple\nargs=()\n'
        '[formatter_simple]\nformat=%(message)s\n'
    )

    utils.init_root_logger(tmp_path)

    assert record_file_config == [str(local_log_config)]


def test_init_root_logger_names_missing_log_config(
        tmp_path, monkeypatch, capsys, record_file_config):
    """Missing logging.conf exits non-zero AND names the file on stderr.

    Most entry points used to ``sys.exit(-1)`` here in total silence; two
    exited with status 0, which reads to a wrapper script as success.
    """
    empty_root = tmp_path / 'not_an_installation'
    empty_root.mkdir()
    monkeypatch.setattr(utils, 'get_project_root', lambda: empty_root)

    with pytest.raises(SystemExit) as excinfo:
        utils.init_root_logger(tmp_path)

    assert excinfo.value.code != 0
    stderr = capsys.readouterr().err
    assert str(empty_root / 'conf' / 'logging.conf') in stderr
    assert record_file_config == []


def test_init_root_logger_names_missing_main_config(
        tmp_path, capsys, record_file_config):
    """A missing main config is reported by name and stops the run."""
    missing = tmp_path / 'nowhere' / 'ofs_dps.conf'

    with pytest.raises(SystemExit) as excinfo:
        utils.init_root_logger(tmp_path, missing)

    assert excinfo.value.code != 0
    assert str(missing) in capsys.readouterr().err
    # Logging must not be reconfigured when the run is going to abort.
    assert record_file_config == []


def test_init_root_logger_logs_selected_config(tmp_path, record_file_config, caplog):
    conf = tmp_path / 'ofs_dps.test.conf'
    conf.write_text('[directories]\nhome = ./\n')

    with caplog.at_level(logging.INFO):
        utils.init_root_logger(tmp_path, conf)

    messages = ' '.join(record.getMessage() for record in caplog.records)
    assert str(conf) in messages
    assert str(get_project_root() / 'conf' / 'logging.conf') in messages


# ---------------------------------------------------------------------------
# Utils(-c) resolution: a relative config also falls back to the install
# ---------------------------------------------------------------------------

def test_relative_config_falls_back_to_installation_root(tmp_path, monkeypatch):
    """The GUI passes the literal 'conf/ofs_dps.conf'; an external cwd used
    to make that a hard FileNotFoundError."""
    installed = get_project_root() / 'conf' / 'ofs_dps.conf.example'
    monkeypatch.chdir(tmp_path)

    resolved = Utils('conf/ofs_dps.conf.example').get_config_file()

    assert resolved == installed.resolve()


def test_relative_config_prefers_working_directory(tmp_path, monkeypatch):
    local = tmp_path / 'conf'
    local.mkdir()
    local_conf = local / 'ofs_dps.conf.example'
    local_conf.write_text('[directories]\nhome = ./\n')
    monkeypatch.chdir(tmp_path)

    resolved = Utils('conf/ofs_dps.conf.example').get_config_file()

    assert resolved == local_conf.resolve()


def test_bogus_relative_config_still_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        Utils('conf/definitely_not_here.conf')
