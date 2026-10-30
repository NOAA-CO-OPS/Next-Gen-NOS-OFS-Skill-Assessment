"""Every CLI entry point bootstraps its logger from the working directory.

Issue #225: PR #218 taught the 1D plotting pipeline to resolve the shipped
input assets (``conf/logging.conf`` among them) from the working directory
first and the installation root second, so ``-p`` / ``home=`` can point at an
external disk. The other entry points were left behind — each hand-rolled its
own ``conf/logging.conf`` lookup, most of them installation-relative, most
exiting with no diagnostic when the file was not where they expected.

``src/ofs_skill/model_processing/get_node_ofs.py`` was worse than "missing an
enhancement": it derived the path with one parent too few, landing on
``<install>/src/conf/logging.conf`` — a directory that does not exist — so the
``get-node-ofs`` console script exited non-zero with zero output on *every*
invocation, external directory or not.

These tests assert the wiring that fixes both: each entry point passes its own
working directory into ``utils.init_root_logger`` before doing anything else.
"""

from __future__ import annotations

import importlib.util
import logging
import logging.config
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.utils import get_project_root

_BIN = Path(__file__).resolve().parent.parent / 'bin'


def _load_bin_module(name: str, relative: str):
    """Import a script under bin/ (they are not on the package path)."""
    spec = importlib.util.spec_from_file_location(name, _BIN / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _no_global_logging_reconfig(monkeypatch):
    """Stop the real ``logging.config.fileConfig`` from running.

    ``init_root_logger`` configures logging from conf/logging.conf, which
    mutates the *global* logging configuration and by default disables
    existing loggers — that silently breaks ``caplog`` assertions in
    unrelated tests later in the same process. Neutralizing it keeps these
    tests hermetic; what they assert (which path each entry point resolves,
    and how far execution gets) does not depend on the handlers.
    """
    monkeypatch.setattr(logging.config, 'fileConfig', lambda *a, **k: None)


class _BootstrapReached(Exception):
    """Raised by the recorder so the entry point stops right after setup."""


@pytest.fixture
def record_bootstrap(monkeypatch):
    """Replace ``init_root_logger`` with a recorder that halts the caller.

    Recording the ``base_path`` argument is the whole point: an entry point
    that ignores its working directory cannot support an external ``-p``.
    Raising afterwards keeps the test off the network and off disk.
    """
    calls: list[tuple] = []

    def _recorder(base_path=None, config_file=None):
        calls.append((base_path, config_file))
        raise _BootstrapReached

    monkeypatch.setattr(utils, 'init_root_logger', _recorder)
    return calls


def _assert_base_path(calls, expected):
    assert len(calls) == 1, 'entry point did not bootstrap its logger'
    base_path, _config_file = calls[0]
    assert base_path is not None, 'working directory was not passed through'
    assert Path(str(base_path)) == Path(str(expected))


# ---------------------------------------------------------------------------
# get_node_ofs — the entry point that was hard-broken
# ---------------------------------------------------------------------------

def test_get_node_ofs_bootstraps_from_working_directory(tmp_path, record_bootstrap):
    from ofs_skill.model_processing.get_node_ofs import get_node_ofs

    prop = SimpleNamespace(
        ofs='cbofs',
        path=str(tmp_path),
        config_file=None,
        start_date_full='2024-01-01T00:00:00Z',
        end_date_full='2024-01-02T00:00:00Z',
    )

    with pytest.raises(_BootstrapReached):
        get_node_ofs(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_get_node_ofs_gets_past_logger_setup(tmp_path, capsys):
    """Regression for the silent exit-255.

    The old code resolved ``<install>/src/conf/logging.conf``, so the guard
    always fired and ``sys.exit(-1)`` ran before a single line was emitted.
    With a real (installed) logging.conf the run must now reach parameter
    validation, which is where a bad date range is caught and *reported*.
    """
    from ofs_skill.model_processing.get_node_ofs import get_node_ofs

    prop = SimpleNamespace(
        ofs='cbofs',
        path=str(tmp_path),
        config_file=None,
        start_date_full='not-a-date',
        end_date_full='also-not-a-date',
        var_list='water_level',
    )

    with pytest.raises(SystemExit) as excinfo:
        get_node_ofs(prop, None)

    assert excinfo.value.code != 0
    assert 'Please check Start Date' in capsys.readouterr().out


# ---------------------------------------------------------------------------
# The entry points named in the issue, and their siblings
# ---------------------------------------------------------------------------

def test_get_satellite_bootstraps_from_working_directory(tmp_path, record_bootstrap):
    sat = _load_bin_module(
        'get_satellite_observations_entrypoint',
        'obs_retrieval/get_satellite_observations.py')

    prop = SimpleNamespace(ofs='cbofs', path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        sat.get_satellite(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_get_station_observations_bootstraps_from_working_directory(
        tmp_path, record_bootstrap):
    from ofs_skill.obs_retrieval.get_station_observations import (
        get_station_observations,
    )

    prop = SimpleNamespace(
        ofs='cbofs',
        path=str(tmp_path),
        config_file=None,
        start_date_full='2024-01-01T00:00:00Z',
        end_date_full='2024-01-02T00:00:00Z',
        datum='MLLW',
        stationowner='co-ops',
        var_list='water_level',
    )

    with pytest.raises(_BootstrapReached):
        get_station_observations(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_ofs_inventory_stations_bootstraps_from_working_directory(
        tmp_path, record_bootstrap):
    from ofs_skill.obs_retrieval.ofs_inventory_stations import ofs_inventory_stations

    with pytest.raises(_BootstrapReached):
        ofs_inventory_stations(
            'cbofs', '2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z',
            str(tmp_path), 'co-ops', None,
        )

    _assert_base_path(record_bootstrap, tmp_path)


def test_param_val_bootstraps_from_working_directory(tmp_path, record_bootstrap):
    from ofs_skill.visualization import processing_2d

    prop1 = SimpleNamespace(path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        processing_2d.param_val(None, prop1)

    _assert_base_path(record_bootstrap, tmp_path)


def test_get_model_data_bootstraps_from_working_directory(tmp_path, record_bootstrap):
    gmd = _load_bin_module('get_model_data_entrypoint', 'utils/get_model_data.py')

    prop = SimpleNamespace(ofs='cbofs', path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        gmd.get_model_data(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_summary_barplots_bootstraps_from_working_directory(
        tmp_path, record_bootstrap):
    barplots = _load_bin_module(
        'create_summary_barplots_entrypoint',
        'visualization/create_summary_barplots.py')

    prop = SimpleNamespace(
        ofs='cbofs', path=str(tmp_path), config_file=None,
        whichcasts=['nowcast'], var_list=['water_level'],
        ofsfiletype='stations',
    )

    with pytest.raises(_BootstrapReached):
        barplots._bootstrap(prop)

    _assert_base_path(record_bootstrap, tmp_path)


def test_make_om_view_bootstraps_from_working_directory(tmp_path, record_bootstrap):
    om = _load_bin_module('make_OM_view_entrypoint', 'skill_assessment/make_OM_view.py')

    prop = SimpleNamespace(ofs='cbofs', path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        om.make_OM_view(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_open_boundary_transect_bootstraps_from_working_directory(
        tmp_path, record_bootstrap):
    """This one used to resolve logging.conf from the *current* directory,
    so the console script only worked when launched from the install root."""
    obt = _load_bin_module(
        'make_open_boundary_transect_entrypoint',
        'open_boundary/make_open_boundary_transect.py')

    prop = SimpleNamespace(ofs='cbofs', path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        obt.make_open_boundary_transects(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


# ---------------------------------------------------------------------------
# The loud failure actually reaches the entry points
# ---------------------------------------------------------------------------

def test_entry_point_names_missing_log_config_on_stderr(
        tmp_path, monkeypatch, capsys):
    """A missing logging.conf must abort *with a diagnostic*, everywhere.

    This is the behavior PR #218 gave the 1D pipeline. Before this change
    ``make_OM_view`` (like most entry points) called a bare ``sys.exit(-1)``
    here, so an operator saw an exit code and nothing else. Asserted through
    a real entry point rather than the helper, because the point of the issue
    is that the entry points inherit it.
    """
    empty_root = tmp_path / 'not_an_installation'
    empty_root.mkdir()
    monkeypatch.setattr(utils, 'get_project_root', lambda: empty_root)

    om = _load_bin_module(
        'make_OM_view_stderr_entrypoint', 'skill_assessment/make_OM_view.py')
    prop = SimpleNamespace(ofs='cbofs', path=str(tmp_path), config_file=None)

    with pytest.raises(SystemExit) as excinfo:
        om.make_OM_view(prop, None)

    assert excinfo.value.code != 0
    assert str(empty_root / 'conf' / 'logging.conf') in capsys.readouterr().err


def test_entry_point_exit_code_is_nonzero_not_success(
        tmp_path, monkeypatch, capsys):
    """A bare ``sys.exit()`` — status 0 — used to hide this failure.

    ``process_schism_stations`` printed 'No log file! Cannot continue.' and
    then exited *successfully*, so a wrapper script checking ``$?`` saw the
    run as fine. It also never consulted the installation root, so pointing
    the monkeypatched root at an empty directory has no effect on the old
    code — this test fails there both on the exit code and on the message.
    """
    empty_root = tmp_path / 'not_an_installation'
    empty_root.mkdir()
    monkeypatch.setattr(utils, 'get_project_root', lambda: empty_root)

    schism = _load_bin_module(
        'process_schism_stations_stderr_entrypoint',
        'utils/process_schism_stations_cli.py')
    prop = SimpleNamespace(ofs='secofs', path=str(tmp_path), config_file=None)

    with pytest.raises(SystemExit) as excinfo:
        schism.process_schism_stations(prop, None)

    assert excinfo.value.code not in (0, None)
    assert str(empty_root / 'conf' / 'logging.conf') in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Shipped assets other than logging.conf
# ---------------------------------------------------------------------------

def test_ice_climatology_reads_installed_assets(tmp_path):
    """Great Lakes ice climatology ships in the installation's conf/.

    ``ice_climatology`` joined them onto ``prop.path``, so ice skill
    assessment failed outright for an external working directory.
    """
    import numpy as np

    do_iceskill = _load_bin_module(
        'do_iceskill_entrypoint', 'skill_assessment/do_iceskill.py')

    uniq_count = sum(
        1 for _ in (get_project_root() / 'conf' / 'unique_dates.csv')
        .read_text().splitlines()[1:]
    )
    ice_clim = np.zeros((uniq_count, 2, 2))
    prop = SimpleNamespace(ofs='leofs', path=str(tmp_path))

    icecover_hist, _icecover_hist_2d = do_iceskill.ice_climatology(
        prop, [datetime(2024, 1, 15)], ice_clim)

    assert len(icecover_hist) == 1


def test_icecover_model_setup_logger_passes_working_directory(
        tmp_path, record_bootstrap):
    """``_setup_logger`` had no base-path parameter at all, so the ice cover
    model CLI could only ever find the installation's logging.conf."""
    icm = _load_bin_module(
        'get_icecover_model_entrypoint', 'model_processing/get_icecover_model.py')

    prop = SimpleNamespace(ofs='leofs', path=str(tmp_path), config_file=None)

    with pytest.raises(_BootstrapReached):
        icm.get_icecover_model(prop, None)

    _assert_base_path(record_bootstrap, tmp_path)


def test_icecover_model_setup_logger_returns_supplied_logger(tmp_path):
    """A caller-supplied logger short-circuits the bootstrap entirely."""
    icm = _load_bin_module(
        'get_icecover_model_entrypoint2', 'model_processing/get_icecover_model.py')
    supplied = logging.getLogger('supplied-for-icecover-model')

    assert icm._setup_logger(supplied, None, str(tmp_path)) is supplied


def _icecover_model_args(path):
    return SimpleNamespace(
        OFS='leofs', Path=path, config=None,
        StartDate='2024-02-01T00:00:00Z', EndDate='2024-02-02T00:00:00Z',
        Whichcasts='nowcast', Forecast_Hr=None,
    )


def test_icecover_model_builds_extents_path_without_trailing_slash(tmp_path):
    """``-p`` without a trailing slash used to produce a bogus path.

    The CLI built this by string concatenation (``args.Path +
    'ofs_extents/'``), so ``-p /work`` yielded ``/workofs_extents/``. It now
    goes through ``resolve_asset_path``, which joins properly and falls back
    to the installation copy of the shapefiles.
    """
    icm = _load_bin_module(
        'get_icecover_model_args_entrypoint',
        'model_processing/get_icecover_model.py')

    prop1 = icm._build_properties(_icecover_model_args(str(tmp_path)))

    assert Path(prop1.ofs_extents_path) == get_project_root() / 'ofs_extents'
    assert prop1.path == str(tmp_path)


def test_icecover_model_builds_extents_path_when_p_omitted(tmp_path):
    """``-p`` is optional on this CLI, so ``args.Path`` is None.

    Concatenating None raised TypeError before argument assembly finished,
    which made the console script unusable without ``-p``.
    """
    icm = _load_bin_module(
        'get_icecover_model_args_entrypoint2',
        'model_processing/get_icecover_model.py')

    prop1 = icm._build_properties(_icecover_model_args(None))

    assert Path(prop1.ofs_extents_path) == get_project_root() / 'ofs_extents'


def test_icecover_model_prefers_working_directory_extents(tmp_path):
    """A working-directory copy of ofs_extents/ wins, which is what lets a
    user override the shipped extents without editing the installation."""
    icm = _load_bin_module(
        'get_icecover_model_args_entrypoint3',
        'model_processing/get_icecover_model.py')
    local_extents = tmp_path / 'ofs_extents'
    local_extents.mkdir()

    prop1 = icm._build_properties(_icecover_model_args(str(tmp_path)))

    assert Path(prop1.ofs_extents_path) == local_extents


# ---------------------------------------------------------------------------
# check_pipeline: -c was joined onto -p, so an external -p could never
# locate the config
# ---------------------------------------------------------------------------

class _RecordingUtils:
    """Stand-in for utils.Utils that records the config path it was given."""

    seen: list[str] = []

    def __init__(self, config_file=None):
        _RecordingUtils.seen.append(str(config_file))

    def read_config_section(self, _section, _logger):
        return {
            'home': './', 'data_dir': 'data', 'control_files_dir': 'control_files',
            'observations_dir': 'observations', '1d_station_dir': '1d_station',
            'model_dir': 'model', '1d_node_dir': '1d_node',
            'skill_dir': 'skill', '1d_pair_dir': '1d_pair', 'visual_dir': 'visual',
        }


@pytest.fixture
def check_pipeline_module():
    return _load_bin_module('check_pipeline_entrypoint', 'utils/check_pipeline.py')


def _check_pipeline_args(home):
    return SimpleNamespace(
        OFS='cbofs', Var_Selection='wl', Whichcasts=['nowcast'],
        Path=str(home), config='conf/ofs_dps.conf',
    )


def test_check_pipeline_finds_installed_config_from_external_path(
        tmp_path, monkeypatch, check_pipeline_module, capsys):
    _RecordingUtils.seen = []
    monkeypatch.setattr(check_pipeline_module.utils, 'Utils', _RecordingUtils)

    check_pipeline_module.main(_check_pipeline_args(tmp_path))

    assert _RecordingUtils.seen == [
        str(get_project_root() / 'conf' / 'ofs_dps.conf')
    ]
    assert 'No configuration file found!' not in capsys.readouterr().out


def test_check_pipeline_prefers_working_directory_config(
        tmp_path, monkeypatch, check_pipeline_module):
    local_conf = tmp_path / 'conf'
    local_conf.mkdir()
    (local_conf / 'ofs_dps.conf').write_text('[directories]\nhome = ./\n')

    _RecordingUtils.seen = []
    monkeypatch.setattr(check_pipeline_module.utils, 'Utils', _RecordingUtils)

    check_pipeline_module.main(_check_pipeline_args(tmp_path))

    assert _RecordingUtils.seen == [str(local_conf / 'ofs_dps.conf')]
