"""Shipped inputs and generated outputs stay on the right side of the line.

Issue #225 asks for PR #218's external-working-directory support in the CLI
entry points outside the 1D pipeline. Logger bootstrap is covered in
``entrypoint_logger_bootstrap_test.py``; what is asserted here is the other
two halves of the same problem:

* An *input* asset that ships with the installation (``ofs_extents/*.shp``,
  ``conf/*``) must resolve through ``utils.resolve_asset_path`` — working
  directory first, installation second — so a run launched from an external
  directory finds it.
* An *output* must land under the working directory, never in the
  installation. A shared installation is typically read-only, and even when
  it is writable, one user's derived artifact must not become shared state.

Each test names the entry point it guards and the concrete failure it
reproduces.
"""

from __future__ import annotations

import importlib.util
import logging
import logging.config
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ofs_skill.obs_retrieval import utils
from ofs_skill.obs_retrieval.utils import get_project_root

_BIN = Path(__file__).resolve().parent.parent / 'bin'

logger = logging.getLogger(__name__)


def _load_bin_module(name: str, relative: str):
    """Import a script under bin/ (they are not on the package path)."""
    spec = importlib.util.spec_from_file_location(name, _BIN / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _no_global_logging_reconfig(monkeypatch):
    """Keep ``logging.config.fileConfig`` from mutating global logging state,
    which by default disables existing loggers and breaks ``caplog``
    assertions in unrelated tests later in the same process."""
    monkeypatch.setattr(logging.config, 'fileConfig', lambda *a, **k: None)


# ---------------------------------------------------------------------------
# get_hf_radar: the ofs_extents shapefile was cwd-relative
# ---------------------------------------------------------------------------

@pytest.fixture
def hf_radar_module():
    return _load_bin_module(
        'get_hf_radar_asset_entrypoint', 'obs_retrieval/get_hf_radar.py')


def test_hf_radar_finds_installed_shapefile_from_external_cwd(
        tmp_path, monkeypatch, hf_radar_module):
    """``-C`` makes this entry point external by design, yet ``-o`` resolved
    the shapefile as ``./ofs_extents/{ofs}.shp``, so anywhere but the
    installation root it aborted with 'Shapefile not found'."""
    monkeypatch.chdir(tmp_path)

    ofs, shapefile_path = hf_radar_module.resolve_bounds(
        None, 'cbofs', 'ofs_extents')

    assert ofs == 'cbofs'
    assert shapefile_path == get_project_root() / 'ofs_extents' / 'cbofs.shp'
    assert shapefile_path.exists()


def test_hf_radar_prefers_working_directory_shapefile(
        tmp_path, monkeypatch, hf_radar_module):
    """A local ofs_extents/ still overrides the installed shapefiles."""
    local_extents = tmp_path / 'ofs_extents'
    local_extents.mkdir()
    (local_extents / 'cbofs.shp').write_bytes(b'')
    monkeypatch.chdir(tmp_path)

    _ofs, shapefile_path = hf_radar_module.resolve_bounds(
        None, 'cbofs', 'ofs_extents')

    assert shapefile_path == local_extents / 'cbofs.shp'


def test_hf_radar_explicit_bounds_are_used_verbatim(tmp_path, hf_radar_module):
    """``-b`` names the user's own file, which may live anywhere."""
    bounds = tmp_path / 'somewhere' / 'my_area.shp'

    ofs, shapefile_path = hf_radar_module.resolve_bounds(
        str(bounds), None, 'ofs_extents')

    assert ofs == 'my_area'
    assert shapefile_path == bounds


def test_hf_radar_honors_configured_extents_dir(
        tmp_path, monkeypatch, hf_radar_module):
    """The directory name comes from ``ofs_extents_dir``, not a literal."""
    custom = tmp_path / 'my_extents'
    custom.mkdir()
    (custom / 'cbofs.shp').write_bytes(b'')
    monkeypatch.chdir(tmp_path)

    _ofs, shapefile_path = hf_radar_module.resolve_bounds(
        None, 'cbofs', 'my_extents')

    assert shapefile_path == custom / 'cbofs.shp'


# ---------------------------------------------------------------------------
# get_icecover_observations: -p was ignored for output entirely
# ---------------------------------------------------------------------------

_ICE_DIR_PARAMS = {
    'home': './',
    'data_dir': 'data',
    'observations_dir': 'observations',
    '2d_satellite_dir': '2d_satellite',
    '2d_satellite_ice_dir': '2d_satellite_ice',
    'ofs_extents_dir': 'ofs_extents',
}


def _ice_prop(path):
    return SimpleNamespace(
        ofs='leofs',
        path=str(path),
        config_file=None,
        start_date_full='2024-02-01T00:00:00Z',
        end_date_full='2024-02-02T00:00:00Z',
        data_observations_2d_satellite_path='',
    )


def test_icecover_observations_output_dir_follows_p(tmp_path):
    """The GLSEA download directory was never derived from ``-p``.

    ``data_observations_2d_satellite_path`` stayed at its ``''``
    ModelProperties default, so every downstream ``'' + '/' + name`` became
    an absolute path at the filesystem root: the run logged
    'Directory not found: ' and then died on
    ``PermissionError: '/2024_032_glsea_ice.nc'``.
    """
    ice = _load_bin_module(
        'get_icecover_observations_entrypoint',
        'obs_retrieval/get_icecover_observations.py')
    prop = _ice_prop(tmp_path)

    ice.parameter_dir_validation(prop, dict(_ICE_DIR_PARAMS), logger)

    expected = tmp_path / 'data' / 'observations' / '2d_satellite_ice'
    assert Path(prop.data_observations_2d_satellite_path) == expected
    assert expected.is_dir()


def test_icecover_observations_output_dir_is_absolute(tmp_path, monkeypatch):
    """Resolved to an absolute path so download / concat / mask stages agree
    no matter what the process working directory is."""
    ice = _load_bin_module(
        'get_icecover_observations_entrypoint2',
        'obs_retrieval/get_icecover_observations.py')
    work = tmp_path / 'work'
    work.mkdir()
    monkeypatch.chdir(tmp_path)
    prop = _ice_prop('work')

    ice.parameter_dir_validation(prop, dict(_ICE_DIR_PARAMS), logger)

    assert os.path.isabs(prop.data_observations_2d_satellite_path)
    assert Path(prop.data_observations_2d_satellite_path) == (
        work / 'data' / 'observations' / '2d_satellite_ice')


def test_icecover_observations_absolute_data_dir_wins(tmp_path):
    """An absolute ``data_dir`` on another disk is used as-is."""
    ice = _load_bin_module(
        'get_icecover_observations_entrypoint3',
        'obs_retrieval/get_icecover_observations.py')
    ext = tmp_path / 'bigdisk' / 'ofs_data'
    dir_params = dict(_ICE_DIR_PARAMS, data_dir=str(ext))
    prop = _ice_prop(tmp_path)

    ice.parameter_dir_validation(prop, dir_params, logger)

    assert Path(prop.data_observations_2d_satellite_path) == (
        ext / 'observations' / '2d_satellite_ice')


def test_icecover_observations_keeps_caller_supplied_output_dir(tmp_path):
    """``do_iceskill`` resolves this path itself before calling in, so a
    value already on the properties object must not be overwritten."""
    ice = _load_bin_module(
        'get_icecover_observations_entrypoint4',
        'obs_retrieval/get_icecover_observations.py')
    chosen = tmp_path / 'chosen'
    prop = _ice_prop(tmp_path)
    prop.data_observations_2d_satellite_path = str(chosen)

    ice.parameter_dir_validation(prop, dict(_ICE_DIR_PARAMS), logger)

    assert Path(prop.data_observations_2d_satellite_path) == chosen
    assert chosen.is_dir()


# ---------------------------------------------------------------------------
# get_shapefile_intersection: both outputs went to the wrong place
# ---------------------------------------------------------------------------

@pytest.fixture
def intersection_module():
    return _load_bin_module(
        'get_shapefile_intersection_entrypoint',
        'utils/get_shapefile_intersection.py')


@pytest.fixture
def stubbed_intersection(monkeypatch, intersection_module):
    """Replace the geometry and network stages with in-memory stand-ins.

    What is under test is where the two output files land, not the overlay
    math or the station providers.
    """
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box

    def _fake_read_file(_path):
        return gpd.GeoDataFrame({'geometry': [box(0, 0, 2, 2)]}, crs='EPSG:4326')

    written = []

    def _fake_to_file(self, path, *args, **kwargs):
        written.append(str(path))
        Path(path).write_bytes(b'')

    monkeypatch.setattr(intersection_module.gpd, 'read_file', _fake_read_file)
    monkeypatch.setattr(gpd.GeoDataFrame, 'to_file', _fake_to_file)
    monkeypatch.setattr(
        intersection_module, 'ofs_geometry', lambda *a, **k: None)
    monkeypatch.setattr(
        intersection_module, 'retrieving_inventories',
        lambda *a, **k: pd.DataFrame({'ID': ['x']}))
    monkeypatch.setattr(
        intersection_module, 'filter_inventory', lambda df, *a, **k: df)
    return written


def test_intersection_writes_both_outputs_under_home_path(
        tmp_path, intersection_module, stubbed_intersection):
    """Neither output honored ``-p``.

    The overlap shapefile was written to the ``resolve_asset_path`` result —
    an *input* lookup, so on an external ``-p`` it landed in the
    installation — and the inventory CSV went to a hardcoded
    ``control_files`` that nothing created, so ``to_csv`` raised
    ``OSError: Cannot save file into a non-existent directory``.
    """
    intersection_module.get_shapefile_intersection(
        'cbofs', 'dbofs', str(tmp_path), 'co-ops', logger)

    overlap = tmp_path / 'ofs_extents' / 'cbofs_dbofs_overlap.shp'
    inventory = (tmp_path / 'control_files' /
                 'inventory_all_cbofs_dbofs_overlap.csv')
    assert stubbed_intersection == [str(overlap)]
    assert inventory.is_file()

    # Nothing may be created inside the installation.
    assert not (get_project_root() / 'ofs_extents' /
                'cbofs_dbofs_overlap.shp').exists()


def test_intersection_honors_configured_directory_names(
        tmp_path, monkeypatch, intersection_module, stubbed_intersection):
    """``control_files`` was a literal; both names come from the config."""
    class _Utils:
        def __init__(self, config_file=None):
            pass

        def read_config_section(self, _section, _logger):
            return {
                'ofs_extents_dir': 'extents',
                'control_files_dir': 'ctl',
            }

    monkeypatch.setattr(intersection_module, 'Utils', _Utils)

    intersection_module.get_shapefile_intersection(
        'cbofs', 'dbofs', str(tmp_path), 'co-ops', logger)

    assert stubbed_intersection == [
        str(tmp_path / 'extents' / 'cbofs_dbofs_overlap.shp')]
    assert (tmp_path / 'ctl' /
            'inventory_all_cbofs_dbofs_overlap.csv').is_file()


# ---------------------------------------------------------------------------
# processing_2d: the generated domain mask was cached in the installation
# ---------------------------------------------------------------------------

def _mask_prop(path, **extra):
    return SimpleNamespace(ofs='cbofs', path=str(path), config_file=None, **extra)


def test_domain_mask_cache_is_written_under_the_working_directory(tmp_path):
    """``np.save`` targeted ``<install>/ofs_extents/{ofs}_mask.npy``.

    No ``*_mask.npy`` ships with the installation, so the very first 2D run
    for any OFS tried to write into the install tree — a hard failure on the
    read-only shared installation this whole issue is about, and shared
    mutable state everywhere else.
    """
    from ofs_skill.visualization import processing_2d

    _shapefile, cache = processing_2d._domain_mask_paths(
        _mask_prop(tmp_path), logger)

    expected = tmp_path / 'data' / 'model' / '2d_masks' / 'cbofs_mask.npy'
    assert Path(cache) == expected
    assert expected.parent.is_dir()
    assert get_project_root() not in Path(cache).parents


def test_domain_mask_shapefile_matches_the_validated_one(tmp_path):
    """``create_2dplot`` validates ``prop1.ofs_extents_path`` up front, then
    ``interp_grid`` looked somewhere else — an installation-only path built
    from ``__file__``. A user shapefile under an external ``-p`` passed
    validation and then failed deep inside interpolation."""
    from ofs_skill.visualization import processing_2d

    local_extents = tmp_path / 'ofs_extents'
    local_extents.mkdir()
    prop1 = _mask_prop(tmp_path, ofs_extents_path=str(local_extents) + '/')

    shapefile, _cache = processing_2d._domain_mask_paths(prop1, logger)

    assert Path(shapefile) == local_extents / 'cbofs.shp'


def test_domain_mask_shapefile_falls_back_to_the_installation(tmp_path):
    """With no ofs_extents_path on the properties object and no local copy,
    the installed shapefile is still found."""
    from ofs_skill.visualization import processing_2d

    shapefile, _cache = processing_2d._domain_mask_paths(
        _mask_prop(tmp_path), logger)

    assert Path(shapefile) == get_project_root() / 'ofs_extents' / 'cbofs.shp'


# ---------------------------------------------------------------------------
# Utils(-c): the installation fallback must not be silent
# ---------------------------------------------------------------------------

def test_relative_config_fallback_warns_naming_both_paths(
        tmp_path, monkeypatch, caplog):
    """Falling back to the installed config changes ``home=``, and therefore
    where every output file lands. That cannot happen quietly."""
    monkeypatch.chdir(tmp_path)

    with caplog.at_level(logging.WARNING):
        resolved = utils.Utils('conf/ofs_dps.conf.example').get_config_file()

    installed = get_project_root() / 'conf' / 'ofs_dps.conf.example'
    assert resolved == installed.resolve()
    messages = ' '.join(record.getMessage() for record in caplog.records)
    assert str(tmp_path / 'conf' / 'ofs_dps.conf.example') in messages
    assert str(installed.resolve()) in messages


def test_relative_config_found_locally_does_not_warn(tmp_path, monkeypatch, caplog):
    local = tmp_path / 'conf'
    local.mkdir()
    (local / 'ofs_dps.conf.example').write_text('[directories]\nhome = ./\n')
    monkeypatch.chdir(tmp_path)

    with caplog.at_level(logging.WARNING):
        utils.Utils('conf/ofs_dps.conf.example')

    assert caplog.records == []


def test_missing_relative_config_error_names_both_candidates(
        tmp_path, monkeypatch):
    """The message used to name only the working-directory candidate, hiding
    the fact that the installation root had been searched too."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError) as excinfo:
        utils.Utils('conf/definitely_not_here.conf')

    message = str(excinfo.value)
    assert str(tmp_path / 'conf' / 'definitely_not_here.conf') in message
    assert str(get_project_root() / 'conf' / 'definitely_not_here.conf') in message


def test_empty_base_path_resolves_to_the_installation(tmp_path, monkeypatch):
    """``''`` is the ModelProperties default for 'not set'.

    Treating it as a working directory made ``resolve_asset_path`` return a
    bare relative path such as ``conf/logging.conf``, which silently depended
    on the process working directory never changing.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'conf').mkdir()
    (tmp_path / 'conf' / 'logging.conf').write_text('')

    resolved = utils.resolve_asset_path('', 'conf', 'logging.conf')

    assert Path(resolved) == get_project_root() / 'conf' / 'logging.conf'
    assert os.path.isabs(resolved)
