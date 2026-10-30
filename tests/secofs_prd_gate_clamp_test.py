"""The ``.prd`` reuse gate in ``get_skill`` judges files by what the
catalog can provide, not by the raw request (issue #254).

Scenario: run window 2026-05-01 to 2026-07-31; the catalog validator
dropped the model files before 2026-05-06 06:00, so the loaded model
dataset, and therefore every extracted ``.prd``, starts there. A file
covering exactly that span must survive the gate and must not trigger a
re-extraction, otherwise every variable deletes and regenerates identical
data (the "thrash loop").

The tests drive the gate ``get_skill`` actually calls, with the same
arguments ``get_skill`` passes, and one of them drives ``get_skill``
itself. Everything heavy (config, obs fetch, model extraction, pairing)
is stubbed; the gate logic runs for real.
"""
import importlib
import logging
import os
import time
from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import xarray as xr

# The package re-exports the get_skill *function* under the module's
# name, so import the module explicitly.
gs = importlib.import_module('ofs_skill.skill_assessment.get_skill')

WINDOW_START = datetime(2026, 5, 1)
WINDOW_END = datetime(2026, 7, 31)
CATALOG_START = datetime(2026, 5, 6, 6)   # first file the validator kept
STEP = timedelta(hours=6)

# ctl structure as the gate indexes it: [-1] -> station ids, [1] -> nodes.
READ_OFS_CTL = [None, ['340'], None, None, ['8571421']]
EXPECTED_PRD = '8571421_secofs_wl_340_nowcast_stations_model.prd'
EXPECTED_OBS = '8571421_secofs_wl_station.obs'

DIRS = {
    'home': '', 'ofs_extents_dir': 'ofs_extents',
    'control_files_dir': 'control_files', 'data_dir': 'data',
    'observations_dir': 'observations', '1d_station_dir': '1d_station',
    'model_dir': 'model', '1d_node_dir': '1d_node', 'skill_dir': 'skill',
    '1d_pair_dir': '1d_pair', 'stats_dir': 'stats', 'visual_dir': 'visual',
    'visual_maps': 'maps', '2d_dir': '2d', 'control_files': 'control_files',
}


def _write_series(path, start, end, step=STEP):
    rows = []
    t = start
    while t <= end:
        jd = (t - datetime(t.year, 1, 1)).total_seconds() / 86400.0 + 1
        rows.append(f'{jd:.6f} {t.year} {t.month} {t.day} {t.hour} '
                    f'{t.minute} 0.500\n')
        t += step
    with open(path, 'w', encoding='utf-8') as fh:
        fh.writelines(rows)


def _backdate(path, seconds=3600):
    """Make the file look like an earlier process wrote it, so
    ``created_this_run`` is False and only the coverage logic decides."""
    old = time.time() - seconds
    os.utime(path, (old, old))


def _fake_model(start, end, step=STEP):
    times = np.arange(np.datetime64(start),
                      np.datetime64(end) + np.timedelta64(1, 's'),
                      np.timedelta64(int(step.total_seconds()), 's'))
    return xr.Dataset({'elev': ('time', np.zeros(times.size))},
                      coords={'time': times})


def _prop(tmp_path):
    return SimpleNamespace(
        ofs='secofs', whichcast='nowcast', ofsfiletype='stations',
        forecast_hr=None, datum='MLLW', stationowner='co-ops',
        start_date_full=WINDOW_START.strftime('%Y-%m-%dT%H:%M:%SZ'),
        end_date_full=WINDOW_END.strftime('%Y-%m-%dT%H:%M:%SZ'),
        data_model_1d_node_path=str(tmp_path),
        data_observations_1d_station_path=str(tmp_path),
        continue_run=False, currents_bins_csv=None,
        user_input_location=False, config_file=None,
    )


def _write_filename_key(node_dir, start, end, step=STEP):
    """Write the key get_node_ofs leaves behind: one row per model
    timestep with the ``DateTime`` index label first, as
    ``serieskey.to_csv(filepath, index_label='DateTime')`` emits it."""
    path = os.path.join(node_dir, 'secofs_nowcast_filename_key.csv')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write('DateTime,filename\n')
        t = start
        while t <= end:
            fh.write(f'{t:%Y-%m-%d %H:%M:%S},'
                     f'nos.secofs.stations.nowcast.{t:%Y%m%d}.t{t:%H}z.nc\n')
            t += step
    return path


def _hold_manifest_open(monkeypatch):
    """The cache-manifest parameter gate (#263/#335) is a separate feature;
    hold it open so only the coverage logic decides."""
    monkeypatch.setattr(gs.cache_manifest, 'artifact_is_fresh',
                        lambda *a, **k: True, raising=False)


def _stub_extraction(monkeypatch):
    calls = []
    monkeypatch.setattr(gs, 'get_node_ofs',
                        lambda prop, logger, model_dataset=None:
                        calls.append(prop) or None)
    return calls


def _run_prd_gate(p, model):
    return gs._ensure_prd_files(READ_OFS_CTL, p, 'wl',
                                logging.getLogger('gate'), model,
                                'water_level')


def test_prd_covering_catalog_span_is_kept(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    _hold_manifest_open(monkeypatch)
    calls = _stub_extraction(monkeypatch)
    p = _prop(tmp_path)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, CATALOG_START, WINDOW_END)
    _backdate(prd_path)

    _run_prd_gate(p, _fake_model(CATALOG_START, WINDOW_END))

    why = [r.getMessage() for r in caplog.records
           if r.levelno >= logging.WARNING]
    assert os.path.isfile(prd_path), (
        f'.prd covering the catalog span was deleted; warnings: {why!r}')
    assert not calls, 'gate re-triggered get_node_ofs (thrash loop)'


def test_prd_genuinely_stale_is_still_deleted(tmp_path, monkeypatch):
    """Control: a file from an older window (ends in April) must still go."""
    _hold_manifest_open(monkeypatch)
    calls = _stub_extraction(monkeypatch)
    p = _prop(tmp_path)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, datetime(2026, 2, 1), datetime(2026, 4, 30))
    _backdate(prd_path)

    _run_prd_gate(p, _fake_model(CATALOG_START, WINDOW_END))

    assert not os.path.isfile(prd_path), 'stale file kept'
    assert calls, 'no re-extraction for a stale file'


def test_rerun_without_loaded_model_clamps_from_filename_key(
        tmp_path, monkeypatch, caplog):
    """A rerun in a new process has cached ctl files and no model loaded
    when the first variable reaches the gate. The filename key the last
    extraction wrote records what the catalog provided, so the gate must
    still keep a file covering that span rather than re-extract it."""
    caplog.set_level(logging.INFO)
    _hold_manifest_open(monkeypatch)
    calls = _stub_extraction(monkeypatch)
    p = _prop(tmp_path)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, CATALOG_START, WINDOW_END)
    _backdate(prd_path)
    _write_filename_key(p.data_model_1d_node_path, CATALOG_START, WINDOW_END)

    _run_prd_gate(p, None)

    assert os.path.isfile(prd_path), 'rerun deleted the first variable'
    assert not calls
    assert any('clamping the staleness-check window' in r.getMessage()
               for r in caplog.records)


def test_filename_key_from_another_period_is_ignored(tmp_path, monkeypatch):
    """A key left by a run over some other window says nothing about this
    one; a genuinely stale file must still be regenerated."""
    _hold_manifest_open(monkeypatch)
    calls = _stub_extraction(monkeypatch)
    p = _prop(tmp_path)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, datetime(2026, 2, 1), datetime(2026, 4, 30))
    _backdate(prd_path)
    _write_filename_key(p.data_model_1d_node_path,
                        datetime(2026, 2, 1), datetime(2026, 4, 30))

    _run_prd_gate(p, None)

    assert not os.path.isfile(prd_path), 'stale file kept'
    assert calls


def test_continuation_run_is_not_defeated_by_an_earlier_runs_key(
        tmp_path, monkeypatch, caplog):
    """The key's end is where the last run stopped, which is exactly the
    span a --Continue_Run extends. Only its start may narrow the window,
    or the old file reads as complete and the tail path is never taken."""
    caplog.set_level(logging.INFO)
    _hold_manifest_open(monkeypatch)
    calls = []
    monkeypatch.setattr(
        gs, 'get_node_ofs',
        lambda prop, logger, model_dataset=None: calls.append(
            (prop.start_date_full,
             getattr(prop, 'continuation_prd_merge', False))) or None)
    p = _prop(tmp_path)
    p.continue_run = True
    prior_end = datetime(2026, 6, 30)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, WINDOW_START, prior_end)
    _backdate(prd_path)
    _write_filename_key(p.data_model_1d_node_path, WINDOW_START, prior_end)

    _run_prd_gate(p, None)

    assert calls, 'no extraction at all'
    assert calls[0][1] is True, (
        f'first extraction was not a continuation tail: {calls!r}')
    assert calls[0][0] < prior_end.strftime('%Y-%m-%dT%H:%M:%SZ')


def test_filename_key_time_bounds_reads_what_get_node_ofs_writes(tmp_path):
    from ofs_skill.utils.timeseries_coverage import filename_key_time_bounds

    window = (WINDOW_START, WINDOW_END)
    path = _write_filename_key(str(tmp_path), CATALOG_START, WINDOW_END)
    assert filename_key_time_bounds(path, window) == (CATALOG_START,
                                                      WINDOW_END)
    # Non-overlapping span, missing file, unreadable file, no window.
    other = _write_filename_key(str(tmp_path), datetime(2026, 2, 1),
                                datetime(2026, 4, 30))
    assert filename_key_time_bounds(other, window) is None
    assert filename_key_time_bounds(os.path.join(str(tmp_path), 'no.csv'),
                                    window) is None
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write('not,a,key\n1,2,3\n')
    assert filename_key_time_bounds(path, window) is None
    assert filename_key_time_bounds(path, None) is None


def test_prd_written_by_this_process_is_never_deleted(tmp_path, monkeypatch):
    """A file the running process extracted cannot be "left over": with no
    model bounds to clamp to, only ``created_this_run`` keeps it."""
    _hold_manifest_open(monkeypatch)
    calls = _stub_extraction(monkeypatch)
    p = _prop(tmp_path)
    prd_path = os.path.join(p.data_model_1d_node_path, EXPECTED_PRD)
    _write_series(prd_path, CATALOG_START, WINDOW_END)   # fresh mtime

    _run_prd_gate(p, None)

    assert os.path.isfile(prd_path), 'file written this run was deleted'
    assert not calls


def test_obs_fetched_by_this_process_is_never_deleted(tmp_path, monkeypatch,
                                                      caplog):
    """Same guard on the ``.obs`` side: the provider has no more data, so
    deleting and re-fetching a short file this run produced only loops."""
    caplog.set_level(logging.INFO)
    _hold_manifest_open(monkeypatch)
    fetches = []
    monkeypatch.setattr(gs, 'get_station_observations',
                        lambda *a, **k: fetches.append(a) or None)
    p = _prop(tmp_path)
    obs_path = os.path.join(p.data_observations_1d_station_path,
                            EXPECTED_OBS)
    _write_series(obs_path, CATALOG_START, WINDOW_END)   # fresh mtime
    read_station_ctl_file = [[['8571421']]]

    gs._ensure_obs_files(read_station_ctl_file, p, 'wl',
                         logging.getLogger('gate'))

    assert os.path.isfile(obs_path), 'obs fetched this run was deleted'
    assert not fetches


class _FakeUtils:
    def __init__(self, *_a, **_k):
        pass

    def read_config_section(self, section, _logger=None):
        if section == 'directories':
            return dict(DIRS)
        if section == 'datums':
            return {'datum_list': 'MLLW'}
        return {}

    def get_config_file(self):
        return None


def test_get_skill_keeps_prd_covering_catalog_span(tmp_path, monkeypatch,
                                                   caplog):
    """Drive ``get_skill`` itself, so the gate under test is the one it
    really binds and calls, with the arguments it really passes, in the
    shape of a rerun: cached ctl files, no model loaded, a filename key
    from the previous run, and the cache-manifest check running for
    real."""
    caplog.set_level(logging.INFO)
    home = str(tmp_path)
    os.makedirs(os.path.join(home, 'ofs_extents'))
    open(os.path.join(home, 'ofs_extents', 'secofs.shp'), 'w').close()
    node_dir = os.path.join(home, 'data', 'model', '1d_node')
    os.makedirs(node_dir)
    prd_path = os.path.join(node_dir, EXPECTED_PRD)
    _write_series(prd_path, CATALOG_START, WINDOW_END)
    _backdate(prd_path)
    # A rerun: the previous run left its filename key, and no model
    # dataset is loaded when the first variable reaches the gate.
    _write_filename_key(node_dir, CATALOG_START, WINDOW_END)

    prop = SimpleNamespace(
        path=home, ofs='secofs', whichcast='nowcast', forecast_hr=None,
        start_date_full='2026-05-01T00:00:00Z',
        end_date_full='2026-07-31T00:00:00Z',
        stationowner='co-ops', var_list='water_level', datum='MLLW',
        ofsfiletype='stations', horizonskill=False, config_file=None,
        continue_run=False, user_input_location=False,
        currents_bins_csv=None, station_ledger=None, model_source='schism',
    )
    # Record the manifest entry the way get_node_ofs does, from the parsed
    # prop (stationowner is a list by then), so the real parameter check
    # runs against it below and must agree that the file is current.
    cm = gs.cache_manifest
    as_recorded = SimpleNamespace(**vars(prop))
    as_recorded.stationowner = ['co-ops']
    cm.record_artifact(
        prd_path,
        cm.run_signature(as_recorded, variable='water_level',
                         extra={'whichcast': 'nowcast'}),
        node_dir, None)

    def stub(name, val):
        monkeypatch.setattr(gs, name, val, raising=False)

    stub('utils', SimpleNamespace(
        Utils=_FakeUtils,
        resolve_asset_path=lambda base, d: os.path.join(base, d),
        init_root_logger=lambda *a, **k: logging.getLogger('e2e')))
    stub('get_parallel_config', lambda *a, **k: {'parallel_workflow': False})
    stub('get_station_observations', lambda *a, **k: None)
    stub('station_ctl_file_extract', lambda *_a: [[], [], [], [], [], []])
    stub('reconcile_inventory', lambda *a, **k: None)
    stub('ofs_ctlfile_extract', lambda *a, **k: READ_OFS_CTL)
    stub('_get_valid_cached_model', lambda _p: None)
    stub('_set_cached_model', lambda _p, _d: None)
    stub('_close_cached_model', lambda _p: None)
    stub('_attach_ledger_view', lambda *a, **k: None)
    stub('_emit_ledger', lambda *a, **k: None)
    stub('skill', lambda *a, **k: [])
    extractions = _stub_extraction(monkeypatch)
    # Only the obs-ctl freshness check is stubbed; the .prd manifest
    # check runs for real.
    monkeypatch.setattr(cm, 'ensure_fresh', lambda *a, **k: None,
                        raising=False)

    gs.get_skill(prop, logging.getLogger('e2e'))

    why = [r.getMessage() for r in caplog.records
           if r.levelno >= logging.WARNING]
    assert os.path.isfile(prd_path), (
        f'.prd covering the catalog span was deleted; warnings: {why!r}')
    assert not extractions, 'model extraction re-triggered (thrash loop)'
