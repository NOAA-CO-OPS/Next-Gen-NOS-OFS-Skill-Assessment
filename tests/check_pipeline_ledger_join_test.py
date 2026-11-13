"""
Tests for the station-drop ledger join in ``bin/utils/check_pipeline.py``
and for the combined ledger emitted by ``get_skill`` (issue #224).

``check_pipeline.py`` audits how far each station got through the 7-stage
pipeline; the ledger written by a skill run records *why* a station stopped.
These tests cover the join between them:

* ``_load_ledger_reasons`` translates the ledger's variable vocabulary into
  the tool's own, expands cast-independent (``whichcast=all``) rows across
  the audited casts, collapses CO-OPS ADCP virtual bin IDs onto their parent
  station, and prefers the earliest pipeline stage when a station was
  recorded at several.
* ``main`` populates ``Drop_Stage``/``Drop_Reason`` for a station that
  stalled and leaves them blank for one that completed, and still writes a
  usable summary when no ledger file exists at all.

The get_skill half asserts the file-clutter fix itself: one combined
``station_ledger_{ofs}.csv`` per OFS accumulating every variable and cast,
rather than one file per combination.

Everything runs offline against ``tmp_path`` and the checked-in pipeline
fixtures; no network, model, or configuration outside the temp tree.
"""

from __future__ import annotations

import csv
import importlib.util
import logging
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ofs_skill.model_processing.station_ledger import (
    LEDGER_COLUMNS,
    PAIRING_STAGES,
    StationLedger,
)
from ofs_skill.skill_assessment.get_skill import (
    _attach_ledger_view,
    _emit_ledger,
    skill,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
CHECK_PIPELINE_PATH = REPO_ROOT / 'bin' / 'utils' / 'check_pipeline.py'
FIXTURES = Path(__file__).resolve().parent / 'fixtures' / 'pipeline'

logger = logging.getLogger('check_pipeline_ledger_join_test')


@pytest.fixture(scope='module')
def check_pipeline():
    """Load ``bin/utils/check_pipeline.py`` as an importable module."""
    spec = importlib.util.spec_from_file_location(
        'check_pipeline_under_test', CHECK_PIPELINE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_ledger(path: Path, rows: list[dict]) -> None:
    """Write a ledger CSV in the combined format."""
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(LEDGER_COLUMNS), lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, '') for col in LEDGER_COLUMNS})


def _drop_row(**kwargs) -> dict:
    row = {
        'ofs': 'cbofs',
        'variable': 'water_level',
        'whichcast': 'nowcast',
        'filetype': 'stations',
        'record_type': 'drop',
        'stage': 'obs_ctl',
        'station_id': '8594900',
        'reason': 'no observations',
    }
    row.update(kwargs)
    return row


# ---------------------------------------------------------------------------
# 1. _load_ledger_reasons
# ---------------------------------------------------------------------------


def test_load_ledger_reasons_missing_file_is_empty(check_pipeline, tmp_path):
    assert check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast']) == ({}, {})


def test_load_ledger_reasons_translates_temperature_term(check_pipeline, tmp_path):
    # The ledger says 'water_temperature'; the tool's Variable column says
    # 'temperature'. Without the translation the join silently misses every
    # temperature row.
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(variable='water_temperature', station_id='8574680',
                  stage='pairing', whichcast='nowcast',
                  reason='no valid paired series'),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    assert reasons[('8574680', 'temperature', 'nowcast')] == (
        'pairing', 'no valid paired series')


def test_load_ledger_reasons_expands_cast_independent_rows(check_pipeline, tmp_path):
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(stage='node_match', whichcast='all',
                  reason='nearest model location 6.2 km away'),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast', 'forecast_b'])
    assert ('8594900', 'water_level', 'nowcast') in reasons
    assert ('8594900', 'water_level', 'forecast_b') in reasons


def test_load_ledger_reasons_collapses_virtual_bin_ids(check_pipeline, tmp_path):
    # check_pipeline keys on the parent station from the inventory, so a
    # per-bin drop must be findable under the parent ID.
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(variable='currents', station_id='cb0402_b05',
                  stage='pairing', whichcast='nowcast',
                  reason='no valid paired series'),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    assert reasons[('cb0402', 'currents', 'nowcast')][0] == 'pairing'


def test_load_ledger_reasons_keeps_bin_prunes_out_of_the_reasons(
        check_pipeline, tmp_path):
    # Below-bottom bin pruning removes bins, not stations: the parent is
    # still assessed on whatever remains. Counting the prunes separately is
    # what stops a routine prune from labelling a healthy station "dropped".
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(variable='currents', station_id='cb0402_b05',
                  stage='depth_match', whichcast='all',
                  reason='bin depth 9.00 m exceeds model water depth 7.00 m'),
        _drop_row(variable='currents', station_id='cb0402_b06',
                  stage='depth_match', whichcast='all',
                  reason='bin depth 9.50 m exceeds model water depth 7.00 m'),
    ])
    reasons, prunes = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    assert reasons == {}
    count, sample = prunes[('cb0402', 'currents', 'nowcast')]
    assert count == 2
    assert 'exceeds model water depth' in sample


def test_load_ledger_reasons_skips_other_filetypes(check_pipeline, tmp_path):
    # The combined ledger holds stations and fields rows side by side. This
    # tool audits the stations product, so a fields-run failure must not be
    # reported against a stations row.
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(filetype='fields', stage='pairing',
                  reason='fields-run failure'),
        _drop_row(filetype='', station_id='8575512', stage='pairing',
                  reason='legacy ledger with no filetype column value'),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    assert list(reasons) == [('8575512', 'water_level', 'nowcast')]


def test_load_ledger_reasons_prefers_the_earliest_stage(check_pipeline, tmp_path):
    # A station recorded at both obs_ctl and pairing stopped at obs_ctl;
    # the later record is a consequence, not the explanation.
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        _drop_row(stage='pairing', reason='no valid paired series'),
        _drop_row(stage='obs_ctl', whichcast='all',
                  reason='no water_level observations retrievable'),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    stage, reason = reasons[('8594900', 'water_level', 'nowcast')]
    assert stage == 'obs_ctl'
    assert 'retrievable' in reason


def test_load_ledger_reasons_ignores_stage_rows_and_formula_guards(
        check_pipeline, tmp_path):
    _write_ledger(tmp_path / 'station_ledger_cbofs.csv', [
        {'ofs': 'cbofs', 'variable': 'water_level', 'whichcast': 'all',
         'filetype': 'stations', 'record_type': 'stage', 'stage': 'inventory',
         'count_in': '4', 'count_out': '4'},
        _drop_row(station_id="'=8594900", reason="'+bad"),
    ])
    reasons, _ = check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast'])
    # Stage rows contribute nothing; the drop row's leading quote guard is
    # stripped back off on read.
    assert list(reasons) == [('=8594900', 'water_level', 'nowcast')]
    assert reasons[('=8594900', 'water_level', 'nowcast')][1] == '+bad'


def test_load_ledger_reasons_unreadable_file_degrades(check_pipeline, tmp_path):
    ledger_dir = tmp_path / 'station_ledger_cbofs.csv'
    ledger_dir.mkdir()  # a directory where a file is expected
    assert check_pipeline._load_ledger_reasons(
        tmp_path, 'cbofs', ['nowcast']) == ({}, {})


# ---------------------------------------------------------------------------
# 2. End-to-end: check_pipeline main() writes the two new columns
# ---------------------------------------------------------------------------


def _seed_pipeline_tree(tmp_path: Path) -> Path:
    """Lay out a minimal home directory check_pipeline can audit."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from helpers.api_mocks import write_minimal_ofs_config  # noqa: PLC0415

    write_minimal_ofs_config(tmp_path)
    ctl = tmp_path / 'control_files'
    ctl.mkdir()
    shutil.copy(FIXTURES / 'inventory_all_cbofs.csv',
                ctl / 'inventory_all_cbofs.csv')
    shutil.copy(FIXTURES / 'cbofs_wl_station.ctl', ctl / 'cbofs_wl_station.ctl')
    shutil.copy(FIXTURES / 'cbofs_wl_model_station.ctl',
                ctl / 'cbofs_wl_model_station.ctl')
    for sub in (
        ('data', 'observations', '1d_station'),
        ('data', 'model', '1d_node'),
        ('data', 'skill', '1d_pair'),
        ('data', 'visual'),
    ):
        (tmp_path.joinpath(*sub)).mkdir(parents=True, exist_ok=True)
    return ctl


def _run_check_pipeline(check_pipeline, tmp_path, monkeypatch):
    """Run main() with visualisation stubbed out, return the summary rows."""
    monkeypatch.setattr(check_pipeline, 'generate_visualizations',
                        lambda *a, **k: None)
    args = SimpleNamespace(
        OFS='cbofs', Var_Selection='wl', Whichcasts=['nowcast'],
        Path=str(tmp_path), config='ofs_dps.conf',
    )
    check_pipeline.main(args)
    summary = tmp_path / 'pipeline_summary_cbofs_wl.csv'
    assert summary.exists()
    with open(summary, newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def test_main_annotates_rows_with_the_ledger_reason(
        check_pipeline, tmp_path, monkeypatch):
    ctl = _seed_pipeline_tree(tmp_path)
    _write_ledger(ctl / 'station_ledger_cbofs.csv', [
        _drop_row(station_id='8594900', stage='obs_ctl', whichcast='all',
                  reason='no water_level observations retrievable for the '
                         'run window'),
    ])

    rows = {r['Station_ID']: r for r in
            _run_check_pipeline(check_pipeline, tmp_path, monkeypatch)}

    # 8594900 is in the inventory but absent from the obs ctl fixture.
    stalled = rows['8594900']
    assert stalled['1_In_Inventory'] == 'Yes'
    assert stalled['2_In_OBS_CTL'] == 'No'
    assert stalled['Drop_Stage'] == 'obs_ctl'
    assert 'retrievable' in stalled['Drop_Reason']

    # 8637689 reached the control files; nothing dropped it, so the new
    # columns stay blank rather than inventing an explanation.
    progressed = rows['8637689']
    assert progressed['2_In_OBS_CTL'] == 'Yes'
    assert progressed['Drop_Stage'] == ''
    assert progressed['Drop_Reason'] == ''


def test_main_without_a_ledger_still_writes_the_columns(
        check_pipeline, tmp_path, monkeypatch):
    _seed_pipeline_tree(tmp_path)
    rows = _run_check_pipeline(check_pipeline, tmp_path, monkeypatch)
    assert rows, 'the auditor must still produce its matrix'
    assert all(r['Drop_Stage'] == '' and r['Drop_Reason'] == ''
               and r['Bins_Pruned'] == '' for r in rows)


def _complete_station_artifacts(tmp_path: Path) -> None:
    """Give 8637689 the full set of artifacts so all seven stages pass."""
    shutil.copy(FIXTURES / '8637689_cbofs_wl_station.obs',
                tmp_path / 'data' / 'observations' / '1d_station'
                / '8637689_cbofs_wl_station.obs')
    shutil.copy(FIXTURES / '8637689_cbofs_wl_45_nowcast_stations_model.prd',
                tmp_path / 'data' / 'model' / '1d_node'
                / '8637689_cbofs_wl_45_nowcast_stations_model.prd')
    shutil.copy(FIXTURES / 'cbofs_wl_8637689_45_nowcast_stations_pair.int',
                tmp_path / 'data' / 'skill' / '1d_pair'
                / 'cbofs_wl_8637689_45_nowcast_stations_pair.int')
    (tmp_path / 'data' / 'visual'
     / '8637689_cbofs_water_level_nowcast.html').write_text(
        '<html></html>', encoding='utf-8')


def test_main_does_not_annotate_a_station_that_completed(
        check_pipeline, tmp_path, monkeypatch):
    # The core false-positive guard. A drop row left behind by an earlier
    # run (or a per-bin currents record) must never contradict this audit's
    # own matrix: a station showing Yes at all seven stages was not dropped.
    ctl = _seed_pipeline_tree(tmp_path)
    _complete_station_artifacts(tmp_path)
    _write_ledger(ctl / 'station_ledger_cbofs.csv', [
        _drop_row(station_id='8637689', stage='pairing', whichcast='nowcast',
                  reason='no valid paired OBS/OFS series'),
        _drop_row(variable='currents', station_id='8637689_b03',
                  stage='depth_match', whichcast='all',
                  reason='bin depth 9.00 m exceeds model water depth 7.00 m'),
    ])

    rows = {r['Station_ID']: r for r in
            _run_check_pipeline(check_pipeline, tmp_path, monkeypatch)}
    completed = rows['8637689']
    assert all(completed[col] == 'Yes' for col in (
        '1_In_Inventory', '2_In_OBS_CTL', '3_OBS_Generated',
        '4_In_Model_CTL', '5_PRD_Generated', '6_INT_Generated',
        '7_HTML_Generated'))
    assert completed['Drop_Stage'] == ''
    assert completed['Drop_Reason'] == ''


def test_main_suppresses_a_reason_that_contradicts_the_matrix(
        check_pipeline, tmp_path, monkeypatch):
    # 8575512 is in both control files but has no .obs/.prd/.int, so it
    # stalled at stage 3. A node_match record claims it never reached the
    # model control file -- stage 4 -- which this audit shows it did, so
    # the record is stale and must not be printed.
    ctl = _seed_pipeline_tree(tmp_path)
    _write_ledger(ctl / 'station_ledger_cbofs.csv', [
        _drop_row(station_id='8575512', stage='node_match', whichcast='all',
                  reason='nearest model location 6.2 km away'),
    ])

    rows = {r['Station_ID']: r for r in
            _run_check_pipeline(check_pipeline, tmp_path, monkeypatch)}
    stalled = rows['8575512']
    assert stalled['3_OBS_Generated'] == 'No'
    assert stalled['4_In_Model_CTL'] == 'Yes'
    assert stalled['Drop_Stage'] == ''


# ---------------------------------------------------------------------------
# 2b. _explain_row: the guard that keeps the join honest
# ---------------------------------------------------------------------------

ALL_YES = [True] * 7


def test_explain_row_ignores_the_ledger_for_a_completed_station(
        check_pipeline):
    assert check_pipeline._explain_row(
        ALL_YES, ('pairing', 'no valid paired series'), None) == ('', '', '')


def test_explain_row_reports_a_stall_at_the_named_stage(check_pipeline):
    # In inventory, absent from the obs ctl file -> first No at column 2,
    # which is exactly what an obs_ctl drop explains.
    flags = [True] + [False] * 6
    assert check_pipeline._explain_row(
        flags, ('obs_ctl', 'no observations retrievable'), None) == (
            'obs_ctl', 'no observations retrievable', '')


def test_explain_row_reports_a_stall_later_than_the_named_stage(
        check_pipeline):
    # A pairing drop (column 5 at the earliest) still explains a station
    # whose first No is the missing .int at column 6.
    flags = [True, True, True, True, True, False, False]
    stage, reason, _ = check_pipeline._explain_row(
        flags, ('pairing', 'no valid paired series'), None)
    assert (stage, reason) == ('pairing', 'no valid paired series')


def test_explain_row_counts_bin_prunes_without_calling_them_a_drop(
        check_pipeline):
    # A currents parent that stalled at the .int stage with one pruned bin:
    # the prune is reported in its own column, and the station-level reason
    # is whatever the ledger recorded for the station itself.
    flags = [True, True, True, True, True, False, False]
    stage, reason, bins = check_pipeline._explain_row(
        flags, None, (1, 'bin depth 9.00 m exceeds model water depth'))
    assert (stage, reason) == ('', '')
    assert bins == '1'


def test_explain_row_uses_bin_prunes_when_nothing_survived(check_pipeline):
    # Every bin pruned away means the parent never reaches the model
    # control file, and the prune really is the explanation.
    flags = [True, True, True, False, False, False, False]
    stage, reason, bins = check_pipeline._explain_row(
        flags, None, (6, 'bin depth 9.00 m exceeds model water depth 7.00 m'))
    assert stage == 'depth_match'
    assert '6 ADCP bin(s)' in reason
    assert bins == '6'


def test_explain_row_prefers_a_station_level_reason_over_bin_prunes(
        check_pipeline):
    flags = [True, True, True, False, False, False, False]
    stage, _, bins = check_pipeline._explain_row(
        flags, ('node_match', 'nearest model location 6.2 km away'),
        (2, 'bin depth exceeds model water depth'))
    assert stage == 'node_match'
    assert bins == '2'


def test_explain_row_without_any_ledger_record(check_pipeline):
    assert check_pipeline._explain_row(
        [True, False, False, False, False, False, False], None, None) == (
            '', '', '')


# ---------------------------------------------------------------------------
# 3. get_skill emits one combined ledger per OFS
# ---------------------------------------------------------------------------


def _prop(tmp_path, whichcast='nowcast'):
    return SimpleNamespace(
        ofs='cbofs',
        whichcast=whichcast,
        ofsfiletype='stations',
        start_date_full='2026-01-01T00:00:00Z',
        end_date_full='2026-01-02T00:00:00Z',
        control_files_path=str(tmp_path),
    )


def test_ledger_view_is_shared_across_variables_and_casts(tmp_path):
    # One root ledger per run: switching variable (and then whichcast, as
    # create_1dplot does by reusing the same prop) must keep accumulating
    # into the same ledger instead of starting a new one.
    prop = _prop(tmp_path)
    _attach_ledger_view(prop, 'water_level')
    root = prop.station_ledger_root
    prop.station_ledger.drop('A', stage='pairing', reason='x')

    _attach_ledger_view(prop, 'currents')
    assert prop.station_ledger_root is root
    prop.station_ledger.drop('B', stage='pairing', reason='y')

    prop.whichcast = 'forecast_b'
    _attach_ledger_view(prop, 'water_level')
    assert prop.station_ledger_root is root
    prop.station_ledger.drop('C', stage='pairing', reason='z')

    assert [d.station_id for d in root.drops] == ['A', 'B', 'C']
    assert [d.variable for d in root.drops] == [
        'water_level', 'currents', 'water_level']
    assert [d.whichcast for d in root.drops] == [
        'nowcast', 'nowcast', 'forecast_b']


def test_emit_ledger_writes_exactly_one_file_per_ofs(tmp_path):
    prop = _prop(tmp_path)
    for variable in ('water_level', 'currents'):
        _attach_ledger_view(prop, variable)
        prop.station_ledger.drop(f'{variable}-station', stage='pairing',
                                 reason='no valid paired series')
        _emit_ledger(prop, logger)

    written = sorted(p.name for p in tmp_path.glob('station_ledger_*.csv'))
    assert written == ['station_ledger_cbofs.csv'], (
        'the combined ledger must replace the per-variable files'
    )
    with open(tmp_path / 'station_ledger_cbofs.csv', newline='',
              encoding='utf-8') as handle:
        rows = [r for r in csv.DictReader(handle) if r['record_type'] == 'drop']
    assert {r['variable'] for r in rows} == {'water_level', 'currents'}


def test_emit_ledger_merges_a_later_cast_into_the_same_file(tmp_path):
    first = _prop(tmp_path, whichcast='nowcast')
    _attach_ledger_view(first, 'water_level')
    first.station_ledger.drop('8594900', stage='pairing', reason='nowcast run')
    _emit_ledger(first, logger)

    # A separate invocation for another cast (fresh prop, fresh root).
    second = _prop(tmp_path, whichcast='forecast_b')
    _attach_ledger_view(second, 'water_level')
    second.station_ledger.drop('8575512', stage='pairing',
                               reason='forecast_b run')
    _emit_ledger(second, logger)

    with open(tmp_path / 'station_ledger_cbofs.csv', newline='',
              encoding='utf-8') as handle:
        rows = [r for r in csv.DictReader(handle) if r['record_type'] == 'drop']
    assert {(r['station_id'], r['whichcast']) for r in rows} == {
        ('8594900', 'nowcast'), ('8575512', 'forecast_b')}


def test_emit_ledger_survives_an_unwritable_control_files_path(tmp_path):
    # Bookkeeping must never abort a skill run: a bad path is logged, not
    # raised.
    prop = _prop(tmp_path / 'does' / 'not' / 'exist')
    _attach_ledger_view(prop, 'water_level')
    prop.station_ledger.drop('A', stage='pairing', reason='x')
    _emit_ledger(prop, logger)  # must not raise


def test_emit_ledger_without_a_root_is_a_no_op(tmp_path):
    prop = _prop(tmp_path)
    _emit_ledger(prop, logger)
    assert list(tmp_path.glob('station_ledger_*.csv')) == []


def test_legacy_per_variable_files_are_reported_not_deleted(tmp_path, caplog):
    legacy = tmp_path / 'station_ledger_cbofs_wl_nowcast_stations.csv'
    legacy.write_text('ofs,variable\n', encoding='utf-8')

    prop = _prop(tmp_path)
    _attach_ledger_view(prop, 'water_level')
    prop.station_ledger.drop('A', stage='pairing', reason='x')
    with caplog.at_level(logging.INFO,
                         logger='check_pipeline_ledger_join_test'):
        _emit_ledger(prop, logger)

    assert legacy.exists(), 'user artifacts must not be deleted'
    assert 'superseded' in caplog.text

    # The notice is emitted once per run, not once per variable.
    caplog.clear()
    _attach_ledger_view(prop, 'currents')
    with caplog.at_level(logging.INFO,
                         logger='check_pipeline_ledger_join_test'):
        _emit_ledger(prop, logger)
    assert 'superseded' not in caplog.text


def _empty_ctl_pair(line_count):
    """Return (obs, model) ctl structures with no pairable stations.

    ``line_count`` obs lines and an empty model station list, so ``skill``
    records its stage counts but the pairing loop has nothing to submit.
    """
    return [[[f'cb0402_b{i:02d}'] for i in range(line_count)]], [None, []]


def test_skill_records_both_obs_ctl_units_without_duplicating_rows(tmp_path):
    # An ADCP obs control file carries one line per bin, while inventory
    # reconciliation counts parent stations. Recording both under the same
    # stage name made the chain read "3 stations out -> 18 stations in";
    # the line count now has its own stage. model_ctl is cast-independent,
    # so a second whichcast must not append an identical row.
    prop = _prop(tmp_path)
    _attach_ledger_view(prop, 'currents')
    root = prop.station_ledger_root
    prop.station_ledger.note_stage(
        'obs_ctl', count_in=4, count_out=3,
        note='inventory stations that reached the obs station ctl file')

    obs_ctl, model_ctl = _empty_ctl_pair(18)
    skill(obs_ctl, model_ctl, prop, 'cu', logger)

    by_stage = {}
    for record in root.stages:
        by_stage.setdefault(record.stage, []).append(record)
    assert by_stage['obs_ctl'][0].count_out == 3, 'parent-station accounting'
    assert by_stage['obs_ctl_lines'][0].count_out == 18, 'ctl line accounting'
    assert len(by_stage['model_ctl']) == 1

    # A second whichcast reuses the same control files.
    prop.whichcast = 'forecast_b'
    _attach_ledger_view(prop, 'currents')
    skill(obs_ctl, model_ctl, prop, 'cu', logger)

    stage_names = [record.stage for record in root.stages]
    assert stage_names.count('model_ctl') == 1
    assert stage_names.count('obs_ctl_lines') == 1
    assert stage_names.count('obs_ctl') == 1
    # skill_csv is genuinely per cast, so it is recorded twice.
    assert stage_names.count('skill_csv') == 2


def test_skill_declares_the_drop_only_pairing_stages(tmp_path):
    # The pairing loop runs every pass but emits rows only when something
    # fails. Declaring the stages is what lets a clean run supersede an
    # earlier run's pairing drops instead of inheriting them.
    prop = _prop(tmp_path)
    _attach_ledger_view(prop, 'water_level')
    root = prop.station_ledger_root

    obs_ctl, model_ctl = _empty_ctl_pair(2)
    skill(obs_ctl, model_ctl, prop, 'wl', logger)

    for stage in PAIRING_STAGES:
        assert ('water_level', 'nowcast', 'stations', stage) in root.stages_run


def test_root_ledger_carries_the_run_window(tmp_path):
    prop = _prop(tmp_path)
    _attach_ledger_view(prop, 'water_level')
    root: StationLedger = prop.station_ledger_root
    assert root.run_start == '2026-01-01T00:00:00Z'
    assert root.run_end == '2026-01-02T00:00:00Z'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
