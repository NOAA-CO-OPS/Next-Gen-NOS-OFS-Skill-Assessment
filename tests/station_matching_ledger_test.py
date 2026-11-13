"""
Regression tests for observation-station matching, filtering, and the
station-drop accounting ledger (issue #200, active issue #1).

The user reported that for NECOFS water level the pipeline produced fewer
stations than the legacy Fortran package and that changing the model-station
search radius swapped *which* station IDs survived without changing the total
count. Investigation showed the reductions happen at several independent,
previously-silent stages. These tests lock in:

1. ``index_nearest_station`` matches within the km cutoff and marks stations
   beyond it as ``NaN`` (FVCOM branch).
2. Stations beyond the cutoff are recorded on an attached ``StationLedger``
   with an explanatory ``node_match`` reason.
3. Two obs stations resolving to the same model location (many-to-one) are
   retained but surfaced as a ``node_match_collision`` ledger note.
4. The ledger summary/accounting API records per-stage counts and per-station
   drop reasons and never raises.

Issue #224 folded the per-(variable, whichcast) ledger files into one
combined ``station_ledger_{ofs}.csv`` per OFS and added inventory tracking.
Sections 6 and 7 below cover the run-scoped ledger, its per-context views,
cross-invocation CSV merging, and the inventory -> obs-ctl reconciliation.

No network or model downloads are required; everything runs on tiny synthetic
arrays.
"""

from __future__ import annotations

import copy
import csv
import logging
import math
import pickle
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from ofs_skill.model_processing import indexing
from ofs_skill.model_processing.indexing import (
    STATION_MATCH_MAX_DIST_KM,
    index_nearest_station,
)
from ofs_skill.model_processing.station_ledger import (
    CAST_ALL,
    DROP_ONLY_STAGES,
    LEDGER_COLUMNS,
    PAIRING_STAGES,
    StationLedger,
)
from ofs_skill.model_processing.station_ledger_inventory import reconcile_inventory

logger = logging.getLogger('station_matching_ledger_test')


def _fvcom_model(lon, lat):
    return {'lon': np.asarray(lon, dtype=float), 'lat': np.asarray(lat, dtype=float)}


# ---------------------------------------------------------------------------
# 1. Distance cutoff: near stations match, far stations become NaN
# ---------------------------------------------------------------------------


def test_fvcom_matches_within_cutoff_and_nans_beyond():
    # Four model stations; obs A/B/C sit essentially on top of nodes 0/1/2,
    # obs FAR is ~hundreds of km away and must be dropped.
    model = _fvcom_model(
        lon=[-71.00, -70.98, -70.50, -68.00],
        lat=[41.00, 41.00, 41.20, 44.00],
    )
    ctl = [
        ['41.000', '-71.001'],
        ['41.000', '-70.985'],
        ['41.200', '-70.505'],
        ['42.000', '-69.500'],  # far from every node
    ]
    ids = [['A'], ['B'], ['C'], ['FAR']]
    prop = SimpleNamespace(ofs='necofs')

    out = index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)

    assert out[0] == 0
    assert out[1] == 1
    assert out[2] == 2
    assert isinstance(out[3], float) and np.isnan(out[3])


def test_station_just_beyond_cutoff_is_dropped_but_just_inside_kept():
    # Single model node at (41, -71). Place one obs just inside the cutoff
    # and one just outside, along a line of longitude.
    model = _fvcom_model(lon=[-71.00, -60.0], lat=[41.00, 50.0])

    # ~1 deg latitude ~= 111 km, so use small offsets around the km cutoff.
    # 0.03 deg lat ~= 3.3 km (inside 4 km), 0.05 deg ~= 5.6 km (outside).
    ctl_in = [['41.030', '-71.000']]
    ctl_out = [['41.050', '-71.000']]
    ids = [['S']]
    prop = SimpleNamespace(ofs='necofs')

    out_in = index_nearest_station(prop, ctl_in, model, 'fvcom', 'wl', logger, ids)
    out_out = index_nearest_station(prop, ctl_out, model, 'fvcom', 'wl', logger, ids)

    assert out_in[0] == 0, 'station inside cutoff should match node 0'
    assert isinstance(out_out[0], float) and np.isnan(out_out[0]), (
        'station beyond cutoff should be NaN'
    )


# ---------------------------------------------------------------------------
# 2. Ledger records distance-cutoff drops with a reason
# ---------------------------------------------------------------------------


def test_ledger_records_distance_drop():
    model = _fvcom_model(lon=[-71.00, -68.00], lat=[41.00, 44.00])
    ctl = [
        ['41.000', '-71.001'],  # matches node 0
        # ~0.045 deg lat north of node 0 (~5 km): inside the latitude-aware
        # candidate box (reach ~6 km) but beyond the 4 km match cutoff, so
        # it exercises the "measured distance > cutoff" drop reason.
        ['41.045', '-71.000'],
    ]
    ids = [['NEAR'], ['FAR']]
    ledger = StationLedger(ofs='necofs', variable='water_level')
    prop = SimpleNamespace(ofs='necofs', station_ledger=ledger)

    index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)

    dropped_ids = {d.station_id for d in ledger.drops}
    assert dropped_ids == {'FAR'}
    far_drop = next(d for d in ledger.drops if d.station_id == 'FAR')
    assert far_drop.stage == 'node_match'
    assert 'cutoff' in far_drop.reason

    # A node_match stage tally must be recorded: 2 in, 1 matched out.
    stage = next(s for s in ledger.stages if s.stage == 'node_match')
    assert stage.count_in == 2
    assert stage.count_out == 1


def test_no_ledger_is_a_noop():
    """Matching must behave identically when no ledger is attached."""
    model = _fvcom_model(lon=[-71.00], lat=[41.00])
    ctl = [['41.000', '-71.000']]
    ids = [['A']]
    prop = SimpleNamespace(ofs='necofs')  # no station_ledger attribute
    out = index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)
    assert out == [0]


# ---------------------------------------------------------------------------
# 3. Many-to-one collision detection
# ---------------------------------------------------------------------------


def test_many_to_one_collision_is_flagged_and_both_retained():
    # One model node near the coast; two obs stations both resolve to it.
    model = _fvcom_model(lon=[-71.00, -68.00], lat=[41.00, 44.00])
    ctl = [
        ['41.000', '-71.010'],  # ~node 0
        ['41.005', '-71.000'],  # ~node 0
    ]
    ids = [['A'], ['B']]
    ledger = StationLedger(ofs='necofs')
    prop = SimpleNamespace(ofs='necofs', station_ledger=ledger)

    out = index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)

    # Both retained (many-to-one is not a drop).
    assert out == [0, 0]
    collisions = [s for s in ledger.stages if s.stage == 'node_match_collision']
    assert len(collisions) == 1
    assert 'A' in collisions[0].note and 'B' in collisions[0].note
    # No station was dropped by the collision.
    assert ledger.drops == []


def test_triple_collision_reported_as_single_group():
    # Three obs stations all resolve to node 0 -> one grouped warning/note,
    # not two pairwise ones, and all three IDs appear.
    model = _fvcom_model(lon=[-71.00, -68.00], lat=[41.00, 44.00])
    ctl = [
        ['41.000', '-71.010'],
        ['41.005', '-71.000'],
        ['40.998', '-71.005'],
    ]
    ids = [['A'], ['B'], ['C']]
    ledger = StationLedger(ofs='necofs')
    prop = SimpleNamespace(ofs='necofs', station_ledger=ledger)

    out = index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)

    assert out == [0, 0, 0]
    collisions = [s for s in ledger.stages if s.stage == 'node_match_collision']
    assert len(collisions) == 1, 'triple hit must produce ONE grouped note'
    note = collisions[0].note
    assert 'A' in note and 'B' in note and 'C' in note


def test_stofs_name_mismatch_records_ledger_drop():
    # STOFS matches by station-name substring, not distance. An obs ID that
    # no model station name contains must be dropped and recorded.
    station_names = np.array(['8531680_sta', '8510560_sta'], dtype=object)
    model = {'station_name': station_names}
    ctl = [['0.0', '0.0'], ['0.0', '0.0']]
    ids = [['8531680'], ['9999999']]  # second ID matches nothing
    ledger = StationLedger(ofs='stofs_2d_glo')
    prop = SimpleNamespace(ofs='stofs_2d_glo', station_ledger=ledger)

    out = index_nearest_station(prop, ctl, model, 'fvcom', 'wl', logger, ids)

    # First matched (index 0), second unmatched (NaN).
    assert out[0] == 0
    assert isinstance(out[1], float) and np.isnan(out[1])
    dropped = {d.station_id: d for d in ledger.drops}
    assert '9999999' in dropped
    assert dropped['9999999'].stage == 'node_match'
    assert 'STOFS' in dropped['9999999'].reason


# ---------------------------------------------------------------------------
# 4. Ledger accounting API
# ---------------------------------------------------------------------------


def test_ledger_summary_and_csv(tmp_path):
    ledger = StationLedger(
        ofs='necofs', variable='water_level', whichcast='hindcast', filetype='stations'
    )
    ledger.note_stage('obs_ctl', count_in=45, note='stations with retrievable obs data')
    ledger.note_stage('model_ctl', count_out=40)
    ledger.drop(
        '8531680',
        stage='node_match',
        reason='nearest model location 6.2 km away (> 4.0 km cutoff)',
    )
    ledger.drop(
        '8510560', stage='temporal_overlap', reason='no overlapping valid timestamps'
    )

    grouped = ledger.drops_by_stage()
    assert set(grouped) == {'node_match', 'temporal_overlap'}

    # log_summary must never raise.
    ledger.log_summary(logger)

    csv_path = tmp_path / 'ledger.csv'
    written = ledger.to_csv(str(csv_path))
    assert written == str(csv_path)
    assert csv_path.exists()
    text = csv_path.read_text()
    assert '8531680' in text and 'node_match' in text
    assert '8510560' in text and 'temporal_overlap' in text


def test_ledger_drop_is_best_effort_and_never_raises():
    ledger = StationLedger()
    # Passing odd types must not raise.
    ledger.drop(12345, stage='node_match', reason='numeric id')
    ledger.drop(None, stage='node_match', reason='none id')
    assert len(ledger.drops) == 2


def test_ledger_deepcopy_shares_the_run_scoped_sink():
    # ``prop`` is deep-copied per station, per forecast cycle, and per
    # variable in get_node_ofs. If the ledger were copied with it, every
    # drop a worker recorded would be thrown away when the worker returned
    # -- which is how node_match and depth_match records were being lost
    # under parallel_variables=True. The one instance must be shared.
    ledger = StationLedger(ofs='necofs', variable='water_level')
    ledger.note_stage('node_match', count_in=45, count_out=40)
    ledger.drop('8531680', stage='node_match', reason='far')

    dup = copy.deepcopy(ledger)
    assert dup is ledger
    dup.drop('8510560', stage='pairing', reason='x')
    assert len(ledger.drops) == 2, 'a worker copy must write through'

    # A ``prop``-shaped container deep-copies without forking the ledger,
    # and without copying the whole run's record list per station.
    prop = SimpleNamespace(ofs='necofs', station_ledger_root=ledger)
    copy.deepcopy(prop).station_ledger_root.drop(
        '8571421', stage='pairing', reason='y')
    assert len(ledger.drops) == 3

    # Pickling still produces an independent copy: that is the correct
    # behaviour across a process boundary, and it must not raise on the
    # threading.Lock field.
    restored = pickle.loads(pickle.dumps(ledger))
    assert restored is not ledger
    assert restored.ofs == 'necofs'
    assert [d.station_id for d in restored.drops] == [
        '8531680', '8510560', '8571421']
    restored.drop('8594900', stage='pairing', reason='z')  # fresh lock works
    assert len(ledger.drops) == 3


def test_ledger_csv_neutralizes_formula_injection(tmp_path):
    ledger = StationLedger(ofs='necofs')
    # A hostile station ID that would execute as a spreadsheet formula.
    ledger.drop('=cmd|calc', stage='node_match', reason='+SUM(A1)')
    csv_path = tmp_path / 'ledger.csv'
    ledger.to_csv(str(csv_path))
    text = csv_path.read_text()
    # The dangerous leading characters must be quote-prefixed.
    assert "'=cmd|calc" in text
    assert "'+SUM(A1)" in text


# ---------------------------------------------------------------------------
# 5. Module constants and config-driven cutoff
# ---------------------------------------------------------------------------


def test_cutoff_constant_is_the_effective_value():
    # The docstring historically claimed "2 km" while the code used 4 km.
    # Pin the default so code and documentation cannot silently diverge.
    assert STATION_MATCH_MAX_DIST_KM == 4.0


def test_config_cutoff_is_used_for_both_match_and_prefilter():
    # A caller-supplied max_dist_km must drive the match. A tighter cutoff
    # drops a station that a looser one keeps, using the SAME value for the
    # candidate box and the great-circle test.
    model = _fvcom_model(lon=[-71.00, -60.0], lat=[41.00, 50.0])
    # ~0.05 deg lat north of node 0 ~= 5.6 km.
    ctl = [['41.050', '-71.000']]
    ids = [['S']]
    prop = SimpleNamespace(ofs='necofs')

    tight = index_nearest_station(
        prop, ctl, model, 'fvcom', 'wl', logger, ids, max_dist_km=4.0
    )
    loose = index_nearest_station(
        prop, ctl, model, 'fvcom', 'wl', logger, ids, max_dist_km=10.0
    )

    assert isinstance(tight[0], float) and np.isnan(tight[0]), (
        '5.6 km station should be dropped at a 4 km cutoff'
    )
    assert loose[0] == 0, '5.6 km station should match at a 10 km cutoff'


def test_prefilter_box_is_latitude_aware_superset_of_cutoff():
    # The E-W half-width in degrees must grow with latitude so the box always
    # covers at least the km cutoff on the ground. Verify the box reaches the
    # cutoff distance E-W even at a high latitude where a fixed-degree box
    # would have fallen short.
    max_dist = 4.0
    for lat in (0.0, 45.0, 70.0, 85.0):
        lat_half, lon_half = indexing._prefilter_halfwidths_deg(lat, max_dist)
        # Convert the E-W half-width back to km at this latitude and confirm
        # it still covers the cutoff (with the safety factor).
        km_ew = lon_half * indexing._KM_PER_DEG_LAT * math.cos(math.radians(lat))
        assert km_ew >= max_dist, (
            f'box E-W reach {km_ew:.2f} km < cutoff {max_dist} km at {lat} N'
        )
        # Latitude half-width is latitude-independent and also covers cutoff.
        assert lat_half * indexing._KM_PER_DEG_LAT >= max_dist


def test_high_latitude_station_within_cutoff_still_matches():
    # Regression for the Arctic hole: at 70 N a due-E/W station ~3 km away
    # must still be shortlisted by the box and matched. A fixed 0.1 deg box
    # (~3.8 km E-W at 70 N) could have excluded it; the latitude-aware box
    # must not.
    # 0.08 deg lon at 70 N ~= 0.08 * 111.195 * cos(70) ~= 3.0 km.
    model = _fvcom_model(lon=[-150.00, -140.0], lat=[70.00, 60.0])
    ctl = [['70.000', '-150.08']]
    ids = [['ARCTIC']]
    prop = SimpleNamespace(ofs='ciofs')  # non-stofs, fvcom branch

    out = index_nearest_station(
        prop, ctl, model, 'fvcom', 'wl', logger, ids, max_dist_km=4.0
    )
    assert out[0] == 0, 'within-cutoff high-latitude station must match'


def test_ledger_has_stage_and_has_drops():
    # get_skill uses these to decide whether a pass may (re)write the
    # ledger CSV: a pass that never ran node matching and recorded no
    # drops must not clobber the authoritative CSV from the matching pass.
    ledger = StationLedger(ofs='cbofs', variable='water_level')
    assert not ledger.has_stage('node_match')
    assert not ledger.has_drops

    ledger.note_stage('obs_ctl', count_in=18)
    assert not ledger.has_stage('node_match')

    ledger.note_stage('node_match', count_in=18, count_out=15)
    assert ledger.has_stage('node_match')

    assert not ledger.has_drops
    ledger.drop('8551762', stage='node_match', reason='too far')
    assert ledger.has_drops


# ---------------------------------------------------------------------------
# 6. Combined run-scoped ledger and per-context views (issue #224)
# ---------------------------------------------------------------------------


def _read_ledger_rows(path):
    """Read a written ledger CSV back into a list of dicts."""
    with open(path, newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def test_one_ledger_holds_every_variable_and_cast(tmp_path):
    # The whole point of the combined ledger: two variables and two casts
    # land in a single CSV, told apart by their own columns.
    root = StationLedger(ofs='cbofs', run_start='2026-01-01T00:00:00Z',
                         run_end='2026-01-02T00:00:00Z')
    wl_now = root.for_context('water_level', 'nowcast', 'stations')
    cu_fcst = root.for_context('currents', 'forecast_b', 'stations')

    wl_now.drop('8637689', stage='pairing', reason='no paired series')
    cu_fcst.drop('cb0402_b03', stage='pairing', reason='no paired series')

    path = tmp_path / 'station_ledger_cbofs.csv'
    assert root.to_csv(str(path)) == str(path)

    rows = _read_ledger_rows(path)
    assert list(rows[0].keys()) == list(LEDGER_COLUMNS)
    by_id = {r['station_id']: r for r in rows if r['record_type'] == 'drop'}
    assert by_id['8637689']['variable'] == 'water_level'
    assert by_id['8637689']['whichcast'] == 'nowcast'
    assert by_id['cb0402_b03']['variable'] == 'currents'
    assert by_id['cb0402_b03']['whichcast'] == 'forecast_b'
    # The run window travels with every row so a reader can tell which run
    # a retained row came from.
    assert by_id['8637689']['run_start'] == '2026-01-01T00:00:00Z'
    assert by_id['8637689']['run_end'] == '2026-01-02T00:00:00Z'


def test_view_reporting_is_scoped_to_its_own_context():
    # ``has_stage``/``has_drops``/``drops_by_stage`` must not leak another
    # variable's records into this variable's summary.
    root = StationLedger(ofs='cbofs')
    wl = root.for_context('water_level', 'nowcast', 'stations')
    salt = root.for_context('salinity', 'nowcast', 'stations')

    wl.drop('8637689', stage='pairing', reason='no paired series')
    wl.note_stage('model_ctl', count_out=12)

    assert wl.has_drops and wl.has_stage('model_ctl')
    assert not salt.has_drops
    assert not salt.has_stage('model_ctl')
    assert set(wl.drops_by_stage()) == {'pairing'}
    assert salt.drops_by_stage() == {}
    # ...while the root still sees everything.
    assert root.has_drops


def test_variable_override_beats_the_attached_view_context():
    # Regression for the cross-variable mislabelling: the model ctl writer
    # loops over every variable in one call, so a currents bin drop can be
    # recorded while the attached view still says water_level. The record
    # must be filed under currents.
    root = StationLedger(ofs='cbofs')
    wl_view = root.for_context('water_level', 'nowcast', 'stations')
    wl_view.drop('cb0402_b05', stage='depth_match',
                 reason='bin below model bottom', variable='currents')

    cu_view = root.for_context('currents', 'nowcast', 'stations')
    assert [d.station_id for d in cu_view.drops] == ['cb0402_b05']
    assert wl_view.drops == []


def test_cast_independent_stages_are_stamped_all():
    # Control files are built once and reused by every cast, so stamping a
    # node_match drop with the triggering cast would be a lie. Cast-specific
    # stages must still carry the real cast.
    root = StationLedger(ofs='cbofs')
    view = root.for_context('water_level', 'nowcast', 'stations')
    view.drop('A', stage='node_match', reason='too far')
    view.drop('B', stage='pairing', reason='no paired series')

    stamped = {d.station_id: d.whichcast for d in root.drops}
    assert stamped['A'] == CAST_ALL
    assert stamped['B'] == 'nowcast'

    # A view for a *different* cast still sees the cast-independent record
    # but not the other cast's pairing drop.
    other = root.for_context('water_level', 'forecast_b', 'stations')
    assert [d.station_id for d in other.drops] == ['A']


def test_merge_keeps_other_contexts_and_replaces_own_stage(tmp_path):
    path = tmp_path / 'station_ledger_cbofs.csv'

    first = StationLedger(ofs='cbofs')
    first.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='first run')
    first.to_csv(str(path))

    # A later invocation for a different cast must not erase the first.
    second = StationLedger(ofs='cbofs')
    second.for_context('water_level', 'forecast_b', 'stations').drop(
        '8575512', stage='pairing', reason='second run')
    second.to_csv(str(path))

    rows = [r for r in _read_ledger_rows(path) if r['record_type'] == 'drop']
    assert {r['station_id'] for r in rows} == {'8637689', '8575512'}

    # Re-running the *same* context replaces its rows rather than
    # duplicating them.
    third = StationLedger(ofs='cbofs')
    third.for_context('water_level', 'nowcast', 'stations').drop(
        '8594900', stage='pairing', reason='third run')
    third.to_csv(str(path))

    rows = [r for r in _read_ledger_rows(path) if r['record_type'] == 'drop']
    ids = sorted(r['station_id'] for r in rows)
    assert ids == ['8575512', '8594900'], (
        'the nowcast pairing rows should be replaced, forecast_b retained'
    )


def test_merge_does_not_erase_stages_this_pass_never_ran(tmp_path):
    # A cached-control-file pass records inventory/obs_ctl but never runs
    # node matching; the earlier matching pass's node_match rows must
    # survive rather than being wiped.
    path = tmp_path / 'station_ledger_cbofs.csv'

    matching = StationLedger(ofs='cbofs')
    matching.for_context('water_level', 'nowcast', 'stations').drop(
        '8531680', stage='node_match', reason='6.2 km away')
    matching.to_csv(str(path))

    cached = StationLedger(ofs='cbofs')
    cached.for_context('water_level', 'nowcast', 'stations').note_stage(
        'obs_ctl', count_in=4, count_out=4)
    cached.to_csv(str(path))

    rows = _read_ledger_rows(path)
    stages = {(r['stage'], r['record_type']) for r in rows}
    assert ('node_match', 'drop') in stages
    assert ('obs_ctl', 'stage') in stages


def test_a_clean_rerun_supersedes_a_previous_runs_drop_only_stage(tmp_path):
    # The stages that only ever emit drop rows (pairing, temporal_overlap,
    # id_mismatch, ...) produce nothing when they run cleanly. Without an
    # explicit declaration the merge would find no key to supersede and the
    # January drop would still be presented as the June run's explanation.
    path = tmp_path / 'station_ledger_cbofs.csv'

    january = StationLedger(ofs='cbofs', run_start='2026-01-01T00:00:00Z')
    january.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='no valid paired series')
    january.to_csv(str(path))

    june = StationLedger(ofs='cbofs', run_start='2026-06-01T00:00:00Z')
    view = june.for_context('water_level', 'nowcast', 'stations')
    view.note_stage('skill_csv', count_out=4)
    for stage in PAIRING_STAGES:
        view.mark_stage_run(stage)
    june.to_csv(str(path))

    rows = [r for r in _read_ledger_rows(path) if r['record_type'] == 'drop']
    assert rows == [], (
        'a pass that ran the pairing stages cleanly must clear the earlier '
        'run\'s pairing drops'
    )


def test_declaring_a_stage_does_not_touch_another_context(tmp_path):
    # Superseding is scoped: declaring pairing for water_level/nowcast must
    # not clear the currents rows or the other cast's rows.
    path = tmp_path / 'station_ledger_cbofs.csv'

    first = StationLedger(ofs='cbofs')
    first.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='wl nowcast')
    first.for_context('currents', 'nowcast', 'stations').drop(
        'cb0402_b03', stage='pairing', reason='cu nowcast')
    first.for_context('water_level', 'forecast_b', 'stations').drop(
        '8575512', stage='pairing', reason='wl forecast_b')
    first.to_csv(str(path))

    second = StationLedger(ofs='cbofs')
    view = second.for_context('water_level', 'nowcast', 'stations')
    for stage in PAIRING_STAGES:
        view.mark_stage_run(stage)
    second.to_csv(str(path))

    rows = [r for r in _read_ledger_rows(path) if r['record_type'] == 'drop']
    assert sorted(r['station_id'] for r in rows) == ['8575512', 'cb0402_b03']


def test_drop_only_stages_are_all_declared_by_a_call_site():
    # A guard on the constant itself: every drop-only stage must have an
    # owner that declares it, otherwise its rows would be retained forever.
    assert set(PAIRING_STAGES) <= DROP_ONLY_STAGES
    # The remaining two are declared by reconcile_inventory and by the
    # model ctl writer's ADCP bin pruning.
    assert DROP_ONLY_STAGES - set(PAIRING_STAGES) == {
        'inventory_variable_flag', 'depth_match'}


def test_marking_a_cast_independent_stage_is_stamped_all(tmp_path):
    # depth_match is declared by the model ctl writer under whichever cast
    # happens to be running; the declaration must land on the 'all' stamp
    # its rows carry, or it would supersede nothing.
    path = tmp_path / 'station_ledger_cbofs.csv'

    first = StationLedger(ofs='cbofs')
    first.for_context('currents', 'nowcast', 'stations').drop(
        'cb0402_b09', stage='depth_match', reason='below the model bottom')
    first.to_csv(str(path))

    second = StationLedger(ofs='cbofs')
    second.for_context('currents', 'forecast_b', 'stations').mark_stage_run(
        'depth_match')
    second.to_csv(str(path))

    rows = [r for r in _read_ledger_rows(path) if r['record_type'] == 'drop']
    assert rows == []


def test_csv_uses_lf_line_endings(tmp_path):
    # The rest of the pipeline's CSV artifacts are LF; the ledger must not
    # be the odd one out just because it is written with csv.DictWriter.
    path = tmp_path / 'station_ledger_cbofs.csv'
    ledger = StationLedger(ofs='cbofs')
    ledger.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='x')
    ledger.to_csv(str(path))

    raw = path.read_bytes()
    assert b'\r\n' not in raw
    assert raw.count(b'\n') == 2  # header + one drop row


def test_write_leaves_no_temp_files_behind(tmp_path):
    # The write goes through a same-directory temp file and a rename; the
    # temp file must never survive the call.
    path = tmp_path / 'station_ledger_cbofs.csv'
    ledger = StationLedger(ofs='cbofs')
    ledger.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='x')
    ledger.to_csv(str(path))
    ledger.to_csv(str(path))

    assert sorted(f.name for f in tmp_path.iterdir()) == [
        'station_ledger_cbofs.csv']


def test_log_summary_header_count_matches_its_listing(caplog):
    # The WARNING header used to count only the unexpected drops while the
    # listing underneath it accounted for every drop, so the operator log
    # contradicted itself on any run with inventory reconciliation.
    ledger = StationLedger(ofs='cbofs')
    view = ledger.for_context('currents', 'nowcast', 'stations')
    for i in range(20):
        view.drop(f'S{i:02d}', stage='inventory_variable_flag',
                  reason='inventory has_cu flag is False')
    view.drop('8637689', stage='node_match', reason='6.2 km away')
    view.drop('8575512', stage='pairing', reason='no valid paired series')

    with caplog.at_level(logging.INFO, logger='ledger_header_test'):
        view.log_summary(logging.getLogger('ledger_header_test'))

    header = next(
        r.getMessage() for r in caplog.records
        if 'before the skill table' in r.getMessage()
    )
    assert '22 station drop record(s)' in header
    assert '2 unexpected' in header
    assert '20 expected' in header


def test_log_summary_all_expected_drops_stay_at_info(caplog):
    ledger = StationLedger(ofs='cbofs')
    view = ledger.for_context('currents', 'nowcast', 'stations')
    view.drop('S01', stage='inventory_variable_flag', reason='no ADCP')

    with caplog.at_level(logging.INFO, logger='ledger_header_info_test'):
        view.log_summary(logging.getLogger('ledger_header_info_test'))

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert 'all at expected stages' in caplog.text


def test_merge_tolerates_a_corrupt_existing_file(tmp_path):
    # A hand-edited or foreign CSV must degrade to a plain overwrite, never
    # raise -- the ledger's never-raises contract is load-bearing.
    path = tmp_path / 'station_ledger_cbofs.csv'
    path.write_text('not,a,ledger\n1,2,3\n', encoding='utf-8')

    ledger = StationLedger(ofs='cbofs')
    ledger.for_context('water_level', 'nowcast', 'stations').drop(
        '8637689', stage='pairing', reason='x')
    assert ledger.to_csv(str(path)) == str(path)

    rows = _read_ledger_rows(path)
    assert [r['station_id'] for r in rows] == ['8637689']


def test_stage_rows_carry_their_counts(tmp_path):
    ledger = StationLedger(ofs='cbofs')
    ledger.for_context('water_level', 'nowcast', 'stations').note_stage(
        'node_match', count_in=18, count_out=15, note='cutoff 4.0 km')
    path = tmp_path / 'station_ledger_cbofs.csv'
    ledger.to_csv(str(path))

    row = _read_ledger_rows(path)[0]
    assert row['record_type'] == 'stage'
    assert row['count_in'] == '18' and row['count_out'] == '15'
    assert row['note'] == 'cutoff 4.0 km'


def test_view_deepcopy_keeps_the_shared_root():
    root = StationLedger(ofs='cbofs')
    view = root.for_context('water_level', 'nowcast', 'stations')
    view.drop('8637689', stage='pairing', reason='x')

    dup = copy.deepcopy(view)
    assert dup.root is root
    dup.drop('8575512', stage='pairing', reason='y')
    assert [d.station_id for d in root.drops] == ['8637689', '8575512']
    assert len(view.drops) == 2, 'the copy must write through to the root'

    restored = pickle.loads(pickle.dumps(view))
    assert [d.station_id for d in restored.drops] == ['8637689', '8575512']


def test_log_summary_truncates_long_id_lists_and_downgrades_expected(caplog):
    # Inventory tracking can drop hundreds of stations for a variable they
    # simply do not measure. That must not become a wall of WARNINGs.
    ledger = StationLedger(ofs='cbofs')
    view = ledger.for_context('currents', 'nowcast', 'stations')
    for i in range(300):
        view.drop(f'S{i:03d}', stage='inventory_variable_flag',
                  reason='inventory has_cu flag is False')
    view.drop('8637689', stage='node_match', reason='6.2 km away')

    with caplog.at_level(logging.INFO, logger='ledger_summary_test'):
        view.log_summary(logging.getLogger('ledger_summary_test'))

    text = caplog.text
    assert 'and 285 more' in text, 'long ID lists must be truncated'
    warned = ' '.join(
        r.getMessage() for r in caplog.records
        if r.levelno >= logging.WARNING
    )
    assert 'inventory_variable_flag' not in warned
    assert 'node_match' in warned


# ---------------------------------------------------------------------------
# 7. Inventory -> observation control file reconciliation (issue #224)
# ---------------------------------------------------------------------------

INVENTORY_FIXTURE = (
    Path(__file__).resolve().parent / 'fixtures' / 'pipeline'
    / 'inventory_all_cbofs.csv'
)


def _reconciled(tmp_path, variable, ctl_ids, inventory_text=None):
    """Run the reconciliation against a fixture inventory and return a view."""
    inv = tmp_path / 'inventory_all_cbofs.csv'
    inv.write_text(
        INVENTORY_FIXTURE.read_text(encoding='utf-8')
        if inventory_text is None else inventory_text,
        encoding='utf-8',
    )
    root = StationLedger(ofs='cbofs')
    view = root.for_context(variable, 'nowcast', 'stations')
    reconcile_inventory(view, str(inv), ctl_ids, variable, logger)
    return view


def test_inventory_reconcile_flags_stations_absent_from_the_ctl(tmp_path):
    # The fixture inventory holds four CO-OPS water-level stations; the
    # matching ctl fixture only carries two of them.
    view = _reconciled(tmp_path, 'water_level', ['8637689', '8575512'])

    dropped = {d.station_id: d for d in view.drops}
    assert set(dropped) == {'8594900', '8574680'}
    assert all(d.stage == 'obs_ctl' for d in dropped.values())
    assert 'no water_level observations retrievable' in dropped['8594900'].reason

    stages = {s.stage: s for s in view.stages}
    assert stages['inventory'].count_in == 4
    assert stages['inventory'].count_out == 4
    assert stages['obs_ctl'].count_in == 4
    assert stages['obs_ctl'].count_out == 2


def test_inventory_reconcile_separates_the_variable_flag_cause(tmp_path):
    # Every CO-OPS row in the fixture has has_cu=False: those stations were
    # never queried for currents at all, which is a different story from
    # "observations were unavailable" and must be recorded as such.
    view = _reconciled(tmp_path, 'currents', [])

    stages = {s.stage: s for s in view.stages}
    assert stages['inventory'].count_in == 4
    assert stages['inventory'].count_out == 0
    by_stage = view.drops_by_stage()
    assert set(by_stage) == {'inventory_variable_flag'}
    assert len(by_stage['inventory_variable_flag']) == 4
    assert 'has_cu flag is False' in by_stage['inventory_variable_flag'][0].reason
    # Nothing was flagged, so nothing can be blamed on the obs ctl file.
    assert stages['obs_ctl'].count_in == 0


def test_inventory_reconcile_credits_virtual_adcp_bins_to_the_parent(tmp_path):
    inventory = (
        ',Source,ID,X,Y,Name,has_wl,has_temp,has_salt,has_cu\n'
        '1,CO-OPS,cb0402,-76.0,38.0,Bin station,False,False,False,True\n'
    )
    view = _reconciled(tmp_path, 'currents', ['cb0402_b01', 'cb0402_b02'],
                       inventory_text=inventory)
    assert view.drops == [], 'a parent covered by its bins must not be dropped'
    assert {s.stage: s.count_out for s in view.stages}['obs_ctl'] == 1


def test_inventory_reconcile_matches_station_ids_case_insensitively(tmp_path):
    inventory = (
        ',Source,ID,X,Y,Name,has_wl,has_temp,has_salt,has_cu\n'
        '1,NDBC,SLIM2,-76.0,38.0,Solomons,True,True,True,False\n'
    )
    view = _reconciled(tmp_path, 'water_level', ['slim2'],
                       inventory_text=inventory)
    assert view.drops == []


def test_inventory_reconcile_blank_ctl_drops_every_flagged_station(tmp_path):
    view = _reconciled(tmp_path, 'water_level', [])
    assert len(view.drops) == 4
    assert {d.stage for d in view.drops} == {'obs_ctl'}


def test_inventory_reconcile_missing_file_is_a_silent_no_op(tmp_path):
    root = StationLedger(ofs='cbofs')
    view = root.for_context('water_level', 'nowcast', 'stations')
    reconcile_inventory(
        view, str(tmp_path / 'nope.csv'), ['8637689'], 'water_level', logger)
    assert view.drops == [] and view.stages == []
    # A ``None`` ledger must be tolerated too.
    reconcile_inventory(None, str(tmp_path / 'nope.csv'), [], 'water_level',
                        logger)


def test_inventory_reconcile_missing_flag_column_treats_rows_as_flagged(tmp_path):
    # An inventory without the has_<var> column (older or hand-made) must
    # still reconcile rather than reporting every station as dropped.
    inventory = (
        ',Source,ID,X,Y,Name\n'
        '1,CO-OPS,8637689,-76.0,37.2,Yorktown\n'
        '2,CO-OPS,8575512,-76.5,38.9,Annapolis\n'
    )
    view = _reconciled(tmp_path, 'water_level', ['8637689'],
                       inventory_text=inventory)
    assert {d.station_id for d in view.drops} == {'8575512'}
    assert {d.stage for d in view.drops} == {'obs_ctl'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
