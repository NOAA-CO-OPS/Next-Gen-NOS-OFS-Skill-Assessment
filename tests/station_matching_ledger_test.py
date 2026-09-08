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

No network or model downloads are required; everything runs on tiny synthetic
arrays.
"""

from __future__ import annotations

import copy
import logging
import math
import pickle
from types import SimpleNamespace

import numpy as np
import pytest

from ofs_skill.model_processing import indexing
from ofs_skill.model_processing.indexing import (
    STATION_MATCH_MAX_DIST_KM,
    index_nearest_station,
)
from ofs_skill.model_processing.station_ledger import StationLedger

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


def test_ledger_is_deepcopy_and_pickle_safe():
    ledger = StationLedger(ofs='necofs', variable='water_level')
    ledger.note_stage('node_match', count_in=45, count_out=40)
    ledger.drop('8531680', stage='node_match', reason='far')

    # deepcopy must not raise on the threading.Lock field, and the copy
    # must carry the recorded state with its own working lock.
    dup = copy.deepcopy(ledger)
    assert [d.station_id for d in dup.drops] == ['8531680']
    dup.drop('8510560', stage='pairing', reason='x')  # exercises fresh lock
    assert len(dup.drops) == 2
    assert len(ledger.drops) == 1  # original untouched

    # Full pickle round-trip (process-boundary scenario).
    restored = pickle.loads(pickle.dumps(ledger))
    assert restored.ofs == 'necofs'
    assert [d.station_id for d in restored.drops] == ['8531680']


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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
