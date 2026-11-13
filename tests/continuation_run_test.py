"""
Tests for continuation runs (issue #211).

A continuation run passes the full assessment window plus
``--Continue_Run`` and extends the artifacts it already has instead of
regenerating them: an existing ``.obs``/``.prd`` that starts at the
window start but stops short of the end gets only its missing tail
fetched or extracted, merged in at the seam. Everything downstream
(``.int``, skill CSVs, plots) is recomputed over the full series.

The safety property under test throughout is that continuation is only
ever an optimization: every case it cannot handle confidently -- a file
that starts too late, duplicate timestamps, a hole at the seam, an empty
tail -- must fall back to the pre-existing full-regeneration path rather
than write a spliced series.

Covers:
- the three-state classifier ``classify_coverage`` and the
  ``covers_run_window`` wrapper built on it,
- ``continuation_start`` (where a tail fetch resumes),
- the text-level merge in ``ofs_skill.utils.series_continuation``
  (dedup, ordering, seam gap, atomic write, blank-file contract),
- the reuse gates ``_ensure_obs_files`` / ``_ensure_prd_files``,
- ``.int`` invalidation in ``_ensure_paired_data_exists``.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import stat
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ofs_skill.utils import cache_manifest
from ofs_skill.utils.series_continuation import (
    max_step_seconds,
    merge_and_write,
    merge_series_lines,
    read_series_file,
    write_series_file,
)
from ofs_skill.utils.timeseries_coverage import (
    COVERS,
    PREFIX,
    STALE,
    classify_coverage,
    continuation_start,
    covers_run_window,
)

WINDOW_START = datetime(2026, 2, 15, 0, 0)
WINDOW_END = datetime(2026, 4, 1, 0, 0)
# Fixed "now" so no test reads the wall clock.
NOW = datetime(2026, 4, 2, 0, 0)

SERIES_HEADER = 'Julian days, Year, Month, Day, Hours, Minutes, Water level (m)\n'

# The package re-exports get_skill the function, shadowing get_skill the
# module, so the module has to be fetched by name.
get_skill_mod = importlib.import_module('ofs_skill.skill_assessment.get_skill')

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_1DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_1dplot.py'


@pytest.fixture(scope='module', name='create_1dplot_mod')
def fixture_create_1dplot_mod():
    """Import bin/visualization/create_1dplot.py as a module.

    bin/ is not a package, and the sys.modules key is unique to this test
    module so loading the same script from another test file does not
    collide with this one.
    """
    spec = importlib.util.spec_from_file_location(
        'create_1dplot_continuation_under_test', CREATE_1DPLOT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['create_1dplot_continuation_under_test'] = mod
    spec.loader.exec_module(mod)
    return mod


def _row(stamp, value=1.2345):
    """One fixed-width scalar series row, as the writers emit them."""
    julian = 2460000.0 + stamp.timestamp() / 86400.0
    return (f'{julian:13.8f} {stamp.year:4d} {stamp.month:2d} '
            f'{stamp.day:2d} {stamp.hour:2d} {stamp.minute:2d} '
            f'{value:9.4f}')


def _rows(start, count, step=timedelta(hours=1), value=1.2345):
    """A run of ``count`` rows at ``step`` spacing."""
    return [_row(start + i * step, value) for i in range(count)]


def _write_series(path, start, count, step=timedelta(hours=1), header=True,
                  value=1.2345):
    """Write a ``.obs``-shaped file covering ``count`` steps from start."""
    lines = _rows(start, count, step, value)
    path.write_text(
        (SERIES_HEADER if header else '') + '\n'.join(lines) + '\n',
        encoding='utf-8')
    return lines


def _logger():
    """Plain logger for helpers that take one."""
    return logging.getLogger('continuation_test')


def _backdate(path, hours=48):
    """Age a file so created_this_run() sees it as a previous run's."""
    old = os.path.getmtime(path) - hours * 3600
    os.utime(path, (old, old))


# --------------------------------------------------------------------
# classify_coverage
# --------------------------------------------------------------------

class TestClassifyCoverage:
    """The three-state coverage verdict."""

    def test_full_window_covers(self, tmp_path):
        """A file spanning the whole window is COVERS."""
        path = tmp_path / 'full.obs'
        _write_series(path, WINDOW_START, 45 * 24 + 1)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == COVERS

    def test_short_tail_is_prefix(self, tmp_path):
        """A file starting on time but ending early is PREFIX."""
        path = tmp_path / 'prefix.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == PREFIX

    def test_late_start_is_stale(self, tmp_path):
        """A file whose head is missing cannot be extended."""
        path = tmp_path / 'late.obs'
        _write_series(path, WINDOW_START + timedelta(days=10), 40 * 24)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == STALE

    def test_disjoint_earlier_window_is_stale(self, tmp_path):
        """A file from a wholly earlier window is STALE, not PREFIX.

        It starts early enough, but it ends before the window even
        begins -- #202's disjoint case. There is no prefix to build on,
        so extending it would splice two unrelated spans together.
        """
        path = tmp_path / 'disjoint.obs'
        _write_series(path, WINDOW_START - timedelta(days=30), 24)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == STALE

    def test_overlapping_earlier_window_is_prefix(self, tmp_path):
        """A file that starts early but reaches into the window extends.

        The rows before the window start are harmless -- pairing crops
        to the window -- and the overlap proves the file is the head of
        the same series.
        """
        path = tmp_path / 'early.obs'
        _write_series(path, WINDOW_START - timedelta(days=2), 10 * 24)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == PREFIX

    def test_late_head_within_tolerance_is_stale_not_prefix(self, tmp_path):
        """A head that starts late may be reused, but never extended.

        Starting inside the tolerance is close enough to accept the file
        as-is (that is what COVERS means), but appending a tail to it
        would bake the missing head into the result.
        """
        path = tmp_path / 'latehead.obs'
        _write_series(path, WINDOW_START + timedelta(hours=6), 15 * 24)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == STALE

    def test_tolerance_edge_still_covers(self, tmp_path):
        """Ending within the tolerance of the window end is COVERS."""
        path = tmp_path / 'edge.obs'
        _write_series(path, WINDOW_START, 45 * 24 - 6)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == COVERS

    def test_future_window_clamped_to_now(self, tmp_path):
        """A forecast window reaching past now never marks a file short.

        The file stops at *now* because that is all the data that can
        exist; the window running 30 days further into the future must
        not make it look like a prefix in need of extending.
        """
        path = tmp_path / 'fcst.obs'
        _write_series(path, WINDOW_START, 46 * 24 + 1)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END + timedelta(days=30),
            now=NOW) == COVERS

    def test_undecodable_file_is_stale(self, tmp_path):
        """A file that opens but holds no parseable rows is STALE.

        Issue #267: an artifact with zero usable data was reused
        forever, silently producing no pairs. It carries nothing to
        continue from either, so it is regenerated rather than extended.
        """
        path = tmp_path / 'binary.obs'
        path.write_bytes(b'\xff\xfe\x00\x01')
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == STALE

    def test_empty_file_is_stale(self, tmp_path):
        """A 0-byte artifact carries no window and must not be extended."""
        path = tmp_path / 'blank.obs'
        path.write_text('', encoding='utf-8')
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == STALE

    def test_unopenable_file_fails_open(self, tmp_path):
        """A path that cannot be opened at all stays COVERS.

        Regeneration would hit the same OS-level failure, so the file
        keeps flowing through the pre-existing error handling instead of
        being deleted and re-fetched on every run.
        """
        path = tmp_path / 'missing_dir' / 'gone.obs'
        assert classify_coverage(
            path, WINDOW_START, WINDOW_END, now=NOW) == COVERS

    def test_short_window_fails_open(self, tmp_path):
        """Windows shorter than the tolerance cannot be judged."""
        path = tmp_path / 'tiny.obs'
        _write_series(path, WINDOW_START, 2)
        assert classify_coverage(
            path, WINDOW_START, WINDOW_START + timedelta(hours=6),
            now=NOW) == COVERS

    def test_covers_run_window_matches_classifier(self, tmp_path):
        """The boolean wrapper is exactly the COVERS case."""
        for name, start, count in (
                ('a', WINDOW_START, 45 * 24 + 1),
                ('b', WINDOW_START, 15 * 24),
                ('c', WINDOW_START + timedelta(days=10), 40 * 24)):
            path = tmp_path / f'{name}.obs'
            _write_series(path, start, count)
            verdict = classify_coverage(
                path, WINDOW_START, WINDOW_END, now=NOW)
            assert covers_run_window(
                path, WINDOW_START, WINDOW_END, now=NOW) == (verdict == COVERS)


class TestContinuationStart:
    """Where a tail fetch resumes."""

    def test_resumes_before_last_row(self, tmp_path):
        """The tail starts one overlap before the last row on disk."""
        path = tmp_path / 'prefix.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        last = WINDOW_START + timedelta(hours=15 * 24 - 1)
        assert continuation_start(
            path, WINDOW_START, WINDOW_END, timedelta(hours=24),
            now=NOW) == last - timedelta(hours=24)

    def test_clamped_to_window_start(self, tmp_path):
        """A huge overlap cannot back the tail up past the window start."""
        path = tmp_path / 'prefix.obs'
        _write_series(path, WINDOW_START, 24)
        assert continuation_start(
            path, WINDOW_START, WINDOW_END, timedelta(days=365),
            now=NOW) == WINDOW_START

    def test_none_when_not_prefix(self, tmp_path):
        """A COVERS file has no tail to fetch."""
        path = tmp_path / 'full.obs'
        _write_series(path, WINDOW_START, 45 * 24 + 1)
        assert continuation_start(
            path, WINDOW_START, WINDOW_END, timedelta(hours=24),
            now=NOW) is None

    def test_none_when_stale(self, tmp_path):
        """A STALE file must be regenerated, not extended."""
        path = tmp_path / 'late.obs'
        _write_series(path, WINDOW_START + timedelta(days=10), 40 * 24)
        assert continuation_start(
            path, WINDOW_START, WINDOW_END, timedelta(hours=24),
            now=NOW) is None


# --------------------------------------------------------------------
# merge
# --------------------------------------------------------------------

class TestMergeSeriesLines:
    """The text-level append/dedup."""

    def test_appends_disjoint_tail(self):
        """Non-overlapping tail rows are appended in order."""
        head = _rows(WINDOW_START, 24)
        tail = _rows(WINDOW_START + timedelta(hours=24), 24)
        merged, stats = merge_series_lines(head, tail)
        assert merged == head + tail
        assert (stats['kept'], stats['replaced'], stats['added']) == (24, 0, 24)

    def test_overlap_is_replaced_by_new_rows(self):
        """A re-fetched timestamp keeps the newly produced value."""
        head = _rows(WINDOW_START, 24, value=1.0)
        tail = _rows(WINDOW_START + timedelta(hours=12), 24, value=2.0)
        merged, stats = merge_series_lines(head, tail)
        assert len(merged) == 36
        assert stats['replaced'] == 12
        assert merged[12].endswith(f'{2.0:9.4f}')
        assert merged[11].endswith(f'{1.0:9.4f}')

    def test_output_is_sorted(self):
        """Unsorted provider output is ordered by timestamp."""
        head = list(reversed(_rows(WINDOW_START, 6)))
        merged, _ = merge_series_lines(head, [])
        assert merged == _rows(WINDOW_START, 6)

    def test_duplicate_existing_timestamps_refuse(self):
        """A file with repeated timestamps cannot be merged safely."""
        head = _rows(WINDOW_START, 3) + _rows(WINDOW_START, 1)
        assert merge_series_lines(head, _rows(
            WINDOW_START + timedelta(hours=3), 3)) is None

    def test_duplicate_new_timestamps_refuse(self):
        """The same guard applies to the freshly produced tail."""
        tail = _rows(WINDOW_START + timedelta(hours=3), 3) * 2
        assert merge_series_lines(_rows(WINDOW_START, 3), tail) is None

    def test_nan_rows_round_trip_unchanged(self):
        """Missing-data rows are carried across verbatim."""
        nan_row = _row(WINDOW_START, float('nan'))
        merged, _ = merge_series_lines([nan_row], [])
        assert merged == [nan_row]
        assert merged[0].endswith('      nan')

    def test_blank_and_garbage_lines_are_counted_not_kept(self):
        """Unparseable rows are dropped and reported, never emitted."""
        head = _rows(WINDOW_START, 3) + ['', 'not a data row at all']
        merged, stats = merge_series_lines(head, [])
        assert merged == _rows(WINDOW_START, 3)
        assert stats['unparseable'] == 1


class TestMaxStepSeconds:
    """Seam gap detection."""

    def test_regular_series_reports_step(self):
        """A gapless hourly series reports one hour."""
        assert max_step_seconds(_rows(WINDOW_START, 10)) == 3600.0

    def test_gap_is_detected_near_seam(self):
        """A hole where head meets tail is visible in the seam window."""
        seam = WINDOW_START + timedelta(hours=10)
        lines = (_rows(WINDOW_START, 10)
                 + _rows(seam + timedelta(hours=5), 10))
        assert max_step_seconds(
            lines, around=seam, window=timedelta(hours=12)) == 6 * 3600.0

    def test_gap_far_from_seam_is_ignored(self):
        """Mid-series gaps predate the continuation and are not its fault."""
        seam = WINDOW_START + timedelta(days=10)
        lines = (_rows(WINDOW_START, 5)
                 + _rows(WINDOW_START + timedelta(days=2), 5))
        assert max_step_seconds(
            lines, around=seam, window=timedelta(hours=12)) == 0.0

    def test_single_row_has_no_step(self):
        """Fewer than two rows in range means no measurable gap."""
        assert max_step_seconds(_rows(WINDOW_START, 1)) == 0.0


class TestWriteSeriesFile:
    """The atomic writer and the blank-file contract."""

    def test_round_trips_header_and_rows(self, tmp_path):
        """A written file reads back with the same header and rows."""
        path = tmp_path / 'out.obs'
        lines = _rows(WINDOW_START, 5)
        write_series_file(path, SERIES_HEADER, lines)
        assert read_series_file(path) == (SERIES_HEADER, lines)

    def test_empty_lines_write_zero_bytes(self, tmp_path):
        """No data means a 0-byte file, header included."""
        path = tmp_path / 'blank.obs'
        write_series_file(path, SERIES_HEADER, [])
        assert path.stat().st_size == 0

    def test_leaves_no_temp_files_behind(self, tmp_path):
        """The temp file is renamed into place, not left in the dir."""
        path = tmp_path / 'out.obs'
        write_series_file(path, SERIES_HEADER, _rows(WINDOW_START, 3))
        assert [p.name for p in tmp_path.iterdir()] == ['out.obs']

    def test_preserves_file_permissions(self, tmp_path):
        """The rewrite must not narrow the artifact's mode.

        NamedTemporaryFile creates 0600 regardless of umask and
        os.replace carries that to the destination, so without an
        explicit chmod an extended file becomes owner-only. Asserted as
        "unchanged" rather than against a literal, because Windows only
        models the read-only bit and reports 0666 either way.
        """
        path = tmp_path / 'perm.obs'
        _write_series(path, WINDOW_START, 4)
        os.chmod(path, 0o644)
        before = stat.S_IMODE(os.stat(path).st_mode)
        write_series_file(path, SERIES_HEADER, _rows(WINDOW_START, 6))
        assert stat.S_IMODE(os.stat(path).st_mode) == before

    def test_headerless_file_stays_headerless(self, tmp_path):
        """Legacy files without a header round-trip unchanged."""
        path = tmp_path / 'legacy.obs'
        lines = _write_series(path, WINDOW_START, 4, header=False)
        header, data = read_series_file(path)
        assert header is None
        assert data == lines


class TestMergeAndWrite:
    """The end-to-end merge used by the .obs/.prd write sites."""

    def test_extends_file_in_place(self, tmp_path):
        """A prefix file gains its tail and keeps its head."""
        path = tmp_path / 'series.obs'
        head = _write_series(path, WINDOW_START, 24)
        tail = _rows(WINDOW_START + timedelta(hours=20), 12)
        assert merge_and_write(path, tail, SERIES_HEADER, _logger()) is True
        _, data = read_series_file(path)
        assert data == head[:20] + tail

    def test_result_matches_a_fresh_full_window_write(self, tmp_path):
        """Extending equals writing the whole window from scratch.

        This is the acceptance criterion in #211: with stable sources,
        head-plus-tail must be byte-identical to a from-scratch run.
        """
        extended = tmp_path / 'extended.obs'
        fresh = tmp_path / 'fresh.obs'
        _write_series(extended, WINDOW_START, 24)
        merge_and_write(
            extended, _rows(WINDOW_START + timedelta(hours=20), 28),
            SERIES_HEADER, _logger())
        write_series_file(fresh, SERIES_HEADER, _rows(WINDOW_START, 48))
        assert extended.read_bytes() == fresh.read_bytes()

    def test_empty_tail_leaves_file_untouched(self, tmp_path):
        """A tail fetch that found nothing must never truncate a good file."""
        path = tmp_path / 'series.obs'
        _write_series(path, WINDOW_START, 24)
        before = path.read_bytes()
        assert merge_and_write(path, [], SERIES_HEADER, _logger()) is False
        assert path.read_bytes() == before

    def test_seam_gap_refuses_merge(self, tmp_path):
        """A hole at the seam falls back to regeneration."""
        path = tmp_path / 'series.obs'
        _write_series(path, WINDOW_START, 10)
        before = path.read_bytes()
        tail = _rows(WINDOW_START + timedelta(hours=15), 10)
        assert merge_and_write(
            path, tail, SERIES_HEADER, _logger(),
            max_seam_gap_seconds=2 * 3600,
            seam_window=timedelta(hours=12)) is False
        assert path.read_bytes() == before

    def test_seam_within_limit_merges(self, tmp_path):
        """A seam step no wider than the model interval is accepted."""
        path = tmp_path / 'series.obs'
        _write_series(path, WINDOW_START, 10)
        tail = _rows(WINDOW_START + timedelta(hours=10), 10)
        assert merge_and_write(
            path, tail, SERIES_HEADER, _logger(),
            max_seam_gap_seconds=2 * 3600,
            seam_window=timedelta(hours=12)) is True

    def test_duplicate_rows_refuse_merge(self, tmp_path):
        """An existing file with repeated timestamps is left alone."""
        path = tmp_path / 'dupes.obs'
        rows = _rows(WINDOW_START, 3) + _rows(WINDOW_START, 1)
        path.write_text(
            SERIES_HEADER + '\n'.join(rows) + '\n', encoding='utf-8')
        before = path.read_bytes()
        assert merge_and_write(
            path, _rows(WINDOW_START + timedelta(hours=3), 3),
            SERIES_HEADER, _logger()) is False
        assert path.read_bytes() == before

    def test_zero_byte_file_refuses_merge(self, tmp_path):
        """A blank artifact must stay blank, not become header-plus-tail.

        0 bytes records "this station had no data", and the getsize()
        checks throughout the pipeline depend on it staying that way.
        """
        path = tmp_path / 'blank.obs'
        path.write_text('', encoding='utf-8')
        assert merge_and_write(
            path, _rows(WINDOW_START, 3), SERIES_HEADER, _logger()) is False
        assert path.stat().st_size == 0

    def test_seam_check_looks_at_the_join_not_the_overlap(self, tmp_path):
        """The gap check must be anchored on the last row already on disk.

        The tail deliberately starts an overlap before the join, so
        anchoring on the tail's first row would put the check window on
        rows that are merely being replaced and never look at the join.
        """
        path = tmp_path / 'series.obs'
        _write_series(path, WINDOW_START, 24)
        # Tail overlaps the last 6 h of the file, then jumps a day.
        tail = (_rows(WINDOW_START + timedelta(hours=17), 6)
                + _rows(WINDOW_START + timedelta(hours=47), 6))
        before = path.read_bytes()
        assert merge_and_write(
            path, tail, SERIES_HEADER, _logger(),
            max_seam_gap_seconds=6 * 3600,
            seam_window=timedelta(hours=12)) is False
        assert path.read_bytes() == before

    def test_missing_file_refuses_merge(self, tmp_path):
        """No file to extend means the caller writes a fresh one."""
        assert merge_and_write(
            tmp_path / 'absent.obs', _rows(WINDOW_START, 3),
            SERIES_HEADER, _logger()) is False

    def test_legacy_headerless_file_gains_header(self, tmp_path):
        """Extending a pre-header file brings it up to the current format."""
        path = tmp_path / 'legacy.obs'
        _write_series(path, WINDOW_START, 6, header=False)
        assert merge_and_write(
            path, _rows(WINDOW_START + timedelta(hours=6), 6),
            SERIES_HEADER, _logger()) is True
        header, data = read_series_file(path)
        assert header == SERIES_HEADER
        assert len(data) == 12


# --------------------------------------------------------------------
# reuse gates
# --------------------------------------------------------------------

class _GateProp:
    """Minimum prop surface the .obs and .prd reuse gates touch."""

    def __init__(self, tmp_path, continue_run=False,
                 start='2026-02-15T00:00:00Z', end='2026-04-01T00:00:00Z'):
        self.ofs = 'cbofs'
        self.whichcast = 'nowcast'
        self.ofsfiletype = 'stations'
        self.forecast_hr = '00z'
        self.start_date_full = start
        self.end_date_full = end
        self.continue_run = continue_run
        self.continue_overlap_hours = 24.0
        self.data_observations_1d_station_path = str(tmp_path)
        self.data_model_1d_node_path = str(tmp_path)


def _obs_ctl(station_ids):
    """Station ctl structure as _ensure_obs_files indexes it."""
    return [[[sid, '', '', 'CO-OPS'] for sid in station_ids], []]


def _model_ctl(station_ids, nodes):
    """Model ctl structure as _prd_paths indexes it (``[1]`` and ``[-1]``)."""
    return [[], list(nodes), [], [], [], list(station_ids)]


def _stamp(tmp_path, path, name_var='wl', extra=None, **prop_kwargs):
    """Record the manifest entry a real run would have left on ``path``.

    Since the cache-manifest reuse gate landed, every .obs/.prd/.int is
    stamped with the run signature as it is written, and a file with no
    entry reads as left over from an earlier run and is deleted. These
    fixtures are built by hand, so they have to record the same entry or
    the gate never reaches the coverage question under test.
    """
    cache_manifest.record_artifact(
        str(path),
        cache_manifest.run_signature(
            _GateProp(tmp_path, **prop_kwargs),
            variable=_NAME_TO_VARIABLE.get(name_var, name_var),
            extra=extra),
        str(tmp_path))


_NAME_TO_VARIABLE = {
    'wl': 'water_level',
    'temp': 'water_temperature',
    'salt': 'salinity',
    'cu': 'currents',
}


class TestEnsureObsFiles:
    """The observation reuse gate's three outcomes."""

    def test_prefix_plans_a_tail_fetch(self, tmp_path, monkeypatch):
        """A short .obs is kept and its tail start recorded."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        _stamp(tmp_path, path)
        calls = {}

        def _fake_fetch(prop, log, continuation=None):
            calls.setdefault('plan', continuation)
            # Stand in for the tail fetch plus its merge.
            _write_series(path, WINDOW_START, 46 * 24 + 1)

        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations', _fake_fetch)

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert path.exists()
        assert list(calls['plan']) == [str(path)]
        assert calls['plan'][str(path)] < WINDOW_END

    def test_unextendable_file_is_refetched_in_the_same_run(
            self, tmp_path, monkeypatch):
        """A refused merge must not leave a short series for pairing.

        The tail fetch leaves the file alone whenever it cannot merge
        safely, so the gate checks again afterwards and regenerates the
        stragglers over the full window rather than letting this run pair
        against a truncated series.
        """
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        _stamp(tmp_path, path)
        plans = []

        def _fake_fetch(prop, log, continuation=None):
            plans.append(continuation)
            if continuation is None:  # the full-window retry
                _write_series(path, WINDOW_START, 46 * 24 + 1)

        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations', _fake_fetch)

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert len(plans) == 2
        assert plans[0] is not None and plans[1] is None
        assert covers_run_window(path, WINDOW_START, WINDOW_END, now=NOW)

    def test_prefix_without_flag_is_deleted(self, tmp_path, monkeypatch):
        """Without --Continue_Run the same file is regenerated as before."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        calls = {}
        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations',
            lambda prop, log, continuation=None: calls.update(
                plan=continuation))

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path), 'wl', _logger())

        assert not path.exists()
        assert calls['plan'] is None

    def test_stale_file_is_deleted_even_in_continuation(
            self, tmp_path, monkeypatch):
        """A file starting after the window start cannot be extended."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START + timedelta(days=10), 40 * 24)
        calls = {}
        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations',
            lambda prop, log, continuation=None: calls.update(
                plan=continuation))

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert not path.exists()
        assert calls['plan'] is None

    def test_covering_file_triggers_no_fetch(self, tmp_path, monkeypatch):
        """A complete .obs is reused without contacting any provider."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 46 * 24 + 1)
        _stamp(tmp_path, path)
        calls = []
        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations',
            lambda prop, log, continuation=None: calls.append(continuation))

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert calls == []
        assert path.exists()

    def test_empty_file_is_not_extended(self, tmp_path, monkeypatch):
        """A 0-byte .obs records 'no data', not a window."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        path.write_text('', encoding='utf-8')
        calls = []
        monkeypatch.setattr(
            get_skill_mod, 'get_station_observations',
            lambda prop, log, continuation=None: calls.append(continuation))

        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert calls == []


class TestContinuationRespectsRunParameters:
    """A continuation run only ever extends files it could have written.

    Widening the run window is the one signature change ``-cr`` is allowed
    to make. Anything else -- a different datum, station owner or bins
    file -- means the rows on disk are not the rows we would be appending
    to, so the file is deleted and refetched in full rather than spliced.
    """

    def test_parameter_mismatch_is_not_extended(self, tmp_path, monkeypatch):
        """A file built under a different datum is refetched, not extended."""
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        # Stamped by a run that used a different vertical datum.
        cache_manifest.record_artifact(
            str(path), {'ofs': 'cbofs', 'datum': 'IGLD85'}, str(tmp_path))
        calls = {}

        def _fake_fetch(prop, log, continuation=None):
            calls['plan'] = continuation
            _write_series(path, WINDOW_START, 46 * 24 + 1)

        monkeypatch.setattr(get_skill_mod, 'get_station_observations',
                            _fake_fetch)
        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        # Refetched over the whole window, with no continuation plan.
        assert calls['plan'] is None

    def test_unstamped_file_is_not_extended(self, tmp_path, monkeypatch):
        """A file with no manifest entry cannot be shown to match this run.

        Pre-manifest artifacts are rebuilt once rather than extended on
        faith; there is nothing on disk that says what they were built
        from.
        """
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        calls = {}

        def _fake_fetch(prop, log, continuation=None):
            calls['plan'] = continuation
            _write_series(path, WINDOW_START, 46 * 24 + 1)

        monkeypatch.setattr(get_skill_mod, 'get_station_observations',
                            _fake_fetch)
        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert calls['plan'] is None

    def test_widened_window_alone_still_extends(self, tmp_path, monkeypatch):
        """The window differing is exactly what -cr exists for.

        Guards the regression that would make continuation a no-op: the
        signature always disagrees on the window, so comparing it would
        delete every file the run was about to extend.
        """
        path = tmp_path / '8638901_cbofs_wl_station.obs'
        _write_series(path, WINDOW_START, 15 * 24)
        # Stamped by the earlier, shorter run.
        _stamp(tmp_path, path, end='2026-03-02T00:00:00Z')
        calls = {}

        def _fake_fetch(prop, log, continuation=None):
            calls['plan'] = continuation
            _write_series(path, WINDOW_START, 46 * 24 + 1)

        monkeypatch.setattr(get_skill_mod, 'get_station_observations',
                            _fake_fetch)
        get_skill_mod._ensure_obs_files(
            _obs_ctl(['8638901']), _GateProp(tmp_path, continue_run=True),
            'wl', _logger())

        assert calls['plan'], 'the shorter file should have been extended'
        assert path.exists()


class TestEnsurePrdFiles:
    """The model-extraction reuse gate."""

    def test_prefix_runs_a_tail_extraction(self, tmp_path, monkeypatch):
        """A short .prd triggers extraction over the tail window only."""
        path = tmp_path / '8638901_cbofs_wl_45_nowcast_stations_model.prd'
        _write_series(path, WINDOW_START, 15 * 24)
        _stamp(tmp_path, path, extra={'whichcast': 'nowcast'})
        seen = {}

        def _fake_extract(prop, log, model_dataset=None):
            seen['start'] = prop.start_date_full
            seen['merge'] = getattr(prop, 'continuation_prd_merge', False)
            # Stand in for the real extraction plus its merge.
            _write_series(path, WINDOW_START, 46 * 24 + 1)
            return None

        monkeypatch.setattr(get_skill_mod, 'get_node_ofs', _fake_extract)
        get_skill_mod._ensure_prd_files(
            _model_ctl(['8638901'], [45]),
            _GateProp(tmp_path, continue_run=True), 'wl', _logger())

        assert seen['merge'] is True
        assert seen['start'] < '2026-03-02'
        assert seen['start'] > '2026-02-15'

    def test_failed_tail_falls_back_to_full_extraction(
            self, tmp_path, monkeypatch):
        """A tail pass that does not close the gap regenerates in full."""
        path = tmp_path / '8638901_cbofs_wl_45_nowcast_stations_model.prd'
        _write_series(path, WINDOW_START, 15 * 24)
        _stamp(tmp_path, path, extra={'whichcast': 'nowcast'})
        windows = []

        def _fake_extract(prop, log, model_dataset=None):
            windows.append(
                (prop.start_date_full,
                 getattr(prop, 'continuation_prd_merge', False)))
            return None  # never actually extends the file

        monkeypatch.setattr(get_skill_mod, 'get_node_ofs', _fake_extract)
        get_skill_mod._ensure_prd_files(
            _model_ctl(['8638901'], [45]),
            _GateProp(tmp_path, continue_run=True), 'wl', _logger())

        assert len(windows) == 2
        assert windows[0][1] is True
        assert windows[1] == ('2026-02-15T00:00:00Z', False)
        assert not path.exists()

    def test_missing_file_skips_continuation(self, tmp_path, monkeypatch):
        """Nothing on disk means there is no prefix to extend."""
        windows = []
        monkeypatch.setattr(
            get_skill_mod, 'get_node_ofs',
            lambda prop, log, model_dataset=None: windows.append(
                (prop.start_date_full,
                 getattr(prop, 'continuation_prd_merge', False))))

        get_skill_mod._ensure_prd_files(
            _model_ctl(['8638901'], [45]),
            _GateProp(tmp_path, continue_run=True), 'wl', _logger())

        assert windows == [('2026-02-15T00:00:00Z', False)]

    def test_tail_starts_at_the_furthest_behind_station(
            self, tmp_path, monkeypatch):
        """One dataset load must reach the station with the least data."""
        ahead = tmp_path / '111_cbofs_wl_45_nowcast_stations_model.prd'
        behind = tmp_path / '222_cbofs_wl_46_nowcast_stations_model.prd'
        _write_series(ahead, WINDOW_START, 30 * 24)
        _stamp(tmp_path, ahead, extra={'whichcast': 'nowcast'})
        _write_series(behind, WINDOW_START, 15 * 24)
        _stamp(tmp_path, behind, extra={'whichcast': 'nowcast'})
        seen = {}

        def _fake_extract(prop, log, model_dataset=None):
            seen['start'] = prop.start_date_full
            _write_series(ahead, WINDOW_START, 46 * 24 + 1)
            _write_series(behind, WINDOW_START, 46 * 24 + 1)
            return None

        monkeypatch.setattr(get_skill_mod, 'get_node_ofs', _fake_extract)
        get_skill_mod._ensure_prd_files(
            _model_ctl(['111', '222'], [45, 46]),
            _GateProp(tmp_path, continue_run=True), 'wl', _logger())

        # 15 days of hourly rows minus the 24 h overlap lands on Feb 28.
        assert seen['start'].startswith('2026-02-28')


class TestPairInvalidation:
    """.int files from the earlier run are rebuilt in a continuation run."""

    def test_pair_files_removed_under_flag(self, tmp_path, monkeypatch,
                                           create_1dplot_mod):
        """A pair file from the earlier run is dropped so skill is recomputed."""
        pair = (tmp_path
                / 'cbofs_wl_8638901_45_nowcast_stations_pair.int')
        _write_series(pair, WINDOW_START, 46 * 24 + 1)
        _backdate(pair)
        prop = _GateProp(tmp_path, continue_run=True)
        prop.data_skill_1d_pair_path = str(tmp_path)
        prop.whichcasts = ['nowcast']
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        monkeypatch.setattr(create_1dplot_mod, 'get_skill',
                            lambda *a, **k: None)

        create_1dplot_mod._ensure_paired_data_exists(
            _model_ctl(['8638901'], [45]), prop, ('water_level', 'wl'),
            _logger())

        assert not pair.exists()

    def test_pair_written_by_this_run_is_kept(self, tmp_path, monkeypatch,
                                              create_1dplot_mod):
        """A pair this run already rebuilt must not be deleted again.

        The pre-check runs once per variable while get_skill re-pairs
        every variable, so deleting unconditionally would make each
        variable throw away the previous one's work and re-pair the lot.
        """
        pair = (tmp_path
                / 'cbofs_wl_8638901_45_nowcast_stations_pair.int')
        _write_series(pair, WINDOW_START, 46 * 24 + 1)
        _stamp(tmp_path, pair, extra={'whichcast': 'nowcast'})
        prop = _GateProp(tmp_path, continue_run=True)
        prop.data_skill_1d_pair_path = str(tmp_path)
        prop.whichcasts = ['nowcast']
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        calls = []
        monkeypatch.setattr(create_1dplot_mod, 'get_skill',
                            lambda *a, **k: calls.append(1))

        create_1dplot_mod._ensure_paired_data_exists(
            _model_ctl(['8638901'], [45]), prop, ('water_level', 'wl'),
            _logger())

        assert pair.exists()
        assert calls == []

    def test_covering_pair_kept_without_flag(self, tmp_path, monkeypatch,
                                             create_1dplot_mod):
        """Today's behavior is untouched when the flag is off."""
        pair = (tmp_path
                / 'cbofs_wl_8638901_45_nowcast_stations_pair.int')
        _write_series(pair, WINDOW_START, 46 * 24 + 1)
        _stamp(tmp_path, pair, extra={'whichcast': 'nowcast'})
        prop = _GateProp(tmp_path)
        prop.data_skill_1d_pair_path = str(tmp_path)
        prop.whichcasts = ['nowcast']
        prop.start_date_full_before = prop.start_date_full
        prop.end_date_full_before = prop.end_date_full
        monkeypatch.setattr(create_1dplot_mod, 'get_skill',
                            lambda *a, **k: None)

        create_1dplot_mod._ensure_paired_data_exists(
            _model_ctl(['8638901'], [45]), prop, ('water_level', 'wl'),
            _logger())

        assert pair.exists()


class TestTailFetchWindow:
    """Narrowing a station's retrieval window to its tail."""

    def test_no_tail_start_passes_window_through(self):
        """Stations outside the plan keep the full-window dates."""
        from ofs_skill.obs_retrieval.get_station_observations import (
            _tail_fetch_window,
        )
        assert _tail_fetch_window(
            None, 'CO-OPS', '20260212', '20260404', '20260215-00:00:00',
            '20260401-00:00:00') == ('20260212', '20260215-00:00:00')

    def test_tail_start_keeps_the_three_day_padding(self):
        """The coarse date keeps the retrieval padding the full path uses."""
        from ofs_skill.obs_retrieval.get_station_observations import (
            _tail_fetch_window,
        )
        assert _tail_fetch_window(
            datetime(2026, 3, 1, 6, 0), 'CO-OPS', '20260212', '20260404',
            '20260215-00:00:00', '20260401-00:00:00') == (
                '20260226', '20260301-06:00:00')

    def test_window_dependent_providers_keep_the_full_window(self):
        """USGS/NDBC/CHS resolve their series from the queried span.

        A short tail window could land on a different parameter code,
        sensor or datamode than the run that wrote the head, so those
        stations are re-fetched over the whole window and only the merge
        is reused.
        """
        from ofs_skill.obs_retrieval.get_station_observations import (
            _tail_fetch_window,
        )
        for source in ('USGS', 'NDBC', 'CHS'):
            assert _tail_fetch_window(
                datetime(2026, 3, 1, 6, 0), source, '20260212', '20260404',
                '20260215-00:00:00', '20260401-00:00:00') == (
                    '20260212', '20260215-00:00:00')




if __name__ == '__main__':  # pragma: no cover - manual invocation
    pytest.main([__file__])
