"""Staleness checks must not thrash when the catalog can't cover the window.

Regression tests for the SECOFS delete/re-extract loop: the model's
station metadata changed mid-window, the catalog validator dropped the
pre-change files, and every freshly extracted ``.prd`` therefore started
days after the requested window start. ``covers_run_window`` judged the
fresh files stale against the raw window, the check deleted them, and
the pipeline re-ran the full multi-hour extraction once per variable —
recreating identical data every time.

Two independent guards close the loop:

- ``clamp_window_to_coverage`` narrows the checked window to the model
  catalog's actual time span (``dataset_time_bounds``), so an artifact
  covering everything the archive can provide counts as fresh; and
- ``created_this_run`` exempts files whose mtime says the current
  process wrote them — regeneration would reproduce them identically.
"""

import logging
import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import _all_prd_files_complete
from ofs_skill.utils.timeseries_coverage import (
    clamp_window_to_coverage,
    covers_run_window,
    created_this_run,
    dataset_time_bounds,
)

# The window and coverage from the run that surfaced the bug: window
# start 2026-05-01, but the earliest surviving catalog file begins
# 2026-05-06 06:00 after the validator dropped the pre-change files.
WINDOW = (datetime(2026, 5, 1, 0, 0), datetime(2026, 7, 31, 0, 0))
COVERAGE = (datetime(2026, 5, 6, 6, 0), datetime(2026, 7, 31, 0, 0))


def _logger():
    return logging.getLogger('catalog_coverage_clamp_test')


def _write_series(path, start, end, step_hours=1):
    """Write a .prd-shaped series with julian + Y M D H M columns."""
    lines = ['DNUM_JAN1 YEAR MONTH DAY HOUR MINUTE OBS']
    stamp = start
    while stamp <= end:
        lines.append(
            f'2461127.25 {stamp.year} {stamp.month} {stamp.day} '
            f'{stamp.hour} {stamp.minute} 1.5')
        stamp += timedelta(hours=step_hours)
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def _backdate(path, days=30):
    """Push a file's mtime into the past so it reads as a prior run's."""
    old = os.path.getmtime(path) - days * 86400
    os.utime(path, (old, old))


# ---------------------------------------------------------------------------
# clamp_window_to_coverage
# ---------------------------------------------------------------------------


def test_clamp_narrows_window_to_coverage():
    """Start rises to catalog start; end already within coverage."""
    start, end = clamp_window_to_coverage(WINDOW, COVERAGE, _logger())
    assert start == COVERAGE[0]
    assert end == WINDOW[1]


def test_clamp_passthrough_when_window_or_bounds_missing():
    """Either side being None must return the window unchanged."""
    assert clamp_window_to_coverage(None, COVERAGE) is None
    assert clamp_window_to_coverage(WINDOW, None) == WINDOW


def test_clamp_disjoint_coverage_fails_open_downstream(tmp_path):
    """Coverage entirely outside the window inverts the clamped window;
    covers_run_window's short-window guard must then fail open."""
    bounds = (datetime(2026, 9, 1), datetime(2026, 9, 30))
    start, end = clamp_window_to_coverage(WINDOW, bounds, _logger())
    assert start > end
    series = tmp_path / 'x.prd'
    _write_series(series, WINDOW[0], WINDOW[0] + timedelta(days=1))
    _backdate(series)
    assert covers_run_window(series, start, end, logger=_logger())


# ---------------------------------------------------------------------------
# dataset_time_bounds
# ---------------------------------------------------------------------------


def _dataset(time_name, start, hours):
    times = (np.datetime64(start) +
             np.arange(hours) * np.timedelta64(1, 'h'))
    return xr.Dataset(coords={time_name: times})


def test_dataset_time_bounds_schism_time_axis():
    """SCHISM/FVCOM datasets expose their span via the time coord."""
    ds = _dataset('time', '2026-05-06T06:00', 48)
    first, last = dataset_time_bounds(ds)
    assert first == datetime(2026, 5, 6, 6, 0)
    assert last == datetime(2026, 5, 8, 5, 0)


def test_dataset_time_bounds_roms_ocean_time_axis():
    """ROMS datasets expose their span via ocean_time."""
    ds = _dataset('ocean_time', '2026-05-01T00:00', 24)
    first, last = dataset_time_bounds(ds)
    assert first == datetime(2026, 5, 1, 0, 0)
    assert last == datetime(2026, 5, 1, 23, 0)


def test_dataset_time_bounds_none_and_unrecognized():
    """Missing/odd datasets yield None so callers skip clamping."""
    assert dataset_time_bounds(None) is None
    assert dataset_time_bounds(object()) is None
    assert dataset_time_bounds(xr.Dataset()) is None


# ---------------------------------------------------------------------------
# created_this_run
# ---------------------------------------------------------------------------


def test_created_this_run_fresh_file(tmp_path):
    """A file written after process start belongs to this run."""
    path = tmp_path / 'fresh.prd'
    path.write_text('data\n', encoding='utf-8')
    assert created_this_run(path)


def test_created_this_run_backdated_file(tmp_path):
    """An old mtime marks a file as a previous run's artifact."""
    path = tmp_path / 'old.prd'
    path.write_text('data\n', encoding='utf-8')
    _backdate(path)
    assert not created_this_run(path)


def test_created_this_run_missing_file(tmp_path):
    """Missing files return False so callers use normal handling."""
    assert not created_this_run(tmp_path / 'nope.prd')


# ---------------------------------------------------------------------------
# The SECOFS regression, end to end through covers_run_window
# ---------------------------------------------------------------------------


def test_fresh_extraction_passes_only_with_clamp(tmp_path):
    """A series covering exactly the catalog span fails the raw window
    (the bug) but passes the clamped one (the fix)."""
    series = tmp_path / 'secofs_wl.prd'
    _write_series(series, COVERAGE[0], COVERAGE[1], step_hours=6)
    _backdate(series)  # judge coverage, not the mtime exemption

    assert not covers_run_window(
        series, WINDOW[0], WINDOW[1], logger=_logger())

    start, end = clamp_window_to_coverage(WINDOW, COVERAGE, _logger())
    assert covers_run_window(series, start, end, logger=_logger())


# ---------------------------------------------------------------------------
# Resume check integration (_all_prd_files_complete)
# ---------------------------------------------------------------------------


def _resume_prop(prd_dir):
    return SimpleNamespace(
        data_model_1d_node_path=str(prd_dir),
        ofs='secofs',
        whichcast='nowcast',
        ofsfiletype='stations',
        start_date_full='2026-05-01T00:00:00Z',
        end_date_full='2026-07-31T00:00:00Z',
    )


def _resume_ctlfile(n_stations=2):
    nodes = list(range(n_stations))
    depths = [0] * n_stations
    shifts = [0.0] * n_stations
    ids = [f'sta{i:02d}' for i in range(n_stations)]
    return [], nodes, depths, shifts, ids


def test_resume_check_reuses_files_matching_catalog_coverage(tmp_path):
    """Files spanning the full catalog range must be reused when
    time_bounds is passed, and (regression) re-extracted when the raw
    window is used without bounds."""
    prop = _resume_prop(tmp_path)
    ctl = _resume_ctlfile()
    for i in range(2):
        path = tmp_path / (
            f'{ctl[4][i]}_secofs_wl_{ctl[1][i]}_nowcast_stations_model.prd')
        _write_series(path, COVERAGE[0], COVERAGE[1], step_hours=6)
        _backdate(path)  # a prior run's files; only coverage decides

    assert not _all_prd_files_complete(
        prop, ctl, 'wl', None, _logger())
    assert _all_prd_files_complete(
        prop, ctl, 'wl', None, _logger(), time_bounds=COVERAGE)


def test_resume_check_trusts_files_written_by_current_process(tmp_path):
    """Short-coverage files with fresh mtimes are this run's output and
    must be reused even without catalog bounds."""
    prop = _resume_prop(tmp_path)
    ctl = _resume_ctlfile()
    for i in range(2):
        path = tmp_path / (
            f'{ctl[4][i]}_secofs_wl_{ctl[1][i]}_nowcast_stations_model.prd')
        _write_series(path, COVERAGE[0], COVERAGE[1], step_hours=6)

    assert _all_prd_files_complete(prop, ctl, 'wl', None, _logger())
