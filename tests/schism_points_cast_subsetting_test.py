"""Tests for per-cast subsetting of SCHISM (STOFS-3D) points files.

STOFS-3D per-cycle points files carry the nowcast and forecast periods
concatenated on one time axis (verified against operational NODD
output: 1200 six-minute steps spanning cycle - 24 h + dt through
cycle + 96 h). ``fix_schism_points_dataset`` must keep only the
segments belonging to the requested whichcast, exactly as
``fix_adcirc_dataset`` does for STOFS-2D-Global:

- nowcast: each cycle's times <= its cycle time;
- forecast_b: each cycle's times > its cycle time, with the downstream
  dedup (keep='last') stitching overlaps in favor of the newer cycle;
- forecast_a: the single requested initialization's forecast segment.

The synthetic datasets mimic two consecutive STOFS-3D-Atl 12Z cycles
with an hourly timestep (24 nowcast + 96 forecast steps per file); the
subsetter infers the timestep and block boundaries from the time
coordinate itself, so the coarser spacing exercises the same logic.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.intake_scisa import (
    fix_schism_points_dataset,
    needs_schism_points_cast_subsetting,
)

NOWCAST_H = 24
FORECAST_H = 96
CYCLE_1 = np.datetime64('2025-07-01T12:00')
CYCLE_2 = np.datetime64('2025-07-02T12:00')


def _logger():
    return logging.getLogger('schism_points_cast_subsetting_test')


def _cycle_block(cycle, block_id, step=np.timedelta64(1, 'h'),
                 n_now=NOWCAST_H, n_fcst=FORECAST_H):
    """One per-cycle points file: nowcast then forecast, concatenated.

    Times run from ``cycle - n_now*step + step`` through
    ``cycle + n_fcst*step`` (first n_now steps <= cycle are nowcast,
    remainder are forecast), matching the observed operational layout.
    Values encode provenance: block_id*1000 + step index.
    """
    n_total = n_now + n_fcst
    times = cycle - n_now * step + step * np.arange(1, n_total + 1)
    values = block_id * 1000.0 + np.arange(n_total, dtype=float)
    return times, values


def _dataset(blocks, n_station=3):
    """Concatenate per-cycle blocks in file order, like nested intake."""
    times = np.concatenate([b[0] for b in blocks])
    vals = np.concatenate([b[1] for b in blocks])
    zeta = np.tile(vals[:, None], (1, n_station))
    return xr.Dataset(
        data_vars={'zeta': (('time', 'station'), zeta)},
        coords={'time': times},
    )


def _two_cycle_dataset():
    return _dataset([
        _cycle_block(CYCLE_1, block_id=1),
        _cycle_block(CYCLE_2, block_id=2),
    ])


def _prop(whichcast, ofs='stofs_3d_atl', **kwargs):
    return SimpleNamespace(
        model_source='schism',
        ofs=ofs,
        ofsfiletype='stations',
        whichcast=whichcast,
        **kwargs,
    )


def _apply_pipeline_dedup(ds, whichcast):
    """Replicate the duplicate-time removal from intake_model."""
    keep = 'first' if whichcast == 'forecast_a' else 'last'
    return ds.drop_duplicates(dim='time', keep=keep)


def test_nowcast_keeps_only_nowcast_segments():
    """nowcast keeps each cycle's times <= its cycle time, nothing else."""
    ds = fix_schism_points_dataset(
        _prop('nowcast'), _two_cycle_dataset(), _logger())

    times = ds['time'].values
    assert len(times) == 2 * NOWCAST_H
    # Cycle 1 nowcast: (cycle1 - 24h, cycle1]; cycle 2: (cycle2 - 24h, cycle2]
    assert times.min() == CYCLE_1 - np.timedelta64(NOWCAST_H - 1, 'h')
    assert times.max() == CYCLE_2
    # No forecast values may survive: nowcast step indices are < 24
    step_idx = ds['zeta'].values[:, 0] % 1000
    assert (step_idx < NOWCAST_H).all()
    # Segments are disjoint, so the pipeline dedup must be a no-op
    assert len(_apply_pipeline_dedup(ds, 'nowcast')['time']) == len(times)


def test_forecast_b_keeps_only_forecast_segments():
    """forecast_b keeps each cycle's times > its cycle time, nothing else."""
    ds = fix_schism_points_dataset(
        _prop('forecast_b'), _two_cycle_dataset(), _logger())

    times = ds['time'].values
    assert len(times) == 2 * FORECAST_H
    assert times.min() == CYCLE_1 + np.timedelta64(1, 'h')
    assert times.max() == CYCLE_2 + np.timedelta64(FORECAST_H, 'h')
    # Nothing from any nowcast segment may survive
    step_idx = ds['zeta'].values[:, 0] % 1000
    assert (step_idx >= NOWCAST_H).all()


def test_forecast_b_dedup_prefers_newer_cycle_forecast():
    """At overlapping valid times the stitched forecast_b series must
    come from the newer cycle's forecast — never from its nowcast."""
    ds = fix_schism_points_dataset(
        _prop('forecast_b'), _two_cycle_dataset(), _logger())
    ds = _apply_pipeline_dedup(ds, 'forecast_b')

    times = ds['time'].values
    vals = ds['zeta'].values[:, 0]
    block_of = vals // 1000

    # Valid times up to and including cycle 2 exist only in cycle 1's
    # forecast; after cycle 2 both cycles overlap and the newer wins.
    assert (block_of[times <= CYCLE_2] == 1).all()
    assert (block_of[times > CYCLE_2] == 2).all()
    # A valid hour inside cycle 2's nowcast period must be cycle 1's
    # forecast value (this was the defect: keep='last' used to
    # substitute cycle 2's nowcast).
    probe = CYCLE_2 - np.timedelta64(12, 'h')
    val = float(vals[times == probe][0])
    assert val // 1000 == 1
    assert val % 1000 >= NOWCAST_H


def test_forecast_a_keeps_only_requested_initialization():
    """forecast_a keeps the requested cycle's full horizon and drops the
    previous day's initialization entirely."""
    ds = fix_schism_points_dataset(
        _prop('forecast_a', startdate='2025070200', forecast_hr='12z'),
        _two_cycle_dataset(), _logger())

    times = ds['time'].values
    vals = ds['zeta'].values[:, 0]
    assert len(times) == FORECAST_H
    assert times.min() == CYCLE_2 + np.timedelta64(1, 'h')
    assert times.max() == CYCLE_2 + np.timedelta64(FORECAST_H, 'h')
    # Everything must come from cycle 2 (the requested initialization);
    # previously keep='first' kept cycle 1's forecast for the first
    # 72 h of the horizon.
    assert (vals // 1000 == 2).all()
    assert (vals % 1000 >= NOWCAST_H).all()
    # forecast_a dedup (keep='first') must not change the selection
    assert len(_apply_pipeline_dedup(ds, 'forecast_a')['time']) == len(times)


def test_forecast_a_duplicated_single_file():
    """intake_model doubles a length-1 urlpath list; both copies match
    the requested cycle and the dedup collapses them."""
    ds = _dataset([
        _cycle_block(CYCLE_2, block_id=2),
        _cycle_block(CYCLE_2, block_id=2),
    ])
    out = fix_schism_points_dataset(
        _prop('forecast_a', startdate='2025070200', forecast_hr='12z'),
        ds, _logger())
    out = _apply_pipeline_dedup(out, 'forecast_a')
    assert len(out['time']) == FORECAST_H


def test_forecast_a_missing_requested_cycle_raises():
    """A forecast_a run whose requested cycle file is absent must fail
    loudly instead of silently scoring another initialization."""
    with pytest.raises(ValueError, match='no timesteps matched'):
        fix_schism_points_dataset(
            _prop('forecast_a', startdate='2025070500', forecast_hr='12z'),
            _two_cycle_dataset(), _logger())


def test_forecast_a_requires_cycle_identification():
    """forecast_a without startdate/forecast_hr cannot pick a cycle."""
    with pytest.raises(ValueError, match='forecast_a requires'):
        fix_schism_points_dataset(
            _prop('forecast_a', startdate=None, forecast_hr=None),
            _two_cycle_dataset(), _logger())


def test_rejects_non_schism_or_non_stations():
    """The subsetter refuses non-SCHISM and non-stations datasets."""
    with pytest.raises(ValueError, match='SCHISM points'):
        fix_schism_points_dataset(
            SimpleNamespace(model_source='adcirc', ofsfiletype='stations',
                            ofs='stofs_2d_glo', whichcast='nowcast'),
            _two_cycle_dataset(), _logger())
    with pytest.raises(ValueError, match='SCHISM points'):
        fix_schism_points_dataset(
            SimpleNamespace(model_source='schism', ofsfiletype='fields',
                            ofs='stofs_3d_atl', whichcast='nowcast'),
            _two_cycle_dataset(), _logger())


def test_dispatch_guard_scopes_to_stofs_3d_points_only():
    """Only STOFS-3D stations datasets take the new subsetting path;
    SCHISM fields, the per-cast SCHISM OFS (loofs2/secofs), and other
    model sources are untouched."""
    def prop_for(model_source, ofs, ofsfiletype):
        return SimpleNamespace(model_source=model_source, ofs=ofs,
                               ofsfiletype=ofsfiletype)

    assert needs_schism_points_cast_subsetting(
        prop_for('schism', 'stofs_3d_atl', 'stations'))
    assert needs_schism_points_cast_subsetting(
        prop_for('schism', 'stofs_3d_pac', 'stations'))
    assert not needs_schism_points_cast_subsetting(
        prop_for('schism', 'stofs_3d_atl', 'fields'))
    assert not needs_schism_points_cast_subsetting(
        prop_for('schism', 'loofs2', 'stations'))
    assert not needs_schism_points_cast_subsetting(
        prop_for('schism', 'secofs', 'stations'))
    assert not needs_schism_points_cast_subsetting(
        prop_for('adcirc', 'stofs_2d_glo', 'stations'))
    assert not needs_schism_points_cast_subsetting(
        prop_for('fvcom', 'necofs', 'stations'))


def test_irregular_block_still_subsets_by_time(caplog):
    """A truncated file (short block) warns but the time-based boundary
    still yields a correct nowcast/forecast split."""
    full = _cycle_block(CYCLE_1, block_id=1)
    # Cycle 2's file truncated to 24 nowcast + 48 forecast steps
    short_times, short_vals = _cycle_block(
        CYCLE_2, block_id=2, n_fcst=48)
    ds = _dataset([full, (short_times, short_vals)])

    with caplog.at_level(logging.WARNING):
        out = fix_schism_points_dataset(_prop('nowcast'), ds, _logger())
    assert any('expected' in rec.message for rec in caplog.records)

    times = out['time'].values
    assert len(times) == 2 * NOWCAST_H
    assert times.max() == CYCLE_2
    assert (out['zeta'].values[:, 0] % 1000 < NOWCAST_H).all()
    assert times.min() == CYCLE_1 - np.timedelta64(NOWCAST_H - 1, 'h')


def test_tiny_dataset_passthrough():
    """A degenerate single-timestep dataset passes through unchanged."""
    ds = _dataset([(np.array([CYCLE_1]), np.array([1000.0]))])
    out = fix_schism_points_dataset(_prop('nowcast'), ds, _logger())
    assert len(out['time']) == 1
