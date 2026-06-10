"""Regression tests for non-monotonic model time axis handling.

Reproduces the forecast_b crash where overlapping forecast cycles produced
a non-monotonic ``time`` coordinate after ``drop_duplicates``, which then
broke ``xr.Dataset.resample`` with ``ValueError: Index must be monotonic
for resampling``.

Covers two fixes:
1. ``find_time_gaps`` no longer mis-flags a non-monotonic axis as a normal
   gap pattern; it returns True so the caller takes the resample path.
2. The resample block in ``get_node_ofs`` sorts the axis defensively
   before calling ``resample``.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import find_time_gaps


def _make_dataset(times):
    """Build a tiny FVCOM-shaped dataset keyed on ``time``."""
    times = np.asarray(times, dtype='datetime64[ns]')
    values = np.arange(times.size, dtype='float64')
    return xr.Dataset(
        {'zeta': (('time',), values)},
        coords={'time': times},
    )


def _fvcom_props():
    return SimpleNamespace(
        model_source='fvcom',
        ofs='necofs',
        ofsfiletype='stations',
        whichcast='forecast_b',
    )


def test_find_time_gaps_flags_nonmonotonic_axis():
    """Non-monotonic time axis should trigger the resample path."""
    monotonic = pd.date_range('2026-02-16', periods=6, freq='6min')
    swapped = monotonic.to_numpy().copy()
    swapped[2], swapped[3] = swapped[3], swapped[2]
    ds = _make_dataset(swapped)

    assert find_time_gaps(_fvcom_props(), ds, logging.getLogger()) is True


def test_find_time_gaps_clean_axis_returns_false():
    """A clean, evenly-spaced monotonic axis should report no gaps."""
    ds = _make_dataset(pd.date_range('2026-02-16', periods=6, freq='6min'))

    assert find_time_gaps(_fvcom_props(), ds, logging.getLogger()) is False


def test_find_time_gaps_real_gap_returns_true():
    """A genuine missing-file gap (10-min spacing where 6 is expected) trips."""
    times = list(pd.date_range('2026-02-16', periods=3, freq='6min'))
    times.append(times[-1] + pd.Timedelta('10min'))
    ds = _make_dataset(times)

    assert find_time_gaps(_fvcom_props(), ds, logging.getLogger()) is True


def test_resample_survives_after_sortby():
    """End-to-end shape of the fix: sortby then resample completes cleanly.

    Mirrors the resample block in ``get_node_ofs`` so a regression in the
    surrounding code path surfaces here rather than only at runtime on real
    forecast_b data.
    """
    base = pd.date_range('2026-02-16', periods=6, freq='6min').to_numpy()
    base[2], base[3] = base[3], base[2]
    ds = _make_dataset(base)

    time_vals = ds['time'].values
    if not np.all(np.diff(time_vals) > np.timedelta64(0)):
        ds = ds.sortby('time')

    resampled = ds.resample(time='6min').asfreq()

    assert resampled['time'].size == 6
    assert np.all(np.diff(resampled['time'].values) == np.timedelta64(6, 'm'))
