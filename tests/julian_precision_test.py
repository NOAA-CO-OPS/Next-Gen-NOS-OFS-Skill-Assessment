"""Tests for the elapsed-days (julian) column precision fix (issue #200).

The ``.obs`` / ``.prd`` series writers rounded the absolute Julian
date to 4 decimals (an 8.64 s grid), so strictly 6-minute series
showed consecutive elapsed-day steps wobbling between 0.0041, 0.0042,
and 0.0043 in the paired ``.int`` files. The writers now round at the
8 decimals the fixed-width format emits, and the two merges that used
the float julian column as a key (``paired_scalar`` and the
forecast-horizon CSV accumulator) merge on timestamps/date components
instead — so cached series written at the old precision still pair
against fresh ones.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from ofs_skill.model_processing.do_horizon_skill_utils import pandas_merge
from ofs_skill.obs_retrieval.format_obs_timeseries import (
    format_scalar,
    format_vector,
)
from ofs_skill.skill_assessment import format_paired_one_d


@pytest.fixture()
def logger():
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger('julian_precision_test')


def _six_minute_frame(n=240, value_col='OBS'):
    times = pd.date_range('2017-03-01 00:00', periods=n, freq='6min')
    return pd.DataFrame({'DateTime': times, value_col: np.ones(n)})


def test_format_scalar_uniform_six_minute_steps():
    """format_scalar 6-min steps are uniform to within 1e-8 day (~1 ms)."""
    ts = _six_minute_frame()
    lines = format_scalar(ts, '20170301-00:00:00', '20170302-00:00:00',
                          lookback_hours=0)
    julian = np.asarray([float(ln.split()[0]) for ln in lines])
    diffs = np.diff(julian)
    # Every step must be 1/240 day to within the 8-decimal write grid.
    assert np.all(np.abs(diffs - 1.0 / 240.0) < 2e-8)
    # Regression: at the old 4-decimal rounding the steps collapsed to
    # an 8.64 s grid and wobbled between 0.0041 and 0.0043.
    assert len({round(d, 6) for d in diffs}) == 1


def test_format_vector_uniform_six_minute_steps():
    """format_vector 6-min steps are uniform to within 1e-8 day (~1 ms)."""
    ts = _six_minute_frame()
    ts['DIR'] = 90.0
    lines = format_vector(ts, '20170301-00:00:00', '20170302-00:00:00',
                          lookback_hours=0)
    julian = np.asarray([float(ln.split()[0]) for ln in lines])
    diffs = np.diff(julian)
    assert np.all(np.abs(diffs - 1.0 / 240.0) < 2e-8)
    assert len({round(d, 6) for d in diffs}) == 1


def _series_df(times, julian_decimals, value=1.0, vector=False):
    """Build an obs/model dataframe shaped like the .obs/.prd readers'."""
    julian = pd.array(pd.Series(times)).to_julian_date()
    julian = np.round(np.asarray(julian, dtype=float), julian_decimals)
    data = {
        0: julian,
        1: times.year,
        2: times.month,
        3: times.day,
        4: times.hour,
        5: times.minute,
        6: np.full(len(times), value),
    }
    if vector:
        data[7] = np.full(len(times), 90.0)
        data[8] = np.full(len(times), value)
        data[9] = np.full(len(times), 0.0)
    return pd.DataFrame(data)


def test_paired_scalar_tolerates_mixed_julian_precision(logger):
    """Cached 4-decimal obs vs fresh 8-decimal model must still pair.

    Pre-fix the merge used the float julian column as a key, so any
    rounding mismatch between the two files silently produced all-NaN
    OBS and a spurious temporal-overlap drop.
    """
    times = pd.date_range('2017-03-01 00:00', periods=100, freq='6min')
    obs_df = _series_df(times, julian_decimals=4, value=1.5)
    ofs_df = _series_df(times, julian_decimals=8, value=2.0)

    result = format_paired_one_d.paired_scalar(
        obs_df, ofs_df, '20170301-00:00:00', '20170301-09:54:00', logger,
        lookback_hours=0)
    assert result is not None
    assert not isinstance(result, format_paired_one_d.PairingStatus)
    _, paired = result
    assert paired['OBS'].notna().all()
    assert paired['OFS'].notna().all()
    assert np.allclose(paired['BIAS'], 0.5)


def test_paired_vector_still_pairs(logger):
    """Vector pairing (already DateTime-keyed) keeps working end to end."""
    times = pd.date_range('2017-03-01 00:00', periods=100, freq='6min')
    obs_df = _series_df(times, julian_decimals=4, value=1.0, vector=True)
    ofs_df = _series_df(times, julian_decimals=8, value=1.0, vector=True)

    result = format_paired_one_d.paired_vector(
        obs_df, ofs_df, '20170301-00:00:00', '20170301-09:54:00', logger,
        lookback_hours=0)
    assert result is not None
    _, paired = result
    assert paired['OBS'].notna().all()
    assert len(paired) == 100


def test_pandas_merge_mixed_precision_does_not_duplicate_rows(tmp_path):
    """Old 4-decimal horizons CSV + fresh 8-decimal cycle: no row fan-out."""
    times = pd.date_range('2017-03-01 00:00', periods=48, freq='6min')

    old = _series_df(times, julian_decimals=4, value=1.0)
    old = old.rename(columns={0: 'julian', 1: 'year', 2: 'month',
                              3: 'day', 4: 'hour', 5: 'minute',
                              6: '20170228-00z_hr'})
    csv_path = tmp_path / 'station_cu_fcst_horizons.csv'
    old.to_csv(csv_path, index=False)

    new = _series_df(times, julian_decimals=8, value=2.0)
    new = new.rename(columns={0: 'julian', 1: 'year', 2: 'month',
                              3: 'day', 4: 'hour', 5: 'minute',
                              6: '20170301-00z_hr'})

    prop_stub = SimpleNamespace(
        datecycles=['20170228-00z_hr', '20170301-00z_hr'])
    merged = pandas_merge(str(csv_path), new, '20170301-00z_hr', prop_stub)

    # Pre-fix: julian was a float merge key, so the rounding mismatch
    # made every timestamp appear twice in the outer merge.
    assert len(merged) == len(times)
    assert merged['20170228-00z_hr'].notna().all()
    assert merged['20170301-00z_hr'].notna().all()


def test_pandas_merge_same_precision_still_merges(tmp_path):
    """The component-column merge stays a clean 1:1 on same-precision data."""
    times = pd.date_range('2017-03-01 00:00', periods=24, freq='6min')
    old = _series_df(times, julian_decimals=8, value=1.0)
    old = old.rename(columns={0: 'julian', 1: 'year', 2: 'month',
                              3: 'day', 4: 'hour', 5: 'minute',
                              6: '20170228-00z_hr'})
    csv_path = tmp_path / 'station_cu_fcst_horizons.csv'
    old.to_csv(csv_path, index=False)

    new = _series_df(times, julian_decimals=8, value=2.0)
    new = new.rename(columns={0: 'julian', 1: 'year', 2: 'month',
                              3: 'day', 4: 'hour', 5: 'minute',
                              6: '20170301-00z_hr'})

    prop_stub = SimpleNamespace(
        datecycles=['20170228-00z_hr', '20170301-00z_hr'])
    merged = pandas_merge(str(csv_path), new, '20170301-00z_hr', prop_stub)
    assert len(merged) == len(times)
    assert 'julian' in merged.columns
