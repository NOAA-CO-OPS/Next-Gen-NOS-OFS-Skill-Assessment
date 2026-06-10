"""Tests for ``_resample_time_vars_only``.

The default ``Dataset.resample(time=...).asfreq()`` aligns every variable
to the new regular grid, including replicated static mesh vars (lon, lat,
siglay, h) that intake's nested-combine duplicated along the time dim
during multi-file concat. On a 316-file NECOFS dataset that materialization
costs ~20 min per whichcast. The helper splits the dataset, resamples only
the time-varying subset, and re-attaches the static vars unchanged.

These tests verify:
- Time-varying vars get reindexed onto the new freq grid.
- Static vars come back untouched (same dtype, same values).
- Gaps in the original index become NaN at the new grid points.
- A dataset with no time-varying data vars is returned unchanged with a
  warning instead of erroring.
"""

import logging

import numpy as np
import pandas as pd
import xarray as xr

from ofs_skill.model_processing.get_node_ofs import _resample_time_vars_only


def _logger():
    return logging.getLogger('resample_time_vars_only_test')


def _make_dataset_with_gap():
    """FVCOM-shaped dataset with a 30-min gap and a static h var."""
    n_node = 5
    times = pd.to_datetime([
        '2026-02-16 00:00',
        '2026-02-16 00:06',
        '2026-02-16 00:12',
        # gap: 00:18 and 00:24 missing
        '2026-02-16 00:30',
        '2026-02-16 00:36',
    ])
    rng = np.random.default_rng(0)
    return xr.Dataset(
        data_vars={
            'zeta': (('time', 'node'),
                     rng.standard_normal((len(times), n_node))),
            'h': (('node',), np.full(n_node, 25.0)),
        },
        coords={'time': times},
    )


def test_time_vars_reindexed_onto_freq_grid():
    """zeta should land on a regular 6-min grid spanning the original range."""
    ds = _make_dataset_with_gap()

    out = _resample_time_vars_only(ds, 'time', '6min', _logger())

    expected = pd.date_range('2026-02-16 00:00', '2026-02-16 00:36', freq='6min')
    assert list(out['time'].values) == list(expected.to_numpy())
    # The two missing slots (00:18, 00:24) should be NaN in zeta.
    nan_mask = np.isnan(out['zeta'].values)
    assert nan_mask[3, :].all()  # 00:18
    assert nan_mask[4, :].all()  # 00:24
    assert not nan_mask[0, :].any()  # 00:00 present


def test_static_vars_untouched():
    """h is not time-varying; resample must not change it."""
    ds = _make_dataset_with_gap()

    out = _resample_time_vars_only(ds, 'time', '6min', _logger())

    np.testing.assert_array_equal(out['h'].values, ds['h'].values)
    assert out['h'].dims == ('node',)


def test_no_time_vars_returns_original(caplog):
    """Dataset with no time-varying data_vars is returned unchanged."""
    ds = xr.Dataset(
        data_vars={'h': (('node',), np.zeros(5))},
        coords={'time': pd.to_datetime(['2026-02-16 00:00'])},
    )

    with caplog.at_level(logging.WARNING, logger='resample_time_vars_only_test'):
        out = _resample_time_vars_only(ds, 'time', '6min', _logger())

    assert out is ds
    msgs = [r.getMessage() for r in caplog.records]
    assert any('no-op' in m.lower() for m in msgs)


def test_summary_log_records_counts(caplog):
    """The helper logs how many time / static vars it handled."""
    ds = _make_dataset_with_gap()

    with caplog.at_level(logging.INFO, logger='resample_time_vars_only_test'):
        _resample_time_vars_only(ds, 'time', '6min', _logger())

    summary = next(r.getMessage() for r in caplog.records
                   if 'Resampled' in r.getMessage())
    assert '1 time-varying' in summary
    assert '1 static' in summary
