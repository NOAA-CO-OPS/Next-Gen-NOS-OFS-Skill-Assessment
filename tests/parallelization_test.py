"""
Tests for parallelization and vectorization changes.

Phase 1: Vectorized 2D grid metrics
Config: Parallelization config reader
"""

import logging

import numpy as np

from ofs_skill.skill_assessment import nos_metrics

logger = logging.getLogger(__name__)


class TestVectorized2DMetrics:
    """Verify vectorized 2D metrics match the original loop-based implementation."""

    @staticmethod
    def _original_loop_impl(diff, errorrange, nan_threshold):
        """Reference implementation: the original double-nested loop."""
        cf2d = np.zeros([diff.shape[1], diff.shape[2]])
        pof2d = np.zeros([diff.shape[1], diff.shape[2]])
        nof2d = np.zeros([diff.shape[1], diff.shape[2]])
        for i in range(diff.shape[1]):
            for j in range(diff.shape[2]):
                if np.count_nonzero(~np.isnan(diff[:, i, j])) >= nan_threshold:
                    pixel_errors = diff[:, i, j]
                    cf2d[i, j] = nos_metrics.central_frequency(
                        pixel_errors, errorrange)
                    pof2d[i, j] = nos_metrics.positive_outlier_freq(
                        pixel_errors, errorrange)
                    nof2d[i, j] = nos_metrics.negative_outlier_freq(
                        pixel_errors, errorrange)
                else:
                    cf2d[i, j] = np.nan
                    pof2d[i, j] = np.nan
                    nof2d[i, j] = np.nan
        return cf2d, pof2d, nof2d

    @staticmethod
    def _vectorized_impl(diff, errorrange, nan_threshold):
        """The new vectorized implementation."""
        nan_find = ~np.isnan(diff)
        nan_sum = np.nansum(nan_find, axis=0)

        valid_mask = nan_sum >= nan_threshold
        within = np.nansum(
            (-errorrange <= diff) & (diff <= errorrange), axis=0)
        cf2d = np.where(valid_mask, within / nan_sum * 100, np.nan)
        pos_outlier = np.nansum(diff >= 2 * errorrange, axis=0)
        pof2d = np.where(valid_mask, pos_outlier / nan_sum * 100, np.nan)
        neg_outlier = np.nansum(diff <= -2 * errorrange, axis=0)
        nof2d = np.where(valid_mask, neg_outlier / nan_sum * 100, np.nan)
        return cf2d, pof2d, nof2d

    def test_known_values(self):
        """Test with a known diff array where results can be verified."""
        # 3 time steps, 2x2 grid
        diff = np.array([
            [[1.0, -1.0], [5.0, np.nan]],
            [[2.0, -2.0], [6.0, np.nan]],
            [[0.5, -0.5], [0.1, 3.0]],
        ])
        errorrange = 3.0
        nan_threshold = 2

        loop_cf, loop_pof, loop_nof = self._original_loop_impl(
            diff, errorrange, nan_threshold)
        vec_cf, vec_pof, vec_nof = self._vectorized_impl(
            diff, errorrange, nan_threshold)

        np.testing.assert_array_almost_equal(vec_cf, loop_cf)
        np.testing.assert_array_almost_equal(vec_pof, loop_pof)
        np.testing.assert_array_almost_equal(vec_nof, loop_nof)

    def test_all_nan_column(self):
        """Test with a column that is all NaN (below threshold)."""
        diff = np.array([
            [[1.0, np.nan], [2.0, 0.5]],
            [[0.5, np.nan], [3.0, -1.0]],
        ])
        errorrange = 1.5
        nan_threshold = 2

        loop_cf, loop_pof, loop_nof = self._original_loop_impl(
            diff, errorrange, nan_threshold)
        vec_cf, vec_pof, vec_nof = self._vectorized_impl(
            diff, errorrange, nan_threshold)

        np.testing.assert_array_almost_equal(vec_cf, loop_cf)
        np.testing.assert_array_almost_equal(vec_pof, loop_pof)
        np.testing.assert_array_almost_equal(vec_nof, loop_nof)

    def test_random_array(self):
        """Test with random data to verify equivalence at scale."""
        rng = np.random.default_rng(42)
        diff = rng.normal(0, 2, size=(20, 50, 50))
        # Sprinkle NaNs
        nan_mask = rng.random(diff.shape) < 0.15
        diff[nan_mask] = np.nan

        errorrange = 3.0
        nan_threshold = 2

        loop_cf, loop_pof, loop_nof = self._original_loop_impl(
            diff, errorrange, nan_threshold)
        vec_cf, vec_pof, vec_nof = self._vectorized_impl(
            diff, errorrange, nan_threshold)

        np.testing.assert_array_almost_equal(vec_cf, loop_cf)
        np.testing.assert_array_almost_equal(vec_pof, loop_pof)
        np.testing.assert_array_almost_equal(vec_nof, loop_nof)

    def test_return_two_d_integration(self):
        """Test the actual return_two_d function produces valid output."""
        from ofs_skill.skill_assessment.metrics_two_d import return_two_d

        rng = np.random.default_rng(123)
        obs = rng.normal(20, 2, size=(10, 30, 30))
        mod = obs + rng.normal(0, 0.5, size=(10, 30, 30))

        result = return_two_d(obs, mod, logger, errorrange=3.0)
        assert len(result) == 8
        # Check shapes
        for arr in result:
            assert arr.shape == (30, 30)
        # CF should be high (small errors relative to errorrange=3)
        cf2d = result[5]
        valid = ~np.isnan(cf2d)
        assert np.mean(cf2d[valid]) > 90


class TestParallelConfig:
    """Test the parallelization config reader."""

    def test_get_parallel_config_returns_defaults(self):
        """Config reader should return valid defaults."""
        from ofs_skill.obs_retrieval.utils import get_parallel_config

        config = get_parallel_config(logger)
        assert isinstance(config, dict)
        assert 'parallel_enabled' in config
        assert 'obs_coops_workers' in config
        assert 'ha_workers' in config
        assert isinstance(config['parallel_enabled'], bool)
        assert isinstance(config['obs_coops_workers'], int)
        assert config['obs_coops_workers'] >= 1

    def test_config_worker_counts_positive(self):
        """All worker counts should be positive integers."""
        from ofs_skill.obs_retrieval.utils import get_parallel_config

        config = get_parallel_config(logger)
        int_keys = [
            'obs_coops_workers', 'obs_usgs_workers', 'obs_ndbc_workers',
            'obs_chs_workers', 'model_download_workers', 'skill_workers',
            'ha_workers', 'plot_workers',
        ]
        for key in int_keys:
            assert config[key] >= 1, f'{key} should be >= 1'

    def test_auto_workers_returns_positive(self):
        """The _auto_workers helper should return >= 1 for all known keys."""
        from ofs_skill.obs_retrieval.utils import _auto_workers

        all_keys = [
            'obs_coops_workers', 'obs_usgs_workers', 'obs_ndbc_workers',
            'obs_chs_workers', 'model_download_workers', 'skill_workers',
            'ha_workers', 'plot_workers',
        ]
        for key in all_keys:
            result = _auto_workers(key)
            assert isinstance(result, int), f'{key}: expected int, got {type(result)}'
            assert result >= 1, f'{key}: expected >= 1, got {result}'
