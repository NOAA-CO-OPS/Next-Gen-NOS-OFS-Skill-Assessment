"""Tests for the nos_metrics module."""

import csv
import math
import os
import tempfile

import numpy as np
import pytest

from ofs_skill.skill_assessment import nos_metrics


# ---------------------------------------------------------------------------
# RMSE
# ---------------------------------------------------------------------------
class TestRmse:
    def test_perfect_prediction(self):
        assert nos_metrics.rmse([1, 2, 3], [1, 2, 3]) == 0.0

    def test_known_value(self):
        # errors = [1, -1, 1, -1] → RMSE = 1.0
        result = nos_metrics.rmse([2, 1, 4, 2], [1, 2, 3, 3])
        assert pytest.approx(result, abs=1e-10) == 1.0


# ---------------------------------------------------------------------------
# Pearson R
# ---------------------------------------------------------------------------
class TestPearsonR:
    def test_perfect_correlation(self):
        r = nos_metrics.pearson_r([2, 4, 6], [1, 2, 3])
        assert pytest.approx(r, abs=1e-10) == 1.0

    def test_zero_variance_returns_nan(self):
        r = nos_metrics.pearson_r([5, 5, 5], [1, 2, 3])
        assert math.isnan(r)


# ---------------------------------------------------------------------------
# Central frequency
# ---------------------------------------------------------------------------
class TestCentralFrequency:
    def test_all_within(self):
        errors = [0.0, 0.1, -0.1]
        assert nos_metrics.central_frequency(errors, 0.15) == 100.0

    def test_none_within(self):
        errors = [10.0, -10.0]
        assert nos_metrics.central_frequency(errors, 0.15) == 0.0

    def test_boundary_included(self):
        """<= threshold should be counted (NOS convention)."""
        errors = [0.15, -0.15]
        assert nos_metrics.central_frequency(errors, 0.15) == 100.0

    def test_empty_returns_nan(self):
        assert math.isnan(nos_metrics.central_frequency([], 0.15))

    def test_nan_ignored(self):
        errors = [0.0, float('nan'), 0.0]
        # 2 valid values, both within → 100%
        assert nos_metrics.central_frequency(errors, 0.15) == 100.0


# ---------------------------------------------------------------------------
# Outlier frequencies
# ---------------------------------------------------------------------------
class TestOutlierFrequencies:
    def test_pof_boundary_included(self):
        """Errors exactly at 2*threshold count as positive outliers."""
        errors = [0.30, 0.0]
        pof = nos_metrics.positive_outlier_freq(errors, 0.15)
        assert pof == 50.0

    def test_nof_boundary_included(self):
        """Errors exactly at -2*threshold count as negative outliers."""
        errors = [-0.30, 0.0]
        nof = nos_metrics.negative_outlier_freq(errors, 0.15)
        assert nof == 50.0

    def test_no_outliers(self):
        errors = [0.0, 0.1, -0.1]
        assert nos_metrics.positive_outlier_freq(errors, 0.15) == 0.0
        assert nos_metrics.negative_outlier_freq(errors, 0.15) == 0.0

    def test_empty_returns_nan(self):
        assert math.isnan(nos_metrics.positive_outlier_freq([], 0.15))
        assert math.isnan(nos_metrics.negative_outlier_freq([], 0.15))


# ---------------------------------------------------------------------------
# MDPO / MDNO
# ---------------------------------------------------------------------------
class TestMdpoMdno:
    def test_no_outliers(self):
        errors = [0.0, 0.0, 0.0]
        assert nos_metrics.max_duration_positive_outliers(errors, 0.15) == 0
        assert nos_metrics.max_duration_negative_outliers(errors, 0.15) == 0

    def test_consecutive_run(self):
        # 2*0.15 = 0.30; three consecutive positive outliers
        errors = [0.0, 0.30, 0.31, 0.40, 0.0]
        assert nos_metrics.max_duration_positive_outliers(errors, 0.15) == 3

    def test_nan_breaks_streak(self):
        errors = [0.30, 0.31, float('nan'), 0.40, 0.50]
        assert nos_metrics.max_duration_positive_outliers(errors, 0.15) == 2

    def test_negative_version(self):
        errors = [0.0, -0.30, -0.31, -0.40, 0.0]
        assert nos_metrics.max_duration_negative_outliers(errors, 0.15) == 3

    def test_empty(self):
        assert nos_metrics.max_duration_positive_outliers([], 0.15) == 0
        assert nos_metrics.max_duration_negative_outliers([], 0.15) == 0


# ---------------------------------------------------------------------------
# check_nos_criteria
# ---------------------------------------------------------------------------
class TestCheckNosCriteria:
    def test_all_pass(self):
        result = nos_metrics.check_nos_criteria(95, 0.5, 0.5)
        assert result == {'cf': 'pass', 'pof': 'pass', 'nof': 'pass'}

    def test_all_fail(self):
        result = nos_metrics.check_nos_criteria(50, 5, 5)
        assert result == {'cf': 'fail', 'pof': 'fail', 'nof': 'fail'}

    def test_boundary_values(self):
        result = nos_metrics.check_nos_criteria(90, 1, 1)
        assert result == {'cf': 'pass', 'pof': 'pass', 'nof': 'pass'}


# ---------------------------------------------------------------------------
# get_error_threshold
# ---------------------------------------------------------------------------
class TestGetErrorThreshold:
    def test_known_defaults(self):
        assert nos_metrics.get_error_threshold('wl') == (0.15, 0.5)
        assert nos_metrics.get_error_threshold('salt') == (3.5, 0.5)
        assert nos_metrics.get_error_threshold('temp') == (3.0, 0.5)
        assert nos_metrics.get_error_threshold('cu') == (0.26, 0.5)
        assert nos_metrics.get_error_threshold('ice_conc') == (10.0, 0.5)

    def test_unknown_variable_raises(self):
        with pytest.raises(KeyError, match='Unknown variable'):
            nos_metrics.get_error_threshold('unknown_var')

    def test_reads_from_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, 'error_ranges.csv')
            with open(csv_path, 'w', newline='') as fh:
                writer = csv.writer(fh)
                writer.writerow(['name_var', 'X1', 'X2'])
                writer.writerow(['wl', '0.20', '0.6'])
            result = nos_metrics.get_error_threshold('wl', csv_path)
            assert result == (0.20, 0.6)

    def test_falls_back_to_defaults_when_no_file(self):
        result = nos_metrics.get_error_threshold('wl', '/nonexistent/path.csv')
        assert result == (0.15, 0.5)

    def test_falls_back_when_var_missing_from_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, 'error_ranges.csv')
            with open(csv_path, 'w', newline='') as fh:
                writer = csv.writer(fh)
                writer.writerow(['name_var', 'X1', 'X2'])
                writer.writerow(['salt', '3.5', '0.5'])
            # 'wl' is not in the CSV, should fall back to built-in default
            result = nos_metrics.get_error_threshold('wl', csv_path)
            assert result == (0.15, 0.5)


# ---------------------------------------------------------------------------
# mean_bias and standard_deviation
# ---------------------------------------------------------------------------
class TestMeanBias:
    def test_simple(self):
        assert nos_metrics.mean_bias([1, 2, 3]) == 2.0

    def test_with_nans(self):
        assert nos_metrics.mean_bias([1, float('nan'), 3]) == 2.0


class TestStandardDeviation:
    def test_simple(self):
        # std of [1, 2, 3] with ddof=0 = sqrt(2/3)
        expected = float(np.std([1, 2, 3]))
        assert pytest.approx(nos_metrics.standard_deviation([1, 2, 3]), abs=1e-10) == expected

    def test_with_nans(self):
        expected = float(np.nanstd([1, float('nan'), 3]))
        assert pytest.approx(nos_metrics.standard_deviation([1, float('nan'), 3]), abs=1e-10) == expected
