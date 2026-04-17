"""Tests for metrics_paired_one_d.skill_vector_dir.

These are the regression tests that the prior (broken) implementation would
have failed: individually wrapping each angle via get_distance_angle(x, 0)
only moves the 0°/360° discontinuity to ±180°, inflating squared errors near
the antipode. The correct circular RMSE uses the signed angular difference
(DIR_BIAS) directly.
"""
import os
import types
from logging import getLogger

import numpy as np
import pandas as pd
import pytest

from ofs_skill.skill_assessment.format_paired_one_d import get_distance_angle
from ofs_skill.skill_assessment.metrics_paired_one_d import (
    _circular_correlation_deg,
    _circular_mean_deg,
    skill_vector_dir,
)


def _write_error_ranges(tmpdir):
    """Write a minimal error_ranges.csv so get_error_threshold resolves."""
    conf_dir = os.path.join(tmpdir, 'conf')
    os.makedirs(conf_dir, exist_ok=True)
    with open(os.path.join(conf_dir, 'error_ranges.csv'), 'w') as fh:
        fh.write('name_var,X1,X2\n')
        fh.write('cu_dir,22.5,1.0\n')


def _make_prop(tmpdir):
    prop = types.SimpleNamespace()
    prop.path = tmpdir
    return prop


def _build_df(obs_dirs, ofs_dirs, freq='h'):
    n = len(obs_dirs)
    times = pd.date_range('2026-01-01', periods=n, freq=freq)
    dir_bias = [get_distance_angle(o, b) for o, b in zip(ofs_dirs, obs_dirs)]
    return pd.DataFrame({
        'DateTime': times,
        'OBS_DIR': obs_dirs,
        'OFS_DIR': ofs_dirs,
        'DIR_BIAS': dir_bias,
    })


# ---------------------------------------------------------------------------
# Circular helpers
# ---------------------------------------------------------------------------

class TestCircularHelpers:
    def test_circular_mean_cardinal(self):
        # Symmetric around north (350 and 10) → mean at 0.
        assert abs(_circular_mean_deg([350.0, 10.0])) < 1e-6 or \
               abs(_circular_mean_deg([350.0, 10.0]) - 360.0) < 1e-6 or \
               abs(_circular_mean_deg([350.0, 10.0])) < 1e-6

    def test_circular_mean_simple(self):
        # All 90° → 90°.
        assert _circular_mean_deg([90.0, 90.0, 90.0]) == pytest.approx(90.0, abs=1e-6)

    def test_circular_mean_nan_tolerant(self):
        val = _circular_mean_deg([90.0, np.nan, 90.0])
        assert val == pytest.approx(90.0, abs=1e-6)

    def test_circular_correlation_perfect(self):
        # Identical series → correlation ≈ 1.
        angles = np.arange(0, 360, 10.0)
        r = _circular_correlation_deg(angles, angles)
        assert r == pytest.approx(1.0, abs=1e-6)

    def test_circular_correlation_noisy_tracking(self):
        # Angles clustered around a mean with small iid noise on both obs and
        # ofs → strong positive correlation (J-S is well-defined here, unlike
        # for a uniform distribution where the mean direction is undefined).
        rng = np.random.default_rng(0)
        obs = 90.0 + 10.0 * rng.standard_normal(200)  # ~N(90°, 10°)
        ofs = obs + 1.0 * rng.standard_normal(200)  # small iid noise
        r = _circular_correlation_deg(obs % 360, ofs % 360)
        assert r > 0.9, f'Co-varying clustered angles should correlate, got {r}'

    def test_circular_correlation_undefined(self):
        # Single pair → NaN (needs >= 2).
        r = _circular_correlation_deg([90.0], [91.0])
        assert np.isnan(r)


# ---------------------------------------------------------------------------
# skill_vector_dir — regression tests for blocker #5
# ---------------------------------------------------------------------------

class TestSkillVectorDirWraparound:
    """These cases would have failed under the pre-fix implementation."""

    def test_rmse_no_wraparound(self, tmp_path):
        """obs=170°, ofs=190° — true error 20°. Prior bug: ~340° (inflated)."""
        _write_error_ranges(tmp_path)
        df = _build_df([170.0] * 10, [190.0] * 10)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        rmse = result[0]
        assert rmse == pytest.approx(20.0, abs=0.01), \
            f'RMSE should be 20° for a uniform 20° model-obs offset; got {rmse}'

    def test_rmse_across_zero_branch(self, tmp_path):
        """obs=355°, ofs=5° — true error 10°. Prior bug: ~350°."""
        _write_error_ranges(tmp_path)
        df = _build_df([355.0] * 10, [5.0] * 10)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        rmse = result[0]
        assert rmse == pytest.approx(10.0, abs=0.01), \
            f'RMSE should be 10° across 0°/360° branch; got {rmse}'

    def test_rmse_at_antipode_branch(self, tmp_path):
        """obs=170°, ofs=190° sits right at the ±180° branch the prior 'fix'
        moved the discontinuity to. Same test as the first, kept explicit for
        review clarity."""
        _write_error_ranges(tmp_path)
        df = _build_df([175.0] * 10, [185.0] * 10)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        rmse = result[0]
        assert rmse == pytest.approx(10.0, abs=0.01), \
            f'RMSE near ±180° branch should be 10°; got {rmse}'

    def test_rmse_perfect_agreement(self, tmp_path):
        """Perfect agreement → RMSE 0."""
        _write_error_ranges(tmp_path)
        df = _build_df([45.0] * 10, [45.0] * 10)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        rmse = result[0]
        assert rmse == pytest.approx(0.0, abs=0.01)

    def test_correlation_perfect(self, tmp_path):
        """Identical direction series → circular correlation ≈ 1."""
        _write_error_ranges(tmp_path)
        angles = list(np.arange(0, 360, 10.0))
        df = _build_df(angles, angles)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        r_value = result[1]
        assert r_value == pytest.approx(1.0, abs=0.01)


# ---------------------------------------------------------------------------
# skill_vector_dir — gap-fragility regression (blocker #7 for dir path)
# ---------------------------------------------------------------------------

class TestSkillVectorDirGaps:
    def test_nan_breaks_mdpo_streak(self, tmp_path):
        """If the series has a NaN in the middle of positive-outlier run,
        the max duration should NOT span the gap."""
        _write_error_ranges(tmp_path)
        # 5 × positive-outlier, NaN, 5 × positive-outlier
        # Threshold is 22.5°, outlier limit = 2*X1 = 45°, so 60° diff is outlier.
        obs = [0.0] * 11
        ofs = [60.0, 60.0, 60.0, 60.0, 60.0, np.nan, 60.0, 60.0, 60.0, 60.0, 60.0]
        df = _build_df(obs, ofs)
        prop = _make_prop(str(tmp_path))
        result = skill_vector_dir(df, 'cu', prop, getLogger('test'))
        mdpo = result[11]  # slot 11 per schema
        # With gap-preservation: max run = 5 samples × 1h = 5h.
        # Without gap-preservation (dropna first): 10h.
        assert mdpo == pytest.approx(5.0, abs=0.01), \
            f'NaN should break the outlier streak; got mdpo={mdpo}h'
