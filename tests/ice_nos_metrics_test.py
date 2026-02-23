"""End-to-end tests verifying nos_metrics produces identical results to the
original inline numpy/scipy calculations used in the ice skill assessment.

Each test creates synthetic ice-concentration-like data (2D arrays with NaN
land masks, values 0-100%) and confirms that the nos_metrics function output
matches the original inline formula exactly.  This guards against regressions
introduced by replacing raw numpy/scipy calls with nos_metrics wrappers in:

  - src/ofs_skill/obs_retrieval/find_ofs_ice_stations.py
  - bin/skill_assessment/do_iceskill.py
"""

import math
import warnings

import numpy as np
import pytest
from scipy.stats import pearsonr as scipy_pearsonr

from ofs_skill.skill_assessment import nos_metrics


# ---------------------------------------------------------------------------
# Helpers — build realistic ice arrays
# ---------------------------------------------------------------------------
def _make_ice_pair(n=200, nan_frac=0.3, seed=42):
    """Return (model, observed) 1-D arrays resembling ice concentration data.

    Values in [0, 100] with a sprinkle of NaN (simulating land mask).
    """
    rng = np.random.RandomState(seed)
    obs = rng.uniform(0, 100, size=n)
    mod = obs + rng.normal(0, 10, size=n)
    mod = np.clip(mod, 0, 100)
    # Inject NaNs at the same positions (land mask)
    nan_idx = rng.choice(n, size=int(n * nan_frac), replace=False)
    obs[nan_idx] = np.nan
    mod[nan_idx] = np.nan
    return mod, obs


def _make_ice_2d(shape=(30, 50, 60), nan_frac=0.2, seed=99):
    """Return (model_3d, observed_3d) arrays shaped (time, lat, lon).

    Used to test that 2D (axis=0) RMSE is intentionally NOT replaced.
    """
    rng = np.random.RandomState(seed)
    obs = rng.uniform(0, 100, size=shape)
    mod = obs + rng.normal(0, 8, size=shape)
    mod = np.clip(mod, 0, 100)
    # Land mask — same spatial pattern every time step
    land = rng.random(shape[1:]) < nan_frac
    obs[:, land] = np.nan
    mod[:, land] = np.nan
    return mod, obs


# ---------------------------------------------------------------------------
# RMSE — 1-D (scalars), the pattern replaced in both ice files
# ---------------------------------------------------------------------------
class TestIceRmse1D:
    """Verify nos_metrics.rmse matches the original inline formula:
        np.sqrt(np.nanmean((mod - obs)**2))
    """

    def test_matches_inline_formula(self):
        mod, obs = _make_ice_pair()
        expected = np.sqrt(np.nanmean((mod - obs) ** 2))
        result = nos_metrics.rmse(mod, obs)
        assert pytest.approx(result, rel=1e-12) == expected

    def test_all_nan_returns_nan(self):
        mod = np.array([np.nan, np.nan, np.nan])
        obs = np.array([np.nan, np.nan, np.nan])
        result = nos_metrics.rmse(mod, obs)
        assert math.isnan(result)

    def test_no_nan_matches(self):
        mod = np.array([10.0, 20.0, 30.0])
        obs = np.array([12.0, 18.0, 33.0])
        expected = np.sqrt(np.nanmean((mod - obs) ** 2))
        assert pytest.approx(nos_metrics.rmse(mod, obs), rel=1e-12) == expected

    def test_single_element(self):
        assert nos_metrics.rmse([50.0], [45.0]) == 5.0

    def test_with_mixed_nan(self):
        """NaN at different positions — nanmean should skip them."""
        mod = np.array([10.0, np.nan, 30.0, 40.0])
        obs = np.array([12.0, 25.0, np.nan, 38.0])
        expected = np.sqrt(np.nanmean((mod - obs) ** 2))
        assert pytest.approx(nos_metrics.rmse(mod, obs), rel=1e-12) == expected


# ---------------------------------------------------------------------------
# RMSE — 2-D with axis=0 is intentionally NOT replaced (returns array)
# ---------------------------------------------------------------------------
class TestIceRmse2DNotReplaced:
    """Confirm that the 2D spatial RMSE pattern (axis=0) returns an array,
    which is why it was left as inline numpy in do_iceskill.py line ~1004.
    """

    def test_2d_rmse_returns_array(self):
        mod, obs = _make_ice_2d()
        diff = mod - obs
        rmse_2d = np.sqrt(np.nanmean(diff ** 2, axis=0))
        assert rmse_2d.shape == mod.shape[1:]
        assert not np.isscalar(rmse_2d)


# ---------------------------------------------------------------------------
# Pearson R — the pattern replaced in find_ofs_ice_stations.py
# ---------------------------------------------------------------------------
class TestIcePearsonR:
    """Verify nos_metrics.pearson_r matches:
        stats.pearsonr(obs_series_nan, mod_series_nan)[0]
    """

    def test_matches_scipy_pearsonr(self):
        mod, obs = _make_ice_pair(nan_frac=0.0)  # no NaN for pearsonr
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            expected = scipy_pearsonr(obs, mod)[0]
        result = nos_metrics.pearson_r(mod, obs)
        assert pytest.approx(result, rel=1e-12) == expected

    def test_nan_free_subset(self):
        """Reproduce the ice code pattern: filter NaN, then correlate."""
        mod, obs = _make_ice_pair()
        mask = ~np.isnan(obs) & ~np.isnan(mod)
        mod_clean = mod[mask]
        obs_clean = obs[mask]
        expected = scipy_pearsonr(obs_clean, mod_clean)[0]
        result = nos_metrics.pearson_r(mod_clean, obs_clean)
        assert pytest.approx(result, rel=1e-12) == expected


# ---------------------------------------------------------------------------
# Mean bias — the pattern replaced in find_ofs_ice_stations.py
# ---------------------------------------------------------------------------
class TestIceMeanBias:
    """Verify nos_metrics.mean_bias matches:
        np.nanmean(mod_series - obs_series)
    """

    def test_matches_inline_formula(self):
        mod, obs = _make_ice_pair()
        errors = mod - obs
        expected = float(np.nanmean(errors))
        result = nos_metrics.mean_bias(errors)
        assert pytest.approx(result, rel=1e-12) == expected

    def test_with_all_nan(self):
        errors = np.array([np.nan, np.nan])
        assert math.isnan(nos_metrics.mean_bias(errors))


# ---------------------------------------------------------------------------
# Standard deviation — the pattern replaced in find_ofs_ice_stations.py
# ---------------------------------------------------------------------------
class TestIceStandardDeviation:
    """Verify nos_metrics.standard_deviation matches:
        np.nanstd(mod_series - obs_series)  (ddof=0)
    """

    def test_matches_inline_formula(self):
        mod, obs = _make_ice_pair()
        errors = mod - obs
        expected = float(np.nanstd(errors))
        result = nos_metrics.standard_deviation(errors)
        assert pytest.approx(result, rel=1e-12) == expected

    def test_single_value(self):
        assert nos_metrics.standard_deviation([5.0]) == 0.0


# ---------------------------------------------------------------------------
# Rounding preservation — ice code wraps metrics in np.round(..., 2)
# ---------------------------------------------------------------------------
class TestIceRounding:
    """The ice call sites wrap nos_metrics results in np.round(..., 2).
    Verify this produces the same values as rounding the original formula.
    """

    def test_rmse_rounded(self):
        mod, obs = _make_ice_pair()
        inline = np.round(np.sqrt(np.nanmean((mod - obs) ** 2)), 2)
        via_metrics = np.round(nos_metrics.rmse(mod, obs), 2)
        assert inline == via_metrics

    def test_bias_rounded(self):
        mod, obs = _make_ice_pair()
        errors = mod - obs
        inline = np.round(np.nanmean(errors), 2)
        via_metrics = np.round(nos_metrics.mean_bias(errors), 2)
        assert inline == via_metrics

    def test_stdev_rounded(self):
        mod, obs = _make_ice_pair()
        errors = mod - obs
        inline = np.round(np.nanstd(errors), 2)
        via_metrics = np.round(nos_metrics.standard_deviation(errors), 2)
        assert inline == via_metrics

    def test_pearson_r_rounded(self):
        mod, obs = _make_ice_pair(nan_frac=0.0)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            inline = np.round(scipy_pearsonr(obs, mod)[0], decimals=2)
        via_metrics = np.round(nos_metrics.pearson_r(mod, obs), decimals=2)
        assert inline == via_metrics


# ---------------------------------------------------------------------------
# Full 1-D station pipeline — mimics find_ofs_ice_stations.py loop body
# ---------------------------------------------------------------------------
class TestIceStationPipeline:
    """Reproduce the exact calculation sequence from find_ofs_ice_stations.py
    and verify all four metrics match when computed via nos_metrics.
    """

    def test_station_loop_equivalence(self):
        mod, obs = _make_ice_pair(n=365, nan_frac=0.25, seed=7)

        # --- Original inline code path ---
        badnans = ~np.isnan(obs) * ~np.isnan(mod)
        obs_nan = obs[badnans]
        mod_nan = mod[badnans]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            r_orig = scipy_pearsonr(obs_nan, mod_nan)[0]
        rmse_orig = np.sqrt(np.nanmean((mod - obs) ** 2))
        bias_orig = np.nanmean(mod - obs)
        stdev_orig = np.nanstd(mod - obs)

        # --- New nos_metrics code path ---
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            r_new = nos_metrics.pearson_r(mod_nan, obs_nan)
        rmse_new = nos_metrics.rmse(mod, obs)
        bias_new = nos_metrics.mean_bias(mod - obs)
        stdev_new = nos_metrics.standard_deviation(mod - obs)

        assert pytest.approx(r_new, rel=1e-12) == r_orig
        assert pytest.approx(rmse_new, rel=1e-12) == rmse_orig
        assert pytest.approx(bias_new, rel=1e-12) == bias_orig
        assert pytest.approx(stdev_new, rel=1e-12) == stdev_orig


# ---------------------------------------------------------------------------
# do_iceskill 2D daily loop — mimics the rmse_all / rmse_either pattern
# ---------------------------------------------------------------------------
class TestIceDailyLoopRmse:
    """Reproduce the rmse_all and rmse_either patterns from do_iceskill.py
    (lines ~801 and ~817) and verify nos_metrics.rmse matches.
    """

    def test_rmse_all_daily_loop(self):
        """Simulates the rmse_all append inside the daily time loop."""
        mod_3d, obs_3d = _make_ice_2d(shape=(10, 40, 50))
        rmse_all_orig = []
        rmse_all_new = []

        for i in range(mod_3d.shape[0]):
            m = mod_3d[i]
            o = obs_3d[i]
            # Original pattern
            rmse_all_orig.append(np.sqrt(np.nanmean((m - o) ** 2)))
            # New pattern
            rmse_all_new.append(nos_metrics.rmse(m, o))

        for orig, new in zip(rmse_all_orig, rmse_all_new):
            assert pytest.approx(new, rel=1e-12) == orig

    def test_rmse_either_with_masked_data(self):
        """Simulates rmse_either where open water is masked out."""
        mod_3d, obs_3d = _make_ice_2d(shape=(10, 40, 50))
        stathresh = 1

        rmse_either_orig = []
        rmse_either_new = []

        for i in range(mod_3d.shape[0]):
            m = np.array(mod_3d[i])
            o = np.array(obs_3d[i])
            # Apply threshold mask (like make_2d_mask in do_iceskill.py)
            combined = m + o
            m[combined < stathresh] = np.nan
            o[combined < stathresh] = np.nan

            rmse_either_orig.append(np.sqrt(np.nanmean((m - o) ** 2)))
            rmse_either_new.append(nos_metrics.rmse(m, o))

        for orig, new in zip(rmse_either_orig, rmse_either_new):
            assert pytest.approx(new, rel=1e-12) == orig
