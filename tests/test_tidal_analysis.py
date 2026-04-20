"""
Unit tests for the tidal_analysis subpackage.

Tests cover:
- Phase 1: Constituent definitions, preprocessing, harmonic analysis,
  tidal prediction
- Phase 2: Filtering, extrema extraction, current analysis, persistence
  forecasting, harmonic constant comparison
"""
import numpy as np
import pandas as pd
import pytest

# -----------------------------------------------------------------------
# Constituent tests
# -----------------------------------------------------------------------

class TestConstituents:
    """Tests for constituents.py definitions."""

    def test_nos_37_count(self):
        """NOS_37_CONSTITUENTS must contain exactly 37 entries."""
        from ofs_skill.tidal_analysis.constituents import NOS_37_CONSTITUENTS
        assert len(NOS_37_CONSTITUENTS) == 37

    def test_nos_37_unique(self):
        """All 37 constituent names must be unique."""
        from ofs_skill.tidal_analysis.constituents import NOS_37_CONSTITUENTS
        assert len(set(NOS_37_CONSTITUENTS)) == 37

    def test_speeds_cover_all_37(self):
        """CONSTITUENT_SPEEDS must have an entry for every NOS constituent."""
        from ofs_skill.tidal_analysis.constituents import (
            CONSTITUENT_SPEEDS,
            NOS_37_CONSTITUENTS,
        )
        missing = [c for c in NOS_37_CONSTITUENTS if c not in CONSTITUENT_SPEEDS]
        assert missing == [], f'Missing speeds for: {missing}'

    def test_m2_speed(self):
        """M2 speed must be approximately 28.984 deg/hr."""
        from ofs_skill.tidal_analysis.constituents import CONSTITUENT_SPEEDS
        assert abs(CONSTITUENT_SPEEDS['M2'] - 28.9841042) < 1e-6

    def test_semidiurnal_speeds_range(self):
        """Semidiurnal constituents should have speeds near 28-31 deg/hr."""
        from ofs_skill.tidal_analysis.constituents import CONSTITUENT_SPEEDS
        semidiurnal = ['M2', 'S2', 'N2', 'K2', '2N2', 'MU2', 'NU2',
                       'L2', 'T2', 'R2', 'LDA2']
        for name in semidiurnal:
            speed = CONSTITUENT_SPEEDS[name]
            assert 27.0 < speed < 32.0, (
                f'{name} speed {speed} out of semidiurnal range'
            )


# -----------------------------------------------------------------------
# Preprocessing tests
# -----------------------------------------------------------------------

class TestPreprocessing:
    """Tests for preprocessing.py."""

    def test_equal_interval_basic(self):
        """to_equal_interval produces a regular 6-min grid."""
        from ofs_skill.tidal_analysis.preprocessing import to_equal_interval

        # Create irregular input (some 6-min, some 12-min gaps)
        times = pd.to_datetime([
            '2024-01-01 00:00', '2024-01-01 00:06', '2024-01-01 00:12',
            '2024-01-01 00:24',  # 12-min gap
            '2024-01-01 00:30', '2024-01-01 00:36',
        ])
        values = np.array([1.0, 1.1, 1.2, 1.4, 1.5, 1.6])

        new_idx, new_vals = to_equal_interval(times, values, '6min')

        # Output should be on a regular 6-min grid
        expected_len = 7  # 00:00 through 00:36 at 6-min = 7 points
        assert len(new_idx) == expected_len
        assert len(new_vals) == expected_len

        # The gap at 00:18 should be filled (< 6 hr)
        assert np.isfinite(new_vals).all()

    def test_large_gap_preserved(self):
        """Gaps larger than max_gap_hours remain as NaN."""
        from ofs_skill.tidal_analysis.preprocessing import to_equal_interval

        # 8-hour gap in the middle
        t1 = pd.date_range('2024-01-01 00:00', periods=10, freq='6min')
        t2 = pd.date_range('2024-01-01 08:00', periods=10, freq='6min')
        times = t1.append(t2)
        values = np.concatenate([np.ones(10), np.ones(10) * 2.0])

        new_idx, new_vals = to_equal_interval(
            times, values, '6min', max_gap_hours=6.0
        )

        # Should have NaN in the large gap region
        assert np.any(np.isnan(new_vals))

    def test_length_mismatch_raises(self):
        """Mismatched time/values lengths raise ValueError."""
        from ofs_skill.tidal_analysis.preprocessing import to_equal_interval

        times = pd.date_range('2024-01-01', periods=5, freq='6min')
        values = np.ones(3)

        with pytest.raises(ValueError, match='same length'):
            to_equal_interval(times, values)

    def test_too_few_points_raises(self):
        """Fewer than 2 points raises ValueError."""
        from ofs_skill.tidal_analysis.preprocessing import to_equal_interval

        times = pd.DatetimeIndex(['2024-01-01'])
        values = np.array([1.0])

        with pytest.raises(ValueError, match='two data points'):
            to_equal_interval(times, values)


# -----------------------------------------------------------------------
# Harmonic analysis tests
# -----------------------------------------------------------------------

class TestHarmonicAnalysis:
    """Tests for harmonic_analysis.py using synthetic signals."""

    @staticmethod
    def _make_synthetic_signal(
        duration_days=30, dt_minutes=6, m2_amp=0.5, k1_amp=0.2,
        m2_phase_deg=45.0, k1_phase_deg=120.0, mean_level=1.0
    ):
        """Create a synthetic M2+K1 tidal signal."""
        n_points = int(duration_days * 24 * 60 / dt_minutes)
        times = pd.date_range(
            '2024-01-01', periods=n_points, freq=f'{dt_minutes}min'
        )
        hours = np.arange(n_points) * dt_minutes / 60.0

        # M2: 28.9841042 deg/hr, K1: 15.0410686 deg/hr
        m2_speed = 28.9841042
        k1_speed = 15.0410686

        signal = (
            mean_level
            + m2_amp * np.cos(np.radians(m2_speed * hours - m2_phase_deg))
            + k1_amp * np.cos(np.radians(k1_speed * hours - k1_phase_deg))
        )
        return times, signal, hours

    def test_synthetic_m2_k1_recovery(self):
        """HA on a clean M2+K1 signal recovers amplitudes within 5%."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        m2_amp, k1_amp = 0.5, 0.2
        times, signal, _ = self._make_synthetic_signal(
            m2_amp=m2_amp, k1_amp=k1_amp
        )

        result = harmonic_analysis(
            time=times, values=signal, latitude=37.0,
            constit=['M2', 'K1'],
        )

        df = result['constituents']
        m2_row = df[df['Name'] == 'M2'].iloc[0]
        k1_row = df[df['Name'] == 'K1'].iloc[0]

        # M2 amplitude within 5%
        assert abs(m2_row['Amplitude'] - m2_amp) / m2_amp < 0.05, (
            f"M2 amp {m2_row['Amplitude']:.4f} vs expected {m2_amp}"
        )
        # K1 tolerance is wider (~15%) because K1/P1 are close in
        # frequency and 30 days is near the Rayleigh resolution limit.
        assert abs(k1_row['Amplitude'] - k1_amp) / k1_amp < 0.15, (
            f"K1 amp {k1_row['Amplitude']:.4f} vs expected {k1_amp}"
        )

    def test_mean_level_recovery(self):
        """HA recovers mean water level (H0)."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        mean_level = 1.5
        times, signal, _ = self._make_synthetic_signal(mean_level=mean_level)

        result = harmonic_analysis(
            time=times, values=signal, latitude=37.0,
            constit=['M2', 'K1'],
        )

        assert abs(result['mean'] - mean_level) < 0.01, (
            f"Mean {result['mean']:.4f} vs expected {mean_level}"
        )

    def test_method_classification(self):
        """Method label reflects record length."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        # 30-day record -> "standard"
        times, signal, _ = self._make_synthetic_signal(duration_days=30)
        result = harmonic_analysis(
            time=times, values=signal, latitude=37.0,
            constit=['M2', 'K1'],
        )
        assert result['method_used'] == 'standard'

    def test_short_record_raises(self):
        """Record shorter than min_duration_days raises ValueError."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        times, signal, _ = self._make_synthetic_signal(duration_days=10)

        with pytest.raises(ValueError, match='less than the minimum'):
            harmonic_analysis(
                time=times, values=signal, latitude=37.0,
                min_duration_days=15.0,
            )

    def test_empty_data_raises(self):
        """All-NaN values raise ValueError."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        times = pd.date_range('2024-01-01', periods=1000, freq='6min')
        values = np.full(1000, np.nan)

        with pytest.raises(ValueError, match='no finite data'):
            harmonic_analysis(time=times, values=values, latitude=37.0)


# -----------------------------------------------------------------------
# Tidal prediction tests
# -----------------------------------------------------------------------

class TestTidalPrediction:
    """Tests for tidal_prediction.py."""

    @staticmethod
    def _make_synthetic_and_analyze(duration_days=30):
        """Analyze a synthetic signal and return (times, original, result)."""
        from ofs_skill.tidal_analysis.harmonic_analysis import harmonic_analysis

        n_points = int(duration_days * 24 * 60 / 6)
        times = pd.date_range('2024-01-01', periods=n_points, freq='6min')
        hours = np.arange(n_points) * 0.1  # 6 min = 0.1 hr

        m2_amp, k1_amp = 0.5, 0.2
        mean_level = 1.0
        signal = (
            mean_level
            + m2_amp * np.cos(np.radians(28.9841042 * hours - 45.0))
            + k1_amp * np.cos(np.radians(15.0410686 * hours - 120.0))
        )

        result = harmonic_analysis(
            time=times, values=signal, latitude=37.0,
            constit=['M2', 'K1'],
        )
        return times, signal, result

    def test_predict_tide_reconstruction(self):
        """predict_tide reconstructs the original signal (RMSE < 0.01)."""
        from ofs_skill.tidal_analysis.tidal_prediction import predict_tide

        times, original, result = self._make_synthetic_and_analyze()
        predicted = predict_tide(times, result['coef'])

        rmse = np.sqrt(np.mean((predicted - original) ** 2))
        assert rmse < 0.01, f'RMSE {rmse:.4f} exceeds 0.01'

    def test_predict_from_constants_m2(self):
        """predict_from_constants with M2 only produces a tidal signal."""
        from ofs_skill.tidal_analysis.tidal_prediction import predict_from_constants

        times = pd.date_range('2024-01-01', periods=240, freq='6min')  # 1 day

        predicted = predict_from_constants(
            time=times,
            amplitudes={'M2': 0.5},
            phases={'M2': 45.0},
            mean_level=1.0,
            latitude=37.0,
        )

        assert len(predicted) == 240
        assert np.isfinite(predicted).all()
        # Signal should oscillate around mean_level=1.0
        assert abs(np.mean(predicted) - 1.0) < 0.1
        # Amplitude should be roughly 0.5
        assert (np.max(predicted) - np.min(predicted)) / 2.0 > 0.3

    def test_predict_from_constants_empty_raises(self):
        """Empty amplitude/phase dicts raise ValueError."""
        from ofs_skill.tidal_analysis.tidal_prediction import predict_from_constants

        times = pd.date_range('2024-01-01', periods=10, freq='6min')

        with pytest.raises(ValueError, match='No common constituents'):
            predict_from_constants(
                time=times,
                amplitudes={},
                phases={},
                mean_level=0.0,
                latitude=37.0,
            )


# -----------------------------------------------------------------------
# Import tests
# -----------------------------------------------------------------------

class TestImports:
    """Verify the public API imports work correctly."""

    def test_import_harmonic_analysis(self):
        from ofs_skill.tidal_analysis import harmonic_analysis
        assert callable(harmonic_analysis)

    def test_import_predict_tide(self):
        from ofs_skill.tidal_analysis import predict_tide
        assert callable(predict_tide)

    def test_import_predict_from_constants(self):
        from ofs_skill.tidal_analysis import predict_from_constants
        assert callable(predict_from_constants)

    def test_import_to_equal_interval(self):
        from ofs_skill.tidal_analysis import to_equal_interval
        assert callable(to_equal_interval)

    def test_import_nos_37(self):
        from ofs_skill.tidal_analysis import NOS_37_CONSTITUENTS
        assert len(NOS_37_CONSTITUENTS) == 37

    def test_import_speeds(self):
        from ofs_skill.tidal_analysis import CONSTITUENT_SPEEDS
        assert 'M2' in CONSTITUENT_SPEEDS

    # Phase 2 import tests

    def test_import_fourier_lowpass_filter(self):
        from ofs_skill.tidal_analysis import fourier_lowpass_filter
        assert callable(fourier_lowpass_filter)

    def test_import_butterworth_lowpass(self):
        from ofs_skill.tidal_analysis import butterworth_lowpass
        assert callable(butterworth_lowpass)

    def test_import_compute_nontidal_residual(self):
        from ofs_skill.tidal_analysis import compute_nontidal_residual
        assert callable(compute_nontidal_residual)

    def test_import_extract_water_level_extrema(self):
        from ofs_skill.tidal_analysis import extract_water_level_extrema
        assert callable(extract_water_level_extrema)

    def test_import_extract_current_extrema(self):
        from ofs_skill.tidal_analysis import extract_current_extrema
        assert callable(extract_current_extrema)

    def test_import_find_slack_water(self):
        from ofs_skill.tidal_analysis import find_slack_water
        assert callable(find_slack_water)

    def test_import_compute_principal_direction(self):
        from ofs_skill.tidal_analysis import compute_principal_direction
        assert callable(compute_principal_direction)

    def test_import_current_harmonic_analysis(self):
        from ofs_skill.tidal_analysis import current_harmonic_analysis
        assert callable(current_harmonic_analysis)

    def test_import_build_persistence_forecast(self):
        from ofs_skill.tidal_analysis import build_persistence_forecast
        assert callable(build_persistence_forecast)

    def test_import_compare_harmonic_constants(self):
        from ofs_skill.tidal_analysis import compare_harmonic_constants
        assert callable(compare_harmonic_constants)

    def test_import_build_constituent_table(self):
        from ofs_skill.tidal_analysis import build_constituent_table
        assert callable(build_constituent_table)

    def test_import_write_constituent_table_csv(self):
        from ofs_skill.tidal_analysis import write_constituent_table_csv
        assert callable(write_constituent_table_csv)

    def test_import_compute_constituent_summary_stats(self):
        from ofs_skill.tidal_analysis import compute_constituent_summary_stats
        assert callable(compute_constituent_summary_stats)

    def test_import_flag_constituent_exceedances(self):
        from ofs_skill.tidal_analysis import flag_constituent_exceedances
        assert callable(flag_constituent_exceedances)

    def test_import_compute_prediction_verification(self):
        from ofs_skill.tidal_analysis import compute_prediction_verification
        assert callable(compute_prediction_verification)


# =======================================================================
# Phase 2 Tests
# =======================================================================

# -----------------------------------------------------------------------
# Filtering tests
# -----------------------------------------------------------------------

class TestFiltering:
    """Tests for filtering.py."""

    def test_fourier_lowpass_removes_tidal(self):
        """Fourier LP filter removes M2 tidal signal, preserves DC offset."""
        from ofs_skill.tidal_analysis.filtering import fourier_lowpass_filter

        # 10 days of 6-min data: DC=2.0 + M2 tidal signal
        dt_hours = 0.1
        n = int(10 * 24 / dt_hours)
        hours = np.arange(n) * dt_hours
        m2_speed = 28.9841042  # deg/hr
        dc = 2.0
        signal = dc + 0.5 * np.cos(np.radians(m2_speed * hours))

        filtered = fourier_lowpass_filter(signal, dt_hours, cutoff_hours=25.0)

        # The filtered result should be close to the DC level
        assert abs(np.mean(filtered) - dc) < 0.01
        # The tidal oscillation should be essentially removed
        assert np.std(filtered) < 0.05

    def test_butterworth_lowpass_removes_tidal(self):
        """Butterworth LP filter removes M2 tidal signal, preserves DC."""
        from ofs_skill.tidal_analysis.filtering import butterworth_lowpass

        dt_hours = 0.1
        n = int(10 * 24 / dt_hours)
        hours = np.arange(n) * dt_hours
        m2_speed = 28.9841042
        dc = 2.0
        signal = dc + 0.5 * np.cos(np.radians(m2_speed * hours))

        filtered = butterworth_lowpass(signal, dt_hours, cutoff_hours=25.0)

        # Interior points should be close to DC (skip edges for Butterworth)
        interior = filtered[100:-100]
        assert abs(np.mean(interior) - dc) < 0.05
        assert np.std(interior) < 0.05

    def test_nontidal_residual(self):
        """compute_nontidal_residual returns observed - predicted."""
        from ofs_skill.tidal_analysis.filtering import compute_nontidal_residual

        observed = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        predicted = np.array([0.8, 1.9, 3.1, 3.8, 5.2])
        residual = compute_nontidal_residual(observed, predicted)

        expected = observed - predicted
        np.testing.assert_allclose(residual, expected)

    def test_nontidal_residual_length_mismatch(self):
        """Mismatched lengths raise ValueError."""
        from ofs_skill.tidal_analysis.filtering import compute_nontidal_residual

        with pytest.raises(ValueError, match='same length'):
            compute_nontidal_residual(np.ones(5), np.ones(3))


# -----------------------------------------------------------------------
# Extremes tests
# -----------------------------------------------------------------------

class TestExtremes:
    """Tests for extremes.py."""

    def test_water_level_extrema_count(self):
        """Pure M2 cosine over 2 days → ~4 HW and ~4 LW."""
        from ofs_skill.tidal_analysis.extremes import extract_water_level_extrema

        # 2 days of 6-min data with M2 signal (~2 cycles/day)
        dt_hours = 0.1
        n = int(2 * 24 / dt_hours)
        times = pd.date_range('2024-01-01', periods=n, freq='6min')
        hours = np.arange(n) * dt_hours
        m2_speed = 28.9841042
        wl = np.cos(np.radians(m2_speed * hours))

        result = extract_water_level_extrema(
            times.values, wl, min_separation_hours=4.0
        )

        # M2 period ~12.42h → ~2 cycles/day → ~4 HW and ~4 LW over 2 days
        assert 3 <= len(result['high_water_times']) <= 5
        assert 3 <= len(result['low_water_times']) <= 5

    def test_water_level_extrema_amplitudes(self):
        """Peaks of a unit cosine should be near ±1."""
        from ofs_skill.tidal_analysis.extremes import extract_water_level_extrema

        dt_hours = 0.1
        n = int(3 * 24 / dt_hours)
        times = pd.date_range('2024-01-01', periods=n, freq='6min')
        hours = np.arange(n) * dt_hours
        m2_speed = 28.9841042
        wl = np.cos(np.radians(m2_speed * hours))

        result = extract_water_level_extrema(
            times.values, wl, min_separation_hours=4.0
        )

        # High water peaks should be close to 1.0
        for amp in result['high_water_amplitudes']:
            assert amp > 0.9, f'HW amplitude {amp} too low'

        # Low water troughs should be close to -1.0
        for amp in result['low_water_amplitudes']:
            assert amp < -0.9, f'LW amplitude {amp} too high'

    def test_find_slack_water_basic(self):
        """Current speed with known zero crossings produces slack events."""
        from ofs_skill.tidal_analysis.extremes import find_slack_water

        # Create a speed signal that oscillates, with zeros near t=6h, 12h
        dt_hours = 0.1
        n = int(24 / dt_hours)
        times = pd.date_range('2024-01-01', periods=n, freq='6min')
        hours = np.arange(n) * dt_hours
        # |sin| so speed is always non-negative, with minima near 0
        speed_ms = 1.0 * np.abs(np.sin(2 * np.pi * hours / 12.0))

        result = find_slack_water(
            times.values, speed_ms, threshold_knots=0.5
        )

        # Should find slack events near t=0, 6h, 12h, 18h, 24h
        assert len(result['slack_events']) >= 2


# -----------------------------------------------------------------------
# Current analysis tests
# -----------------------------------------------------------------------

class TestCurrentAnalysis:
    """Tests for current_analysis.py."""

    def test_principal_direction(self):
        """Synthetic flow along 45° (NE) axis recovers ~45°."""
        from ofs_skill.tidal_analysis.current_analysis import (
            compute_principal_direction,
        )

        np.random.seed(42)
        n = 1000
        # Flow primarily along 45° (NE): equal u and v
        along = np.random.normal(0, 1.0, n)
        cross = np.random.normal(0, 0.1, n)  # small cross-axis noise
        angle = np.radians(45.0)
        u = along * np.sin(angle) + cross * np.cos(angle)
        v = along * np.cos(angle) - cross * np.sin(angle)

        direction = compute_principal_direction(u, v)

        # Should be near 45° or 225° (ambiguous 180° flip)
        diff = min(abs(direction - 45.0), abs(direction - 225.0))
        assert diff < 10.0, (
            f'Principal direction {direction:.1f}° not near 45° or 225°'
        )

    def test_current_harmonic_analysis(self):
        """2D HA on synthetic M2 current recovers ellipse parameters."""
        from ofs_skill.tidal_analysis.current_analysis import (
            current_harmonic_analysis,
        )

        # 30 days of 6-min data with M2 current along 0° (N-S)
        dt_minutes = 6
        n = int(30 * 24 * 60 / dt_minutes)
        times = pd.date_range('2024-01-01', periods=n, freq=f'{dt_minutes}min')
        hours = np.arange(n) * dt_minutes / 60.0
        m2_speed = 28.9841042

        # Semimajor along N-S (v), small semiminor along E-W (u)
        v = 0.5 * np.cos(np.radians(m2_speed * hours))
        u = 0.1 * np.sin(np.radians(m2_speed * hours))

        result = current_harmonic_analysis(
            time=times, u=u, v=v, latitude=37.0, constit=['M2'],
        )

        assert 'ellipses' in result
        assert 'principal_direction' in result
        df = result['ellipses']
        m2_row = df[df['Name'] == 'M2'].iloc[0]

        # Semimajor axis should be close to 0.5
        assert abs(m2_row['Lsmaj'] - 0.5) < 0.1, (
            f"Lsmaj {m2_row['Lsmaj']:.3f} not near 0.5"
        )


# -----------------------------------------------------------------------
# Persistence tests
# -----------------------------------------------------------------------

class TestPersistence:
    """Tests for persistence.py."""

    def test_persistence_forecast_constant_offset(self):
        """Constant residual → forecast = tide + constant offset."""
        from ofs_skill.tidal_analysis.persistence import (
            build_persistence_forecast,
        )

        n = 480  # 2 days at 6-min intervals
        times = pd.date_range('2024-01-01', periods=n, freq='6min')
        dt_hours = 0.1
        hours = np.arange(n) * dt_hours

        # Tide: simple cosine
        tide = np.cos(2 * np.pi * hours / 12.42)
        # Observed = tide + constant offset of 0.3
        offset = 0.3
        observed = tide + offset

        forecast = build_persistence_forecast(
            times.values, observed, tide,
            forecast_horizon_hours=24.0, offset_window_hours=6.0,
        )

        # After the initial window, the forecast should equal tide + offset
        # Check a point well past the window
        valid = np.isfinite(forecast)
        assert np.any(valid), 'No valid forecast points'
        # Where valid, forecast should be close to tide + offset
        valid_idx = np.where(valid)[0]
        mid = valid_idx[len(valid_idx) // 2]
        assert abs(forecast[mid] - (tide[mid] + offset)) < 0.05, (
            f'Forecast {forecast[mid]:.3f} vs expected '
            f'{tide[mid] + offset:.3f}'
        )

    def test_persistence_forecast_shape(self):
        """Output has same shape as input."""
        from ofs_skill.tidal_analysis.persistence import (
            build_persistence_forecast,
        )

        n = 240
        times = pd.date_range('2024-01-01', periods=n, freq='6min')
        observed = np.ones(n)
        tide = np.ones(n) * 0.5

        forecast = build_persistence_forecast(
            times.values, observed, tide,
        )

        assert forecast.shape == (n,)


# -----------------------------------------------------------------------
# HA Comparison tests
# -----------------------------------------------------------------------

class TestHAComparison:
    """Tests for ha_comparison.py."""

    def test_compare_identical(self):
        """Identical constants → zero diffs everywhere."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compare_harmonic_constants,
        )

        amps = np.array([0.5, 0.2, 0.1])
        phases = np.array([45.0, 120.0, 200.0])
        names = ['M2', 'K1', 'O1']

        df = compare_harmonic_constants(
            amps, phases, amps, phases, constituents=names,
        )

        np.testing.assert_allclose(df['Amp_Diff'].values, 0.0, atol=1e-10)
        np.testing.assert_allclose(df['Phase_Diff'].values, 0.0, atol=1e-10)
        np.testing.assert_allclose(df['Vector_Diff'].values, 0.0, atol=1e-10)

    def test_compare_known_diffs(self):
        """Known amplitude/phase differences → verify vector diff formula."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compare_harmonic_constants,
        )

        # Model: amp=0.6, phase=50°; Accepted: amp=0.5, phase=45°
        model_amp = np.array([0.6])
        model_phase = np.array([50.0])
        accepted_amp = np.array([0.5])
        accepted_phase = np.array([45.0])

        df = compare_harmonic_constants(
            model_amp, model_phase, accepted_amp, accepted_phase,
        )

        # Manual vector diff calculation
        delta_rad = np.radians(5.0)  # 50 - 45
        expected_vd = np.sqrt(0.6**2 + 0.5**2 - 2*0.6*0.5*np.cos(delta_rad))
        np.testing.assert_allclose(
            df['Vector_Diff'].values[0], expected_vd, atol=1e-10,
        )
        assert abs(df['Amp_Diff'].values[0] - 0.1) < 1e-10
        assert abs(df['Phase_Diff'].values[0] - 5.0) < 1e-10

    def test_phase_wrapping(self):
        """Phase differences wrap correctly across 360°."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compare_harmonic_constants,
        )

        # Model phase=10°, Accepted phase=350° → diff should be +20°
        # (10 - 350 = -340, wrapped to +20)
        model_amp = np.array([1.0])
        model_phase = np.array([10.0])
        accepted_amp = np.array([1.0])
        accepted_phase = np.array([350.0])

        df = compare_harmonic_constants(
            model_amp, model_phase, accepted_amp, accepted_phase,
        )

        # (10 - 350 + 180) % 360 - 180 = (-160) % 360 - 180 = 200 - 180 = 20
        assert abs(df['Phase_Diff'].values[0] - 20.0) < 1e-10


# -----------------------------------------------------------------------
# Constituent table tests
# -----------------------------------------------------------------------

class TestConstituentTable:
    """Tests for constituent_table.py."""

    @staticmethod
    def _make_synthetic_signal(
        duration_days=30, dt_minutes=6, m2_amp=0.5, k1_amp=0.2,
        m2_phase_deg=45.0, k1_phase_deg=120.0, mean_level=1.0,
    ):
        """Create a synthetic M2+K1 tidal signal."""
        n_points = int(duration_days * 24 * 60 / dt_minutes)
        times = pd.date_range(
            '2024-01-01', periods=n_points, freq=f'{dt_minutes}min'
        )
        hours = np.arange(n_points) * dt_minutes / 60.0
        m2_speed = 28.9841042
        k1_speed = 15.0410686
        signal = (
            mean_level
            + m2_amp * np.cos(np.radians(m2_speed * hours - m2_phase_deg))
            + k1_amp * np.cos(np.radians(k1_speed * hours - k1_phase_deg))
        )
        return times, signal

    def test_water_level_basic(self):
        """WL path: synthetic model + accepted constants → valid table."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50, 'K1': 0.20},
            'phases': {'M2': 45.0, 'K1': 120.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='TEST001',
            accepted_constants=accepted,
            constit=['M2', 'K1'],
        )

        assert len(table) == 2
        m2_row = table[table['Constituent'] == 'M2'].iloc[0]
        assert np.isfinite(m2_row['Model_Amp'])
        assert np.isfinite(m2_row['Ref_Amp'])
        # M2 model amp should be close to 0.5
        assert abs(m2_row['Model_Amp'] - 0.5) < 0.05

    def test_currents_basic(self):
        """Currents path: model and obs signals with known offset → diffs reflect offset."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal(
            m2_amp=0.5, m2_phase_deg=45.0,
        )
        # Obs signal with slightly different M2 amplitude
        obs_time, obs_values = self._make_synthetic_signal(
            m2_amp=0.48, m2_phase_deg=43.0,
        )

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='currents',
            station_id='TEST002',
            obs_time=obs_time,
            obs_values=obs_values,
            constit=['M2', 'K1'],
        )

        assert len(table) == 2
        m2_row = table[table['Constituent'] == 'M2'].iloc[0]
        # Amp diff should be small but nonzero
        assert np.isfinite(m2_row['Amp_Diff'])

    def test_missing_constituents_are_nan(self):
        """Accepted has only M2; K1 ref side should be NaN."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50},
            'phases': {'M2': 45.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='TEST003',
            accepted_constants=accepted,
            constit=['M2', 'K1'],
        )

        k1_row = table[table['Constituent'] == 'K1'].iloc[0]
        assert np.isnan(k1_row['Ref_Amp'])
        assert np.isnan(k1_row['Ref_Phase'])
        # Diffs involving NaN should also be NaN
        assert np.isnan(k1_row['Amp_Diff'])

    def test_column_names_and_order(self):
        """Columns are exactly the expected set in order."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50},
            'phases': {'M2': 45.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='TEST004',
            accepted_constants=accepted,
            constit=['M2', 'K1'],
        )

        expected_cols = [
            'N', 'Constituent', 'Ref_Amp', 'Ref_Phase',
            'Model_Amp', 'Model_Phase', 'Amp_Diff', 'Phase_Diff', 'Vector_Diff',
        ]
        assert list(table.columns) == expected_cols

    def test_n_column_one_based(self):
        """N column is 1-based sequential index."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        constit_list = ['M2', 'S2', 'N2', 'K1', 'O1']
        model_time, model_values = self._make_synthetic_signal(duration_days=30)
        accepted = {
            'amplitudes': {'M2': 0.50, 'K1': 0.20},
            'phases': {'M2': 45.0, 'K1': 120.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='TEST005',
            accepted_constants=accepted,
            constit=constit_list,
        )

        assert len(table) == len(constit_list)
        assert list(table['N']) == list(range(1, len(constit_list) + 1))

    def test_invalid_data_type_raises(self):
        """Invalid data_type raises ValueError."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()

        with pytest.raises(ValueError, match='data_type'):
            build_constituent_table(
                model_time=model_time,
                model_values=model_values,
                latitude=37.0,
                data_type='temperature',
                station_id='TEST006',
            )

    def test_wl_missing_accepted_raises(self):
        """WL with accepted_constants=None raises ValueError."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()

        with pytest.raises(ValueError, match='accepted_constants'):
            build_constituent_table(
                model_time=model_time,
                model_values=model_values,
                latitude=37.0,
                data_type='water_level',
                station_id='TEST007',
                accepted_constants=None,
            )

    def test_currents_missing_obs_raises(self):
        """Currents with obs_time=None raises ValueError."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
        )

        model_time, model_values = self._make_synthetic_signal()

        with pytest.raises(ValueError, match='obs_time'):
            build_constituent_table(
                model_time=model_time,
                model_values=model_values,
                latitude=37.0,
                data_type='currents',
                station_id='TEST008',
            )

    def test_write_csv_creates_file(self, tmp_path):
        """CSV file is created with header comments and parseable body."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
            write_constituent_table_csv,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50, 'K1': 0.20},
            'phases': {'M2': 45.0, 'K1': 120.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='8454000',
            accepted_constants=accepted,
            constit=['M2', 'K1'],
        )

        csv_path = tmp_path / 'test_harcon.csv'
        write_constituent_table_csv(
            table, str(csv_path), station_id='8454000', data_type='water_level',
        )

        assert csv_path.exists()

        # Read back and verify
        text = csv_path.read_text()
        assert text.startswith('# Station: 8454000')
        assert '# Data Type: water_level' in text

        # Parse CSV skipping comment lines
        lines = [ln for ln in text.splitlines() if not ln.startswith('#')]
        from io import StringIO
        reloaded = pd.read_csv(StringIO('\n'.join(lines)))
        assert len(reloaded) == 2
        assert 'Constituent' in reloaded.columns

    def test_write_csv_metadata(self, tmp_path):
        """Extra metadata dict appears in header comments."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
            write_constituent_table_csv,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50},
            'phases': {'M2': 45.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='8454000',
            accepted_constants=accepted,
            constit=['M2'],
        )

        csv_path = tmp_path / 'test_meta.csv'
        write_constituent_table_csv(
            table, str(csv_path),
            station_id='8454000',
            data_type='water_level',
            metadata={'OFS': 'CBOFS', 'Period': '2024-01-01 to 2024-01-31'},
        )

        text = csv_path.read_text()
        assert '# OFS: CBOFS' in text
        assert '# Period: 2024-01-01 to 2024-01-31' in text

    def test_write_csv_with_summary_stats(self, tmp_path):
        """Summary stats appear as footer comments in CSV."""
        from ofs_skill.tidal_analysis.constituent_table import (
            build_constituent_table,
            compute_constituent_summary_stats,
            write_constituent_table_csv,
        )

        model_time, model_values = self._make_synthetic_signal()
        accepted = {
            'amplitudes': {'M2': 0.50, 'K1': 0.20},
            'phases': {'M2': 45.0, 'K1': 120.0},
        }

        table = build_constituent_table(
            model_time=model_time,
            model_values=model_values,
            latitude=37.0,
            data_type='water_level',
            station_id='8454000',
            accepted_constants=accepted,
            constit=['M2', 'K1'],
        )
        stats = compute_constituent_summary_stats(table)

        csv_path = tmp_path / 'test_stats.csv'
        write_constituent_table_csv(
            table, str(csv_path),
            station_id='8454000',
            data_type='water_level',
            summary_stats=stats,
        )

        text = csv_path.read_text()
        assert '# mean_vector_diff:' in text
        assert '# n_valid:' in text


# -----------------------------------------------------------------------
# Prediction + Residual CSV tests
# -----------------------------------------------------------------------

class TestPredictionResidualCSV:
    """Tests for combined prediction/residual CSV output."""

    def test_combined_csv_columns(self, tmp_path):
        """Combined CSV has DateTime, Tidal_Prediction, Nontidal_Residual columns."""
        csv_path = tmp_path / 'test_pred_resid.csv'
        times = pd.date_range('2024-01-01', periods=10, freq='6min')
        prediction = np.random.rand(10)
        residual = np.random.rand(10)

        df = pd.DataFrame({
            'DateTime': times,
            'Tidal_Prediction': prediction,
            'Nontidal_Residual': residual,
        })
        df.to_csv(csv_path, index=False)

        reloaded = pd.read_csv(csv_path)
        assert list(reloaded.columns) == ['DateTime', 'Tidal_Prediction', 'Nontidal_Residual']
        assert len(reloaded) == 10

    def test_combined_csv_values_roundtrip(self, tmp_path):
        """Values survive a write/read roundtrip."""
        csv_path = tmp_path / 'test_roundtrip.csv'
        times = pd.date_range('2024-01-01', periods=5, freq='6min')
        prediction = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        residual = np.array([0.1, 0.2, 0.3, 0.4, 0.5])

        df = pd.DataFrame({
            'DateTime': times,
            'Tidal_Prediction': prediction,
            'Nontidal_Residual': residual,
        })
        df.to_csv(csv_path, index=False)

        reloaded = pd.read_csv(csv_path)
        np.testing.assert_allclose(
            reloaded['Tidal_Prediction'].values, prediction, atol=1e-10,
        )
        np.testing.assert_allclose(
            reloaded['Nontidal_Residual'].values, residual, atol=1e-10,
        )


# -----------------------------------------------------------------------
# Constituent summary stats tests
# -----------------------------------------------------------------------

class TestConstituentSummaryStats:
    """Tests for compute_constituent_summary_stats."""

    def test_basic_stats(self):
        """Summary stats are computed for a simple table."""
        from ofs_skill.tidal_analysis.constituent_table import (
            compute_constituent_summary_stats,
        )

        table = pd.DataFrame({
            'Constituent': ['M2', 'S2', 'K1', 'O1'],
            'Amp_Diff': [0.02, 0.03, 0.01, 0.04],
            'Phase_Diff': [5.0, 8.0, 3.0, 12.0],
            'Vector_Diff': [0.03, 0.04, 0.02, 0.05],
        })

        stats = compute_constituent_summary_stats(table)
        assert stats['n_valid'] == 4
        assert stats['n_major_valid'] == 4
        assert np.isfinite(stats['mean_vector_diff'])
        assert np.isfinite(stats['rmse_amp'])
        assert np.isfinite(stats['rmse_phase'])

    def test_major_filter(self):
        """Only major constituents counted in *_major stats."""
        from ofs_skill.tidal_analysis.constituent_table import (
            compute_constituent_summary_stats,
        )

        table = pd.DataFrame({
            'Constituent': ['M2', 'S2', 'MF', 'MM'],
            'Amp_Diff': [0.02, 0.03, 0.10, 0.08],
            'Phase_Diff': [5.0, 8.0, 30.0, 25.0],
            'Vector_Diff': [0.03, 0.04, 0.12, 0.10],
        })

        stats = compute_constituent_summary_stats(table)
        assert stats['n_valid'] == 4
        assert stats['n_major_valid'] == 2  # Only M2, S2

    def test_all_nan(self):
        """All-NaN vector diffs produce NaN stats."""
        from ofs_skill.tidal_analysis.constituent_table import (
            compute_constituent_summary_stats,
        )

        table = pd.DataFrame({
            'Constituent': ['M2', 'K1'],
            'Amp_Diff': [np.nan, np.nan],
            'Phase_Diff': [np.nan, np.nan],
            'Vector_Diff': [np.nan, np.nan],
        })

        stats = compute_constituent_summary_stats(table)
        assert stats['n_valid'] == 0
        assert np.isnan(stats['mean_vector_diff'])


# -----------------------------------------------------------------------
# Prediction verification tests
# -----------------------------------------------------------------------

class TestPredictionVerification:
    """Tests for compute_prediction_verification."""

    def test_identical_predictions(self):
        """Identical time series → RMSE=0, corr=1."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compute_prediction_verification,
        )

        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        result = compute_prediction_verification(values, values)

        assert abs(result['rmse']) < 1e-10
        assert abs(result['mean_diff']) < 1e-10
        assert abs(result['max_abs_diff']) < 1e-10
        assert abs(result['correlation'] - 1.0) < 1e-10

    def test_known_offset(self):
        """Constant offset → RMSE = |offset|, mean_diff = offset."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compute_prediction_verification,
        )

        model = np.array([1.0, 2.0, 3.0, 4.0])
        official = np.array([1.1, 2.1, 3.1, 4.1])  # offset of -0.1
        result = compute_prediction_verification(model, official)

        assert abs(result['rmse'] - 0.1) < 1e-10
        assert abs(result['mean_diff'] - (-0.1)) < 1e-10
        assert abs(result['correlation'] - 1.0) < 1e-6

    def test_length_mismatch_raises(self):
        """Mismatched arrays raise ValueError."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compute_prediction_verification,
        )

        with pytest.raises(ValueError, match='same length'):
            compute_prediction_verification(np.ones(5), np.ones(3))

    def test_all_nan(self):
        """All NaN → NaN results."""
        from ofs_skill.tidal_analysis.ha_comparison import (
            compute_prediction_verification,
        )

        result = compute_prediction_verification(
            np.full(5, np.nan), np.full(5, np.nan),
        )
        assert np.isnan(result['rmse'])


# -----------------------------------------------------------------------
# Exceedance flag tests
# -----------------------------------------------------------------------

class TestExceedanceFlags:
    """Tests for flag_constituent_exceedances."""

    def test_no_exceedances(self):
        """Small diffs → no flags."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2', 'K1'],
            'Amp_Diff': [0.01, 0.02],
            'Phase_Diff': [2.0, 3.0],
            'Vector_Diff': [0.01, 0.02],
        })

        result = flag_constituent_exceedances(table)
        assert all(result['Exceeds_Threshold'] == '')

    def test_amp_exceedance(self):
        """Large amplitude diff → AMP flag."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [0.10],
            'Phase_Diff': [2.0],
            'Vector_Diff': [0.02],
        })

        result = flag_constituent_exceedances(table)
        assert 'AMP' in result['Exceeds_Threshold'].iloc[0]

    def test_phase_exceedance(self):
        """Large phase diff → PHASE flag."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [0.01],
            'Phase_Diff': [15.0],
            'Vector_Diff': [0.02],
        })

        result = flag_constituent_exceedances(table)
        assert 'PHASE' in result['Exceeds_Threshold'].iloc[0]

    def test_vector_diff_exceedance(self):
        """Large vector diff → VD flag."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [0.01],
            'Phase_Diff': [2.0],
            'Vector_Diff': [0.10],
        })

        result = flag_constituent_exceedances(table)
        assert 'VD' in result['Exceeds_Threshold'].iloc[0]

    def test_multiple_exceedances(self):
        """Multiple thresholds exceeded → comma-separated codes."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [0.10],
            'Phase_Diff': [15.0],
            'Vector_Diff': [0.10],
        })

        result = flag_constituent_exceedances(table)
        flags = result['Exceeds_Threshold'].iloc[0]
        assert 'AMP' in flags
        assert 'PHASE' in flags
        assert 'VD' in flags

    def test_custom_thresholds(self):
        """Custom thresholds are respected."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [0.03],
            'Phase_Diff': [5.0],
            'Vector_Diff': [0.03],
        })

        # Default thresholds: should not flag
        result_default = flag_constituent_exceedances(table)
        assert result_default['Exceeds_Threshold'].iloc[0] == ''

        # Tighter thresholds: should flag
        result_tight = flag_constituent_exceedances(
            table, amp_threshold=0.02, phase_threshold=3.0,
            vector_diff_threshold=0.02,
        )
        flags = result_tight['Exceeds_Threshold'].iloc[0]
        assert 'AMP' in flags
        assert 'PHASE' in flags
        assert 'VD' in flags

    def test_nan_not_flagged(self):
        """NaN values are not flagged as exceedances."""
        from ofs_skill.tidal_analysis.constituent_table import (
            flag_constituent_exceedances,
        )

        table = pd.DataFrame({
            'Constituent': ['M2'],
            'Amp_Diff': [np.nan],
            'Phase_Diff': [np.nan],
            'Vector_Diff': [np.nan],
        })

        result = flag_constituent_exceedances(table)
        assert result['Exceeds_Threshold'].iloc[0] == ''
