"""
Unit tests for S3 fallback functionality in model data retrieval.

This test suite validates that the construct_expected_files() function
correctly generates file names for both 1D (stations) and 2D (fields) plots,
across different OFS systems, forecast modes, and file naming conventions.

Tests run without network access and don't require actual S3 connectivity.
"""

import pytest

# Import from ofs_skill package
from ofs_skill.model_processing.list_of_files import construct_expected_files


class MockLogger:
    """Mock logger for testing without actual logging infrastructure"""

    def info(self, msg, *args):
        """Mock info logging"""
        pass

    def error(self, msg, *args):
        """Mock error logging"""
        pass


class MockProps:
    """Mock ModelProperties object for testing"""

    def __init__(self, ofs, whichcast, ofsfiletype):
        self.ofs = ofs
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype


@pytest.fixture
def logger():
    """Fixture providing a mock logger"""
    return MockLogger()


@pytest.fixture
def test_dir_new():
    """Fixture providing test directory path for new format (post-Sept 2024)"""
    return '../example_data/cbofs/netcdf/2025/12/15'


@pytest.fixture
def test_dir_old():
    """Fixture providing test directory path for old format (pre-Sept 2024)"""
    return '../example_data/cbofs/netcdf/2024/08/15'


class TestStationsFilesNowcast:
    """Test station file generation for nowcast (1D plots)"""

    def test_cbofs_nowcast_stations(self, logger, test_dir_new):
        """Test CBOFS nowcast station files generation"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # CBOFS has 4 forecast cycles (00z, 06z, 12z, 18z)
        assert len(files) == 4, f'Expected 4 files, got {len(files)}'

        # Check first file has correct format
        assert 'cbofs.t00z.20251215.stations.nowcast.nc' in files[0]

        # Verify all files have correct pattern
        assert all('.stations.nowcast.nc' in f for f in files)

        # Verify all 4 cycles are present
        cycles = ['t00z', 't06z', 't12z', 't18z']
        for cycle in cycles:
            assert any(cycle in f for f in files), f'Missing cycle {cycle}'

    def test_dbofs_nowcast_stations(self, logger, test_dir_new):
        """Test DBOFS nowcast station files (same cycle pattern as CBOFS)"""
        prop = MockProps('dbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == 4
        assert 'dbofs.t00z.20251215.stations.nowcast.nc' in files[0]

    def test_creofs_nowcast_stations(self, logger, test_dir_new):
        """Test CREOFS nowcast station files (different forecast cycles)"""
        prop = MockProps('creofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # CREOFS has different cycles: 03z, 09z, 15z, 21z
        assert len(files) == 4
        assert 'creofs.t03z.20251215.stations.nowcast.nc' in files[0]

        # Verify CREOFS-specific cycles
        cycles = ['t03z', 't09z', 't15z', 't21z']
        for cycle in cycles:
            assert any(cycle in f for f in files), f'Missing CREOFS cycle {cycle}'


class TestStationsFilesForecast:
    """Test station file generation for forecast modes (1D plots)"""

    def test_cbofs_forecast_b_stations(self, logger, test_dir_new):
        """Test CBOFS forecast_b station files"""
        prop = MockProps('cbofs', 'forecast_b', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == 4
        # forecast_b maps to 'forecast' in file names
        assert 'cbofs.t00z.20251215.stations.forecast.nc' in files[0]
        assert all('.stations.forecast.nc' in f for f in files)

    def test_forecast_a_stations(self, logger, test_dir_new):
        """Test forecast_a station files"""
        prop = MockProps('cbofs', 'forecast_a', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == 4
        assert 'cbofs.t00z.20251215.stations.forecast.nc' in files[0]


class TestFieldsFilesNowcast:
    """Test fields file generation for nowcast (2D plots)"""

    def test_cbofs_nowcast_fields(self, logger, test_dir_new):
        """Test CBOFS nowcast fields files (hourly, 6 hours per cycle)"""
        prop = MockProps('cbofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # CBOFS nowcast: 4 cycles × 6 hours each = 24 files
        assert len(files) == 24, f'Expected 24 files, got {len(files)}'

        # Check file naming pattern
        assert 'cbofs.t00z.20251215.fields.n001.nc' in files[0]
        assert 'cbofs.t00z.20251215.fields.n006.nc' in files[5]

        # Verify all are nowcast (prefix 'n')
        assert all('.fields.n' in f for f in files)

    def test_dbofs_nowcast_fields(self, logger, test_dir_new):
        """Test DBOFS nowcast fields files"""
        prop = MockProps('dbofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == 24
        assert 'dbofs.t00z.20251215.fields.n001.nc' in files[0]

    def test_wcofs_nowcast_fields(self, logger, test_dir_new):
        """Test WCOFS nowcast fields (3-hour timestep, 1 cycle per day)"""
        prop = MockProps('wcofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # WCOFS: 1 cycle × 24 hours / 3hr timestep = 8 files
        assert len(files) == 8, f'Expected 8 files for WCOFS, got {len(files)}'
        assert 'wcofs.t03z.20251215.fields.n003.nc' in files[0]

        # Verify 3-hour timesteps (n003, n006, n009, ..., n024)
        expected_hours = ['n003', 'n006', 'n009', 'n012', 'n015', 'n018', 'n021', 'n024']
        for hour in expected_hours:
            assert any(hour in f for f in files), f'Missing timestep {hour}'

    def test_gomofs_nowcast_fields(self, logger, test_dir_new):
        """Test GOMOFS nowcast fields (3-hour timestep)"""
        prop = MockProps('gomofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # GOMOFS: 4 cycles × 2 hours / 3hr timestep = 8 files
        # (24 hours / 4 cycles = 6 hours per cycle, / 3hr timestep = 2 files per cycle)
        assert len(files) == 8


class TestFieldsFilesForecast:
    """Test fields file generation for forecast modes (2D plots)"""

    def test_cbofs_forecast_b_fields(self, logger, test_dir_new):
        """Test CBOFS forecast_b fields files"""
        prop = MockProps('cbofs', 'forecast_b', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # forecast_b: 4 cycles × 6 hours = 24 files
        assert len(files) == 24

        # Check forecast prefix 'f' instead of nowcast 'n'
        assert 'cbofs.t00z.20251215.fields.f001.nc' in files[0]
        assert 'cbofs.t00z.20251215.fields.f006.nc' in files[5]
        assert all('.fields.f' in f for f in files)

    def test_cbofs_forecast_a_fields(self, logger, test_dir_new):
        """Test CBOFS forecast_a fields (48-hour forecast)"""
        prop = MockProps('cbofs', 'forecast_a', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # CBOFS forecast_a: 4 cycles × 48 hours = 192 files
        assert len(files) == 192, f'Expected 192 files for forecast_a, got {len(files)}'
        assert 'cbofs.t00z.20251215.fields.f001.nc' in files[0]

    def test_gomofs_forecast_a_fields(self, logger, test_dir_new):
        """Test GOMOFS forecast_a (72-hour forecast)"""
        prop = MockProps('gomofs', 'forecast_a', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # GOMOFS forecast_a: 4 cycles × 72 hours / 3hr timestep = 96 files
        assert len(files) == 96


class TestBackwardsCompatibility:
    """Test backwards compatibility with old file naming format"""

    def test_old_format_nowcast_stations(self, logger, test_dir_old):
        """Test old file naming format (pre-Sept 1, 2024)"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_old, logger)

        assert len(files) == 4
        # Old format: nos.{ofs}.stations.{cast_type}.{date}.t{cycle}z.nc
        assert 'nos.cbofs.stations.nowcast.20240815.t00z.nc' in files[0]
        assert all('nos.cbofs' in f for f in files)

    def test_old_format_forecast_stations(self, logger, test_dir_old):
        """Test old format for forecast files"""
        prop = MockProps('cbofs', 'forecast_b', 'stations')
        files = construct_expected_files(prop, test_dir_old, logger)

        assert 'nos.cbofs.stations.forecast.20240815.t00z.nc' in files[0]

    def test_old_format_nowcast_fields(self, logger, test_dir_old):
        """Test old format for nowcast fields"""
        prop = MockProps('cbofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_old, logger)

        # Old format: nos.{ofs}.fields.n{hour}.{date}.t{cycle}z.nc
        assert 'nos.cbofs.fields.n001.20240815.t00z.nc' in files[0]
        assert all('nos.cbofs.fields.n' in f for f in files)


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_invalid_directory_path(self, logger):
        """Test with invalid/empty directory path"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, '', logger)

        # Should return empty list on error
        assert len(files) == 0

    def test_malformed_directory_path(self, logger):
        """Test with malformed directory path"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, 'invalid/path', logger)

        assert len(files) == 0

    def test_unknown_ofs(self, logger, test_dir_new):
        """Test with unknown OFS (should use default cycles)"""
        prop = MockProps('unknownofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # Unknown OFS defaults to cycle '03'
        assert len(files) == 1
        assert 'unknownofs.t03z.20251215.stations.nowcast.nc' in files[0]


class TestSpecialOFSSystems:
    """Test special OFS systems with unique characteristics"""

    def test_stofs_3d_atl_stations(self, logger, test_dir_new):
        """Test STOFS-3D-ATL (runs once per day at 12z)"""
        prop = MockProps('stofs_3d_atl', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # STOFS-3D runs once per day at 12z
        assert len(files) == 1
        assert 'stofs_3d_atl.t12z.20251215.stations.nowcast.nc' in files[0]

    def test_stofs_3d_pac_stations(self, logger, test_dir_new):
        """Test STOFS-3D-PAC"""
        prop = MockProps('stofs_3d_pac', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == 1
        assert 'stofs_3d_pac.t12z.20251215.stations.nowcast.nc' in files[0]


class TestAllOFSSystems:
    """Comprehensive test across all supported OFS systems"""

    @pytest.mark.parametrize('ofs,expected_cycles', [
        ('cbofs', 4),
        ('dbofs', 4),
        ('gomofs', 4),
        ('ciofs', 4),
        ('leofs', 4),
        ('lmhofs', 4),
        ('loofs', 4),
        ('lsofs', 4),
        ('tbofs', 4),
        ('necofs', 4),
        ('creofs', 4),
        ('ngofs2', 4),
        ('sfbofs', 4),
        ('sscofs', 4),
        ('stofs_3d_atl', 1),
        ('stofs_3d_pac', 1),
    ])
    def test_all_ofs_cycle_counts(self, ofs, expected_cycles, logger, test_dir_new):
        """Test that all OFS systems generate correct number of cycle files"""
        prop = MockProps(ofs, 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        assert len(files) == expected_cycles, \
            f'{ofs} expected {expected_cycles} files, got {len(files)}'


class TestFileNamePatterns:
    """Test file name pattern generation"""

    def test_date_format_in_filename(self, logger, test_dir_new):
        """Verify date is correctly formatted in file names"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # All files should have date 20251215
        assert all('20251215' in f for f in files)

    def test_cycle_format_in_filename(self, logger, test_dir_new):
        """Verify cycle times are correctly formatted (t00z, t06z, etc.)"""
        prop = MockProps('cbofs', 'nowcast', 'stations')
        files = construct_expected_files(prop, test_dir_new, logger)

        # All files should have cycle in format tXXz
        import re
        for f in files:
            assert re.search(r't\d{2}z', f), f'File missing cycle format: {f}'

    def test_hour_format_in_fields(self, logger, test_dir_new):
        """Verify hour strings are zero-padded (n001, not n1)"""
        prop = MockProps('cbofs', 'nowcast', 'fields')
        files = construct_expected_files(prop, test_dir_new, logger)

        # All should have 3-digit hour format
        import re
        for f in files:
            assert re.search(r'[nf]\d{3}', f), f'File missing hour format: {f}'


class TestIntegrationScenarios:
    """Test realistic usage scenarios"""

    def test_complete_1d_workflow(self, logger, test_dir_new):
        """Test complete 1D plotting workflow"""
        # Nowcast
        prop_nowcast = MockProps('cbofs', 'nowcast', 'stations')
        files_nowcast = construct_expected_files(prop_nowcast, test_dir_new, logger)
        assert len(files_nowcast) == 4

        # Forecast_b
        prop_forecast = MockProps('cbofs', 'forecast_b', 'stations')
        files_forecast = construct_expected_files(prop_forecast, test_dir_new, logger)
        assert len(files_forecast) == 4

        # Files should be different (nowcast vs forecast)
        assert files_nowcast[0] != files_forecast[0]

    def test_complete_2d_workflow(self, logger, test_dir_new):
        """Test complete 2D plotting workflow"""
        # Nowcast
        prop_nowcast = MockProps('cbofs', 'nowcast', 'fields')
        files_nowcast = construct_expected_files(prop_nowcast, test_dir_new, logger)
        assert len(files_nowcast) == 24

        # Forecast_b
        prop_forecast = MockProps('cbofs', 'forecast_b', 'fields')
        files_forecast = construct_expected_files(prop_forecast, test_dir_new, logger)
        assert len(files_forecast) == 24

        # Check prefixes are different (n vs f)
        assert '.fields.n' in files_nowcast[0]
        assert '.fields.f' in files_forecast[0]


# Test execution summary
if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
