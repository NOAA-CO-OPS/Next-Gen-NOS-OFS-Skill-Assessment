"""
Pytest test suite for bin/utils/insert_skill_stats.py

Tests the SQLite database insertion functionality for OFS skill assessment statistics.
"""

import pytest
import sqlite3
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import sys
import logging
import argparse

# Add parent directory to path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from bin.utils.insert_skill_stats import (
    get_skill_files,
    main,
    is_datetime
)


@pytest.fixture
def logger():
    """Create a test logger."""
    logger = logging.getLogger('test_logger')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for test files."""
    skill_dir = tmp_path / "skill_stats"
    skill_dir.mkdir()
    db_dir = tmp_path / "db"
    db_dir.mkdir()

    yield {
        'skill_dir': skill_dir,
        'db_dir': db_dir,
        'tmp_path': tmp_path
    }

    # Cleanup happens automatically with tmp_path


@pytest.fixture
def sample_skill_csv():
    """Create sample skill statistics CSV content matching production format."""
    return """,ID,NODE,obs_water_depth,mod_water_depth,rmse,r,bias,bias_perc,bias_dir,central_freq,central_freq_pass_fail,pos_outlier_freq,pos_outlier_freq_pass_fail,neg_outlier_freq,neg_outlier_freq_pass_fail,bias_standard_dev,target_error_range,datum,Y,X,start_date,end_date
0,8638610,1234,1.0,0.9,0.123,0.95,0.05,5.2,10.5,85.0,pass,5.0,pass,3.0,pass,0.08,0.15,,38.0,-76.0,2025-01-15T00:00:00Z,2025-01-16T00:00:00Z
1,8638614,1235,1.0,0.9,0.145,0.92,0.03,3.1,8.2,90.0,pass,4.0,pass,2.0,pass,0.06,0.15,,38.0,-76.0,2025-01-15T00:00:00Z,2025-01-16T00:00:00Z
2,8638863,1236,1.0,0.9,0.098,0.97,0.02,2.5,5.3,92.0,pass,3.0,pass,1.0,pass,0.05,0.15,,38.0,-76.0,2025-01-15T00:00:00Z,2025-01-16T00:00:00Z"""


@pytest.fixture
def sample_skill_csv_old_format():
    """Create sample skill statistics CSV with old date format (YYYYMMDD-HH:MM:SS)."""
    return """,ID,NODE,obs_water_depth,mod_water_depth,rmse,r,bias,bias_perc,bias_dir,central_freq,central_freq_pass_fail,pos_outlier_freq,pos_outlier_freq_pass_fail,neg_outlier_freq,neg_outlier_freq_pass_fail,bias_standard_dev,target_error_range,datum,Y,X,start_date,end_date
0,8638610,1234,1.0,0.9,0.123,0.95,0.05,5.2,10.5,85.0,pass,5.0,pass,3.0,pass,0.08,0.15,,38.0,-76.0,20250115-00:00:00,20250116-00:00:00
1,8638614,1235,1.0,0.9,0.145,0.92,0.03,3.1,8.2,90.0,pass,4.0,pass,2.0,pass,0.06,0.15,,38.0,-76.0,20250115-00:00:00,20250116-00:00:00"""


@pytest.fixture
def sample_skill_csv_no_bias_dir():
    """Create sample skill statistics CSV without bias_dir values (for non-current products)."""
    return """,ID,NODE,obs_water_depth,mod_water_depth,rmse,r,bias,bias_perc,bias_dir,central_freq,central_freq_pass_fail,pos_outlier_freq,pos_outlier_freq_pass_fail,neg_outlier_freq,neg_outlier_freq_pass_fail,bias_standard_dev,target_error_range,datum,Y,X,start_date,end_date
0,8638610,1234,1.0,0.9,0.123,0.95,0.05,5.2,,85.0,pass,5.0,pass,3.0,pass,0.08,0.15,,38.0,-76.0,2025-01-15T00:00:00Z,2025-01-16T00:00:00Z
1,8638614,1235,1.0,0.9,0.145,0.92,0.03,3.1,,90.0,pass,4.0,pass,2.0,pass,0.06,0.15,,38.0,-76.0,2025-01-15T00:00:00Z,2025-01-16T00:00:00Z"""


def create_skill_file(directory, ofs, product, whichcast, content):
    """Helper to create a skill statistics CSV file."""
    filename = f"skill_{ofs}_{product}_{whichcast}.csv"
    filepath = directory / filename
    filepath.write_text(content)
    return filepath


class TestGetSkillFiles:
    """Tests for get_skill_files() function."""

    def test_get_skill_files_success(self, temp_dir, sample_skill_csv, logger):
        """Test successful retrieval of skill files."""
        skill_dir = temp_dir['skill_dir']

        # Create test files
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        create_skill_file(skill_dir, 'cbofs', 'water_temperature', 'forecast_b', sample_skill_csv)
        create_skill_file(skill_dir, 'dbofs', 'salinity', 'nowcast', sample_skill_csv)  # Different OFS

        # Get files for cbofs
        files = get_skill_files('cbofs', skill_dir, logger)

        assert len(files) == 2
        assert 'skill_cbofs_water_level_nowcast.csv' in files
        assert 'skill_cbofs_water_temperature_forecast_b.csv' in files
        assert 'skill_dbofs_salinity_nowcast.csv' not in files  # Different OFS

    def test_get_skill_files_excludes_2d(self, temp_dir, sample_skill_csv, logger):
        """Test that 2D skill files are excluded."""
        skill_dir = temp_dir['skill_dir']

        # Create 1D and 2D files
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        (skill_dir / "skill_cbofs_2d_nowcast.csv").write_text(sample_skill_csv)

        files = get_skill_files('cbofs', skill_dir, logger)

        assert len(files) == 1
        assert 'skill_cbofs_water_level_nowcast.csv' in files
        assert 'skill_cbofs_2d_nowcast.csv' not in files

    def test_get_skill_files_empty_directory(self, temp_dir, logger):
        """Test behavior with empty directory."""
        skill_dir = temp_dir['skill_dir']

        with pytest.raises(SystemExit):
            get_skill_files('cbofs', skill_dir, logger)

    def test_get_skill_files_invalid_path(self, logger):
        """Test behavior with invalid path."""
        with pytest.raises(SystemExit):
            get_skill_files('cbofs', '/nonexistent/path', logger)


class TestIsDatetime:
    """Tests for is_datetime() function."""

    def test_valid_datetime(self):
        """Test valid datetime strings."""
        valid_dates = [
            '2025-01-15 12:30:45',
            '2024-12-31 23:59:59',
            '2023-06-01 00:00:00',
        ]

        for date_str in valid_dates:
            assert is_datetime(date_str) == date_str

    def test_invalid_datetime(self):
        """Test invalid datetime strings that don't match the regex pattern."""
        invalid_dates = [
            '2025-1-15 12:00:00',   # Single digit month (fails regex)
            '2025-01-1 12:00:00',   # Single digit day (fails regex)
            '2025-01-15',           # Missing time
            '2025/01/15 12:00:00',  # Wrong separator (slash instead of dash)
            '25-01-15 12:00:00',    # 2-digit year
            'invalid',              # Not a date
            '2025-20-01 12:00:00',  # Month > 19 (fails regex [0-1][0-9])
            '2025-01-40 12:00:00',  # Day > 39 (fails regex [0-3][0-9])
        ]

        for date_str in invalid_dates:
            with pytest.raises(argparse.ArgumentTypeError):
                is_datetime(date_str)


class TestDatabaseInsertion:
    """Tests for main() database insertion functionality."""

    def test_create_table_daily(self, temp_dir, sample_skill_csv, logger):
        """Test database table creation for daily period."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check database was created
        db_file = db_dir / 'cbofs.db'
        assert db_file.exists()

        # Check table exists
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_skill_stats';")
        tables = cursor.fetchall()
        assert len(tables) == 1
        assert tables[0][0] == 'daily_skill_stats'
        conn.close()

    def test_insert_data_with_bias_dir(self, temp_dir, sample_skill_csv, logger):
        """Test inserting data with bias_dir column (currents)."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file for currents (has bias_dir)
        create_skill_file(skill_dir, 'cbofs', 'currents', 'nowcast', sample_skill_csv)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check data was inserted
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_skill_stats;")
        count = cursor.fetchone()[0]
        assert count == 3  # 3 rows in sample CSV

        # Check bias_dir column has values
        cursor.execute("SELECT bias_dir FROM daily_skill_stats WHERE station_id='8638610';")
        bias_dir = cursor.fetchone()[0]
        assert bias_dir == 10.5
        conn.close()

    def test_insert_data_without_bias_dir(self, temp_dir, sample_skill_csv_no_bias_dir, logger):
        """Test inserting data without bias_dir column (water level, temp, salinity)."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file for water_level (no bias_dir)
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv_no_bias_dir)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check data was inserted
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_skill_stats;")
        count = cursor.fetchone()[0]
        assert count == 2

        # Check bias_dir is NULL
        cursor.execute("SELECT bias_dir FROM daily_skill_stats WHERE station_id='8638610';")
        bias_dir = cursor.fetchone()[0]
        assert bias_dir is None
        conn.close()

    def test_old_date_format_conversion(self, temp_dir, sample_skill_csv_old_format, logger):
        """Test conversion of old date format (YYYYMMDD-HH:MM:SS)."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file with old date format
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv_old_format)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check date was converted correctly
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT begin_date_time FROM daily_skill_stats LIMIT 1;")
        begin_date = cursor.fetchone()[0]
        assert begin_date == '2025-01-15 00:00:00'
        conn.close()

    def test_product_name_parsing(self, temp_dir, sample_skill_csv, logger):
        """Test product name parsing from filename."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create files for different products
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        create_skill_file(skill_dir, 'cbofs', 'water_temperature', 'forecast_b', sample_skill_csv)
        create_skill_file(skill_dir, 'cbofs', 'salinity', 'nowcast', sample_skill_csv)
        create_skill_file(skill_dir, 'cbofs', 'currents', 'nowcast', sample_skill_csv)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check products were parsed correctly
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT product FROM daily_skill_stats ORDER BY product;")
        products = [row[0] for row in cursor.fetchall()]

        assert 'currents' in products
        assert 'salinity' in products
        assert 'water_level' in products
        assert 'water_temperature' in products
        conn.close()

    def test_whichcast_parsing(self, temp_dir, sample_skill_csv, logger):
        """Test whichcast (nowcast/forecast) parsing from filename."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create files for different whichcasts
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'forecast_b', sample_skill_csv)

        # Run main
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check types were parsed correctly
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT type FROM daily_skill_stats ORDER BY type;")
        types = [row[0] for row in cursor.fetchall()]

        assert 'nowcast' in types
        assert 'forecast_b' in types
        conn.close()

    def test_primary_key_constraint(self, temp_dir, sample_skill_csv, logger):
        """Test that duplicate entries are ignored (PRIMARY KEY constraint)."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)

        # Run main twice
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        # Check data was only inserted once
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_skill_stats;")
        count = cursor.fetchone()[0]
        assert count == 3  # Should still be 3, not 6
        conn.close()

    def test_monthly_period(self, temp_dir, logger):
        """Test monthly period database table creation."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create skill file with 30-day range
        monthly_csv = """id,node,start_date,end_date,rmse,r,bias,bias_perc,bias_dir,central_freq,central_freq_pass_fail,pos_outlier_freq,pos_outlier_freq_pass_fail,neg_outlier_freq,neg_outlier_freq_pass_fail,bias_standard_dev,target_error_range
8638610,1234,2025-01-01T00:00:00Z,2025-01-31T00:00:00Z,0.123,0.95,0.05,5.2,10.5,0.85,PASS,0.05,PASS,0.03,PASS,0.08,0.15"""

        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', monthly_csv)

        # Run main with monthly period
        main(skill_dir, db_dir, 'monthly', 'cbofs', logger)

        # Check table exists
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='monthly_skill_stats';")
        tables = cursor.fetchall()
        assert len(tables) == 1
        assert tables[0][0] == 'monthly_skill_stats'
        conn.close()

    def test_period_mismatch_validation(self, temp_dir, sample_skill_csv, logger, capsys):
        """Test that period validation rejects mismatched data."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        # Create daily skill file (1-day range)
        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)

        # Try to insert with monthly period (should skip)
        main(skill_dir, db_dir, 'monthly', 'cbofs', logger)

        # Check no data was inserted
        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM monthly_skill_stats;")
        count = cursor.fetchone()[0]
        assert count == 0  # Should be empty due to period mismatch
        conn.close()

    def test_all_columns_inserted(self, temp_dir, sample_skill_csv, logger):
        """Test that all columns are inserted correctly."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        create_skill_file(skill_dir, 'cbofs', 'currents', 'nowcast', sample_skill_csv)
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get first row
        cursor.execute("""
            SELECT product, station_id, type, begin_date_time, end_date_time,
                   node, rmse, r, bias, bias_perc, bias_dir,
                   central_freq, central_freq_pass_fail,
                   pos_outlier_freq, pos_outlier_freq_pass_fail,
                   neg_outlier_freq, neg_outlier_freq_pass_fail,
                   bias_standard_dev, target_error_range
            FROM daily_skill_stats
            WHERE station_id='8638610';
        """)
        row = cursor.fetchone()

        # Validate all values
        assert row[0] == 'currents'  # product
        assert row[1] == '8638610'   # station_id
        assert row[2] == 'nowcast'   # type
        assert row[3] == '2025-01-15 00:00:00'  # begin_date_time
        assert row[4] == '2025-01-16 00:00:00'  # end_date_time
        assert row[5] == 1234        # node
        assert row[6] == 0.123       # rmse
        assert row[7] == 0.95        # r
        assert row[8] == 0.05        # bias
        assert row[9] == 5.2         # bias_perc
        assert row[10] == 10.5       # bias_dir
        assert row[11] == 85.0       # central_freq
        assert row[12] == 'pass'     # central_freq_pass_fail
        assert row[13] == 5.0        # pos_outlier_freq
        assert row[14] == 'pass'     # pos_outlier_freq_pass_fail
        assert row[15] == 3.0        # neg_outlier_freq
        assert row[16] == 'pass'     # neg_outlier_freq_pass_fail
        assert row[17] == 0.08       # bias_standard_dev
        assert row[18] == 0.15       # target_error_range

        conn.close()


class TestDatabaseSchema:
    """Tests for database schema validation."""

    def test_table_schema(self, temp_dir, sample_skill_csv, logger):
        """Test that table has correct schema."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get table schema
        cursor.execute("PRAGMA table_info(daily_skill_stats);")
        columns = cursor.fetchall()

        column_names = [col[1] for col in columns]
        expected_columns = [
            'datetime_inserted', 'product', 'station_id', 'type',
            'begin_date_time', 'end_date_time', 'node', 'rmse', 'r',
            'bias', 'bias_perc', 'bias_dir', 'central_freq',
            'central_freq_pass_fail', 'pos_outlier_freq',
            'pos_outlier_freq_pass_fail', 'neg_outlier_freq',
            'neg_outlier_freq_pass_fail', 'bias_standard_dev',
            'target_error_range'
        ]

        for col in expected_columns:
            assert col in column_names

        conn.close()

    def test_datetime_inserted_auto(self, temp_dir, sample_skill_csv, logger):
        """Test that datetime_inserted is automatically set."""
        skill_dir = temp_dir['skill_dir']
        db_dir = temp_dir['db_dir']

        create_skill_file(skill_dir, 'cbofs', 'water_level', 'nowcast', sample_skill_csv)
        main(skill_dir, db_dir, 'daily', 'cbofs', logger)

        db_file = db_dir / 'cbofs.db'
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute("SELECT datetime_inserted FROM daily_skill_stats LIMIT 1;")
        dt_inserted = cursor.fetchone()[0]

        # Should be a valid datetime string
        assert dt_inserted is not None
        assert len(dt_inserted) >= 19  # YYYY-MM-DD HH:MM:SS

        conn.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
