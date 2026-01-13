# Test Suite for OFS Skill Assessment

This directory contains automated tests for the Next-Gen NOS OFS Skill Assessment system.

## Test Structure

```
tests/
├── README.md                    # This file
├── test_s3_fallback.py         # Unit tests for S3 fallback functionality
├── test_insert_skill_stats.py  # Unit tests for SQLite database insertion
├── requirements-test.txt        # Test dependencies
└── ... (future test files)
```

## Test Suites

### S3 Fallback Tests

The `test_s3_fallback.py` file contains comprehensive unit tests for the S3 fallback functionality that enables dynamic streaming of model data from the NODD S3 bucket.

### What Gets Tested

- ✅ **1D Plotting (Stations Files)**: Nowcast and Forecast modes
- ✅ **2D Plotting (Fields Files)**: Nowcast and Forecast modes
- ✅ **Multiple OFS Systems**: CBOFS, DBOFS, GOMOFS, CREOFS, WCOFS, STOFS-3D, etc.
- ✅ **File Naming Conventions**: Both old (pre-Sept 2024) and new formats
- ✅ **Forecast Cycles**: Different cycle times for different OFS systems
- ✅ **Time Resolutions**: 1-hour, 3-hour, and 12-hour timesteps
- ✅ **Edge Cases**: Invalid paths, unknown OFS systems, error handling

### Test Coverage Summary

| Category | Tests | Description |
|----------|-------|-------------|
| Stations Nowcast | 3 | 1D nowcast for different OFS |
| Stations Forecast | 2 | 1D forecast_a and forecast_b |
| Fields Nowcast | 4 | 2D nowcast with different timesteps |
| Fields Forecast | 3 | 2D forecast modes |
| Backwards Compatibility | 3 | Old file format support |
| Edge Cases | 3 | Error handling |
| Special OFS | 2 | STOFS-3D systems |
| Comprehensive | 1 | All OFS cycle counts |
| File Patterns | 3 | Naming convention validation |
| Integration | 2 | End-to-end workflows |

**Total: 41 test cases**

### SQLite Insertion Tests

The `test_insert_skill_stats.py` file contains comprehensive unit tests for the SQLite database insertion functionality that stores skill assessment statistics.

#### What Gets Tested

- ✅ **File Discovery**: Finding skill stat CSV files for specific OFS
- ✅ **Date Format Conversion**: Both ISO8601 and YYYYMMDD-HH:MM:SS formats
- ✅ **Product Detection**: water_level, water_temperature, currents, salinity
- ✅ **Database Operations**: Table creation, data insertion, primary key constraints
- ✅ **Data Integrity**: All 19 columns with correct data types
- ✅ **Period Validation**: Daily vs monthly period matching
- ✅ **Bias Direction Handling**: With/without bias_dir column
- ✅ **Edge Cases**: Empty directories, duplicates, schema validation

#### Test Coverage Summary

| Category | Tests | Description |
|----------|-------|-------------|
| File Discovery | 4 | Finding and filtering skill files |
| Date Validation | 2 | Valid/invalid datetime formats |
| Database Insertion | 10 | Table creation, data insert, constraints |
| Schema Validation | 2 | Table schema and auto-fields |

**Total: 18 test cases (38 assertions)**

## Running Tests Locally

### Prerequisites

```bash
# Install pytest and coverage tools
pip install pytest pytest-cov pytest-xdist
```

### Run All Tests

```bash
# From repository root - run all test suites
pytest tests/ -v

# Run specific test suite
pytest tests/test_s3_fallback.py -v          # S3 fallback only
pytest tests/test_insert_skill_stats.py -v   # SQLite insertion only
```

### Run Specific Test Classes

#### S3 Fallback Tests

```bash
# Test only stations files
pytest tests/test_s3_fallback.py::TestStationsFilesNowcast -v

# Test only fields files
pytest tests/test_s3_fallback.py::TestFieldsFilesNowcast -v

# Test backwards compatibility
pytest tests/test_s3_fallback.py::TestBackwardsCompatibility -v
```

#### SQLite Insertion Tests

```bash
# Test file discovery
pytest tests/test_insert_skill_stats.py::TestGetSkillFiles -v

# Test database insertion
pytest tests/test_insert_skill_stats.py::TestDatabaseInsertion -v

# Test schema validation
pytest tests/test_insert_skill_stats.py::TestDatabaseSchema -v
```

### Run with Coverage Report

```bash
# Generate coverage report for all tests
pytest tests/ --cov=bin --cov-report=html

# Generate coverage for specific module
pytest tests/test_s3_fallback.py --cov=bin.model_processing --cov-report=html
pytest tests/test_insert_skill_stats.py --cov=bin.utils.insert_skill_stats --cov-report=html

# Open coverage report in browser
# The report will be in htmlcov/index.html
```

### Run Tests in Parallel

```bash
# Use multiple CPU cores for faster testing
pytest tests/ -n auto
```

### Run Tests with Verbose Output

```bash
# See detailed output for each test
pytest tests/test_s3_fallback.py -v --tb=short
```

## Continuous Integration

Tests run automatically via GitHub Actions on:
- Every push to `main`, `develop`, or `master` branches
- Every pull request to these branches

### Available CI Workflows

#### 1. S3 Fallback Tests (`.github/workflows/test-s3-fallback.yml`)
Triggers when these files are modified:
- `bin/model_processing/list_of_files.py`
- `bin/model_processing/intake_scisa.py`
- `tests/test_s3_fallback.py`
- `conf/ofs_dps.conf`
- `.github/workflows/test-s3-fallback.yml`

#### 2. SQLite Insertion Tests (`.github/workflows/test-insert-skill-stats.yml`)
Triggers when these files are modified:
- `bin/utils/insert_skill_stats.py`
- `tests/test_insert_skill_stats.py`
- `.github/workflows/test-insert-skill-stats.yml`

### CI/CD Workflow Features

**Both workflows:**
1. Test across Python 3.9, 3.10, 3.11, and 3.12
2. Generate coverage reports
3. Upload results to Codecov (Python 3.11 only)
4. Archive test artifacts (on failure only, retention: 3 days)
5. Provide test summary
6. Run code quality checks (pylint, flake8)

### Viewing CI Results

1. Go to the repository on GitHub
2. Click on "Actions" tab
3. Select the desired workflow:
   - "S3 Fallback Tests"
   - "SQLite Insertion Tests"
4. View results for each Python version (3.9, 3.10, 3.11, 3.12)

## Test Design Philosophy

These tests are designed to be:

- **Fast**: No network I/O, no file downloads, runs in < 5 seconds
- **Isolated**: Uses mock objects, no dependencies on external services
- **Deterministic**: Same inputs always produce same outputs
- **Comprehensive**: Covers all OFS systems and edge cases
- **Maintainable**: Clear naming, good documentation

## Adding New Tests

When adding new functionality to S3 fallback:

1. **Create a new test class** following the pattern:
   ```python
   class TestYourNewFeature:
       """Description of what this tests"""

       def test_specific_scenario(self, logger, test_dir_new):
           """Test description"""
           # Arrange
           prop = MockProps('cbofs', 'nowcast', 'stations')

           # Act
           files = construct_expected_files(prop, test_dir_new, logger)

           # Assert
           assert len(files) > 0
           assert 'expected_pattern' in files[0]
   ```

2. **Use descriptive names** that explain what is being tested

3. **Add docstrings** explaining the purpose of the test

4. **Follow AAA pattern**: Arrange, Act, Assert

5. **Run tests locally** before committing:
   ```bash
   pytest tests/test_s3_fallback.py -v
   ```

## Troubleshooting

### Import Errors

If you get import errors when running tests:
```bash
# Make sure you're in the repository root
cd /path/to/Next-Gen-NOS-OFS-Skill-Assessment

# Run pytest from root
pytest tests/test_s3_fallback.py -v
```

### Path Issues

Tests use relative imports. Ensure your working directory is the repository root.

### Test Failures

If tests fail:
1. Check the error message and traceback
2. Run the specific failing test with verbose output:
   ```bash
   pytest tests/test_s3_fallback.py::TestClassName::test_method_name -vv
   ```
3. Review recent changes to `bin/model_processing/list_of_files.py`

## Future Test Development

Planned test additions:
- [ ] Integration tests with actual S3 access (marked as slow/integration)
- [ ] Tests for `construct_s3_url()` function
- [ ] Performance benchmarks
- [ ] Tests for error logging behavior
- [ ] Tests for create_1dplot and create_2dplot end-to-end

## Contributing

When contributing tests:
1. Ensure all existing tests still pass
2. Add tests for new functionality
3. Maintain > 80% code coverage for modified files
4. Follow existing code style and patterns
5. Document any special test requirements

## Questions?

For questions about the test suite, please:
- Review this README
- Check test file docstrings
- Open an issue on GitHub
- Contact the development team

---

**Last Updated**: December 2025
**Test Framework**: pytest 7.x+
**Python Compatibility**: 3.9, 3.10, 3.11, 3.12
