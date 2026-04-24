"""
Unit tests for error handling improvements in list_of_files().

Validates that:
- Unparseable filenames are skipped without aborting the entire scan
- Unreadable directories don't kill processing of subsequent directories
- All files unparseable still raises SystemExit(1)
- Empty directories are handled gracefully
- Improved diagnostic logging includes context (ofs, whichcast, etc.)
"""

from unittest.mock import patch

import pytest

from ofs_skill.model_processing.list_of_files import list_of_files


class MockLogger:
    """Mock logger that records all messages for assertions."""

    def __init__(self):
        self.infos = []
        self.warnings = []
        self.errors = []
        self.debugs = []

    def info(self, msg, *args, **kwargs):
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg % args if args else msg)

    def error(self, msg, *args, **kwargs):
        self.errors.append(msg % args if args else msg)

    def debug(self, msg, *args, **kwargs):
        self.debugs.append(msg % args if args else msg)


class MockProps:
    """Mock ModelProperties for testing."""

    def __init__(self, ofs='cbofs', whichcast='nowcast', ofsfiletype='fields',
                 startdate='2024090100', enddate='2024090223',
                 forecast_hr='00z'):
        self.ofs = ofs
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype
        self.startdate = startdate
        self.enddate = enddate
        self.forecast_hr = forecast_hr


@pytest.fixture
def logger():
    return MockLogger()


@pytest.fixture
def props_nowcast():
    return MockProps(whichcast='nowcast', ofsfiletype='fields')


@pytest.fixture
def props_forecast_a():
    return MockProps(whichcast='forecast_a', ofsfiletype='fields')


@pytest.fixture
def props_forecast_b():
    return MockProps(whichcast='forecast_b', ofsfiletype='fields')


@pytest.fixture
def props_hindcast():
    return MockProps(whichcast='hindcast', ofsfiletype='stations')


class TestPerFileErrorResilience:
    """Test that unparseable filenames are skipped without aborting."""

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_nowcast_skips_bad_files_keeps_good(self, mock_exists, mock_listdir,
                                                 mock_utils, props_nowcast, logger):
        """One malformed filename among valid ones: valid files returned, bad skipped."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        # Mix of valid new-format files and a malformed one that matches the
        # 'fields.n' pattern but has too few segments to parse (triggers IndexError)
        mock_listdir.return_value = [
            'cbofs.t00z.20240901.fields.n001.nc',  # valid new format (6 segments)
            'cbofs.fields.n001.nc',  # malformed: matches 'fields.n' but only 4 segments -> IndexError
            'cbofs.t06z.20240901.fields.n001.nc',  # valid
        ]

        result = list_of_files(props_nowcast, ['/fake/dir'], logger)

        # Should have 2 valid files (the malformed one is skipped)
        assert len(result) == 2
        # All results should be real model files
        assert all('fields.n' in f for f in result)
        # Should have logged warnings about skipped files
        assert any('Skipped' in w for w in logger.warnings)
        # Should have debug messages for the malformed file
        assert any('cbofs.fields.n001.nc' in d for d in logger.debugs)

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_forecast_a_skips_bad_files(self, mock_exists, mock_listdir,
                                        mock_utils, props_forecast_a, logger):
        """forecast_a: malformed filenames skipped, valid ones returned."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = [
            'cbofs.t00z.20240901.fields.f001.nc',  # valid
            'cbofs.t00z.fields.f001.nc',  # malformed: matches 'fields.f' + cycle_z but only 5 segments
            'cbofs.t00z.20240901.fields.f002.nc',  # valid
        ]

        result = list_of_files(props_forecast_a, ['/fake/dir'], logger)

        assert len(result) == 2
        assert all('fields.f' in f for f in result)
        assert any('Skipped' in w for w in logger.warnings)

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_forecast_b_skips_bad_files(self, mock_exists, mock_listdir,
                                        mock_utils, props_forecast_b, logger):
        """forecast_b: malformed filenames skipped, valid ones returned."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = [
            'cbofs.t00z.20240901.fields.f001.nc',  # valid (f0xx matches forecast_b)
            'cbofs.fields.f001.nc',  # malformed: matches 'fields.f' + 'f0' but too few segments
            'cbofs.t00z.20240901.fields.f002.nc',  # valid
        ]

        result = list_of_files(props_forecast_b, ['/fake/dir'], logger)

        assert len(result) == 2
        assert all('fields.f' in f for f in result)
        assert any('Skipped' in w for w in logger.warnings)

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_hindcast_skips_bad_files(self, mock_exists, mock_listdir,
                                      mock_utils, props_hindcast, logger):
        """hindcast: malformed filenames skipped, valid ones returned."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = [
            'cbofs.t00z.20240901.stations.h001.nc',  # valid
            'cbofs.stations.h001.nc',  # malformed: matches 'stations.h' but too few segments
            'cbofs.t06z.20240901.stations.h001.nc',  # valid
        ]

        result = list_of_files(props_hindcast, ['/fake/dir'], logger)

        assert len(result) == 2
        assert all('stations.h' in f for f in result)
        assert any('Skipped' in w for w in logger.warnings)


class TestPerDirectoryErrorResilience:
    """Test that one bad directory doesn't kill subsequent directories."""

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_bad_dir_among_good_dirs(self, mock_exists, mock_listdir,
                                      mock_utils, props_nowcast, logger):
        """Dir 2 raises OSError; files from dirs 1 and 3 still returned."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }

        def listdir_side_effect(path):
            if path == '/dir2':
                raise PermissionError('Permission denied')
            return ['cbofs.t00z.20240901.fields.n001.nc']

        mock_listdir.side_effect = listdir_side_effect

        result = list_of_files(props_nowcast, ['/dir1', '/dir2', '/dir3'], logger)

        # Should have files from dir1 and dir3 (1 each)
        assert len(result) == 2
        # Should have logged an error about dir2
        assert any('Cannot read directory' in e for e in logger.errors)
        assert any('/dir2' in e for e in logger.errors)


class TestAllFilesBadStillExits:
    """Test that SystemExit(1) is still raised when no valid files found."""

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_all_unparseable_raises_system_exit(self, mock_exists, mock_listdir,
                                                 mock_utils, props_nowcast, logger):
        """All files are junk — should still abort with SystemExit."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = [
            'README.md',
            '.gitkeep',
            'notes.txt',
        ]

        with pytest.raises(SystemExit) as exc_info:
            list_of_files(props_nowcast, ['/fake/dir'], logger)
        assert exc_info.value.code == 1

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_abort_message_includes_diagnostics(self, mock_exists, mock_listdir,
                                                 mock_utils, props_nowcast, logger):
        """Error message at abort includes ofs, whichcast, filetype, dirs, dates."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = ['junk.txt']

        with pytest.raises(SystemExit):
            list_of_files(props_nowcast, ['/fake/dir'], logger)

        # Check the error log includes diagnostic context
        abort_msgs = [e for e in logger.errors if 'No model files found' in e]
        assert len(abort_msgs) == 1
        msg = abort_msgs[0]
        assert 'cbofs' in msg
        assert 'nowcast' in msg
        assert 'fields' in msg
        assert '/fake/dir' in msg
        assert '2024090100' in msg


class TestEmptyDirectory:
    """Test that empty directories are handled gracefully."""

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_empty_dir_no_crash(self, mock_exists, mock_listdir,
                                 mock_utils, props_nowcast, logger):
        """Empty directory contributes zero files; no crash."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }
        mock_listdir.return_value = []

        with pytest.raises(SystemExit):
            list_of_files(props_nowcast, ['/empty/dir'], logger)

    @patch('ofs_skill.model_processing.list_of_files.utils')
    @patch('ofs_skill.model_processing.list_of_files.listdir')
    @patch('os.path.exists', return_value=True)
    def test_empty_dir_mixed_with_good_dir(self, mock_exists, mock_listdir,
                                            mock_utils, props_nowcast, logger):
        """One empty dir + one dir with valid files: valid files returned."""
        mock_utils.Utils.return_value.read_config_section.return_value = {
            'use_s3_fallback': 'False'
        }

        def listdir_side_effect(path):
            if path == '/empty':
                return []
            return ['cbofs.t00z.20240901.fields.n001.nc']

        mock_listdir.side_effect = listdir_side_effect

        result = list_of_files(props_nowcast, ['/empty', '/good'], logger)

        assert len(result) == 1
        assert 'fields.n001' in result[0]
