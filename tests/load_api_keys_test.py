"""Tests for load_api_keys utility function."""

import os

import pytest

from ofs_skill.obs_retrieval.utils import load_api_keys


@pytest.fixture()
def api_keys_file(tmp_path):
    """Create a temporary api_keys.conf file."""
    conf = tmp_path / 'api_keys.conf'
    conf.write_text(
        '# Comment line\n'
        '\n'
        'API_USGS_PAT=test-key-12345\n'
        'ANOTHER_KEY=another-value\n'
    )
    return str(conf)


def test_key_loaded_from_file(api_keys_file, monkeypatch):
    """Keys in the config file are loaded when not already in the environment."""
    monkeypatch.delenv('API_USGS_PAT', raising=False)
    monkeypatch.delenv('ANOTHER_KEY', raising=False)

    load_api_keys(api_keys_file)

    assert os.environ['API_USGS_PAT'] == 'test-key-12345'
    assert os.environ['ANOTHER_KEY'] == 'another-value'


def test_env_var_takes_precedence(api_keys_file, monkeypatch):
    """Existing env vars are not overwritten by the config file."""
    monkeypatch.setenv('API_USGS_PAT', 'existing-key')

    load_api_keys(api_keys_file)

    assert os.environ['API_USGS_PAT'] == 'existing-key'


def test_missing_file_is_silent(tmp_path):
    """A missing config file does not raise an error."""
    missing = str(tmp_path / 'nonexistent.conf')
    # Should not raise
    load_api_keys(missing)


def test_comments_and_blank_lines_skipped(tmp_path, monkeypatch):
    """Comment lines and blank lines are ignored."""
    conf = tmp_path / 'keys.conf'
    conf.write_text(
        '# this is a comment\n'
        '\n'
        '   \n'
        '# another comment\n'
        'VALID_KEY=valid-value\n'
    )
    monkeypatch.delenv('VALID_KEY', raising=False)

    load_api_keys(str(conf))

    assert os.environ['VALID_KEY'] == 'valid-value'


def test_empty_values_not_loaded(tmp_path, monkeypatch):
    """Keys with empty values are not set in the environment."""
    conf = tmp_path / 'keys.conf'
    conf.write_text('EMPTY_KEY=\n')
    monkeypatch.delenv('EMPTY_KEY', raising=False)

    load_api_keys(str(conf))

    assert 'EMPTY_KEY' not in os.environ


def test_lines_without_equals_skipped(tmp_path, monkeypatch):
    """Lines that have no '=' delimiter are skipped."""
    conf = tmp_path / 'keys.conf'
    conf.write_text(
        'NO_EQUALS_HERE\n'
        'GOOD_KEY=good-value\n'
    )
    monkeypatch.delenv('GOOD_KEY', raising=False)

    load_api_keys(str(conf))

    assert os.environ['GOOD_KEY'] == 'good-value'
    assert 'NO_EQUALS_HERE' not in os.environ


def test_value_with_equals_sign(tmp_path, monkeypatch):
    """Values containing '=' characters are preserved correctly."""
    conf = tmp_path / 'keys.conf'
    conf.write_text('TOKEN=abc=def=ghi\n')
    monkeypatch.delenv('TOKEN', raising=False)

    load_api_keys(str(conf))

    assert os.environ['TOKEN'] == 'abc=def=ghi'
