"""Datum-environment faults must not be logged as "data not found".

A station is dropped from the observation control file either because
the provider genuinely has no data, or because the vertical datum
conversion failed. Until issues #127/#216/#295 both were reported
identically at INFO level, naming the provider -- which is why 192 USGS
stations disappeared from every production run for months without anyone
seeing a single suspicious log line.
"""
from __future__ import annotations

import importlib
import logging

import pyproj.exceptions
import pytest

# Import the module itself. A plain ``from ofs_skill.obs_retrieval import
# write_obs_ctlfile`` would bind the same-named *function* that the
# package __init__ re-exports, shadowing the module under test.
write_obs_ctlfile = importlib.import_module(
    'ofs_skill.obs_retrieval.write_obs_ctlfile')


@pytest.fixture()
def logger(caplog):
    """A real logger whose records caplog can inspect."""
    caplog.set_level(logging.DEBUG)
    return logging.getLogger('obs_ctl_datum_failure_logging_test')


def _raise_proj_failure(*args, **kwargs):
    """Stand in for any call that ends in an unbuildable PROJ pipeline."""
    raise PROJ_FAILURE


PROJ_FAILURE = pyproj.exceptions.ProjError(
    'Invalid projection +proj=pipeline: Internal Proj Error: proj_create: '
    'Error 1029 (File not found or invalid)')


@pytest.mark.parametrize('provider', ['USGS', 'CO-OPS', 'NDBC', 'CHS'])
def test_datum_failure_is_error_level_not_info(caplog, logger, provider):
    """The environment fault is reported at ERROR, distinctly worded."""
    write_obs_ctlfile._log_station_failure(
        provider, 'water_level', 8638901, PROJ_FAILURE, logger)

    records = caplog.records
    assert records, 'nothing was logged at all'
    assert {r.levelno for r in records} == {logging.ERROR}, [
        (r.levelname, r.message) for r in records]

    message = records[0].message
    # It must not claim the provider had no data -- it did.
    assert 'data not found' not in message, message
    assert 'DROPPED' in message, message
    assert 'vertical datum' in message, message
    assert '8638901' in message, message
    assert provider in message, message


def test_real_no_data_still_reads_as_no_data(caplog, logger):
    """A genuine empty-station failure keeps its INFO "data not found"."""
    write_obs_ctlfile._log_station_failure(
        'USGS', 'water_level', 12345678,
        KeyError('Datum'), logger)

    records = caplog.records
    assert {r.levelno for r in records} == {logging.INFO}, [
        (r.levelname, r.message) for r in records]
    assert 'data not found' in records[0].message


def test_the_two_failures_never_look_alike(caplog, logger):
    """A broken datum environment and an empty station are distinguishable."""
    write_obs_ctlfile._log_station_failure(
        'USGS', 'water_level', 8638901, PROJ_FAILURE, logger)
    datum_records = [(r.levelno, r.message) for r in caplog.records]

    caplog.clear()
    write_obs_ctlfile._log_station_failure(
        'USGS', 'water_level', 8638901, ValueError('empty frame'), logger)
    nodata_records = [(r.levelno, r.message) for r in caplog.records]

    assert datum_records != nodata_records
    assert datum_records[0][0] > nodata_records[0][0]


def test_usgs_handler_routes_a_datum_failure_to_the_error_path(
        monkeypatch, caplog, logger):
    """End to end through the handler that dropped the 192 stations.

    The retrieval succeeds (so this is emphatically not missing data) and
    the datum conversion is what fails.
    """
    import pandas as pd

    frame = pd.DataFrame({'Datum': ['NAVD88'], 'DEP01': [1.0]})
    monkeypatch.setattr(write_obs_ctlfile, 'retrieve_usgs_station',
                        lambda *a, **k: frame)

    def boom(*args, **kwargs):
        raise PROJ_FAILURE

    monkeypatch.setattr(write_obs_ctlfile.vdatum_resilient, 'convert', boom)

    result = write_obs_ctlfile._process_usgs_station(
        8638901, 'Test Station', -76.33, 36.94,
        '2025-07-01T00:00:00Z', '2025-07-02T00:00:00Z',
        'water_level', 'wl', 'MLLW', 'cbofs', logger)

    # The station is still dropped -- but now it is loudly reported.
    assert result == []
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, [(r.levelname, r.message) for r in caplog.records]
    assert 'DROPPED' in errors[0].message
    assert 'data not found' not in errors[0].message


# ---------------------------------------------------------------------------
# Every provider, driven through its real handler.
#
# Asserting on ``_log_station_failure`` alone proves nothing about the
# handlers: the provider name is a literal the test passes in and gets
# back. Deleting the routing from three of the four handlers left that
# test green, so each handler is exercised here for real.
# ---------------------------------------------------------------------------


def _assert_dropped_loudly(result, caplog, provider):
    """The station is gone from the control file, and it is not quiet."""
    assert result == []
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, [(r.levelname, r.message) for r in caplog.records]
    message = errors[0].message
    assert 'DROPPED' in message, message
    assert provider in message, message
    assert 'data not found' not in message, message


def test_coops_handler_routes_a_datum_failure_to_the_error_path(
        monkeypatch, caplog, logger):
    """CO-OPS: observations retrieved under a fallback datum, then no pipeline."""
    import pandas as pd

    frame = pd.DataFrame({'DateTime': ['2025-07-01T00:00:00Z'], 'OBS': [1.0]})
    calls = {'n': 0}

    def _retrieve(*args, **kwargs):
        calls['n'] += 1
        # The station has no MLLW series, so the handler walks its datum
        # fallback list and settles on NAVD -- which then has to be
        # converted to the requested MLLW.
        return None if calls['n'] == 1 else frame

    monkeypatch.setattr(write_obs_ctlfile, 'retrieve_t_and_c_station',
                        _retrieve)
    monkeypatch.setattr(write_obs_ctlfile.vdatum_resilient, 'convert',
                        _raise_proj_failure)

    result = write_obs_ctlfile._process_coops_station(
        8638901, 'Test Station', -76.33, 36.94,
        '2025-07-01T00:00:00Z', '2025-07-02T00:00:00Z',
        'water_level', 'wl', 'MLLW', ['NAVD', 'NAVD88', 'MLLW'],
        'cbofs', logger)

    _assert_dropped_loudly(result, caplog, 'CO-OPS')


def test_ndbc_handler_routes_a_datum_failure_to_the_error_path(
        monkeypatch, caplog, logger):
    """NDBC: an MLLW buoy that cannot be converted to the requested datum."""
    import pandas as pd

    frame = pd.DataFrame({'Datum': ['MLLW', 'MLLW'], 'DEP01': [1.0, 1.0]})
    monkeypatch.setattr(write_obs_ctlfile, 'retrieve_ndbc_station',
                        lambda *a, **k: frame)
    monkeypatch.setattr(write_obs_ctlfile.vdatum_resilient, 'convert',
                        _raise_proj_failure)

    result = write_obs_ctlfile._process_ndbc_station(
        44041, 'Test Buoy', -76.33, 36.94,
        '2025-07-01T00:00:00Z', '2025-07-02T00:00:00Z',
        'water_level', 'wl', 'NAVD88', 'cbofs', logger)

    _assert_dropped_loudly(result, caplog, 'NDBC')


def test_chs_handler_routes_a_datum_failure_to_the_error_path(
        monkeypatch, caplog, logger):
    """CHS already logged at ERROR, but as "data not processed".

    That wording sends the reader after the provider when the fault is a
    broken PROJ install, so this handler routes ProjError through the
    shared reporter too. The failure is raised from the retrieval call
    because everything after it needs live CHS metadata lookups.
    """
    monkeypatch.setattr(write_obs_ctlfile, '_get_chs_code',
                        lambda *a, **k: 'TEST')
    monkeypatch.setattr(write_obs_ctlfile, 'retrieve_chs_station',
                        _raise_proj_failure)

    result = write_obs_ctlfile._process_chs_station(
        '00065', 'Test Station', -63.5, 44.6,
        '2025-07-01T00:00:00Z', '2025-07-02T00:00:00Z',
        'water_level', 'wl', 'MLLW', 'necofs', logger)

    _assert_dropped_loudly(result, caplog, 'CHS')


def test_chs_keeps_its_own_wording_for_a_real_provider_failure(
        monkeypatch, caplog, logger):
    """A non-PROJ CHS fault must not be relabelled as a datum problem."""
    monkeypatch.setattr(write_obs_ctlfile, '_get_chs_code',
                        lambda *a, **k: 'TEST')

    def _boom(*args, **kwargs):
        raise ValueError('CHS API returned nothing')

    monkeypatch.setattr(write_obs_ctlfile, 'retrieve_chs_station', _boom)

    result = write_obs_ctlfile._process_chs_station(
        '00065', 'Test Station', -63.5, 44.6,
        '2025-07-01T00:00:00Z', '2025-07-02T00:00:00Z',
        'water_level', 'wl', 'MLLW', 'necofs', logger)

    assert result == []
    messages = [r.message for r in caplog.records]
    assert any('data not processed' in m for m in messages), messages
    assert not any('DROPPED' in m for m in messages), messages
