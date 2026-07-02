"""Unit tests for ``_mask_schism_sentinels``.

Validates that the helper masks both pure SCHISM dry-cell sentinels
(``-999.0``) and the blended transitional values that NCEP STOFS-3D-Atl
post-processing emits across wet/dry interfaces (e.g. ``-596.91`` between
``-999`` and a real ocean value), and that it logs at the right level so
the upstream data issue is traceable in the run log.
"""

import logging

import numpy as np

from ofs_skill.model_processing.get_node_ofs import (
    _SCHISM_PHYSICAL_BOUNDS,
    _mask_schism_sentinels,
)

_LOGGER_NAME = 'ofs_skill.model_processing.get_node_ofs'


def test_pure_fill_only_masked_with_info(caplog):
    """All-(-999) and real values: 1 NaN + INFO log, no WARNING."""
    arr = np.array([10.0, -999.0, 12.0])
    log = logging.getLogger(_LOGGER_NAME)
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        cleaned = _mask_schism_sentinels(arr, 'temp', '8518750', 'stofs_3d_atl', log)

    assert np.isnan(cleaned[1])
    assert cleaned[0] == 10.0 and cleaned[2] == 12.0
    info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
    warn_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warn_msgs) == 0
    assert any('1 pure-fill' in r.getMessage() for r in info_msgs)


def test_blended_values_masked_with_warning(caplog):
    """Blended -596.91 / -396.11 are masked with a WARNING."""
    arr = np.array([10.0, -999.0, -596.91, -396.11, 12.0])
    log = logging.getLogger(_LOGGER_NAME)
    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        cleaned = _mask_schism_sentinels(arr, 'temp', '8518750', 'stofs_3d_atl', log)

    assert np.isnan(cleaned[1]) and np.isnan(cleaned[2]) and np.isnan(cleaned[3])
    assert cleaned[0] == 10.0 and cleaned[4] == 12.0
    warn_msgs = [r.getMessage() for r in caplog.records
                 if r.levelno == logging.WARNING]
    assert len(warn_msgs) == 1
    msg = warn_msgs[0]
    assert '2 blended sentinel values' in msg
    assert '-596.91' in msg or '-596.9' in msg
    assert '-396.11' in msg or '-396.1' in msg
    assert '1 pure-fill' in msg
    assert '8518750' in msg
    assert 'stofs_3d_atl' in msg


def test_temp_below_physical_bound_masked():
    """Values like -10 °C and 60 °C fall outside [-5, 50] and are NaN."""
    arr = np.array([-10.0, 5.0, 60.0])
    log = logging.getLogger(_LOGGER_NAME)
    cleaned = _mask_schism_sentinels(arr, 'temp', 's', 'ofs', log)
    assert np.isnan(cleaned[0]) and np.isnan(cleaned[2])
    assert cleaned[1] == 5.0


def test_salinity_uses_salt_bounds():
    """Salinity bounds are [-1, 50] PSU; -50 fails, 35 passes."""
    arr = np.array([35.0, -50.0])
    log = logging.getLogger(_LOGGER_NAME)
    cleaned = _mask_schism_sentinels(arr, 'salinity', 's', 'ofs', log)
    assert cleaned[0] == 35.0
    assert np.isnan(cleaned[1])


def test_currents_speed_clamp():
    """Speed of 50 m/s is unphysical and gets masked under 'currents'."""
    arr = np.array([0.5, 50.0, 1.2])
    log = logging.getLogger(_LOGGER_NAME)
    cleaned = _mask_schism_sentinels(arr, 'currents', 's', 'ofs', log)
    assert cleaned[0] == 0.5 and cleaned[2] == 1.2
    assert np.isnan(cleaned[1])


def test_water_level_keeps_loose_bound():
    """An unknown 'kind' falls back to the |val| >= 999 default mask."""
    arr = np.array([-3.5, 0.0, 2.5])
    log = logging.getLogger(_LOGGER_NAME)
    cleaned = _mask_schism_sentinels(arr, 'wl', 's', 'ofs', log)
    assert np.allclose(cleaned, arr)


def test_no_log_when_clean(caplog):
    """No log record is emitted when nothing needs masking."""
    arr = np.array([10.0, 11.0, 12.0])
    log = logging.getLogger(_LOGGER_NAME)
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        cleaned = _mask_schism_sentinels(arr, 'temp', 's', 'ofs', log)
    assert np.allclose(cleaned, arr)
    assert all(r.name != _LOGGER_NAME for r in caplog.records)


def test_currents_uv_bound_present():
    """The raw u/v bound is registered (used by format_currents)."""
    assert _SCHISM_PHYSICAL_BOUNDS['currents_uv'] == (-10.0, 10.0)


def test_blended_uv_value_masked_for_currents_uv():
    """A blended -596 in raw u/v is masked under the currents_uv kind."""
    arr = np.array([0.5, -596.91, -999.0, 0.7])
    log = logging.getLogger(_LOGGER_NAME)
    cleaned = _mask_schism_sentinels(arr, 'currents_uv', 's', 'ofs', log)
    assert cleaned[0] == 0.5 and cleaned[3] == 0.7
    assert np.isnan(cleaned[1]) and np.isnan(cleaned[2])
