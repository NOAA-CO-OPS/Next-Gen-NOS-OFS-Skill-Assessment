"""Tests for the station-match distance cutoff config reader.

``get_station_match_max_dist`` reads ``[settings] station_match_max_dist_km``
so the same value drives both the great-circle match test and the pre-filter
box in ``index_nearest_station`` (issue #200). These tests confirm parsing,
fallback, and validation behaviour without any network access.
"""

from __future__ import annotations

import logging

from ofs_skill.model_processing.indexing import STATION_MATCH_MAX_DIST_KM
from ofs_skill.obs_retrieval.utils import get_station_match_max_dist

logger = logging.getLogger('station_match_dist_config_test')


def _write_conf(tmp_path, body: str):
    path = tmp_path / 'ofs_dps.conf'
    path.write_text(body)
    return str(path)


def test_reads_value_from_settings(tmp_path):
    cfg = _write_conf(
        tmp_path,
        '[settings]\nstation_match_max_dist_km=6.5\n',
    )
    assert get_station_match_max_dist(logger, config_file=cfg) == 6.5


def test_missing_key_falls_back_to_default(tmp_path):
    cfg = _write_conf(tmp_path, '[settings]\nstatic_plots=False\n')
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )


def test_missing_section_falls_back_to_default(tmp_path):
    cfg = _write_conf(tmp_path, '[directories]\nhome=/tmp\n')
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )


def test_non_numeric_value_falls_back_to_default(tmp_path):
    cfg = _write_conf(
        tmp_path,
        '[settings]\nstation_match_max_dist_km=notanumber\n',
    )
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )


def test_non_positive_value_falls_back_to_default(tmp_path):
    cfg = _write_conf(
        tmp_path,
        '[settings]\nstation_match_max_dist_km=0\n',
    )
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )


def test_nan_value_falls_back_to_default(tmp_path):
    # float('nan') passes a plain "<= 0" guard (all NaN comparisons are
    # False) and would silently drop every station downstream — it must
    # fall back to the default like any other invalid value.
    cfg = _write_conf(
        tmp_path,
        '[settings]\nstation_match_max_dist_km=nan\n',
    )
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )


def test_infinite_value_falls_back_to_default(tmp_path):
    # inf parses as a valid positive float but makes the cutoff (and the
    # candidate box) unbounded, defeating the proximity requirement.
    cfg = _write_conf(
        tmp_path,
        '[settings]\nstation_match_max_dist_km=inf\n',
    )
    assert (
        get_station_match_max_dist(logger, config_file=cfg)
        == STATION_MATCH_MAX_DIST_KM
    )
