"""Regression tests for ``plotting_2d.list_of_json_files`` filename handling.

The 2D observation and model JSON directories are shared with files whose
names don't follow the ``{ofs}_{YYYYMMDD-HHz}_...`` pattern that
``list_of_json_files`` parses. In particular, ``get_hf_radar.py`` writes
``{ofs}_{YYYYMMDD}_ssu_hfradar.json`` (daily, date-only tag) and
``{ofs}_{YYYYMMDD_HHMM}_ssu_hfradar.json`` (hourly) into
``observations/2d``. Those names slipped past the substring exclusions and
crashed the SST stats listing with::

    ValueError: time data '20260726' does not match format '%Y%m%d-%Hz'

These tests build a directory mixing well-formed satellite/model JSONs with
HF radar JSONs and other stray files, and assert the listing selects only the
well-formed files instead of raising.
"""

import logging
from types import SimpleNamespace

import pytest

from ofs_skill.visualization.plotting_2d import list_of_json_files

logger = logging.getLogger(__name__)


def _make_prop():
    return SimpleNamespace(
        ofs='sscofs',
        whichcast='nowcast',
        start_date_full='20260726-00:00:00',
        end_date_full='20260727-00:00:00',
    )


def _touch(directory, *names):
    for name in names:
        (directory / name).write_text('{}')


def test_satellite_listing_skips_hfradar_files(tmp_path):
    """HF radar JSONs (date-only tags) must not crash or pollute the listing."""
    _touch(
        tmp_path,
        'sscofs_20260726-06z_sst_SPoRT.json',
        'sscofs_20260726-18z_sst_SPoRT.json',
        'sscofs_20260726-06z_lnc_SPoRT.json',
        # Daily HF radar tag: date only — used to raise ValueError
        'sscofs_20260726_ssu_hfradar.json',
        'sscofs_20260726_ssv_hfradar.json',
        # Hourly HF radar tag: extra underscore segment
        'sscofs_20260726_0000_ssu_hfradar.json',
        # HF radar ASCII grids (already excluded via 'mag'/'dir')
        'sscofs_mag_20260726_hfradar.txt',
        'sscofs_dir_20260726_hfradar.txt',
    )
    files, dates = list_of_json_files(str(tmp_path), _make_prop(), logger)
    assert len(files) == 2
    assert all('SPoRT' in f and 'lnc' not in f for f in files)
    assert dates == ['20260726-06z', '20260726-18z']


def test_listing_skips_non_json_and_unparseable_names(tmp_path):
    """Stray non-JSON files and malformed names are skipped, not fatal."""
    _touch(
        tmp_path,
        'sscofs_20260726-06z_sst_SPoRT.json',
        'notes.txt',
        'sscofs.nc',
        'sscofs_badname.json',
    )
    files, dates = list_of_json_files(str(tmp_path), _make_prop(), logger)
    assert len(files) == 1
    assert dates == ['20260726-06z']


def test_model_listing_still_selected(tmp_path):
    """Well-formed model JSONs for the requested whichcast still match."""
    _touch(
        tmp_path,
        'sscofs_20260726-06z_sst_model.nowcast.json',
        'sscofs_20260726-18z_sst_model.nowcast.json',
        'sscofs_20260726-daily_sst_model.nowcast.json',
        'sscofs_20260726-06z_ssu_model.nowcast.json',
        'sscofs_20260726-06z_sst_model.forecast_b.json',
    )
    files, dates = list_of_json_files(str(tmp_path), _make_prop(), logger)
    assert len(files) == 2
    assert all('sst_model.nowcast' in f for f in files)
    assert dates == ['20260726-06z', '20260726-18z']


def test_all_files_filtered_raises_filenotfound(tmp_path):
    """If nothing usable remains, the existing FileNotFoundError still fires."""
    _touch(tmp_path, 'sscofs_20260726_ssu_hfradar.json')
    with pytest.raises(FileNotFoundError):
        list_of_json_files(str(tmp_path), _make_prop(), logger)
