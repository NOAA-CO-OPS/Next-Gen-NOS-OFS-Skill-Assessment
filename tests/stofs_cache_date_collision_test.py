"""
Regression tests for the STOFS streaming-cache date collision (issue
#267).

STOFS points filenames are identical in every NODD date directory
(``stofs_2d_glo.20260701/stofs_2d_glo.t00z.points.cwl.nc`` and
``stofs_2d_glo.20260702/stofs_2d_glo.t00z.points.cwl.nc`` share one
basename), so a cache keyed by basename alone serves one date's file
for every other date:

- across runs, a cached copy from any earlier window poisons every
  later run (all ``.prd`` files come out empty when the windows are
  disjoint), and
- within a single multi-day run, days 2..N silently read day 1's file
  (the concatenated axis becomes day-1's cycles repeated, with the
  characteristic ``-(24h - dt)`` rewind after per-cast subsetting).

The fix keys each cached copy by the URL's parent (date) directory
plus basename — ``cached_path_for_url`` — used consistently by the
download resolver (``_resolve_remote_stations_files``) and the cache
scrub (``scrub_cached_copies``).
"""

import logging
import os

import pytest

from ofs_skill.model_processing.intake_scisa import (
    _resolve_remote_stations_files,
)
from ofs_skill.model_processing.model_file_validation import (
    cached_path_for_url,
)

LOG = logging.getLogger('stofs_cache_date_collision_test')

BUCKET = 'https://noaa-gestofs-pds.s3.amazonaws.com'
BASENAME = 'stofs_2d_glo.t00z.points.cwl.nc'
URL_DAY1 = f'{BUCKET}/stofs_2d_glo.20260701/{BASENAME}'
URL_DAY2 = f'{BUCKET}/stofs_2d_glo.20260702/{BASENAME}'


@pytest.fixture(name='fake_download')
def fixture_fake_download(monkeypatch):
    """Stub urllib downloads to write the URL itself as file content.

    Lets tests verify exactly which URL each cached file came from
    without any network access. Returns the list of URLs downloaded.
    """
    downloaded = []

    def _fake_urlretrieve(url, filename):
        downloaded.append(url)
        with open(filename, 'w', encoding='utf-8') as file_handle:
            file_handle.write(url)
        return filename, None

    monkeypatch.setattr(
        'ofs_skill.model_processing.intake_scisa.urllib.request'
        '.urlretrieve', _fake_urlretrieve)
    return downloaded


# ---------------------------------------------------------------------
# cached_path_for_url mapping
# ---------------------------------------------------------------------

def test_same_basename_different_dates_map_to_distinct_paths(tmp_path):
    path1 = cached_path_for_url(URL_DAY1, str(tmp_path))
    path2 = cached_path_for_url(URL_DAY2, str(tmp_path))
    assert path1 != path2
    # The basename itself must be preserved — downstream code (the
    # ``filename`` coordinate, format sniffing) reads it.
    assert os.path.basename(path1) == BASENAME
    assert os.path.basename(path2) == BASENAME
    assert os.path.basename(os.path.dirname(path1)) == \
        'stofs_2d_glo.20260701'


def test_protocol_chain_and_query_string_are_stripped(tmp_path):
    chained = f'simplecache::{URL_DAY1}?versionId=abc'
    assert cached_path_for_url(chained, str(tmp_path)) == \
        cached_path_for_url(URL_DAY1, str(tmp_path))


def test_unsafe_parent_cannot_escape_cache_dir(tmp_path):
    cache = str(tmp_path / 'cache')
    for url in (f'{BUCKET}/../{BASENAME}',
                f'{BUCKET}/./{BASENAME}',
                BASENAME):
        resolved = os.path.realpath(cached_path_for_url(url, cache))
        assert resolved.startswith(os.path.realpath(cache))


# ---------------------------------------------------------------------
# download resolver
# ---------------------------------------------------------------------

def test_resolver_downloads_each_date_separately(tmp_path, fake_download):
    resolved = _resolve_remote_stations_files(
        [URL_DAY1, URL_DAY2], str(tmp_path), LOG)
    assert len(resolved) == 2
    assert resolved[0] != resolved[1]
    assert fake_download == [URL_DAY1, URL_DAY2]
    # Each cached file must hold its own date's content.
    with open(resolved[0], encoding='utf-8') as file_handle:
        assert file_handle.read() == URL_DAY1
    with open(resolved[1], encoding='utf-8') as file_handle:
        assert file_handle.read() == URL_DAY2


def test_resolver_reuses_matching_cached_copy(tmp_path, fake_download):
    first = _resolve_remote_stations_files([URL_DAY1], str(tmp_path), LOG)
    again = _resolve_remote_stations_files([URL_DAY1], str(tmp_path), LOG)
    assert again == first
    # Only the first call downloads; the second is served from cache.
    assert fake_download == [URL_DAY1]


def test_resolver_ignores_legacy_flat_basename_copy(tmp_path,
                                                    fake_download):
    """A poisoned pre-fix cache (flat ``<cache>/<basename>`` file from
    some other date) must never be served; the correct file is
    downloaded to its date-keyed location instead."""
    legacy = tmp_path / BASENAME
    legacy.write_text('stale data from another date', encoding='utf-8')
    resolved = _resolve_remote_stations_files(
        [URL_DAY1], str(tmp_path), LOG)
    assert fake_download == [URL_DAY1]
    with open(resolved[0], encoding='utf-8') as file_handle:
        assert file_handle.read() == URL_DAY1
    # The legacy file is left alone (inert), not read and not deleted.
    assert legacy.read_text(encoding='utf-8') == \
        'stale data from another date'


def test_resolver_passes_local_paths_through(tmp_path, fake_download):
    local = tmp_path / 'local.nc'
    local.write_text('local', encoding='utf-8')
    resolved = _resolve_remote_stations_files(
        [str(local), URL_DAY1], str(tmp_path / 'cache'), LOG)
    assert resolved[0] == str(local)
    assert fake_download == [URL_DAY1]


def test_resolver_falls_back_to_url_on_download_failure(tmp_path,
                                                        monkeypatch):
    def _fail(url, filename):
        raise OSError('network down')

    monkeypatch.setattr(
        'ofs_skill.model_processing.intake_scisa.urllib.request'
        '.urlretrieve', _fail)
    resolved = _resolve_remote_stations_files(
        [URL_DAY1], str(tmp_path), LOG)
    assert resolved == [URL_DAY1]
    # No .part debris left behind for the failed download.
    cached = cached_path_for_url(URL_DAY1, str(tmp_path))
    assert not os.path.exists(cached)
    assert not os.path.exists(cached + '.part')
