"""Tests for bin/obs_retrieval/get_satellite_observations.py.

Covers issue #120: partial GOES SST downloads are silently cached and
later surface as cryptic HDF5 traces from masksat_by_ofs opening a
missing concat file. The fixes validate per-hour download size, make
concat_sat honest about write success, fix the stale masked-file
rebuild logic, and unify path prefixes across stages.
"""

import importlib.util
import logging
import os
import socket
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Import the module from bin/ (not on the normal package path)
# ---------------------------------------------------------------------------
_MODULE_PATH = (
    Path(__file__).resolve().parent.parent
    / 'bin' / 'obs_retrieval' / 'get_satellite_observations.py'
)
_spec = importlib.util.spec_from_file_location(
    'get_satellite_observations', _MODULE_PATH,
)
assert _spec is not None and _spec.loader is not None
sat = importlib.util.module_from_spec(_spec)

logging.basicConfig(level=logging.DEBUG)
sys.modules['get_satellite_observations'] = sat
_spec.loader.exec_module(sat)


@pytest.fixture
def logger():
    return logging.getLogger('test_get_satellite_observations')


def _make_byte_file(path, n_bytes):
    """Create a file at ``path`` of exactly ``n_bytes`` bytes."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(b'\x00' * n_bytes)


class _FakeStreamingResponse:
    """Minimal urlopen response stand-in that yields ``total`` bytes
    of zero-padding in chunked reads."""

    def __init__(self, total):
        self.remaining = total

    def read(self, n):
        if self.remaining <= 0:
            return b''
        give = min(n, self.remaining)
        self.remaining -= give
        return b'\x00' * give

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _patch_urlopen_yielding(n_bytes):
    """Returns a patch context that makes urlopen yield exactly ``n_bytes``."""
    return patch.object(
        sat.urllib.request, 'urlopen',
        return_value=_FakeStreamingResponse(n_bytes),
    )


# ===========================================================================
# _download_single_file — size validation + cleanup behavior
# ===========================================================================
class TestDownloadSizeValidation:
    """A truncated download or undersized cache must be rejected and cleaned
    up; the per-hour failure returns None and the caller skips that hour."""

    URL = (
        'https://example.test/'
        '20260425170000-NOAA-L3C_GHRSST-SSTsubskin-'
        'ABI_G19-ACSPO_V3.00-v02.1-fv01.0.nc'
    )

    def test_rejects_undersized_raw_download(self, tmp_path, logger):
        """48 KB truncated raw download is removed; returns None."""
        obs2d_dir = str(tmp_path)

        with _patch_urlopen_yielding(48 * 1024):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        # Raw download cleaned up.
        raw_path = os.path.join(obs2d_dir, self.URL.rsplit('/', 1)[-1])
        assert not os.path.exists(raw_path)
        # Trimmed sat_fname was never written.
        sat_fname = os.path.join(
            obs2d_dir, 'G19',
            self.URL.rsplit('/', 1)[-1].rsplit('.', 1)[0].split('.')[0]
            + '_sst.nc',
        )
        assert not os.path.exists(sat_fname)

    def test_rejects_undersized_cached_file(self, tmp_path, logger):
        """A pre-existing 10 KB cached file is discarded and a re-download
        is attempted; if the retry also fails, returns None."""
        obs2d_dir = str(tmp_path)
        # Pre-create the corrupt cached file.
        sat_fname = os.path.join(
            obs2d_dir, 'G19',
            self.URL.rsplit('/', 1)[-1].rsplit('.', 1)[0].split('.')[0]
            + '_sst.nc',
        )
        _make_byte_file(sat_fname, 10 * 1024)
        assert os.path.exists(sat_fname)

        # Make the retry fail too — undersized again (48 KB < 1 MB raw min).
        with _patch_urlopen_yielding(48 * 1024):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        # Cached corrupt file was removed (the new download was also
        # rejected, so sat_fname stays absent at the end).
        assert not os.path.exists(sat_fname)

    def test_returns_none_on_connection_error(self, tmp_path, logger):
        """A mid-stream connection reset (OSError subclass) returns None;
        any partial file is cleaned up."""
        obs2d_dir = str(tmp_path)
        os.makedirs(os.path.join(obs2d_dir, 'G19'), exist_ok=True)

        with patch.object(
            sat.urllib.request, 'urlopen',
            side_effect=ConnectionResetError('connection reset by peer'),
        ):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        raw_path = os.path.join(obs2d_dir, self.URL.rsplit('/', 1)[-1])
        assert not os.path.exists(raw_path)

    def test_download_aborts_on_size_cap_exceeded(self, tmp_path, logger):
        """A misbehaving upstream that streams more than _RAW_SAT_MAX_BYTES
        is aborted mid-stream; partial file is removed; returns None."""
        obs2d_dir = str(tmp_path)
        os.makedirs(os.path.join(obs2d_dir, 'G19'), exist_ok=True)

        oversize_total = sat._RAW_SAT_MAX_BYTES + 4 * sat._DOWNLOAD_CHUNK_BYTES
        with _patch_urlopen_yielding(oversize_total):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        raw_path = os.path.join(obs2d_dir, self.URL.rsplit('/', 1)[-1])
        assert not os.path.exists(raw_path)

    def test_download_returns_none_on_timeout(self, tmp_path, logger):
        """A connect/read timeout raises socket.timeout (an OSError);
        the helper catches it, cleans up, and returns None."""
        obs2d_dir = str(tmp_path)
        os.makedirs(os.path.join(obs2d_dir, 'G19'), exist_ok=True)

        with patch.object(
            sat.urllib.request, 'urlopen',
            side_effect=socket.timeout('read timed out'),
        ):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        raw_path = os.path.join(obs2d_dir, self.URL.rsplit('/', 1)[-1])
        assert not os.path.exists(raw_path)

    def test_download_aborts_on_wall_clock_timeout(
        self, tmp_path, logger, monkeypatch,
    ):
        """A slow-loris upstream sending bytes within the per-read timeout
        but never finishing must be aborted by the wall-clock guard."""
        obs2d_dir = str(tmp_path)
        os.makedirs(os.path.join(obs2d_dir, 'G19'), exist_ok=True)

        # Force the wall-clock budget to expire immediately on the
        # second monotonic() call (inside the loop). The first call
        # captures the deadline; subsequent calls jump past it.
        clock = [0.0]

        def fake_monotonic():
            clock[0] += sat._DOWNLOAD_TOTAL_TIMEOUT_SECONDS + 1
            return clock[0]

        monkeypatch.setattr(sat.time, 'monotonic', fake_monotonic)

        # urlopen returns a stream that would otherwise yield healthy
        # bytes — but the wall-clock check trips before any read.
        with _patch_urlopen_yielding(8 * 1024 * 1024):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result is None
        raw_path = os.path.join(obs2d_dir, self.URL.rsplit('/', 1)[-1])
        assert not os.path.exists(raw_path)

    def test_accepts_healthy_cached_file(self, tmp_path, logger):
        """A healthy cached file is returned as-is, no re-download."""
        obs2d_dir = str(tmp_path)
        sat_fname = os.path.join(
            obs2d_dir, 'G19',
            self.URL.rsplit('/', 1)[-1].rsplit('.', 1)[0].split('.')[0]
            + '_sst.nc',
        )
        _make_byte_file(sat_fname, 100 * 1024)  # > _TRIMMED_SAT_MIN_BYTES

        # urlopen must NOT be called.
        with patch.object(
            sat.urllib.request, 'urlopen',
            side_effect=AssertionError('should not download'),
        ):
            result = sat._download_single_file(
                self.URL, obs2d_dir, logger, 'GOES',
            )

        assert result == sat_fname


# ===========================================================================
# get_sat — partial-failure resilience
# ===========================================================================
class TestGetSatPartialFailure:
    def test_continues_when_some_downloads_fail(self, tmp_path, logger):
        """If some _download_single_file calls return None, get_sat returns
        the successful subset and does not raise."""
        urls = [f'https://example.test/file_{i}_G19.nc' for i in range(3)]

        # First two succeed (return path strings), third returns None.
        def fake_dl(url, obs2d_dir, log, sat_type):
            if 'file_2' in url:
                return None
            return f'/fake/{url.rsplit("/", 1)[-1]}'

        with patch.object(sat, '_download_single_file', side_effect=fake_dl):
            result = sat.get_sat(urls, str(tmp_path), logger, 'GOES')

        assert len(result) == 2
        assert all(r is not None for r in result)


# ===========================================================================
# concat_sat — write honesty
# ===========================================================================
class TestConcatSatHonesty:
    """concat_sat must never return a path string for a file it didn't
    successfully write."""

    def _make_inputs(self, tmp_path):
        """Make a directory with one valid .nc input, return (obs2d_dir,
        list_of_files, prop1)."""
        obs2d_dir = tmp_path / 'obs2d'
        sat_subdir = obs2d_dir / 'G19'
        sat_subdir.mkdir(parents=True)
        # Create a real netCDF input so xr.open_dataset/concat have
        # something to work with.
        import numpy as np
        import xarray as xr
        ds = xr.Dataset(
            {'sst': (('time', 'lat', 'lon'), np.zeros((1, 4, 4)))},
            coords={'time': [0], 'lat': range(4), 'lon': range(4)},
        )
        # File basename must contain '00-' so concat_sat's name-munging
        # produces a meaningful save_path (.split('00-')[-1]).
        input_path = sat_subdir / (
            '20260425170000-NOAA-L3C_GHRSST-SSTsubskin-'
            'ABI_G19-ACSPO_V3.00_sst.nc'
        )
        ds.to_netcdf(str(input_path))
        list_of_files = [str(input_path)]
        prop1 = SimpleNamespace(
            start_date_full='2026-04-25T17:00:00Z',
            end_date_full='2026-04-26T17:00:00Z',
        )
        return str(obs2d_dir), list_of_files, prop1

    def test_exits_when_to_netcdf_silently_no_ops(self, tmp_path, logger):
        """If to_netcdf returns without writing the file, concat_sat must
        sys.exit rather than return a phantom path."""
        obs2d_dir, list_of_files, prop1 = self._make_inputs(tmp_path)

        # Patch to_netcdf to silently do nothing. Disable the purge sweep
        # so it doesn't consume the small synthetic test inputs.
        with patch.object(sat, '_purge_undersized_sat_files'):
            with patch('xarray.Dataset.to_netcdf', return_value=None):
                with pytest.raises(SystemExit):
                    sat.concat_sat(list_of_files, obs2d_dir, logger, prop1)

    def test_reraises_keyboard_interrupt(self, tmp_path, logger):
        """KeyboardInterrupt during write must propagate, not be swallowed."""
        obs2d_dir, list_of_files, prop1 = self._make_inputs(tmp_path)

        with patch.object(sat, '_purge_undersized_sat_files'):
            with patch(
                'xarray.Dataset.to_netcdf', side_effect=KeyboardInterrupt,
            ):
                with pytest.raises(KeyboardInterrupt):
                    sat.concat_sat(list_of_files, obs2d_dir, logger, prop1)

    def test_exits_on_oserror_during_write(self, tmp_path, logger):
        """OSError (disk full, permission denied) during write must abort."""
        obs2d_dir, list_of_files, prop1 = self._make_inputs(tmp_path)

        with patch.object(sat, '_purge_undersized_sat_files'):
            with patch(
                'xarray.Dataset.to_netcdf', side_effect=OSError('disk full'),
            ):
                with pytest.raises(SystemExit):
                    sat.concat_sat(list_of_files, obs2d_dir, logger, prop1)


# ===========================================================================
# _purge_undersized_sat_files
# ===========================================================================
class TestPurgeUndersized:
    def test_removes_undersized_and_keeps_healthy(self, tmp_path, logger):
        """Sweeps a directory, removing only files below threshold."""
        d = tmp_path / 'G19'
        d.mkdir()
        bad = d / 'bad_sst.nc'
        good = d / 'good_sst.nc'
        not_sst = d / 'unrelated.nc'
        _make_byte_file(str(bad), 10 * 1024)
        _make_byte_file(str(good), 100 * 1024)
        _make_byte_file(str(not_sst), 10 * 1024)

        sat._purge_undersized_sat_files(str(d), 50 * 1024, logger)

        assert not bad.exists()
        assert good.exists()
        assert not_sst.exists()  # only *_sst.nc are swept

    def test_handles_missing_directory(self, tmp_path, logger):
        """Nonexistent directory is a no-op, not a crash."""
        sat._purge_undersized_sat_files(
            str(tmp_path / 'does-not-exist'), 50 * 1024, logger,
        )


# ===========================================================================
# _masked_file_is_fresh
# ===========================================================================
class TestMaskedFileFreshness:
    """Stale or undersized masked outputs must trigger rebuild, not be
    silently reused — that was the latent bug in the original
    if/else structure at the masked-file check."""

    def test_missing_file_not_fresh(self, tmp_path):
        """A nonexistent path is unambiguously not fresh."""
        assert not sat._masked_file_is_fresh(str(tmp_path / 'missing.nc'))

    def test_fresh_large_file_is_fresh(self, tmp_path):
        """Recently written, sufficiently large file: skip rebuild."""
        path = tmp_path / 'cbofs.nc'
        _make_byte_file(str(path), 200 * 1024)
        assert sat._masked_file_is_fresh(str(path))

    def test_old_file_not_fresh(self, tmp_path):
        """File older than 1h: must trigger rebuild even if size is fine."""
        path = tmp_path / 'cbofs.nc'
        _make_byte_file(str(path), 200 * 1024)
        # 2 hours old.
        old = path.stat().st_mtime - 7200
        os.utime(str(path), (old, old))
        assert not sat._masked_file_is_fresh(str(path))

    def test_undersized_file_not_fresh(self, tmp_path):
        """File below 50 KB: must trigger rebuild even if mtime is fresh."""
        path = tmp_path / 'cbofs.nc'
        _make_byte_file(str(path), 10 * 1024)  # < 50 KB threshold
        assert not sat._masked_file_is_fresh(str(path))


# ===========================================================================
# _verify_masked_output
# ===========================================================================
class TestVerifyMaskedOutput:
    """Post-clip verification ensures masked outputs land on disk —
    catches the failure mode where to_netcdf reports success but
    leaves nothing for downstream stages to read."""

    def test_passes_for_healthy_file(self, tmp_path, logger):
        """Healthy file (>= 50 KB): no exception."""
        path = tmp_path / 'cbofs.nc'
        _make_byte_file(str(path), 200 * 1024)
        # Should not raise.
        sat._verify_masked_output(str(path), logger)

    def test_exits_for_missing_file(self, tmp_path, logger):
        """Nonexistent path aborts the run."""
        with pytest.raises(SystemExit):
            sat._verify_masked_output(str(tmp_path / 'missing.nc'), logger)

    def test_exits_for_undersized_file(self, tmp_path, logger):
        """File below 50 KB threshold aborts the run."""
        path = tmp_path / 'cbofs.nc'
        _make_byte_file(str(path), 10 * 1024)
        with pytest.raises(SystemExit):
            sat._verify_masked_output(str(path), logger)


# ===========================================================================
# parameter_dir_validation — path resolution
# ===========================================================================
class TestPathResolution:
    def test_relative_prop_path_resolved_to_absolute(
        self, tmp_path, logger, monkeypatch,
    ):
        """When prop.path is relative, parameter_dir_validation resolves it
        against cwd so all stages agree on the absolute prefix."""
        # Set up a fake repo root with the required folders.
        ofs_extents = tmp_path / 'ofs_extents'
        ofs_extents.mkdir()
        (ofs_extents / 'cbofs.shp').write_text('fake')

        monkeypatch.chdir(tmp_path)

        prop = SimpleNamespace(
            ofs='cbofs',
            path='./',
            start_date_full='2026-04-25T17:00:00Z',
            end_date_full='2026-04-26T17:00:00Z',
        )
        dir_params = {
            'home': str(tmp_path),
            'ofs_extents_dir': 'ofs_extents',
            'data_dir': 'data',
            'observations_dir': 'observations',
            '2d_satellite_dir': '2d_satellite',
        }

        sat.parameter_dir_validation(prop, dir_params, logger)

        resolved = prop.data_observations_2d_satellite_path
        assert os.path.isabs(resolved)
        assert resolved.startswith(str(tmp_path.resolve()))
        assert os.path.isdir(resolved)
