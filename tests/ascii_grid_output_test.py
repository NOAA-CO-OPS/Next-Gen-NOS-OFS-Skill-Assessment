"""Tests for ESRI ASCII Grid output and current vector computation."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pytest

from ofs_skill.visualization.processing_2d import (
    _build_ascii_grid_filename,
    _compute_current_mag_dir,
    write_2d_array_to_ascii_grid,
)


class TestWrite2dArrayToAsciiGrid:
    """Tests for write_2d_array_to_ascii_grid."""

    def test_basic_write_and_read(self, tmp_path):
        """Round-trip: write a grid, read it back, verify header and data.

        Inputs are cell centers; xllcorner/yllcorner are the SW corner
        of the SW cell (cell center minus half cellsize).
        """
        lon = np.array([[10.0, 10.5, 11.0], [10.0, 10.5, 11.0]])
        lat = np.array([[20.0, 20.0, 20.0], [20.5, 20.5, 20.5]])
        data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])

        outfile = str(tmp_path / 'test.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        with open(outfile) as f:
            lines = f.readlines()

        assert lines[0].strip() == 'ncols        3'
        assert lines[1].strip() == 'nrows        2'
        assert 'xllcorner' in lines[2]
        assert float(lines[2].split()[-1]) == pytest.approx(9.75)
        assert 'yllcorner' in lines[3]
        assert float(lines[3].split()[-1]) == pytest.approx(19.75)
        assert 'cellsize' in lines[4]
        assert float(lines[4].split()[-1]) == pytest.approx(0.5)
        assert lines[5].strip() == 'NODATA_value -9999'

        assert len(lines) == 8

    def test_nan_replaced_with_nodata(self, tmp_path):
        """NaN values should be written as the NODATA value."""
        lon = np.array([[0.0, 1.0], [0.0, 1.0]])
        lat = np.array([[0.0, 0.0], [1.0, 1.0]])
        data = np.array([[1.0, np.nan], [np.nan, 4.0]])

        outfile = str(tmp_path / 'nan_test.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        with open(outfile) as f:
            lines = f.readlines()

        # Data rows start at line 6, first row is northernmost (flipped)
        north_row = lines[6].strip().split()
        south_row = lines[7].strip().split()

        # North row (originally row index 1: [nan, 4.0]) -> flipped to first
        assert north_row[0] == '-9999'
        assert float(north_row[1]) == pytest.approx(4.0)

        # South row (originally row index 0: [1.0, nan]) -> flipped to second
        assert float(south_row[0]) == pytest.approx(1.0)
        assert south_row[1] == '-9999'

    def test_row_order_north_to_south(self, tmp_path):
        """First data row should be northernmost latitude."""
        # Square cells (dx == dy) so the header keeps the single
        # ``cellsize`` keyword (6-line header). Non-square cells would
        # add a separate ``dy`` line -- covered separately in
        # test_non_square_cells_emit_dx_dy.
        lon = np.array([[0.0, 1.0], [0.0, 1.0], [0.0, 1.0]])
        lat = np.array([[10.0, 10.0], [11.0, 11.0], [12.0, 12.0]])
        # Row 0 is south (lat=10), row 2 is north (lat=12)
        data = np.array([[100.0, 100.0], [200.0, 200.0], [300.0, 300.0]])

        outfile = str(tmp_path / 'order_test.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        with open(outfile) as f:
            lines = f.readlines()

        # First data row (line 6) should be northernmost (300)
        first_data = [float(v) for v in lines[6].strip().split()]
        assert first_data == [300.0, 300.0]

        # Last data row (line 8) should be southernmost (100)
        last_data = [float(v) for v in lines[8].strip().split()]
        assert last_data == [100.0, 100.0]

    def test_custom_nodata_value(self, tmp_path):
        """Custom NODATA value should be used in header and data."""
        lon = np.array([[0.0, 1.0], [0.0, 1.0]])
        lat = np.array([[0.0, 0.0], [1.0, 1.0]])
        data = np.array([[1.0, np.nan], [3.0, 4.0]])

        outfile = str(tmp_path / 'custom_nodata.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile, nodata_value=-999)

        with open(outfile) as f:
            lines = f.readlines()

        assert lines[5].strip() == 'NODATA_value -999'
        # The NaN in south row (row 0, flipped to last)
        south_row = lines[7].strip().split()
        assert south_row[1] == '-999'

    def test_non_square_cells_emit_dx_dy(self, tmp_path):
        """Non-square cells (HF radar shape) write dx/dy, not cellsize.

        Issue #144: cell-centered grids with dx != dy were silently
        collapsed onto a single ``cellsize = dx``, dropping dy and
        biasing every row's latitude.
        """
        # USEGC-shape: dx=0.058 deg, dy=0.054 deg
        lons1d = np.array([-77.0, -76.942, -76.884])
        lats1d = np.array([37.0, 37.054, 37.108])
        lon, lat = np.meshgrid(lons1d, lats1d)
        data = np.arange(9, dtype=float).reshape(3, 3)

        outfile = str(tmp_path / 'nonsquare.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        hdr = {}
        with open(outfile) as f:
            for _ in range(7):
                key, val = f.readline().split()
                hdr[key] = val
        assert 'dx' in hdr and 'dy' in hdr
        assert 'cellsize' not in hdr
        assert float(hdr['dx']) == pytest.approx(0.058, abs=1e-9)
        assert float(hdr['dy']) == pytest.approx(0.054, abs=1e-9)
        # SW cell center is (-77.0, 37.0); SW corner is half-cell SW.
        assert float(hdr['xllcorner']) == pytest.approx(-77.0 - 0.058/2)
        assert float(hdr['yllcorner']) == pytest.approx(37.0 - 0.054/2)

    def test_xllcorner_yllcorner_match_asc_writer(self, tmp_path):
        """The .txt header corners must match what the .asc writer would
        emit for the same source grid (issue #144 root cause).

        ``bin/obs_retrieval/get_hf_radar.export_ascii`` uses
        ``rasterio.from_origin(lons.min() - dx/2, lats.max() + dy/2)``
        which makes ``xllcorner == lons.min() - dx/2`` and
        ``yllcorner == lats.min() - dy/2``. The .txt writer must
        agree so vector arrows render at the same geographic location
        as the .asc raster on the front end.
        """
        # Mimic an HFR-style cell-center grid (south-up source axis).
        lons1d = np.linspace(-76.0, -75.0, 11)
        lats1d = np.linspace(37.0, 38.0, 11)
        lon, lat = np.meshgrid(lons1d, lats1d)
        data = np.zeros_like(lon)

        outfile = str(tmp_path / 'corners.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        hdr = {}
        with open(outfile) as f:
            for _ in range(6):
                key, val = f.readline().split()
                hdr[key] = val
        cellsize = float(hdr['cellsize'])
        assert float(hdr['xllcorner']) == pytest.approx(
            lons1d.min() - cellsize / 2,
        )
        assert float(hdr['yllcorner']) == pytest.approx(
            lats1d.min() - cellsize / 2,
        )

    def test_descending_lats_handled(self, tmp_path):
        """North-up source (lats descending along axis 0) must still
        produce north-first data rows and a correct yllcorner.
        """
        lons1d = np.array([0.0, 1.0])
        lats1d = np.array([3.0, 2.0, 1.0])  # descending = north-up
        lon, lat = np.meshgrid(lons1d, lats1d)
        # Row 0 (lat=3, north) is the largest values
        data = np.array([[300.0, 300.0], [200.0, 200.0], [100.0, 100.0]])

        outfile = str(tmp_path / 'descending.txt')
        write_2d_array_to_ascii_grid(data, lon, lat, outfile)

        with open(outfile) as f:
            lines = f.readlines()
        hdr = {ln.split()[0]: ln.split()[1] for ln in lines[:6]}
        # SW cell center = (0.0, 1.0); cellsize = 1.0
        assert float(hdr['xllcorner']) == pytest.approx(-0.5)
        assert float(hdr['yllcorner']) == pytest.approx(0.5)
        assert float(hdr['cellsize']) == pytest.approx(1.0)
        # First written row is northernmost (300s)
        assert [float(v) for v in lines[6].split()] == [300.0, 300.0]
        assert [float(v) for v in lines[8].split()] == [100.0, 100.0]


class TestComputeCurrentMagDir:
    """Tests for _compute_current_mag_dir."""

    def test_eastward_current(self):
        """Pure eastward current: mag=1, dir=90."""
        ssu = np.array([[1.0]])
        ssv = np.array([[0.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert mag[0, 0] == pytest.approx(1.0)
        assert dirn[0, 0] == pytest.approx(90.0)

    def test_northward_current(self):
        """Pure northward current: mag=1, dir=0."""
        ssu = np.array([[0.0]])
        ssv = np.array([[1.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert mag[0, 0] == pytest.approx(1.0)
        assert dirn[0, 0] == pytest.approx(0.0)

    def test_westward_current(self):
        """Pure westward current: mag=1, dir=270."""
        ssu = np.array([[-1.0]])
        ssv = np.array([[0.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert mag[0, 0] == pytest.approx(1.0)
        assert dirn[0, 0] == pytest.approx(270.0)

    def test_southward_current(self):
        """Pure southward current: mag=1, dir=180."""
        ssu = np.array([[0.0]])
        ssv = np.array([[-1.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert mag[0, 0] == pytest.approx(1.0)
        assert dirn[0, 0] == pytest.approx(180.0)

    def test_diagonal_current(self):
        """Northeast current: mag=sqrt(2), dir=45."""
        ssu = np.array([[1.0]])
        ssv = np.array([[1.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert mag[0, 0] == pytest.approx(np.sqrt(2))
        assert dirn[0, 0] == pytest.approx(45.0)

    def test_nan_propagation(self):
        """NaN in either component produces NaN in output."""
        ssu = np.array([[np.nan, 1.0]])
        ssv = np.array([[1.0, np.nan]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        assert np.isnan(mag[0, 0])
        assert np.isnan(mag[0, 1])
        assert np.isnan(dirn[0, 0])
        assert np.isnan(dirn[0, 1])

    def test_2d_array(self):
        """Works correctly on 2D arrays."""
        ssu = np.array([[1.0, 0.0], [0.0, -1.0]])
        ssv = np.array([[0.0, 1.0], [-1.0, 0.0]])
        mag, dirn = _compute_current_mag_dir(ssu, ssv)
        np.testing.assert_allclose(mag, [[1.0, 1.0], [1.0, 1.0]])
        np.testing.assert_allclose(dirn, [[90.0, 0.0], [180.0, 270.0]])


class TestBuildAsciiGridFilename:
    """Tests for _build_ascii_grid_filename."""

    def test_nowcast_filename(self):
        """Nowcast generates n-prefixed filename with step number."""
        result = _build_ascii_grid_filename(
            '/out', 'cbofs',
            datetime(2026, 1, 20, 3, 0, tzinfo=timezone.utc),
            'mag', 'nowcast', 4,
        )
        assert result == os.path.join('/out', 'cbofs_mag_20260120_n004.txt')

    def test_first_step(self):
        """First step generates 001."""
        result = _build_ascii_grid_filename(
            '/out', 'wcofs',
            datetime(2026, 1, 20, 3, 0, tzinfo=timezone.utc),
            'dir', 'nowcast', 1,
        )
        assert result == os.path.join('/out', 'wcofs_dir_20260120_n001.txt')

    def test_forecast_filename(self):
        """Forecast generates f-prefixed filename."""
        result = _build_ascii_grid_filename(
            '/out', 'dbofs',
            datetime(2026, 1, 20, 6, 0, tzinfo=timezone.utc),
            'mag', 'forecast_a', 7,
        )
        assert result == os.path.join('/out', 'dbofs_mag_20260120_f007.txt')

    def test_daily_filename(self):
        """Daily filename uses 'daily' suffix instead of step number."""
        result = _build_ascii_grid_filename(
            '/out', 'cbofs',
            datetime(2026, 1, 20, 0, 0, tzinfo=timezone.utc),
            'mag', 'nowcast', 0,
            is_daily=True,
        )
        assert result == os.path.join('/out', 'cbofs_mag_20260120_daily.txt')

    def test_hindcast_filename(self):
        """Hindcast generates h-prefixed filename."""
        result = _build_ascii_grid_filename(
            '/out', 'loofs2',
            datetime(2026, 1, 20, 2, 0, tzinfo=timezone.utc),
            'dir', 'hindcast', 3,
        )
        assert result == os.path.join('/out', 'loofs2_dir_20260120_h003.txt')
