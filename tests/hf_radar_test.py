"""Tests for bin/obs_retrieval/get_hf_radar.py"""

import importlib.util
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

# ---------------------------------------------------------------------------
# Import the module from bin/ (not on the normal package path)
# ---------------------------------------------------------------------------
_MODULE_PATH = Path(__file__).resolve().parent.parent / 'bin' / 'obs_retrieval' / 'get_hf_radar.py'
_spec = importlib.util.spec_from_file_location('get_hf_radar', _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
hf = importlib.util.module_from_spec(_spec)

# Provide a logger before the module body runs (it expects logger at module scope)
logging.basicConfig(level=logging.DEBUG)
hf.logger = logging.getLogger('test_hf_radar')  # type: ignore[attr-defined]
_spec.loader.exec_module(hf)
# Re-assign after exec since the module sets logger = None at line 74
hf.logger = logging.getLogger('test_hf_radar')  # type: ignore[attr-defined]


def _read_asc_data_rows(filepath):
    """Read an ESRI ASCII Grid file and return (header_dict, data_rows).

    Works whether the header uses 'cellsize' (6 lines) or 'dx'/'dy' (7 lines).
    """
    lines = filepath.read_text().strip().split('\n')
    header = {}
    data_start = 0
    for i, line in enumerate(lines):
        parts = line.strip().split()
        try:
            float(parts[0])
            data_start = i
            break
        except ValueError:
            header[parts[0].lower()] = parts[1]
    data_rows = []
    for line in lines[data_start:]:
        data_rows.append([float(v) for v in line.strip().split()])
    return header, data_rows


# ===========================================================================
# wrap_lon
# ===========================================================================
class TestWrapLon:
    def test_positive_within_range(self):
        assert hf.wrap_lon(90) == 90

    def test_negative_within_range(self):
        assert hf.wrap_lon(-90) == -90

    def test_wrap_360(self):
        assert hf.wrap_lon(360) == pytest.approx(0)

    def test_wrap_270(self):
        assert hf.wrap_lon(270) == pytest.approx(-90)

    def test_wrap_negative_270(self):
        assert hf.wrap_lon(-270) == pytest.approx(90)

    def test_wrap_180(self):
        assert hf.wrap_lon(180) == pytest.approx(-180)

    def test_zero(self):
        assert hf.wrap_lon(0) == 0


# ===========================================================================
# ensure_clockwise
# ===========================================================================
class TestEnsureClockwise:
    def test_ccw_polygon_is_reversed(self):
        ccw = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        result = hf.ensure_clockwise(ccw)
        assert result == ccw[::-1]

    def test_cw_polygon_unchanged(self):
        cw = [(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]
        result = hf.ensure_clockwise(cw)
        assert result == cw


# ===========================================================================
# polygons_intersect_2d
# ===========================================================================
class TestPolygonsIntersect2D:
    def test_overlapping(self):
        p1 = [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]
        p2 = [(1, 1), (3, 1), (3, 3), (1, 3), (1, 1)]
        assert hf.polygons_intersect_2d(p1, p2) is True

    def test_non_overlapping(self):
        p1 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        p2 = [(5, 5), (6, 5), (6, 6), (5, 6), (5, 5)]
        assert hf.polygons_intersect_2d(p1, p2) is False

    def test_contained(self):
        outer = [(-10, -10), (10, -10), (10, 10), (-10, 10), (-10, -10)]
        inner = [(-1, -1), (1, -1), (1, 1), (-1, 1), (-1, -1)]
        assert hf.polygons_intersect_2d(outer, inner) is True

    def test_touching_edge(self):
        p1 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
        p2 = [(1, 0), (2, 0), (2, 1), (1, 1), (1, 0)]
        assert hf.polygons_intersect_2d(p1, p2) is True


# ===========================================================================
# parse_wkt_polygon
# ===========================================================================
class TestParseWktPolygon:
    def test_valid_wkt(self):
        wkt = 'POLYGON ((-77 36, -77 40, -74 40, -74 36, -77 36))'
        result = hf.parse_wkt_polygon(wkt)
        assert len(result) == 5
        assert result[0] == (-77.0, 36.0)
        assert result[-1] == result[0]

    def test_unclosed_polygon_gets_closed(self):
        wkt = 'POLYGON ((-77 36, -77 40, -74 40, -74 36))'
        result = hf.parse_wkt_polygon(wkt)
        assert result[0] == result[-1]

    def test_invalid_wkt_raises(self):
        with pytest.raises(ValueError, match='Invalid WKT'):
            hf.parse_wkt_polygon('NOT A POLYGON')


# ===========================================================================
# parse_utc_timestamp
# ===========================================================================
class TestParseUtcTimestamp:
    def test_valid_timestamp(self):
        result = hf.parse_utc_timestamp('2026031512')
        assert result == datetime(2026, 3, 15, 12)

    def test_invalid_returns_none(self):
        result = hf.parse_utc_timestamp('not-a-date')
        assert result is None

    def test_empty_string_returns_none(self):
        result = hf.parse_utc_timestamp('')
        assert result is None


# ===========================================================================
# get_geospatial_bounds
# ===========================================================================
class TestGetGeospatialBounds:
    def test_with_geospatial_bounds_attr(self):
        ds = xr.Dataset()
        ds.attrs['geospatial_bounds'] = 'POLYGON ((-77 36, -77 40, -74 40, -74 36, -77 36))'
        result = hf.get_geospatial_bounds(ds)
        assert 'POLYGON' in result
        assert '-77' in result

    def test_with_bbox_attr(self):
        ds = xr.Dataset()
        ds.attrs['bbox'] = [-77, 36, -74, 40]
        result = hf.get_geospatial_bounds(ds)
        assert 'POLYGON' in result
        parsed = hf.parse_wkt_polygon(result)
        assert (-77.0, 36.0) in parsed

    def test_with_geospatial_min_max_attrs(self):
        ds = xr.Dataset()
        ds.attrs['geospatial_lat_min'] = 36
        ds.attrs['geospatial_lat_max'] = 40
        ds.attrs['geospatial_lon_min'] = -77
        ds.attrs['geospatial_lon_max'] = -74
        result = hf.get_geospatial_bounds(ds)
        assert 'POLYGON' in result

    def test_with_lat_lon_coords(self):
        ds = xr.Dataset({
            'temp': (['lat', 'lon'], np.zeros((3, 4))),
        }, coords={
            'lat': [36, 38, 40],
            'lon': [-77, -76, -75, -74],
        })
        result = hf.get_geospatial_bounds(ds)
        assert 'POLYGON' in result
        parsed = hf.parse_wkt_polygon(result)
        lons = [p[0] for p in parsed]
        lats = [p[1] for p in parsed]
        assert min(lons) == -77.0
        assert max(lats) == 40.0

    def test_no_bounds_info_returns_none(self):
        ds = xr.Dataset({'temp': (['x', 'y'], np.zeros((2, 2)))})
        result = hf.get_geospatial_bounds(ds)
        assert result is None


# ===========================================================================
# export_ascii
# ===========================================================================
class TestExportAscii:
    def test_creates_asc_file(self, tmp_path):
        lats = np.array([38.0, 37.0, 36.0])
        lons = np.array([-77.0, -76.0, -75.0])
        data = np.array([
            [0.5, 1.0, np.nan],
            [0.3, 0.7, 0.9],
            [np.nan, 0.2, 0.4],
        ])
        da = xr.DataArray(data, coords=[lats, lons], dims=['lat', 'lon'])
        outfile = tmp_path / 'test_mag.asc'

        hf.export_ascii(da, outfile)

        assert outfile.exists()
        assert (tmp_path / 'test_mag.prj').exists()

        content = outfile.read_text()
        assert 'ncols' in content
        assert 'nrows' in content
        assert 'NODATA_value' in content
        assert '-9999' in content

    def test_nodata_written_for_nan(self, tmp_path):
        lats = np.array([38.0, 37.0])
        lons = np.array([-77.0, -76.0])
        data = np.array([[np.nan, 1.0], [2.0, np.nan]])
        da = xr.DataArray(data, coords=[lats, lons], dims=['lat', 'lon'])
        outfile = tmp_path / 'test_nan.asc'

        hf.export_ascii(da, outfile)

        _, data_rows = _read_asc_data_rows(outfile)
        all_vals = [v for row in data_rows for v in row]
        assert -9999.0 in all_vals

    def test_south_up_data_gets_flipped(self, tmp_path):
        # lats ascending = south-up -> should be flipped
        lats = np.array([36.0, 37.0, 38.0])
        lons = np.array([-77.0, -76.0])
        data = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        da = xr.DataArray(data, coords=[lats, lons], dims=['lat', 'lon'])
        outfile = tmp_path / 'test_flip.asc'

        hf.export_ascii(da, outfile)

        _, data_rows = _read_asc_data_rows(outfile)
        # First data row should be the northernmost (lat=38) values: 5, 6
        assert data_rows[0][0] == pytest.approx(5.0, abs=0.1)
        assert data_rows[0][1] == pytest.approx(6.0, abs=0.1)


# ===========================================================================
# clip_by_ofs
# ===========================================================================
class TestClipByOfs:
    @pytest.fixture
    def sample_ds_and_gdf(self):
        """Create a small dataset and a GeoDataFrame covering part of it."""
        lats = np.arange(36.0, 40.0, 0.5)
        lons = np.arange(-78.0, -74.0, 0.5)
        u = np.ones((len(lats), len(lons)))
        ds = xr.Dataset(
            {'u': (['lat', 'lon'], u)},
            coords={'lat': lats, 'lon': lons},
        )
        # GeoDataFrame covering roughly the southern half
        gdf = gpd.GeoDataFrame(
            geometry=[box(-78, 36, -74, 37.5)],
            crs='EPSG:4326',
        )
        return ds, gdf

    def test_mask_shape_matches_grid(self, sample_ds_and_gdf):
        ds, gdf = sample_ds_and_gdf
        mask = hf.clip_by_ofs(ds, gdf)
        assert mask.shape == (len(ds['lat']), len(ds['lon']))

    def test_mask_excludes_points_outside_boundary(self, sample_ds_and_gdf):
        ds, gdf = sample_ds_and_gdf
        mask = hf.clip_by_ofs(ds, gdf)
        # Points north of 37.5 should be masked out
        north_mask = mask.sel(lat=slice(38.0, 39.5))
        assert north_mask.sum().item() == 0

    def test_mask_includes_points_inside_boundary(self, sample_ds_and_gdf):
        ds, gdf = sample_ds_and_gdf
        mask = hf.clip_by_ofs(ds, gdf)
        # Points south of 37.5 should be included
        south_mask = mask.sel(lat=slice(36.0, 37.0))
        assert south_mask.sum().item() > 0


# ===========================================================================
# process_files — daily mode
# ===========================================================================
class TestProcessFilesDaily:
    @pytest.fixture
    def mock_daily_data(self, tmp_path):
        """Create a mock dataset with 25 hours of u/v data."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 37.5, 38.0])
        lons = np.array([-123.0, -122.5, -122.0])

        rng = np.random.default_rng(42)
        u = rng.uniform(-0.5, 0.5, (25, 3, 3))
        v = rng.uniform(-0.5, 0.5, (25, 3, 3))

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], u),
                'v': (['time', 'lat', 'lon'], v),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )

        gdf = gpd.GeoDataFrame(
            geometry=[box(-123.5, 36.5, -121.5, 38.5)],
            crs='EPSG:4326',
        )
        return ds, gdf, tmp_path

    def test_daily_creates_output_files(self, mock_daily_data):
        ds, gdf, out_dir = mock_daily_data
        date_obj = datetime(2026, 3, 15)
        matching = {'usegc': ds}

        hf.process_files(matching, date_obj, out_dir, gdf, 'sfbofs', 'daily')

        # Daily .asc files (existing format)
        assert (out_dir / 'sfbofs_hfradar_mag_20260315.asc').exists()
        assert (out_dir / 'sfbofs_hfradar_dir_20260315.asc').exists()
        assert (out_dir / 'sfbofs_hfradar_mag_20260315.prj').exists()
        assert (out_dir / 'sfbofs_hfradar_dir_20260315.prj').exists()

        # Daily JSON + txt files (new formats)
        obs_2d = out_dir / '2d'
        assert (obs_2d / 'sfbofs_20260315_ssu_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_20260315_ssv_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_mag_20260315_hfradar.txt').exists()
        assert (obs_2d / 'sfbofs_dir_20260315_hfradar.txt').exists()

        # Hourly .asc files should also be created (both modes always produced)
        # 25 hours span 00:00-24:00; the 25th (00:00 next day) has date 20260316
        hourly_asc = list(out_dir.glob('sfbofs_hfradar_mag_*_*.asc'))
        assert len(hourly_asc) == 25

        # Hourly JSON files should also be created
        hourly_json = list(obs_2d.glob('sfbofs_*_*_ssu_hfradar.json'))
        assert len(hourly_json) == 25

    def test_daily_min_count_threshold(self, tmp_path):
        """Cells with < 13 valid observations should be NaN in output."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        u = np.full((25, 2, 2), 0.5)
        v = np.full((25, 2, 2), 0.5)
        # Make cell (0,0) have only 5 valid obs out of 25
        u[5:, 0, 0] = np.nan
        v[5:, 0, 0] = np.nan

        ds = xr.Dataset(
            {'u': (['time', 'lat', 'lon'], u), 'v': (['time', 'lat', 'lon'], v)},
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')
        matching = {'usegc': ds}

        hf.process_files(matching, datetime(2026, 3, 15), tmp_path, gdf, 'test', 'daily')

        mag_file = tmp_path / 'test_hfradar_mag_20260315.asc'
        assert mag_file.exists()
        _, data_rows = _read_asc_data_rows(mag_file)
        # First data row (northernmost lat=38) — cell (1,0) and (1,1) should
        # have valid data since all 25 observations are present
        assert data_rows[0][0] != -9999  # lat=38, lon=-123 — 25 valid obs
        assert data_rows[0][1] != -9999  # lat=38, lon=-122 — 25 valid obs
        # Second data row (lat=37) — cell (0,0) has only 5 valid obs → -9999
        assert data_rows[1][0] == -9999  # lat=37, lon=-123 — only 5 valid obs


# ===========================================================================
# process_files — hourly mode
# ===========================================================================
class TestProcessFilesHourly:
    def test_hourly_creates_per_hour_files(self, tmp_path):
        times = pd.date_range('2026-03-15', periods=3, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((3, 2, 2))),
                'v': (['time', 'lat', 'lon'], np.ones((3, 2, 2))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')
        matching = {'uswc': ds}

        start = datetime(2026, 3, 15, 0)
        end = datetime(2026, 3, 15, 2)
        hf.process_files(matching, datetime(2026, 3, 15), tmp_path, gdf,
                         'sfbofs', 'hourly', start, end)

        # Hourly .asc files
        assert (tmp_path / 'sfbofs_hfradar_mag_20260315_0000.asc').exists()
        assert (tmp_path / 'sfbofs_hfradar_mag_20260315_0100.asc').exists()
        assert (tmp_path / 'sfbofs_hfradar_mag_20260315_0200.asc').exists()

        # Hourly JSON + txt files
        obs_2d = tmp_path / '2d'
        assert (obs_2d / 'sfbofs_20260315_0000_ssu_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_20260315_0100_ssu_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_20260315_0200_ssu_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_mag_20260315_0000_hfradar.txt').exists()

        # Daily averaged files should also be created
        assert (tmp_path / 'sfbofs_hfradar_mag_20260315.asc').exists()
        assert (obs_2d / 'sfbofs_20260315_ssu_hfradar.json').exists()
        assert (obs_2d / 'sfbofs_mag_20260315_hfradar.txt').exists()

    def test_none_start_end_uses_default_window(self, tmp_path):
        """None start/end should use default 24h window, not crash."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((25, 2, 2))),
                'v': (['time', 'lat', 'lon'], np.ones((25, 2, 2))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')
        matching = {'uswc': ds}

        # None start/end falls back to default 24h window
        hf.process_files(matching, datetime(2026, 3, 15), tmp_path, gdf,
                         'sfbofs', 'hourly', None, None)

        # Daily averaged outputs should be created using the default window
        assert (tmp_path / 'sfbofs_hfradar_mag_20260315.asc').exists()


# ===========================================================================
# process_files — empty / no-data scenarios
# ===========================================================================
class TestProcessFilesEdgeCases:
    def test_empty_matching_files(self, tmp_path):
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')
        # Should complete without error
        hf.process_files({}, datetime(2026, 3, 15), tmp_path, gdf, 'test', 'daily')
        assert list(tmp_path.glob('*.asc')) == []

    def test_all_nan_data_skipped(self, tmp_path):
        """Dataset with all-NaN u/v should be skipped (isfinite check)."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.full((25, 2, 2), np.nan)),
                'v': (['time', 'lat', 'lon'], np.full((25, 2, 2), np.nan)),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'usegc': ds}, datetime(2026, 3, 15), tmp_path, gdf, 'test', 'daily')
        assert list(tmp_path.glob('*.asc')) == []


# ===========================================================================
# check_for_overlap — mocked THREDDS
# ===========================================================================
class TestCheckForOverlap:
    def test_finds_overlapping_source(self, tmp_path):
        """check_for_overlap should find sources whose bounds overlap the OFS."""
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])
        times = pd.date_range('2026-03-15', periods=25, freq='h')

        mock_ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((25, 2, 2)) * 0.5),
                'v': (['time', 'lat', 'lon'], np.ones((25, 2, 2)) * 0.3),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        mock_ds.attrs['geospatial_bounds'] = (
            'POLYGON ((-130 30, -130 45, -115 45, -115 30, -130 30))'
        )

        gdf = gpd.GeoDataFrame(
            geometry=[box(-123.5, 36.5, -121.5, 38.5)],
            crs='EPSG:4326',
        )

        with patch('xarray.open_dataset', return_value=mock_ds):
            hf.check_for_overlap(
                datetime(2026, 3, 15), tmp_path, gdf, 'sfbofs', 'daily'
            )

        assert (tmp_path / 'sfbofs_hfradar_mag_20260315.asc').exists()
        assert (tmp_path / 'sfbofs_hfradar_dir_20260315.asc').exists()

    def test_no_overlap_produces_no_files(self, tmp_path):
        """Sources that don't overlap the OFS boundary should be skipped."""
        lats = np.array([60.0, 61.0])
        lons = np.array([170.0, 171.0])

        mock_ds = xr.Dataset(
            coords={'lat': lats, 'lon': lons},
        )
        mock_ds.attrs['geospatial_bounds'] = (
            'POLYGON ((169 59, 169 62, 172 62, 172 59, 169 59))'
        )

        # OFS is on US East Coast — no overlap with Alaska-area source
        gdf = gpd.GeoDataFrame(
            geometry=[box(-77, 36, -74, 40)],
            crs='EPSG:4326',
        )

        with patch('xarray.open_dataset', return_value=mock_ds):
            hf.check_for_overlap(
                datetime(2026, 3, 15), tmp_path, gdf, 'cbofs', 'daily'
            )

        assert list(tmp_path.glob('*.asc')) == []

    def test_thredds_failure_continues(self, tmp_path):
        """If THREDDS is unreachable, the script should continue (not crash)."""
        gdf = gpd.GeoDataFrame(
            geometry=[box(-123.5, 36.5, -121.5, 38.5)],
            crs='EPSG:4326',
        )

        with patch('xarray.open_dataset', side_effect=OSError('Connection refused')):
            hf.check_for_overlap(
                datetime(2026, 3, 15), tmp_path, gdf, 'sfbofs', 'daily'
            )

        assert list(tmp_path.glob('*.asc')) == []


# ===========================================================================
# Direction convention
# ===========================================================================
class TestDirectionConvention:
    def test_east_current_gives_90_degrees(self, tmp_path):
        """Purely eastward flow (u>0, v=0) should give direction = 90°."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((25, 2, 2))),
                'v': (['time', 'lat', 'lon'], np.zeros((25, 2, 2))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'src': ds}, datetime(2026, 3, 15), tmp_path, gdf, 'test', 'daily')

        _, data_rows = _read_asc_data_rows(tmp_path / 'test_hfradar_dir_20260315.asc')
        data_vals = [v for row in data_rows for v in row if v != -9999]
        assert len(data_vals) > 0
        for val in data_vals:
            assert val == pytest.approx(90.0, abs=0.1)

    def test_north_current_gives_0_degrees(self, tmp_path):
        """Purely northward flow (u=0, v>0) should give direction = 0°."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.zeros((25, 2, 2))),
                'v': (['time', 'lat', 'lon'], np.ones((25, 2, 2))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'src': ds}, datetime(2026, 3, 15), tmp_path, gdf, 'test', 'daily')

        _, data_rows = _read_asc_data_rows(tmp_path / 'test_hfradar_dir_20260315.asc')
        data_vals = [v for row in data_rows for v in row if v != -9999]
        assert len(data_vals) > 0
        for val in data_vals:
            assert val == pytest.approx(0.0, abs=0.1)


# ===========================================================================
# CLI argument validation (via subprocess)
# ===========================================================================
class TestCLIValidation:
    _script = str(_MODULE_PATH)

    def _run(self, args):
        return subprocess.run(
            [sys.executable, self._script] + args,
            capture_output=True, text=True, timeout=10,
        )

    def test_help_flag(self):
        result = self._run(['--help'])
        assert result.returncode == 0
        assert '-d' in result.stdout
        assert '-C' in result.stdout

    def test_missing_required_args(self):
        result = self._run([])
        assert result.returncode == 2
        assert 'required' in result.stderr

    def test_hourly_without_start_end(self):
        result = self._run([
            '-d', '20260315', '-C', '/tmp/test', '-o', 'sfbofs', '-m', 'hourly',
        ])
        assert result.returncode == 2
        assert 'start' in result.stderr.lower() or 'end' in result.stderr.lower()

    def test_neither_bounds_nor_ofs(self):
        result = self._run(['-d', '20260315', '-C', '/tmp/test'])
        assert result.returncode == 2
        assert 'bounds' in result.stderr.lower() or 'ofs' in result.stderr.lower()


# ===========================================================================
# _build_hfradar_output_paths
# ===========================================================================
class TestBuildHfradarOutputPaths:
    def test_daily_paths(self, tmp_path):
        paths = hf._build_hfradar_output_paths(tmp_path, 'cbofs', '20260315')
        assert paths['ssu_json'] == tmp_path / '2d' / 'cbofs_20260315_ssu_hfradar.json'
        assert paths['ssv_json'] == tmp_path / '2d' / 'cbofs_20260315_ssv_hfradar.json'
        assert paths['mag_txt'] == tmp_path / '2d' / 'cbofs_mag_20260315_hfradar.txt'
        assert paths['dir_txt'] == tmp_path / '2d' / 'cbofs_dir_20260315_hfradar.txt'
        assert (tmp_path / '2d').is_dir()

    def test_hourly_paths(self, tmp_path):
        paths = hf._build_hfradar_output_paths(
            tmp_path, 'sfbofs', '20260315', '20260315_0300',
        )
        assert paths['ssu_json'] == tmp_path / '2d' / 'sfbofs_20260315_0300_ssu_hfradar.json'
        assert paths['mag_txt'] == tmp_path / '2d' / 'sfbofs_mag_20260315_0300_hfradar.txt'


# ===========================================================================
# JSON output structure
# ===========================================================================
class TestJsonStructure:
    def test_json_has_required_keys(self, tmp_path):
        """JSON files should have lats, lons, sst, val_min, val_max keys."""

        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 37.5, 38.0])
        lons = np.array([-123.0, -122.5, -122.0])

        rng = np.random.default_rng(42)
        u = rng.uniform(-0.5, 0.5, (25, 3, 3))
        v = rng.uniform(-0.5, 0.5, (25, 3, 3))

        ds = xr.Dataset(
            {'u': (['time', 'lat', 'lon'], u), 'v': (['time', 'lat', 'lon'], v)},
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'usegc': ds}, datetime(2026, 3, 15), tmp_path, gdf,
                         'test', 'daily')

        json_file = tmp_path / '2d' / 'test_20260315_ssu_hfradar.json'
        assert json_file.exists()

        with open(json_file) as f:
            data = json.load(f)

        assert 'lats' in data
        assert 'lons' in data
        assert 'sst' in data
        assert 'val_min' in data
        assert 'val_max' in data
        assert isinstance(data['lats'], list)
        assert isinstance(data['lats'][0], list)  # 2D array

    def test_json_val_min_max_reasonable(self, tmp_path):
        """val_min/val_max should reflect actual data range."""

        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 37.5, 38.0])
        lons = np.array([-123.0, -122.5, -122.0])

        # Constant u=0.3, v=0.0 so SSU values should be ~0.3
        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.full((25, 3, 3), 0.3)),
                'v': (['time', 'lat', 'lon'], np.zeros((25, 3, 3))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'usegc': ds}, datetime(2026, 3, 15), tmp_path, gdf,
                         'test', 'daily')

        json_file = tmp_path / '2d' / 'test_20260315_ssu_hfradar.json'
        with open(json_file) as f:
            data = json.load(f)

        # Perimeter is set to null by write_2d_arrays_to_json, but interior
        # values should be ~0.3
        assert data['val_min'] is not None
        assert data['val_max'] is not None
        assert data['val_min'] == pytest.approx(0.3, abs=0.01)


# ===========================================================================
# ASCII grid txt output structure
# ===========================================================================
class TestTxtStructure:
    def test_txt_has_valid_header(self, tmp_path):
        """ASCII grid txt files should have 6-line ESRI header."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 37.5, 38.0])
        lons = np.array([-123.0, -122.5, -122.0])

        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((25, 3, 3))),
                'v': (['time', 'lat', 'lon'], np.ones((25, 3, 3))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'usegc': ds}, datetime(2026, 3, 15), tmp_path, gdf,
                         'test', 'daily')

        txt_file = tmp_path / '2d' / 'test_mag_20260315_hfradar.txt'
        assert txt_file.exists()

        header, data_rows = _read_asc_data_rows(txt_file)
        assert 'ncols' in header
        assert 'nrows' in header
        assert 'xllcorner' in header
        assert 'yllcorner' in header
        assert 'cellsize' in header
        assert 'nodata_value' in header
        assert int(header['ncols']) == 3
        assert int(header['nrows']) == 3
        assert len(data_rows) == 3

    def test_txt_direction_matches_asc(self, tmp_path):
        """Direction values in txt should match the asc for same data."""
        times = pd.date_range('2026-03-15', periods=25, freq='h')
        lats = np.array([37.0, 38.0])
        lons = np.array([-123.0, -122.0])

        # Purely eastward flow → direction = 90°
        ds = xr.Dataset(
            {
                'u': (['time', 'lat', 'lon'], np.ones((25, 2, 2))),
                'v': (['time', 'lat', 'lon'], np.zeros((25, 2, 2))),
            },
            coords={'time': times, 'lat': lats, 'lon': lons},
        )
        gdf = gpd.GeoDataFrame(geometry=[box(-124, 36, -121, 39)], crs='EPSG:4326')

        hf.process_files({'usegc': ds}, datetime(2026, 3, 15), tmp_path, gdf,
                         'test', 'daily')

        _, txt_rows = _read_asc_data_rows(tmp_path / '2d' / 'test_dir_20260315_hfradar.txt')
        _, asc_rows = _read_asc_data_rows(tmp_path / 'test_hfradar_dir_20260315.asc')

        txt_vals = [v for row in txt_rows for v in row if v != -9999]
        asc_vals = [v for row in asc_rows for v in row if v != -9999]

        assert len(txt_vals) > 0
        assert len(asc_vals) > 0
        for tv in txt_vals:
            assert tv == pytest.approx(90.0, abs=0.5)
        for av in asc_vals:
            assert av == pytest.approx(90.0, abs=0.5)
