"""Tests for the below-model-bottom ADCP bin pruning (issue #200).

The CO-OPS metadata API occasionally reports ADCP bin depths deeper
than the model water column at the matched node (e.g. NECOFS cb0201:
model depth 10.48 m, bins reported down to 13.41 m). The vertical
nearest-layer search clamps every such bin to the bottom model layer,
which used to emit several model-ctl lines identical except for the
virtual-bin ID — the same model series compared against several obs
bins, duplicating ``mod_water_depth`` rows in the skill CSVs.

Covers the ``_drop_bins_below_model_bottom`` policy directly (keep at
most one bottom-layer comparison per station/node, ledger accounting,
non-virtual IDs untouched, unknown bathymetry no-op) plus an
end-to-end FVCOM-stations run through ``write_ofs_ctlfile`` asserting
the written ctl no longer carries duplicated bottom rows. Also pins
the ``_station_metadata`` obs-depth read to coord token [3] — the old
``[-2]`` read landed on ``height_from_bottom`` for 7-token CO-OPS
currents rows, reporting the same ``obs_water_depth`` for every bin of
a station.
"""

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.station_ledger import StationLedger
from ofs_skill.model_processing.write_ofs_ctlfile import (
    _drop_bins_below_model_bottom,
    write_ofs_ctlfile,
)
from ofs_skill.skill_assessment.get_skill import _station_metadata
from ofs_skill.utils.file_headers import OBS_CTL_HEADER


@pytest.fixture()
def logger():
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger('bins_below_model_bottom_test')


def _make_extract(rows):
    """Build a station_ctl_file_extract-shaped (info, coords) pair.

    ``rows`` is a list of (station_id, coord_tokens) tuples.
    """
    info = [[sid, f'{sid}_cu_x_CO-OPS', f'Station {sid}'] for sid, _ in rows]
    coords = [list(tokens) for _, tokens in rows]
    return [info, coords]


def _cu_coords(depth, hfb=0.0, mounting='up'):
    """Build a 7-token CO-OPS currents coord line (hfb = height_from_bottom)."""
    return ['41.000', '-71.000', '0.0', f'{depth:.2f}', '0.0',
            f'{hfb:.2f}', mounting]


def _run_drop(rows, nodes, layers, depths, h, logger, ledger=None,
              model_source='fvcom'):
    extract = _make_extract(rows)
    station_id = [sid for sid, _ in rows]
    prop = SimpleNamespace(model_source=model_source,
                           station_ledger=ledger)
    model = {'h': np.asarray(h, dtype=float)}
    return _drop_bins_below_model_bottom(
        prop, extract, station_id, list(nodes), list(layers),
        list(depths), model, logger)


def test_drops_all_when_bottom_layer_already_covered(logger):
    """In-column bin already at the bottom layer -> every too-deep bin drops."""
    # Node 0 water depth 10.48 m; layer 0 is the bottom layer.
    rows = [
        ('cb0201_b07', _cu_coords(9.0)),    # in-column, argmin -> bottom
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(12.64)),
        ('cb0201_b10', _cu_coords(13.41)),
    ]
    drop = _run_drop(rows, nodes=[0, 0, 0, 0], layers=[0, 0, 0, 0],
                     depths=[10.48, 10.48, 10.48, 10.48], h=[10.48, 50.0],
                     logger=logger)
    assert drop == {1, 2, 3}


def test_keeps_shallowest_when_bottom_layer_uncovered(logger):
    """No in-column bin at the bottom layer -> shallowest too-deep bin kept."""
    rows = [
        ('cb0201_b02', _cu_coords(2.0)),    # surface layer
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(12.64)),
        ('cb0201_b10', _cu_coords(13.41)),
    ]
    drop = _run_drop(rows, nodes=[0, 0, 0, 0], layers=[2, 0, 0, 0],
                     depths=[0.0, 10.48, 10.48, 10.48], h=[10.48, 50.0],
                     logger=logger)
    # b08 (shallowest too-deep) survives as the bottom-layer comparison.
    assert drop == {2, 3}


def test_single_near_bottom_bin_is_kept(logger):
    """A lone bin slightly past the model bottom stays (cb1301-style)."""
    rows = [('cb1301_b10', _cu_coords(10.52, hfb=0.18, mounting='side'))]
    drop = _run_drop(rows, nodes=[0], layers=[0], depths=[10.48],
                     h=[10.48, 50.0], logger=logger)
    assert drop == set()


def test_in_column_bins_never_drop(logger):
    """Bins within the model water column are always kept."""
    rows = [
        ('cb0201_b01', _cu_coords(1.5)),
        ('cb0201_b05', _cu_coords(6.0)),
        ('cb0201_b07', _cu_coords(9.0)),
    ]
    drop = _run_drop(rows, nodes=[0, 0, 0], layers=[2, 1, 0],
                     depths=[0.0, 5.24, 10.48], h=[10.48, 50.0], logger=logger)
    assert drop == set()


def test_non_virtual_ids_ignored(logger):
    """NDBC/USGS-style IDs (no _bNN suffix) are never pruned."""
    rows = [
        ('46237', ['41.0', '-71.0', '0.0', '15.0', '0.0', '0.0']),
        ('0158964', ['41.0', '-71.0', '0.0', '20.0', '0.0', '0.0']),
    ]
    drop = _run_drop(rows, nodes=[0, 0], layers=[0, 0],
                     depths=[10.48, 10.48], h=[10.48, 50.0], logger=logger)
    assert drop == set()


def test_unknown_bathymetry_is_a_noop(logger):
    """Bathymetry lookup failure (no h/z) -> keep everything."""
    rows = [
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(12.64)),
    ]
    extract = _make_extract(rows)
    prop = SimpleNamespace(model_source='fvcom', station_ledger=None)
    drop = _drop_bins_below_model_bottom(
        prop, extract, [sid for sid, _ in rows], [0, 0], [0, 0],
        [10.48, 10.48], {}, logger)
    assert drop == set()


def test_nan_nodes_are_skipped(logger):
    """Unmatched (NaN-node) stations are ignored by the pruner."""
    rows = [
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(12.64)),
    ]
    drop = _run_drop(rows, nodes=[np.nan, np.nan], layers=[0, 0],
                     depths=[np.nan, np.nan], h=[10.48, 50.0], logger=logger)
    assert drop == set()


def test_groups_are_per_parent_station(logger):
    """Each parent keeps its own bottom-layer comparison."""
    rows = [
        ('aa0001_b05', _cu_coords(11.0)),
        ('aa0001_b06', _cu_coords(12.0)),
        ('bb0002_b03', _cu_coords(11.5)),
        ('bb0002_b04', _cu_coords(12.5)),
    ]
    drop = _run_drop(rows, nodes=[0, 0, 1, 1], layers=[0, 0, 0, 0],
                     depths=[10.48, 10.48, 9.9, 9.9], h=[10.48, 9.9],
                     logger=logger)
    # Shallowest too-deep bin of each parent survives.
    assert drop == {1, 3}


def test_mismatched_list_lengths_bail_out(logger):
    """Misaligned parallel lists disable pruning instead of mis-indexing."""
    rows = [('cb0201_b08', _cu_coords(11.10))]
    extract = _make_extract(rows)
    prop = SimpleNamespace(model_source='fvcom', station_ledger=None)
    drop = _drop_bins_below_model_bottom(
        prop, extract, ['cb0201_b08'], [0, 0], [0], [10.48],
        {'h': np.asarray([10.48])}, logger)
    assert drop == set()


def test_ledger_declares_the_stage_even_with_nothing_to_prune(logger):
    """A clean prune pass must still declare that depth_match ran.

    depth_match only writes rows when a bin is removed, so without the
    declaration the combined ledger would keep an earlier run's prunings
    alive forever.
    """
    ledger = StationLedger(ofs='cbofs', variable='water_level',
                           whichcast='nowcast', filetype='stations')
    rows = [('cb0201_b01', _cu_coords(1.5)), ('cb0201_b05', _cu_coords(6.0))]
    drop = _run_drop(rows, nodes=[0, 0], layers=[2, 0],
                     depths=[0.0, 10.48], h=[10.48, 50.0],
                     logger=logger, ledger=ledger)

    assert drop == set()
    assert ledger.drops == []
    # Stamped under currents (bin pruning is currents-only) and under the
    # cast-independent 'all' whichcast, matching the rows it supersedes.
    assert ('currents', 'all', 'stations', 'depth_match') in ledger.stages_run


def test_ledger_records_each_drop(logger):
    """Every pruned bin lands on the station ledger with stage depth_match."""
    ledger = StationLedger(ofs='necofs', variable='currents',
                           whichcast='hindcast', filetype='stations')
    rows = [
        ('cb0201_b07', _cu_coords(9.0)),
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(12.64)),
    ]
    drop = _run_drop(rows, nodes=[0, 0, 0], layers=[0, 0, 0],
                     depths=[10.48, 10.48, 10.48], h=[10.48, 50.0],
                     logger=logger, ledger=ledger)
    assert drop == {1, 2}
    dropped_ids = {d.station_id for d in ledger.drops}
    assert dropped_ids == {'cb0201_b08', 'cb0201_b09'}
    assert all(d.stage == 'depth_match' for d in ledger.drops)
    assert all('exceeds model water depth' in d.reason
               for d in ledger.drops)


def test_warning_names_station_and_depths(logger, caplog):
    """The drop warning names the parent, dropped bins, and kept bin."""
    rows = [
        ('cb0201_b02', _cu_coords(2.0)),
        ('cb0201_b08', _cu_coords(11.10)),
        ('cb0201_b09', _cu_coords(13.41)),
    ]
    with caplog.at_level(logging.WARNING):
        drop = _run_drop(rows, nodes=[0, 0, 0], layers=[2, 0, 0],
                         depths=[0.0, 10.48, 10.48], h=[10.48, 50.0],
                         logger=logger)
    assert drop == {2}
    text = ' '.join(rec.getMessage() for rec in caplog.records)
    assert 'cb0201' in text
    assert 'cb0201_b09 (13.41 m)' in text
    assert 'cb0201_b08' in text  # named as the kept bottom-layer bin


# ---------------------------------------------------------------------------
# End-to-end: FVCOM stations currents run through write_ofs_ctlfile
# ---------------------------------------------------------------------------


@pytest.fixture()
def fvcom_stations_dataset(tmp_path):
    """Small FVCOM stations dataset with one shallow node (10.48 m)."""
    n_station = 4
    n_siglay = 3

    lon_1d = np.linspace(-71.0, -68.0, n_station, dtype=np.float64)
    lat_1d = np.linspace(41.0, 44.0, n_station, dtype=np.float64)
    h_1d = np.asarray([10.48, 20.0, 25.0, 30.0], dtype=np.float64)
    # Realistic FVCOM sigma-layer CENTERS (not levels): the deepest
    # layer midpoint sits h/(2N) above the seabed, as in production
    # grids, so the fixture exercises the same clamping geometry.
    centers = -(2.0 * np.arange(n_siglay, dtype=np.float64) + 1.0) \
        / (2.0 * n_siglay)
    siglay = np.tile(centers[::-1][:, None], (1, n_station))
    ds = xr.Dataset(
        data_vars={
            'lon': (('station',), lon_1d),
            'lat': (('station',), lat_1d),
            'h': (('station',), h_1d),
            'siglay': (('siglay', 'station'), siglay),
            'u': (
                ('time', 'siglay', 'station'),
                np.zeros((2, n_siglay, n_station), dtype=np.float64),
            ),
        },
        coords={
            'time': (
                np.datetime64('2026-02-16T00')
                + np.arange(2) * np.timedelta64(1, 'h')
            ),
        },
    )
    path = tmp_path / 'fvcom_stations.nc'
    ds.to_netcdf(path)
    ds.close()
    return xr.open_dataset(path), lat_1d, lon_1d


def _write_minimal_config(tmp_path):
    cfg_path = tmp_path / 'ofs_dps.conf'
    cfg_path.write_text(
        '[directories]\n'
        f'home={tmp_path.as_posix()}\n'
        'model_historical_dir=%(home)s/example_data\n'
        'netcdf_dir=netcdf\n'
    )
    return cfg_path


def test_write_ofs_ctlfile_prunes_below_bottom_bins(
    tmp_path, fvcom_stations_dataset, logger,
):
    """Bins deeper than the model water column collapse to one ctl row."""
    model, lat_1d, lon_1d = fvcom_stations_dataset
    cfg_path = _write_minimal_config(tmp_path)
    control_dir = tmp_path / 'control_files'
    control_dir.mkdir()

    # Six virtual bins at the shallow node (h=10.48): three in-column,
    # three reported below the model seabed. One NDBC-style station at
    # a deep node as a control.
    lat0, lon0 = float(lat_1d[0]), float(lon_1d[0])
    lat1, lon1 = float(lat_1d[1]), float(lon_1d[1])
    lines = []
    bins = [(1, 2.0), (4, 6.0), (7, 9.0),
            (8, 11.10), (9, 12.64), (10, 13.41)]
    for num, depth in bins:
        sid = f'cb0201_b{num:02d}'
        lines.append(f'{sid} {sid}_cu_tbofs_CO-OPS "Test ADCP (bin {num})"')
        lines.append(f'  {lat0:.3f} {lon0:.3f} 0.0 {depth:.2f} 0.0 0.00 up')
    lines.append('46237 46237_cu_tbofs_NDBC "NDBC control"')
    lines.append(f'  {lat1:.3f} {lon1:.3f} 0.0 15.00 0.0 0.0')
    obs_ctl = control_dir / 'tbofs_cu_station.ctl'
    obs_ctl.write_text(OBS_CTL_HEADER + '\n'.join(lines) + '\n')

    prop = SimpleNamespace(
        config_file=str(cfg_path),
        ofs='tbofs',
        var_list=['currents'],
        ofsfiletype='stations',
        user_input_location=False,
        model_source='fvcom',
        control_files_path=str(control_dir),
        datum='MLLW',
        station_ledger=None,
    )

    write_ofs_ctlfile(prop, model, logger)

    out_path = control_dir / 'tbofs_cu_model_station.ctl'
    assert out_path.exists()
    assert out_path.stat().st_size > 0

    rows = [ln.split() for ln in out_path.read_text().splitlines()[1:]
            if ln.strip()]
    ids = [r[4] for r in rows]

    # The three below-bottom duplicates are gone; everything else stays.
    assert 'cb0201_b09' not in ids
    assert 'cb0201_b10' not in ids
    # b07 (9 m) argmins to the bottom layer of the 3-layer column
    # (0 / -5.24 / -10.48), so the bottom is already covered in-column
    # and b08 drops too.
    assert 'cb0201_b08' not in ids
    assert ids == ['cb0201_b01', 'cb0201_b04', 'cb0201_b07', '46237']

    # No two rows for the same node may share a layer + depth (the
    # issue #200 duplication signature).
    cb_rows = [r for r in rows if r[4].startswith('cb0201')]
    node_layer = [(r[0], r[1]) for r in cb_rows]
    assert len(node_layer) == len(set(node_layer))


def test_write_ofs_ctlfile_keeps_lone_below_bottom_bin(
    tmp_path, fvcom_stations_dataset, logger,
):
    """A station whose only bins are below-bottom keeps exactly one row."""
    model, lat_1d, lon_1d = fvcom_stations_dataset
    cfg_path = _write_minimal_config(tmp_path)
    control_dir = tmp_path / 'control_files'
    control_dir.mkdir()

    lat0, lon0 = float(lat_1d[0]), float(lon_1d[0])
    lines = []
    for num, depth in [(8, 11.10), (9, 12.64)]:
        sid = f'cb0201_b{num:02d}'
        lines.append(f'{sid} {sid}_cu_tbofs_CO-OPS "Test ADCP (bin {num})"')
        lines.append(f'  {lat0:.3f} {lon0:.3f} 0.0 {depth:.2f} 0.0 0.00 up')
    (control_dir / 'tbofs_cu_station.ctl').write_text(
        OBS_CTL_HEADER + '\n'.join(lines) + '\n')

    prop = SimpleNamespace(
        config_file=str(cfg_path),
        ofs='tbofs',
        var_list=['currents'],
        ofsfiletype='stations',
        user_input_location=False,
        model_source='fvcom',
        control_files_path=str(control_dir),
        datum='MLLW',
        station_ledger=None,
    )

    write_ofs_ctlfile(prop, model, logger)

    rows = [ln.split() for ln in
            (control_dir / 'tbofs_cu_model_station.ctl')
            .read_text().splitlines()[1:] if ln.strip()]
    ids = [r[4] for r in rows]
    # Shallowest below-bottom bin survives as the bottom-layer pairing.
    assert ids == ['cb0201_b08']


# ---------------------------------------------------------------------------
# obs_water_depth metadata read (coord token [3], not [-2])
# ---------------------------------------------------------------------------


def test_station_metadata_obs_depth_currents_seven_tokens():
    """7-token CO-OPS currents row: obs_depth must be the bin depth."""
    station_ctl = [
        [['cb0201_b08', 'cb0201_b08_cu_x_CO-OPS', 'Test (bin 8)']],
        [['41.000', '-71.000', '0.0', '11.10', '0.0', '3.50', 'up']],
    ]
    ofs_ctl = [None, ['0'], None, None, ['cb0201_b08'], [10.5]]
    meta = _station_metadata(station_ctl, ofs_ctl, 0, 0)
    # Pre-fix this returned '3.50' (height_from_bottom, token [-2]).
    assert meta['obs_depth'] == '11.10'


def test_station_metadata_obs_depth_scalar_five_tokens():
    """5-token scalar row: token [3] and legacy [-2] coincide."""
    station_ctl = [
        [['8638610', '8638610_wl_x_CO-OPS', 'Test WL']],
        [['36.850', '-76.012', '0.12', '5.0', 'MLLW']],
    ]
    ofs_ctl = [None, ['0'], None, None, ['8638610'], [0.0]]
    meta = _station_metadata(station_ctl, ofs_ctl, 0, 0)
    assert meta['obs_depth'] == '5.0'
