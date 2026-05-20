"""Regression test for ``write_ofs_ctlfile`` under ``data_vars='minimal'``.

PR #163 commit ``e26291f`` flipped ``intake_scisa`` to combine multi-file
station datasets with ``data_vars='minimal'``, which keeps static mesh
vars (``lon``, ``lat``, ``h``, ``zcoords``) at their native 1-D shape
instead of replicating them along the concat time dim. ``indexing.py``
was migrated to handle both shapes via ``_static_coord_1d``, but
``write_ofs_ctlfile.py`` was missed in that pass and continued to use
``[0, node_idx]`` time-leading accesses in several branches. On a fresh
run (no ``.ctl`` files cached) those raise ``IndexError`` because the
coords are now 1-D.

This test pins the fix by:

1. Building a synthetic two-file FVCOM stations dataset and combining
   them with ``data_vars='minimal'``/``concat_dim='time'`` — exactly
   what ``intake_scisa`` does post-PR.
2. Running the stations-mode FVCOM branch of ``write_ofs_ctlfile``
   through to a written ``.ctl`` file.
3. Parsing the file back and asserting the lat/lon columns match the
   source 1-D arrays at the requested node indices.

We deliberately exercise only one branch (FVCOM stations / scalar) —
the other branches (FVCOM fields, SCHISM stations, STOFS-3D x/y,
ADCIRC) follow the same ``_static_coord_1d`` + ``[node_idx]`` pattern
and would all fail under the same condition pre-fix, so one branch is
sufficient to lock in the migration.
"""

import logging
import os
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile

# ---------------------------------------------------------------------------
# Fixture: synthetic multi-file FVCOM stations dataset
# ---------------------------------------------------------------------------


@pytest.fixture()
def fvcom_minimal_dataset(tmp_path):
    """Build two small FVCOM stations files + combine via xr.open_mfdataset.

    With ``data_vars='minimal'`` and a single time-concat dim, static
    vars (``lon``, ``lat``, ``h``, ``siglay``) come back at their native
    shape (no time replication). ``zeta`` is the only time-varying var
    here.

    Returns
    -------
    (xr.Dataset, np.ndarray, np.ndarray)
        The combined dataset and the original 1-D lon / lat arrays the
        caller can use to verify the written ctl file.
    """
    n_station = 6
    n_siglay = 3
    n_time = 4

    lon_1d = np.linspace(-71.0, -68.0, n_station, dtype=np.float64)
    lat_1d = np.linspace(41.0, 44.0, n_station, dtype=np.float64)
    h_1d = np.linspace(5.0, 30.0, n_station, dtype=np.float64)
    siglay = np.tile(
        np.linspace(-1.0, 0.0, n_siglay, dtype=np.float64)[:, None],
        (1, n_station),
    )

    def _make_file(path, t_offset):
        ds = xr.Dataset(
            data_vars={
                'lon': (('station',), lon_1d),
                'lat': (('station',), lat_1d),
                'h': (('station',), h_1d),
                'siglay': (('siglay', 'station'), siglay),
                'zeta': (
                    ('time', 'station'),
                    np.zeros((n_time, n_station), dtype=np.float64),
                ),
            },
            coords={
                'time': (
                    np.datetime64('2026-02-16T00')
                    + (t_offset + np.arange(n_time)) * np.timedelta64(1, 'h')
                ),
            },
        )
        ds.to_netcdf(path)
        ds.close()

    f1 = tmp_path / 'fvcom_stations_a.nc'
    f2 = tmp_path / 'fvcom_stations_b.nc'
    _make_file(f1, 0)
    _make_file(f2, n_time)

    combined = xr.open_mfdataset(
        [str(f1), str(f2)],
        data_vars='minimal',
        combine='nested',
        concat_dim='time',
    )

    # Pre-condition: static coords MUST remain 1-D under 'minimal'.
    # If a future xarray bump changes that, the test fails loudly here
    # rather than surfacing as a misleading downstream assertion.
    assert combined['lon'].dims == ('station',), combined['lon'].dims
    assert combined['lat'].dims == ('station',), combined['lat'].dims

    return combined, lon_1d, lat_1d


# ---------------------------------------------------------------------------
# Fixture: minimal config file + prop / extract / station_ctl files
# ---------------------------------------------------------------------------


def _write_minimal_config(tmp_path):
    """Write a minimal INI config the writer needs for read_config_section.

    ``write_ofs_ctlfile`` reads the ``directories`` section to assemble
    ``prop.model_path`` even though the path is not actually used when
    only the ctl writer branch runs end-to-end. We supply just enough
    keys to make ``configparser`` happy.
    """
    cfg_path = tmp_path / 'ofs_dps.conf'
    cfg_path.write_text(
        '[directories]\n'
        f'home={tmp_path.as_posix()}\n'
        'model_historical_dir=%(home)s/example_data\n'
        'netcdf_dir=netcdf\n'
    )
    return cfg_path


def _write_obs_station_ctl(control_dir, ofs, name_var, stations):
    """Write a minimal obs station.ctl file the writer will read.

    Format (per station_ctl_file_extract):
        <ID> <ID>_<SRC> "<name>"
          <lat> <lon> <depth> <pad> <datum>
    """
    path = control_dir / f'{ofs}_{name_var}_station.ctl'
    lines = []
    for sid, lat, lon, depth in stations:
        lines.append(f'{sid} {sid}_COOPS "Station {sid}"')
        lines.append(f'  {lat} {lon} {depth} 0.0 MLLW')
    path.write_text('\n'.join(lines) + '\n')
    return path


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_write_ofs_ctlfile_fvcom_stations_minimal_intake(
    tmp_path, fvcom_minimal_dataset,
):
    """End-to-end smoke test: writer must not raise on 1-D static coords."""
    combined, lon_1d, lat_1d = fvcom_minimal_dataset

    cfg_path = _write_minimal_config(tmp_path)
    control_dir = tmp_path / 'control_files'
    control_dir.mkdir()

    # Pick node indices we'll later assert against the written ctl.
    chosen_nodes = [1, 3, 5]
    stations = [
        # (id, lat, lon, depth) — coords mirror three of the model stations
        # so index_nearest_node lands deterministically on chosen_nodes.
        (f'9000{ni:02d}', float(lat_1d[ni]), float(lon_1d[ni]), 0.0)
        for ni in chosen_nodes
    ]
    _write_obs_station_ctl(control_dir, 'tbofs', 'wl', stations)

    prop = SimpleNamespace(
        config_file=str(cfg_path),
        ofs='tbofs',
        var_list=['water_level'],
        ofsfiletype='stations',
        user_input_location=False,
        model_source='fvcom',
        control_files_path=str(control_dir),
        datum='MLLW',
    )

    logger = logging.getLogger('test_write_ofs_ctlfile_minimal_intake')
    logger.setLevel(logging.DEBUG)

    # Should NOT raise; pre-fix this raised IndexError at
    # ``model['lat'][0, list_of_nearest_node[i]]`` because lon/lat are
    # now 1-D under data_vars='minimal'.
    write_ofs_ctlfile(prop, combined, logger)

    out_path = control_dir / 'tbofs_wl_model_station.ctl'
    assert out_path.exists(), 'writer did not produce a model station ctl file'
    assert os.path.getsize(out_path) > 0, 'model station ctl file is empty'

    written_lines = [
        ln for ln in out_path.read_text().splitlines() if ln.strip()
    ]
    assert len(written_lines) == len(chosen_nodes), (
        f'expected {len(chosen_nodes)} ctl rows, got {len(written_lines)}: '
        f'{written_lines!r}'
    )

    # Each row format: <node> <layer> <lat>  <lon>  <id>  <depth>
    for row, node_idx in zip(written_lines, chosen_nodes):
        tokens = row.split()
        assert len(tokens) >= 6, f'malformed row: {row!r}'
        written_node = int(tokens[0])
        written_lat = float(tokens[2])
        written_lon = float(tokens[3])

        # Node index resolved by nearest-node search must match the
        # one we forged via identical obs coords.
        assert written_node == node_idx, (
            f'expected node {node_idx}, got {written_node} (row={row!r})'
        )
        # 1-D source values must round-trip through the writer at 3 dp.
        assert written_lat == pytest.approx(float(lat_1d[node_idx]), abs=5e-4)
        # The writer subtracts ``lon_wrap=360`` for non-necofs FVCOM
        # because the indexer added 360 to obs_lon. Source lon values
        # in our fixture are already negative (W. Atl), so the round
        # trip is: stored = lon_1d[node_idx] - 360, then -360 again
        # in writer => stored_in_ctl = lon_1d[node_idx] - 360.
        # Actually the writer reads ``lon_1d[node]`` (raw, negative)
        # and subtracts ``lon_wrap=360``, yielding lon - 360 in the
        # ctl. We assert that to keep the test honest about behaviour.
        assert written_lon == pytest.approx(
            float(lon_1d[node_idx]) - 360.0, abs=5e-4,
        )
