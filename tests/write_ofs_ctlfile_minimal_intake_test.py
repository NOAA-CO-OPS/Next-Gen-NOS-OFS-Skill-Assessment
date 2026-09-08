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

import pytest

from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile
from tests.helpers.fvcom_minimal import write_minimal_config, write_obs_station_ctl


@pytest.mark.integration
def test_write_ofs_ctlfile_fvcom_stations_minimal_intake(
    tmp_path, fvcom_minimal_dataset,
):
    """End-to-end smoke test: writer must not raise on 1-D static coords."""
    combined, lon_1d, lat_1d = fvcom_minimal_dataset

    cfg_path = write_minimal_config(tmp_path)
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
    write_obs_station_ctl(control_dir, 'tbofs', 'wl', stations)

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

    # The model .ctl file now contains a single header row; skip it
    written_lines = written_lines[1:]

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
        # For non-necofs FVCOM the writer reads the raw (negative,
        # W. Atlantic) source lon and subtracts lon_wrap=360, so the ctl
        # stores lon - 360. Assert that documented behaviour.
        assert written_lon == pytest.approx(
            float(lon_1d[node_idx]) - 360.0, abs=5e-4,
        )
