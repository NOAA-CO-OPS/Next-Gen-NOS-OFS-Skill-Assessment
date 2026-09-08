"""
Regression tests for STOFS-3D stations model depth indexing (issue: model
ctl-file creation for temp/salt/currents crashed with UnboundLocalError).

The STOFS-3D points (stations) files are surface-only: ``temperature``
and ``salinity`` are sampled at the water surface and ``u``/``v`` are
surface velocities, all on ``(time, station)`` with no vertical
coordinate. The SCHISM stations branch of ``index_nearest_depth`` only
knew ``loofs2`` and ``secofs``, so for ``stofs_3d_atl``/``stofs_3d_pac``
the local ``model_depths`` was never assigned and the first use raised
``UnboundLocalError`` — killing model ctl-file creation for every
variable except water level (which exits early).

These tests cover:
- the surface-only STOFS-3D branch for temp/salt/cu (layer 0, depth 0.0);
- non-regression for the loofs2 and secofs depth-array shapes;
- the explicit NotImplementedError for unknown SCHISM OFS (instead of
  an UnboundLocalError from a silent fall-through);
- end-to-end ctl-file creation through ``write_ofs_ctlfile`` with a
  synthetic dataset shaped like the real STOFS-3D-Atl points file.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.model_processing.indexing import index_nearest_depth
from ofs_skill.model_processing.write_ofs_ctlfile import write_ofs_ctlfile
from ofs_skill.utils.file_headers import strip_model_ctl_header

LOGGER = logging.getLogger('stofs_stations_model_depths_test')


def _stations_prop(ofs: str) -> SimpleNamespace:
    """Minimal prop object for the stations branch of index_nearest_depth."""
    return SimpleNamespace(ofs=ofs, ofsfiletype='stations')


def _stofs_points_like(n_time: int = 4, n_station: int = 3) -> dict:
    """Synthetic mapping shaped like the STOFS-3D points file variables.

    Mirrors the real ``stofs_3d_atl.tXXz.points.cwl.temp.salt.vel.nc``
    layout: every variable is 2-D ``(time, station)``; there is no
    vertical coordinate (no zCoordinates/zcor/zcoords).
    """
    shape = (n_time, n_station)
    return {
        'zeta': np.zeros(shape),
        'temperature': np.full(shape, 20.0),
        'salinity': np.full(shape, 30.0),
        'u': np.full(shape, 0.1),
        'v': np.full(shape, -0.1),
        'x': np.array([-66.983, -70.244, -71.05])[:n_station],
        'y': np.array([44.905, 43.658, 42.35])[:n_station],
    }


# Coord rows as parsed from an obs station ctl file: [lat, lon, ?, depth, ...]
_OBS_ROWS = [
    ['44.905', '-66.983', '0.0', '0.00', '0.0'],
    ['43.658', '-70.244', '0.0', '4.50', '0.0'],
    ['42.350', '-71.050', '0.0', '2.00', '0.0'],
]


@pytest.mark.parametrize('ofs', ['stofs_3d_atl', 'stofs_3d_pac'])
@pytest.mark.parametrize('name_var', ['temp', 'salt', 'cu'])
def test_stofs_3d_stations_surface_only_depths(ofs, name_var):
    """STOFS-3D stations files are surface-only: every matched station
    gets layer 0 / depth 0.0, unmatched stations get NaN — and no
    UnboundLocalError."""
    prop = _stations_prop(ofs)
    index_min_dist = [0, 2, np.nan]

    index_min_depth, depth_value = index_nearest_depth(
        prop, index_min_dist, _stofs_points_like(), _OBS_ROWS,
        'schism', name_var, ofs, LOGGER,
    )

    assert index_min_depth[0] == 0
    assert index_min_depth[1] == 0
    assert np.isnan(index_min_depth[2])
    assert depth_value[0] == 0.0
    assert depth_value[1] == 0.0
    assert np.isnan(depth_value[2])
    assert len(index_min_depth) == len(index_min_dist)
    assert len(depth_value) == len(index_min_dist)


def test_stofs_3d_stations_wl_unchanged():
    """Water level keeps its existing surface handling."""
    prop = _stations_prop('stofs_3d_atl')
    index_min_depth, depth_value = index_nearest_depth(
        prop, [0, 1], _stofs_points_like(), _OBS_ROWS,
        'schism', 'wl', 'stofs_3d_atl', LOGGER,
    )
    assert index_min_depth == [0, 0]
    assert list(depth_value) == [0.0, 0.0]


def test_loofs2_stations_depth_selection_nonregression():
    """loofs2 keeps its zcoords (node, depth) nearest-depth lookup."""
    prop = _stations_prop('loofs2')
    # 3 nodes x 4 depth levels, model depths negative (SCHISM convention)
    zcoords = np.tile(np.array([-0.5, -2.0, -5.0, -10.0]), (3, 1))
    model = {'zcoords': zcoords}
    obs_rows = [['43.658', '-70.244', '0.0', '4.50', '0.0']]

    index_min_depth, depth_value = index_nearest_depth(
        prop, [1], model, obs_rows, 'schism', 'temp', 'loofs2', LOGGER,
    )

    # obs depth 4.5 m -> nearest model level is -5.0 m (index 2)
    assert index_min_depth == [2]
    assert depth_value[0] == pytest.approx(5.0)


def test_secofs_stations_depth_selection_nonregression():
    """secofs keeps its zCoordinates (time, depth, node) lookup."""
    prop = _stations_prop('secofs')
    # 1 time x 4 depth levels x 3 nodes
    z = np.tile(np.array([-0.5, -2.0, -5.0, -10.0])[:, None], (1, 1, 3))
    model = {'zCoordinates': z}
    obs_rows = [['43.658', '-70.244', '0.0', '4.50', '0.0']]

    index_min_depth, depth_value = index_nearest_depth(
        prop, [1], model, obs_rows, 'schism', 'temp', 'secofs', LOGGER,
    )

    assert index_min_depth == [2]
    assert depth_value[0] == pytest.approx(5.0)


def test_secofs_stations_missing_depth_surface_fallback():
    """secofs without zCoordinates falls back to the surface layer."""
    prop = _stations_prop('secofs')
    # salinity (time, depth, station): surface layer index = shape[1] - 1
    model = {'salinity': np.zeros((5, 4, 3))}
    obs_rows = _OBS_ROWS[:2]

    index_min_depth, depth_value = index_nearest_depth(
        prop, [0, 1], model, obs_rows, 'schism', 'salt', 'secofs', LOGGER,
    )

    assert index_min_depth == [3, 3]
    assert list(depth_value) == [0.0, 0.0]


def test_unknown_schism_ofs_raises_not_implemented():
    """A SCHISM OFS without wired-in depth handling must fail loudly,
    not with an UnboundLocalError from a silent fall-through."""
    prop = _stations_prop('futureofs')
    with pytest.raises(NotImplementedError, match='futureofs'):
        index_nearest_depth(
            prop, [0], _stofs_points_like(), _OBS_ROWS[:1],
            'schism', 'temp', 'futureofs', LOGGER,
        )


# ---------------------------------------------------------------------------
# End-to-end ctl-file creation through write_ofs_ctlfile
# ---------------------------------------------------------------------------

_STATION_NAMES = [
    'PSBM1 SOUS41 8410140 ME Eastport',
    'CASM1 SOUS41 8418150 ME Portland',
    'BHBM3 SOUS41 8443970 MA Boston',
]


def _stofs_points_dataset() -> xr.Dataset:
    """Synthetic xarray Dataset shaped like the STOFS-3D-Atl points file."""
    n_time, n_station = 4, 3
    shape = (n_time, n_station)
    return xr.Dataset(
        {
            'zeta': (('time', 'station'), np.zeros(shape)),
            'temperature': (('time', 'station'), np.full(shape, 20.0)),
            'salinity': (('time', 'station'), np.full(shape, 30.0)),
            'u': (('time', 'station'), np.full(shape, 0.1)),
            'v': (('time', 'station'), np.full(shape, -0.1)),
            'station_name': (('station',), np.array(_STATION_NAMES)),
            'x': (('station',), np.array([-66.983, -70.244, -71.05])),
            'y': (('station',), np.array([44.905, 43.658, 42.35])),
        },
        coords={'time': np.arange(n_time)},
    )


def _write_obs_ctl(path, name_var):
    """Write a minimal obs station ctl file (two lines per station)."""
    rows = [
        ('8410140', 'Eastport', '44.905', '-66.983', '0.00'),
        ('8418150', 'Portland', '43.658', '-70.244', '4.50'),
        # Not present in the model station names -> should be skipped
        ('9999999', 'Nowhere', '10.000', '-60.000', '0.00'),
    ]
    with open(path, 'w', encoding='utf-8') as fh:
        for sid, name, lat, lon, depth in rows:
            fh.write(f'{sid} {sid}_{name_var}_stofs_3d_atl_CO-OPS "{name}"\n')
            fh.write(f'  {lat} {lon} 0.0  {depth}  0.0\n')


@pytest.mark.parametrize('variable, name_var', [
    ('water_temperature', 'temp'),
    ('salinity', 'salt'),
    ('currents', 'cu'),
])
def test_write_ofs_ctlfile_stofs_stations(tmp_path, variable, name_var):
    """Model ctl files for temp/salt/cu are created for STOFS-3D stations
    with the standard 6-column layout, layer 0 and depth 0.0."""
    ctl_dir = tmp_path / 'control_files'
    ctl_dir.mkdir()
    _write_obs_ctl(ctl_dir / f'stofs_3d_atl_{name_var}_station.ctl', name_var)

    conf = tmp_path / 'ofs_dps.conf'
    conf.write_text(
        '[directories]\n'
        f'model_historical_dir={tmp_path.as_posix()}\n'
        'netcdf_dir=netcdf\n',
        encoding='utf-8',
    )

    prop = SimpleNamespace(
        ofs='stofs_3d_atl',
        ofsfiletype='stations',
        model_source='schism',
        var_list=[variable],
        user_input_location=False,
        control_files_path=str(ctl_dir),
        config_file=str(conf),
    )

    write_ofs_ctlfile(prop, _stofs_points_dataset(), LOGGER)

    out = ctl_dir / f'stofs_3d_atl_{name_var}_model_station.ctl'
    assert out.is_file(), 'model ctl file was not created'
    lines = strip_model_ctl_header(
        out.read_text(encoding='utf-8').splitlines())
    # Two of the three obs stations match model station names
    assert len(lines) == 2
    for line, (sid, lat, lon) in zip(
            lines,
            [('8410140', '44.905', '-66.983'),
             ('8418150', '43.658', '-70.244')]):
        cols = line.split()
        # node layer lat lon station_id depth
        assert len(cols) == 6
        assert cols[1] == '0'
        assert cols[2] == lat
        assert cols[3] == lon
        assert cols[4] == sid
        assert cols[5] == '0.0'
