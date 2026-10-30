"""
Regression tests for SPoRT-only 2D runs (issue #122).

When NESDIS GOES L3C is unavailable for the requested window -- routine
during a NESDIS THREDDS outage -- SPoRT is the only satellite source.
``parse_leaflet_json`` used to write the SPoRT observation JSONs and then
return before the shared model-write section, leaving ``data/model/2d``
empty. The run computed the model regular grid, threw it away, and the
downstream plotting step aborted with::

    ERROR - Problem calling plotting_2d.plot_2d - ABORT
    ERROR - Exception: No files found in directory ./data/model/2d

These tests build a minimal FVCOM model file and a minimal SPoRT
satellite file and run the real function, asserting on the files that
land on disk.
"""

import logging
import logging.config
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from ofs_skill.visualization import processing_2d

LOG = logging.getLogger('processing_2d_sport_only_test')

NT, NN, NSIG = 3, 40, 3
LON = np.linspace(-76.5, -76.0, NN)
LAT = np.linspace(37.0, 37.5, NN)


@pytest.fixture(autouse=True)
def _no_global_logging_reconfig(monkeypatch):
    """Stop param_val reconfiguring the global logging system.

    ``fileConfig`` mutates global logging state and disables existing
    loggers, which silently breaks caplog assertions in tests that run
    later in the same process.
    """
    monkeypatch.setattr(
        logging.config, 'fileConfig', lambda *a, **k: None,
    )


def _model_file(tmp_path):
    """Minimal FVCOM fields dataset, as intake_model would return it."""
    rng = np.random.default_rng(0)
    path = tmp_path / 'model.nc'
    xr.Dataset(
        {
            'lon': (('node',), LON), 'lat': (('node',), LAT),
            'lonc': (('nele',), LON), 'latc': (('nele',), LAT),
            'temp': (('time', 'siglay', 'node'),
                     rng.normal(20, 1, (NT, NSIG, NN))),
            'salinity': (('time', 'siglay', 'node'),
                         rng.normal(30, 1, (NT, NSIG, NN))),
            'u': (('time', 'siglay', 'nele'),
                  rng.normal(0, 0.2, (NT, NSIG, NN))),
            'v': (('time', 'siglay', 'nele'),
                  rng.normal(0, 0.2, (NT, NSIG, NN))),
            'zeta': (('time', 'node'), rng.normal(0, 0.1, (NT, NN))),
        },
        coords={'time': np.datetime64('2026-04-25T17')
                + np.arange(NT) * np.timedelta64(1, 'h')},
    ).to_netcdf(path)
    return xr.open_dataset(path)


def _sport_file(tmp_path, with_latency=True):
    """Minimal masked SPoRT satellite file.

    The name must contain 'sport' -- that substring is how the source is
    identified.
    """
    rng = np.random.default_rng(1)
    path = tmp_path / 'obs' / '2d_satellite' / 'cbofs_sport.nc'
    path.parent.mkdir(parents=True, exist_ok=True)
    secs = ((np.datetime64('2026-04-26T06') - np.datetime64('1981-01-01'))
            / np.timedelta64(1, 's'))
    data = {'analysed_sst': (('time', 'lat', 'lon'),
                             rng.normal(293, 1, (1, NN, NN)))}
    if with_latency:
        data['latency'] = (('time', 'lat', 'lon'),
                           rng.normal(1, 0.1, (1, NN, NN)))
    xr.Dataset(data, coords={'time': ('time', np.array([secs])),
                             'lat': LAT, 'lon': LON}).to_netcdf(path)
    return str(path)


def _prop(tmp_path):
    return SimpleNamespace(
        model_source='fvcom', ofs='cbofs', path=str(tmp_path),
        config_file=None,
        data_model_2d_json_path=str(tmp_path / 'data' / 'model' / '2d'),
        data_observations_2d_json_path=str(
            tmp_path / 'data' / 'observations' / '2d'),
        start_date_full='2026-04-25T17:00:00Z',
        end_date_full='2026-04-25T19:00:00Z',
        whichcast='nowcast', ofsfiletype='fields',
    )


def _counts(prop):
    from pathlib import Path
    model = Path(prop.data_model_2d_json_path)
    obs = Path(prop.data_observations_2d_json_path)
    return (len(list(model.glob('*.json'))) if model.exists() else 0,
            len(list(obs.glob('*.json'))) if obs.exists() else 0)


@pytest.mark.integration
def test_sport_only_run_writes_model_json(tmp_path):
    """The regression: SPoRT as the only satellite source must still
    produce the model 2D JSONs that plotting_2d requires."""
    prop = _prop(tmp_path)
    processing_2d.parse_leaflet_json(
        _model_file(tmp_path), _sport_file(tmp_path), prop)

    n_model, n_obs = _counts(prop)
    assert n_model > 0, (
        'model 2D JSONs were not written; plotting_2d will abort with '
        '"No files found in directory .../model/2d"')
    assert n_obs > 0, 'SPoRT observation JSONs were not written'


@pytest.mark.integration
def test_sport_only_run_still_writes_sport_json(tmp_path):
    """Fixing the model write must not cost the SPoRT output."""
    from pathlib import Path
    prop = _prop(tmp_path)
    processing_2d.parse_leaflet_json(
        _model_file(tmp_path), _sport_file(tmp_path), prop)

    sport = list(Path(prop.data_observations_2d_json_path).glob('*SPoRT*'))
    assert sport, 'no *SPoRT*.json files written'


@pytest.mark.integration
def test_write_model_false_skips_the_model_write(tmp_path):
    """When L3C already wrote the model JSONs for this window, the SPoRT
    pass should not repeat them."""
    prop = _prop(tmp_path)
    processing_2d.parse_leaflet_json(
        _model_file(tmp_path), _sport_file(tmp_path), prop,
        write_model=False)

    n_model, n_obs = _counts(prop)
    assert n_model == 0, 'model JSONs rewritten despite write_model=False'
    assert n_obs > 0, 'SPoRT JSONs should still be written'


@pytest.mark.integration
def test_malformed_sport_file_degrades_to_model_only(tmp_path):
    """A SPoRT file missing an expected variable must not end the run."""
    prop = _prop(tmp_path)
    processing_2d.parse_leaflet_json(
        _model_file(tmp_path),
        _sport_file(tmp_path, with_latency=False), prop)

    n_model, _ = _counts(prop)
    assert n_model > 0, 'model JSONs lost when the SPoRT file was unusable'


@pytest.mark.integration
def test_no_satellite_run_still_writes_model_json(tmp_path):
    """The pre-existing no-satellite path must be unaffected."""
    prop = _prop(tmp_path)
    processing_2d.parse_leaflet_json(_model_file(tmp_path), None, prop)

    n_model, n_obs = _counts(prop)
    assert n_model > 0
    assert n_obs == 0


class _SpyVariables:
    """dict-like proxy that records which variables are read."""

    def __init__(self, variables, log):
        self._variables = variables
        self._log = log

    def __getitem__(self, key):
        self._log.append(key)
        return self._variables[key]

    def __contains__(self, key):
        return key in self._variables


class _SpyDataset:
    """xarray Dataset proxy recording variable access.

    Lets a test assert on what the function actually *reads*, which is
    where the cost is -- loading the model stack forces the whole
    (time, ...) array into memory.
    """

    def __init__(self, dataset):
        self._dataset = dataset
        self.accessed: list[str] = []

    @property
    def variables(self):
        return _SpyVariables(self._dataset.variables, self.accessed)

    def __getitem__(self, key):
        self.accessed.append(key)
        return self._dataset[key]


HEAVY_VARS = {'temp', 'zeta', 'salinity', 'u', 'v'}


@pytest.mark.integration
def test_write_model_false_skips_the_model_data_load(tmp_path):
    """The dual-source case must not pay for the model read twice.

    Loading the model variables dominates this function's run time, and
    the satellite interpolation needs only the target grid and the time
    axis -- so a pass that is not writing model JSONs should not load
    them.
    """
    prop = _prop(tmp_path)
    spy = _SpyDataset(_model_file(tmp_path))
    processing_2d.parse_leaflet_json(
        spy, _sport_file(tmp_path), prop, write_model=False)

    loaded = HEAVY_VARS & set(spy.accessed)
    assert not loaded, f'model variables loaded despite write_model=False: {loaded}'
    # The cheap coordinate reads the satellite branch does need.
    assert {'lon', 'lat'} <= set(spy.accessed)


@pytest.mark.integration
def test_write_model_true_loads_the_model_data(tmp_path):
    """Control for the test above: the writing pass does load them."""
    prop = _prop(tmp_path)
    spy = _SpyDataset(_model_file(tmp_path))
    processing_2d.parse_leaflet_json(
        spy, _sport_file(tmp_path), prop, write_model=True)

    assert HEAVY_VARS <= set(spy.accessed)
