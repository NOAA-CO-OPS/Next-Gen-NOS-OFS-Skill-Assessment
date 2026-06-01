"""
Regression test for issue #104.

The parallel station-plot dispatch in ``create_1dplot.create_1dplot_2nd_part``
must hand each worker a fully isolated deep copy of ``prop``. Earlier versions
used ``copy.copy(prop)`` (shallow) and relied on an inner ``copy.deepcopy``
inside ``_process_station_plot`` — which raced against other workers walking
the same shared object graph and produced torn ``start_date_full`` /
``end_date_full`` snapshots. On NECOFS those torn snapshots collapsed
water-level plots to a single data point.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CREATE_1DPLOT_PATH = REPO_ROOT / 'bin' / 'visualization' / 'create_1dplot.py'


@pytest.fixture(scope='module')
def create_1dplot_mod():
    spec = importlib.util.spec_from_file_location(
        'create_1dplot_under_test', CREATE_1DPLOT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['create_1dplot_under_test'] = mod
    spec.loader.exec_module(mod)
    return mod


class _StubProp:
    """Minimum surface used by the parallel dispatch path."""

    def __init__(self):
        self.ofs = 'necofs'
        self.whichcasts = ['nowcast', 'forecast_b']
        self.whichcast = 'nowcast'
        self.start_date_full = '2026-02-16T00:00:00Z'
        self.end_date_full = '2026-04-28T00:00:00Z'
        self.control_files_path = '/tmp/unused'
        self.data_skill_1d_pair_path = '/tmp/unused'
        self.data_model_1d_node_path = '/tmp/unused'
        self.visuals_1d_station_path = '/tmp/unused'
        self.ofsfiletype = 'stations'


class _MockLogger:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass


def test_parallel_dispatch_gives_each_worker_a_deepcopy(create_1dplot_mod):
    """Each worker must see a prop whose mutable attributes are distinct
    objects from the caller's prop — proving ``copy.deepcopy`` (not
    ``copy.copy``) at the dispatch site."""

    prop = _StubProp()
    var_info = ('Water Level', 'wl', [
        'year', 'month', 'day', 'hour', 'minute', 'obs', 'mod'])
    # 4 stations triggers the parallel branch (num_stations > 1) and
    # exercises multiple worker submits.
    num_stations = 4
    read_ofs_ctl_file = [
        [None] * num_stations,        # 0
        list(range(num_stations)),    # 1: node_index per station
        [None] * num_stations,        # 2
        [f'sta{i}' for i in range(num_stations)],  # -1: station IDs
    ]

    captured = []

    def fake_process_station_plot(
            i, ctl_file, station_ctl, received_prop, _var_info, _logger):
        captured.append(received_prop)
        return f'sta{i}'

    with patch.object(create_1dplot_mod, '_process_station_plot',
                      side_effect=fake_process_station_plot), \
         patch.object(create_1dplot_mod, 'station_ctl_file_extract',
                      return_value=[[], []]), \
         patch.object(create_1dplot_mod, '_ensure_paired_data_exists'), \
         patch.object(create_1dplot_mod, 'get_parallel_config',
                      return_value={'parallel_plotting': True,
                                    'plot_workers': 4}):
        create_1dplot_mod.create_1dplot_2nd_part(
            read_ofs_ctl_file, prop, var_info, _MockLogger())

    assert len(captured) == num_stations, (
        f'Expected {num_stations} worker invocations, got {len(captured)}')

    # Each worker's prop must be a distinct object from the caller's prop.
    for received in captured:
        assert received is not prop, (
            'Worker received the caller-owned prop directly — '
            'shallow/no copy at dispatch')

    # Mutable attributes must not share identity with the caller's prop.
    # This is the core deepcopy assertion: a shallow copy would leave
    # ``whichcasts`` pointing at the same list object.
    for received in captured:
        assert received.whichcasts is not prop.whichcasts, (
            'Worker prop.whichcasts shares the same list object as the '
            'caller — dispatch is using copy.copy, not copy.deepcopy '
            '(regression of issue #104)')

    # Mutating one worker's prop must not propagate to other workers or
    # to the caller.
    captured[0].whichcasts.append('forecast_a')
    captured[0].start_date_full = '1999-01-01T00:00:00Z'
    for other in captured[1:]:
        assert 'forecast_a' not in other.whichcasts
        assert other.start_date_full == prop.start_date_full
    assert 'forecast_a' not in prop.whichcasts
    assert prop.start_date_full == '2026-02-16T00:00:00Z'
