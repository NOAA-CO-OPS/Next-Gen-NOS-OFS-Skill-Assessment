"""
Regression: inventory dedup must preserve distinct station IDs even when
their lat/lon round to the same 2-decimal grid cell.

Motivating case — Tampa Bay PORTS at Old Port Tampa:
    8726607 (tide gauge,  has_wl)   lat=27.8578 lng=-82.5528
    t02010  (currents ADCP, has_cu) lat=27.8629 lng=-82.5537

Both round to (27.86, -82.55). An earlier implementation of
`get_inventory_datasets` collapsed them via a spatial groupby and dropped
the currents ADCP from the inventory; the downstream currents retrieval
loop then fired against the tide-gauge ID and got 404/400 from CO-OPS.
"""
from __future__ import annotations

import logging

import pandas as pd
import pytest

from ofs_skill.obs_retrieval.ofs_inventory_stations import (
    get_inventory_datasets,
)

_LOGGER = logging.getLogger('inventory_dedup_test')


def _tampa_polygon() -> list[tuple[float, float]]:
    """Polygon covering Tampa Bay PORTS area as (lon, lat) tuples.

    Matches the (lon, lat) convention used by `ofs_geometry` and consumed
    by `get_inventory_datasets` via `Polygon(geo[0])`.
    """
    return [
        (-83.0, 27.4),
        (-82.3, 27.4),
        (-82.3, 28.0),
        (-83.0, 28.0),
        (-83.0, 27.4),
    ]


def _coops_inventory_old_port_tampa() -> pd.DataFrame:
    """Two real CO-OPS stations at Old Port Tampa, in MDAPI order.

    Tide gauge (8726607) appears first because the upstream
    `inventory_t_c_station` pass over `water_level` runs before the
    `currents` pass that introduces the ADCP. The dedup must not let
    insertion order determine which ID survives.
    """
    return pd.DataFrame([
        {
            'ID': '8726607',
            'X': -82.5528,
            'Y': 27.8578,
            'Source': 'CO-OPS',
            'Name': 'Old Port Tampa',
            'has_wl': True,
            'has_temp': False,
            'has_salt': False,
            'has_cu': False,
        },
        {
            'ID': 't02010',
            'X': -82.5537,
            'Y': 27.8629,
            'Source': 'CO-OPS',
            'Name': 'Old Port Tampa',
            'has_wl': False,
            'has_temp': False,
            'has_salt': False,
            'has_cu': True,
        },
    ])


def test_colocated_distinct_coops_stations_both_survive_dedup():
    geo = (_tampa_polygon(),)
    t_c = _coops_inventory_old_port_tampa()

    merged = get_inventory_datasets(
        geo=geo, t_c=t_c, usgs=None, ndbc=None, chs=None, logger=_LOGGER,
    )

    ids = set(merged['ID'])
    assert {'8726607', 't02010'} <= ids, (
        'Both Old Port Tampa stations must survive the inventory dedup; '
        f'got IDs: {sorted(ids)}'
    )

    wl_row = merged[merged['ID'] == '8726607'].iloc[0]
    assert bool(wl_row['has_wl']) is True
    assert bool(wl_row['has_cu']) is False, (
        'has_cu must stay on the ADCP row, not migrate onto the tide gauge'
    )

    cu_row = merged[merged['ID'] == 't02010'].iloc[0]
    assert bool(cu_row['has_cu']) is True
    assert bool(cu_row['has_wl']) is False, (
        'has_wl must stay on the tide gauge row, not migrate onto the ADCP'
    )


def test_true_within_source_id_dupes_are_collapsed_with_flag_or():
    """Defensive: if a source ever emits the same ID twice with disjoint
    capability flags, the rows should collapse and the flags should OR."""
    geo = (_tampa_polygon(),)
    t_c = pd.DataFrame([
        {
            'ID': 't02010', 'X': -82.5537, 'Y': 27.8629,
            'Source': 'CO-OPS', 'Name': 'Old Port Tampa',
            'has_wl': False, 'has_temp': False,
            'has_salt': False, 'has_cu': True,
        },
        {
            'ID': 't02010', 'X': -82.5537, 'Y': 27.8629,
            'Source': 'CO-OPS', 'Name': 'Old Port Tampa',
            'has_wl': False, 'has_temp': True,
            'has_salt': False, 'has_cu': False,
        },
    ])

    merged = get_inventory_datasets(
        geo=geo, t_c=t_c, usgs=None, ndbc=None, chs=None, logger=_LOGGER,
    )

    assert len(merged[merged['ID'] == 't02010']) == 1
    row = merged[merged['ID'] == 't02010'].iloc[0]
    assert bool(row['has_cu']) is True
    assert bool(row['has_temp']) is True


def test_cross_source_colocated_stations_preserved():
    """A CO-OPS station and a co-located NDBC station must both survive —
    the dedup intentionally never crosses sources."""
    geo = (_tampa_polygon(),)
    t_c = pd.DataFrame([{
        'ID': '8726607', 'X': -82.5528, 'Y': 27.8578,
        'Source': 'CO-OPS', 'Name': 'Old Port Tampa',
        'has_wl': True, 'has_temp': False,
        'has_salt': False, 'has_cu': False,
    }])
    ndbc = pd.DataFrame([{
        'ID': 'FAKE_NDBC', 'X': -82.5528, 'Y': 27.8578,
        'Source': 'NDBC', 'Name': 'Hypothetical NDBC at same location',
        'has_wl': False, 'has_temp': True,
        'has_salt': False, 'has_cu': False,
    }])

    merged = get_inventory_datasets(
        geo=geo, t_c=t_c, usgs=None, ndbc=ndbc, chs=None, logger=_LOGGER,
    )

    sources = set(zip(merged['Source'], merged['ID']))
    assert ('CO-OPS', '8726607') in sources
    assert ('NDBC', 'FAKE_NDBC') in sources


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
