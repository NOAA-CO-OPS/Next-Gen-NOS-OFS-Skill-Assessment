"""
Regression: the CHS `operating` flag must survive the inventory merge.

`get_inventory_datasets` dedups with `groupby(...).agg(agg_dict)`, and
pandas `.agg` with a dict DROPS every column the dict does not name. The
flag was added to `inventory_chs_station` but not to `agg_dict`, so it was
discarded before the inventory CSV was written and the retrieval-stage
filter that consumes it became dead code -- every decommissioned CHS
station was still probed, which is the cost the flag exists to avoid.

Testing `inventory_chs_station` alone does not catch this: the column is
present there and only disappears one layer down.
"""
from __future__ import annotations

import logging

import pandas as pd

from ofs_skill.obs_retrieval.ofs_inventory_stations import (
    get_inventory_datasets,
)

_LOGGER = logging.getLogger('inventory_operating_column_test')


def _bay_of_fundy_polygon() -> list[tuple[float, float]]:
    """(lon, lat) polygon around the Bay of Fundy, matching ofs_geometry."""
    return [
        (-67.5, 44.0),
        (-63.5, 44.0),
        (-63.5, 46.0),
        (-67.5, 46.0),
        (-67.5, 44.0),
    ]


def _chs_frame() -> pd.DataFrame:
    return pd.DataFrame({
        'ID': ['00052', '00065', '00140'],
        'X': [-66.0, -66.06, -65.5],
        'Y': [45.0, 45.27, 44.6],
        'Source': ['CHS'] * 3,
        'Name': ['Five Fathom Hole', 'Saint John', 'Herring Cove'],
        'has_wl': [True, True, True],
        'has_temp': [False, False, False],
        'has_salt': [False, False, False],
        'has_cu': [False, False, False],
        'operating': [False, True, False],
    })


def _coops_frame() -> pd.DataFrame:
    """A provider that supplies no `operating` value."""
    return pd.DataFrame({
        'ID': ['8410140'],
        'X': [-66.98],
        'Y': [44.9],
        'Source': ['CO-OPS'],
        'Name': ['Eastport'],
        'has_wl': [True],
        'has_temp': [True],
        'has_salt': [False],
        'has_cu': [False],
    })


def test_operating_column_survives_the_merge():
    """Without this the retrieval-stage filter never sees the flag."""
    result = get_inventory_datasets(
        [_bay_of_fundy_polygon()], _coops_frame(), None, None, _chs_frame(),
        _LOGGER,
    )

    assert 'operating' in result.columns

    chs = result[result['Source'] == 'CHS'].set_index('ID')
    assert bool(chs.loc['00065', 'operating']) is True
    assert bool(chs.loc['00052', 'operating']) is False
    assert bool(chs.loc['00140', 'operating']) is False


def test_providers_without_the_flag_default_to_operating():
    """An unknown flag must never drop a station."""
    result = get_inventory_datasets(
        [_bay_of_fundy_polygon()], _coops_frame(), None, None, _chs_frame(),
        _LOGGER,
    )

    coops = result[result['Source'] == 'CO-OPS']
    assert not coops.empty
    assert bool(coops['operating'].iloc[0]) is True


def test_merge_succeeds_when_chs_is_excluded():
    """A -so run without CHS has no `operating` column to aggregate."""
    result = get_inventory_datasets(
        [_bay_of_fundy_polygon()], _coops_frame(), None, None, None,
        _LOGGER,
    )

    assert not result.empty
    assert 'operating' not in result.columns
