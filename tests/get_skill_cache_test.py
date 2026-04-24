"""
Unit tests for the provenance-tagged model cache in get_skill.

Validates that _cache_key / _get_valid_cached_model / _set_cached_model
correctly invalidate the cache when any loader input on prop changes,
preventing nowcast data from leaking into forecast_b .prd files (and vice
versa) across multi-whichcast runs.
"""

import pytest

from ofs_skill.skill_assessment.get_skill import (
    _cache_key,
    _get_valid_cached_model,
    _set_cached_model,
)


class MockProps:
    """Minimal prop object carrying only the fields the cache key reads."""

    def __init__(self, ofs='cbofs', whichcast='nowcast',
                 forecast_hr=None, start_date_full='2026-03-28T00:00:00Z',
                 end_date_full='2026-03-28T23:00:00Z',
                 ofsfiletype='stations'):
        self.ofs = ofs
        self.whichcast = whichcast
        self.forecast_hr = forecast_hr
        self.start_date_full = start_date_full
        self.end_date_full = end_date_full
        self.ofsfiletype = ofsfiletype


@pytest.fixture
def props():
    return MockProps()


def _sentinel(tag):
    """Distinguishable stand-in for an xarray.Dataset."""
    return {'_sentinel': tag}


def test_cache_reused_when_key_matches(props):
    ds = _sentinel('nowcast-ds')
    _set_cached_model(props, ds)
    assert _get_valid_cached_model(props) is ds


def test_cache_invalidated_on_whichcast_change(props):
    _set_cached_model(props, _sentinel('nowcast-ds'))
    props.whichcast = 'forecast_b'
    assert _get_valid_cached_model(props) is None


def test_cache_invalidated_on_forecast_hr_change(props):
    props.whichcast = 'forecast_a'
    props.forecast_hr = '06z'
    _set_cached_model(props, _sentinel('fa-06z-ds'))
    props.forecast_hr = '12z'
    assert _get_valid_cached_model(props) is None


def test_cache_invalidated_on_start_date_change(props):
    _set_cached_model(props, _sentinel('ds'))
    props.start_date_full = '2026-04-01T00:00:00Z'
    assert _get_valid_cached_model(props) is None


def test_cache_invalidated_on_end_date_change(props):
    _set_cached_model(props, _sentinel('ds'))
    props.end_date_full = '2026-04-01T23:00:00Z'
    assert _get_valid_cached_model(props) is None


def test_cache_invalidated_on_ofsfiletype_change(props):
    _set_cached_model(props, _sentinel('stations-ds'))
    props.ofsfiletype = 'fields'
    assert _get_valid_cached_model(props) is None


def test_cache_invalidated_on_ofs_change(props):
    _set_cached_model(props, _sentinel('cbofs-ds'))
    props.ofs = 'sfbofs'
    assert _get_valid_cached_model(props) is None


def test_set_cached_model_ignores_none(props):
    _set_cached_model(props, None)
    assert not hasattr(props, '_cached_model')
    assert not hasattr(props, '_cached_model_key')


def test_no_key_attribute_treated_as_miss(props):
    # Simulate a prop that has _cached_model from some pre-helper code path
    # but no _cached_model_key stamp. Must be treated as a miss so the
    # provenance contract holds.
    props._cached_model = _sentinel('untagged-ds')
    assert _get_valid_cached_model(props) is None


def test_cache_key_is_tuple_of_six_fields(props):
    key = _cache_key(props)
    assert isinstance(key, tuple)
    assert len(key) == 6
    assert key == ('cbofs', 'nowcast', None,
                   '2026-03-28T00:00:00Z', '2026-03-28T23:00:00Z',
                   'stations')


def test_round_trip_after_invalidation_and_reset(props):
    """After invalidation, re-stamping under the new key should hit again."""
    _set_cached_model(props, _sentinel('nowcast-ds'))
    props.whichcast = 'forecast_b'
    assert _get_valid_cached_model(props) is None
    new_ds = _sentinel('forecast_b-ds')
    _set_cached_model(props, new_ds)
    assert _get_valid_cached_model(props) is new_ds
