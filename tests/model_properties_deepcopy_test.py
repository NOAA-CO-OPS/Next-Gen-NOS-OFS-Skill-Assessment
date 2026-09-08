"""
Tests for ModelProperties.__deepcopy__.

The plotting fan-out deep-copies ``prop`` per station (twice), and the
per-variable extraction dispatch deep-copies it per thread. When the
cached model dataset (``prop._cached_model``, a lazy xarray/dask graph
referencing hundreds of backing files) rode along, every copy duplicated
the whole graph. ``__deepcopy__`` must skip the cache attributes while
still producing fully isolated copies of everything else.
"""

import copy

from ofs_skill.model_processing.model_properties import ModelProperties


class _ExpensiveToCopy:
    """Stand-in for the cached xarray dataset: raises if deep-copied."""

    def __deepcopy__(self, memo):
        raise AssertionError(
            'cached model dataset must not be deep-copied')


def _make_prop():
    prop = ModelProperties()
    prop.ofs = 'necofs'
    prop.whichcast = 'nowcast'
    prop.var_list = ['water_level', 'currents']
    prop.start_date_full = '2026-02-16T00:00:00Z'
    return prop


def test_deepcopy_skips_cached_model():
    prop = _make_prop()
    prop._cached_model = _ExpensiveToCopy()
    prop._cached_model_key = ('necofs', 'nowcast')

    clone = copy.deepcopy(prop)

    assert not hasattr(clone, '_cached_model')
    assert not hasattr(clone, '_cached_model_key')
    # The original keeps its cache untouched.
    assert isinstance(prop._cached_model, _ExpensiveToCopy)


def test_deepcopy_isolates_mutable_attributes():
    prop = _make_prop()
    clone = copy.deepcopy(prop)

    assert clone.var_list == ['water_level', 'currents']
    clone.var_list.append('salinity')
    assert prop.var_list == ['water_level', 'currents']

    clone.ofs = 'cbofs'
    assert prop.ofs == 'necofs'


def test_deepcopy_without_cache_attributes():
    """Props that never had a cached model copy cleanly."""
    prop = _make_prop()
    clone = copy.deepcopy(prop)
    assert clone.ofs == prop.ofs
    assert not hasattr(clone, '_cached_model')


def test_deepcopy_inside_container():
    """deepcopy of a structure holding the prop honors the guard."""
    prop = _make_prop()
    prop._cached_model = _ExpensiveToCopy()
    holder = {'prop': prop}

    clone_holder = copy.deepcopy(holder)

    assert not hasattr(clone_holder['prop'], '_cached_model')
    assert clone_holder['prop'].ofs == 'necofs'
