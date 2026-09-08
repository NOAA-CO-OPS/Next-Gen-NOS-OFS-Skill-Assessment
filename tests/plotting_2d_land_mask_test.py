"""Regression tests for the land overlay used by the static 2D maps.

``plot_2d_scalar_map`` and ``plot_2d_current_quiver_map`` paint land over
the gridded data so interpolation cannot bleed ashore. Natural Earth's
``physical/land`` polygons, however, treat the Great Lakes as land -- lakes
are published as a separate feature rather than being cut out of land. The
overlay therefore covered the *entire* domain of every Great Lakes OFS, and
``plot_leafletJSON.py`` produced a map with correct extent, coastlines and
colorbar range but no data drawn at all.

This is the same misclassification ``processing_2d`` already works around on
the data side, where the global land mask is skipped for ``GREAT_LAKES_OFS``
and the OFS shapefile mask is relied on instead.

``_land_without_lakes`` subtracts the lakes so the overlay covers only real
land. When the geometry cannot be built it returns ``None``, and callers
must then draw land *beneath* the data -- degrading to slight bleed-through
rather than back to a blank map.
"""

import logging

import pytest

from ofs_skill.visualization import plotting_2d

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset the module-level cache around each test."""
    plotting_2d._LAND_WITHOUT_LAKES = None
    yield
    plotting_2d._LAND_WITHOUT_LAKES = None


def test_returns_none_and_does_not_retry_when_geometry_fails(monkeypatch):
    """A failed build must return None and be remembered, not retried.

    Callers treat ``None`` as "draw land beneath the data", so a missing
    Natural Earth asset costs a little bleed-through instead of hiding the
    field. Retrying per figure would re-attempt a download that has already
    failed once.
    """
    calls = []

    def _boom(*_args, **_kwargs):
        calls.append(1)
        raise OSError('natural earth asset unavailable')

    monkeypatch.setattr('shapely.ops.unary_union', _boom)

    assert plotting_2d._land_without_lakes(logger) is None
    assert plotting_2d._LAND_WITHOUT_LAKES is False
    # Second call is served from the cache, without another attempt.
    assert plotting_2d._land_without_lakes(logger) is None
    assert len(calls) == 1


def test_cached_feature_is_reused(monkeypatch):
    """A built feature is served from the cache, not rebuilt per figure.

    The difference costs ~0.5 s; the maps are generated in a loop over every
    variable and timestamp, so rebuilding each time would be noticeable.
    """
    def _boom(*_args, **_kwargs):
        raise AssertionError('rebuilt instead of using the cache')

    monkeypatch.setattr('shapely.ops.unary_union', _boom)

    sentinel = object()
    plotting_2d._LAND_WITHOUT_LAKES = sentinel
    assert plotting_2d._land_without_lakes(logger) is sentinel


def _natural_earth_available():
    """True when the Natural Earth land/lakes shapefiles resolve offline."""
    try:
        import cartopy.feature as cfeature
        next(iter(cfeature.LAND.geometries()))
        next(iter(cfeature.LAKES.geometries()))
    except Exception:
        return False
    return True


@pytest.mark.skipif(
    not _natural_earth_available(),
    reason='Natural Earth land/lakes shapefiles not available offline',
)
def test_great_lakes_are_not_covered_but_real_land_is():
    """The overlay must exclude the lakes and still cover actual land."""
    from shapely.geometry import Point

    feature = plotting_2d._land_without_lakes(logger)
    assert feature is not None
    geom = next(iter(feature.geometries()))

    # (lon, lat) points inside each Great Lake must not be covered.
    for name, lon, lat in (
        ('Lake Erie', -81.0, 42.0),
        ('Lake Ontario', -77.5, 43.6),
        ('Lake Michigan', -87.0, 43.5),
        ('Lake Superior', -87.5, 47.5),
    ):
        assert not geom.contains(Point(lon, lat)), name

    # Real land is still covered, so coastal bleed-through stays hidden.
    assert geom.contains(Point(-82.5, 40.0)), 'inland Ohio'
    # Open water outside any lake was never part of the land polygons.
    assert not geom.contains(Point(-70.0, 39.0)), 'open Atlantic'
