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
from shapely.geometry import Polygon

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

def test_lake_probes_guard_returns_false_when_probes_outside_lakes(monkeypatch):
    """The guard must return False if probes do not intersect the lakes."""
    class MockLakesFeature:
        def geometries(self):
            # A small polygon far from the Great Lakes (e.g., near Null Island)
            yield Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])

    # Patch cartopy.feature.LAKES before the function imports it
    monkeypatch.setattr('cartopy.feature.LAKES', MockLakesFeature())

    # Call the guard function directly
    assert _lake_probes_are_resolvable() is False

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


# (name, lon, lat) inside each Great Lake. These are each lake's interior
# representative point rather than an eyeballed centre, which roughly
# doubles the smallest clearance to a lake boundary (Erie 0.14 deg -> 0.34
# deg). cfeature.LAKES is cartopy's 110m dataset, where the lakes are coarse
# enough that a point 0.14 deg from an edge sits inside the vertex-spacing
# noise -- see _lake_probes_are_resolvable below.
_LAKE_PROBES = (
    ('Lake Superior', -87.6, 47.8),
    ('Lake Michigan', -87.1, 43.9),
    ('Lake Erie', -81.1, 42.2),
    ('Lake Ontario', -77.8, 43.7),
)


def _lake_probes_are_resolvable():
    """True when the installed Natural Earth data can answer this test.

    The previous guard only checked that *something* loaded, which is not
    the property the assertions rely on. cartopy fetches these shapefiles
    over the network at runtime, and the Great Lakes are narrow at 110m, so
    a different Natural Earth vintage -- or a cartopy release whose default
    points at one -- moves the boundary vertices enough to flip a probe from
    inside the lake to outside it. That produced intermittent CI failures on
    unrelated PRs, naming a different lake on each platform (issue #329).

    A probe only carries signal when the installed data puts it inside BOTH
    the land polygons (the misclassification under test) and the lakes
    polygons (what must be subtracted). When it does not, this test has
    nothing to say, and skipping beats failing a PR that changed none of it.
    """
    try:
        import cartopy.feature as cfeature
        from shapely.geometry import Point
        from shapely.ops import unary_union

        land = unary_union(list(cfeature.LAND.geometries()))
        lakes = unary_union(list(cfeature.LAKES.geometries()))
    except Exception:
        return False

    return all(
        land.contains(Point(lon, lat)) and lakes.contains(Point(lon, lat))
        for _name, lon, lat in _LAKE_PROBES
    )


@pytest.mark.skipif(
    not _lake_probes_are_resolvable(),
    reason=(
        'Installed Natural Earth data does not place the Great Lakes probes '
        'inside both the land and lakes polygons, so this assertion has no '
        'signal (see issue #329)'
    ),
)
def test_great_lakes_are_not_covered_but_real_land_is():
    """The overlay must exclude the lakes and still cover actual land."""
    from shapely.geometry import Point

    feature = plotting_2d._land_without_lakes(logger)
    assert feature is not None
    geom = next(iter(feature.geometries()))

    # Points inside each Great Lake must not be covered.
    for name, lon, lat in _LAKE_PROBES:
        assert not geom.contains(Point(lon, lat)), name

    # Real land is still covered, so coastal bleed-through stays hidden.
    assert geom.contains(Point(-82.5, 40.0)), 'inland Ohio'
    # Open water outside any lake was never part of the land polygons.
    assert not geom.contains(Point(-70.0, 39.0)), 'open Atlantic'
