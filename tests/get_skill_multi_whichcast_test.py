"""
Integration tests for the multi-whichcast bug fix in get_skill.

These tests exercise the real cache-consumption path in
`ofs_ctlfile_extract` (the first thing `_skill_for_variable` calls) and
mirror the caller pattern in `_skill_for_variable` itself. They prove that
after a whichcast mutation on `prop`, the next call to get_node_ofs
receives `model_dataset=None` so that a fresh intake_model runs against
the new file list — fixing the reported bug where forecast_b .prd files
were written from nowcast data.
"""

from unittest.mock import MagicMock, patch

import pytest

from ofs_skill.skill_assessment.get_skill import (
    _get_valid_cached_model,
    _set_cached_model,
    ofs_ctlfile_extract,
)


class MockLogger:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass
    def debug(self, *a, **k): pass


class MockProps:
    def __init__(self, whichcast='nowcast', ofsfiletype='stations',
                 forecast_hr=None,
                 start_date_full='2026-03-28T00:00:00Z',
                 end_date_full='2026-03-28T23:00:00Z'):
        self.ofs = 'cbofs'
        self.whichcast = whichcast
        self.ofsfiletype = ofsfiletype
        self.forecast_hr = forecast_hr
        self.start_date_full = start_date_full
        self.end_date_full = end_date_full
        self.control_files_path = '/fake/control_files'


@pytest.fixture
def logger():
    return MockLogger()


def _sentinel(tag):
    return MagicMock(name=f'dataset-{tag}', spec=['__class__'])


# ---------------------------------------------------------------------------
# Direct caller-pattern tests: exercise the exact sequence _skill_for_variable
# uses, without needing to run the full get_skill orchestrator.
# ---------------------------------------------------------------------------

def test_nowcast_then_forecast_b_re_invokes_intake(logger):
    """The reported bug: switching whichcast must not pass a stale cache."""
    prop = MockProps(whichcast='nowcast')

    # Load 1: nowcast
    cached = _get_valid_cached_model(prop)
    assert cached is None, 'no cache on first call'
    nowcast_ds = _sentinel('nowcast')
    _set_cached_model(prop, nowcast_ds)

    # Caller mutates whichcast (as create_1dplot.py:119 does)
    prop.whichcast = 'forecast_b'

    # Load 2: forecast_b — caller queries cache again
    cached = _get_valid_cached_model(prop)
    assert cached is None, (
        'stale nowcast cache must NOT be returned for forecast_b — '
        'this is the bug'
    )

    # Simulate the forecast_b intake returning a new dataset
    forecast_b_ds = _sentinel('forecast_b')
    _set_cached_model(prop, forecast_b_ds)
    assert _get_valid_cached_model(prop) is forecast_b_ds


def test_same_whichcast_reuses_cache(logger):
    """Guard against over-invalidation — same whichcast must hit."""
    prop = MockProps(whichcast='nowcast')
    ds = _sentinel('nowcast')
    _set_cached_model(prop, ds)

    # Second lookup under the same state — must reuse.
    assert _get_valid_cached_model(prop) is ds


def test_forecast_a_cycle_change_invalidates(logger):
    """forecast_a with different forecast_hr must re-load."""
    prop = MockProps(whichcast='forecast_a', forecast_hr='06z')
    _set_cached_model(prop, _sentinel('fa-06z'))
    prop.forecast_hr = '12z'
    assert _get_valid_cached_model(prop) is None


def test_date_range_change_invalidates(logger):
    """A different run date range must re-load even at same whichcast."""
    prop = MockProps(whichcast='nowcast')
    _set_cached_model(prop, _sentinel('march-28'))
    prop.start_date_full = '2026-03-29T00:00:00Z'
    prop.end_date_full = '2026-03-29T23:00:00Z'
    assert _get_valid_cached_model(prop) is None


# ---------------------------------------------------------------------------
# ofs_ctlfile_extract integration: real function, mocked filesystem +
# get_node_ofs. Proves the helpers land correctly in production code.
# ---------------------------------------------------------------------------

@patch('ofs_skill.skill_assessment.get_skill.os.path.getsize', return_value=0)
@patch('ofs_skill.skill_assessment.get_skill.os.path.isfile',
       return_value=False)
@patch('ofs_skill.skill_assessment.get_skill.get_node_ofs')
def test_ofs_ctlfile_extract_caches_then_reuses(
        mock_get_node_ofs, mock_isfile, mock_getsize, logger):
    """First call triggers get_node_ofs; cache is stamped; second call
    under same whichcast reuses the stamped dataset."""
    nowcast_ds = _sentinel('nowcast')
    mock_get_node_ofs.return_value = nowcast_ds

    prop = MockProps(whichcast='nowcast', ofsfiletype='stations')

    # First pass: cache is empty, caller reads with _get_valid_cached_model.
    cached = _get_valid_cached_model(prop)
    ofs_ctlfile_extract(prop, 'wl', logger, model_dataset=cached)

    assert mock_get_node_ofs.call_count == 1
    assert mock_get_node_ofs.call_args.kwargs['model_dataset'] is None
    assert _get_valid_cached_model(prop) is nowcast_ds


@patch('ofs_skill.skill_assessment.get_skill.os.path.getsize', return_value=0)
@patch('ofs_skill.skill_assessment.get_skill.os.path.isfile',
       return_value=False)
@patch('ofs_skill.skill_assessment.get_skill.get_node_ofs')
def test_ofs_ctlfile_extract_rejects_stale_cache_across_whichcasts(
        mock_get_node_ofs, mock_isfile, mock_getsize, logger):
    """Direct reproducer: nowcast run stamps cache, then caller switches
    to forecast_b. The caller's _get_valid_cached_model must return None,
    so get_node_ofs is invoked fresh and writes forecast_b-sourced .prd."""
    nowcast_ds = _sentinel('nowcast')
    forecast_b_ds = _sentinel('forecast_b')
    mock_get_node_ofs.side_effect = [nowcast_ds, forecast_b_ds]

    prop = MockProps(whichcast='nowcast', ofsfiletype='stations')

    # ---- Round 1: nowcast ----
    cached = _get_valid_cached_model(prop)
    ofs_ctlfile_extract(prop, 'wl', logger, model_dataset=cached)

    # ---- Driver mutates whichcast (matches create_1dplot.py:119) ----
    prop.whichcast = 'forecast_b'

    # ---- Round 2: forecast_b ----
    cached = _get_valid_cached_model(prop)
    # This is the critical assertion: the caller receives None, NOT the
    # stale nowcast dataset. Previously this returned nowcast_ds, which
    # was forwarded into get_node_ofs and skipped intake_model, causing
    # the bug.
    assert cached is None, (
        'stale nowcast cache leaked into forecast_b — this is the bug'
    )
    ofs_ctlfile_extract(prop, 'wl', logger, model_dataset=cached)

    # get_node_ofs called twice, second call with model_dataset=None.
    assert mock_get_node_ofs.call_count == 2
    first_call_kwargs = mock_get_node_ofs.call_args_list[0].kwargs
    second_call_kwargs = mock_get_node_ofs.call_args_list[1].kwargs
    assert first_call_kwargs['model_dataset'] is None
    assert second_call_kwargs['model_dataset'] is None

    # Cache now holds the forecast_b dataset stamped with forecast_b key.
    assert _get_valid_cached_model(prop) is forecast_b_ds


@patch('ofs_skill.skill_assessment.get_skill.os.path.getsize', return_value=0)
@patch('ofs_skill.skill_assessment.get_skill.os.path.isfile',
       return_value=False)
@patch('ofs_skill.skill_assessment.get_skill.get_node_ofs')
def test_ofs_ctlfile_extract_passes_fresh_cache_within_same_whichcast(
        mock_get_node_ofs, mock_isfile, mock_getsize, logger):
    """When caller passes a cached dataset AND the key still matches,
    get_node_ofs receives it and short-circuits intake_model."""
    ds = _sentinel('nowcast')
    mock_get_node_ofs.return_value = ds

    prop = MockProps(whichcast='nowcast', ofsfiletype='stations')

    # First call populates cache.
    ofs_ctlfile_extract(prop, 'wl', logger,
                        model_dataset=_get_valid_cached_model(prop))
    # Second call: caller reads cache and forwards.
    cached = _get_valid_cached_model(prop)
    assert cached is ds
    ofs_ctlfile_extract(prop, 'salt', logger, model_dataset=cached)

    assert mock_get_node_ofs.call_count == 2
    assert mock_get_node_ofs.call_args_list[1].kwargs['model_dataset'] is ds
