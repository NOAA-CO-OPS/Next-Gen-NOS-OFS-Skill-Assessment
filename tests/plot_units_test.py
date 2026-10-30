"""Guards for the canonical plot-unit mapping (issue #217)."""
from __future__ import annotations

import logging

import pytest

from ofs_skill.skill_assessment import nos_metrics
from ofs_skill.utils import plot_units
from ofs_skill.visualization import plot_forecast_hours

# Every long variable name get_skill._skill_for_variable can emit.
_GET_SKILL_VARIABLES = [
    'currents',
    'currents_dir',
    'water_level',
    'water_level_hw',
    'water_level_lw',
    'water_temperature',
    'salinity',
]


def test_key_space_matches_error_thresholds():
    """Every variable with a target-error threshold has a unit, and vice
    versa -- so a new variable cannot be half-registered."""
    assert set(plot_units._UNITS) == set(nos_metrics._DEFAULT_THRESHOLDS)


@pytest.mark.parametrize('variable,expected', [
    ('wl', 'wl'), ('water_level', 'wl'),
    ('water_level_hw', 'wl'), ('water_level_lw', 'wl'),
    ('water_temperature', 'temp'), ('salinity', 'salt'),
    ('currents', 'cu'), ('currents_dir', 'cu_dir'),
    ('sst', 'temp'), ('ssh', 'wl'),
])
def test_both_vocabularies_normalize(variable, expected):
    assert plot_units.canonical_key(variable) == expected


@pytest.mark.parametrize('variable', _GET_SKILL_VARIABLES)
def test_every_get_skill_variable_has_a_unit(variable):
    """The _ALIASES table is where every real caller enters, so the
    threshold-key invariant above is not enough on its own."""
    assert plot_units.unit(variable) != ''
    assert plot_units.unit_suffix(variable).startswith(' (')


@pytest.mark.parametrize('variable', _GET_SKILL_VARIABLES)
def test_every_get_skill_variable_resolves_a_threshold_key(variable):
    """The unit and the target error must be looked up under one key."""
    key = plot_units.canonical_key(variable)
    assert key in nos_metrics._DEFAULT_THRESHOLDS


def test_current_direction_is_degrees_not_speed():
    assert plot_units.unit('currents') == 'm/s'
    assert plot_units.unit('cu') == 'm/s'
    assert plot_units.unit('currents_dir') == 'degrees'
    assert plot_units.unit('cu_dir') == 'degrees'


def test_unknown_variable_yields_no_unit_not_the_variable_name():
    assert plot_units.unit('some_variable') == ''
    assert plot_units.unit_suffix('some_variable') == ''
    assert plot_units.with_unit('RMSE', 'some_variable') == 'RMSE'
    assert plot_units.value_with_unit(0.15, 'some_variable') == '0.15'


def test_quantity_label_fallback_can_echo_the_input():
    assert plot_units.quantity_label('some_variable') == 'Unknown'
    assert plot_units.quantity_label(
        'some_variable', fallback='some_variable') == 'some_variable'
    assert plot_units.quantity_label('water_level_hw') == \
        'Water level high water extrema'


def test_plain_labels_carry_no_html():
    for variable in plot_units._UNITS:
        assert '<' not in plot_units.unit_suffix(variable)
        assert '<' not in plot_units.with_unit('RMSE', variable)
        assert '<' not in plot_units.value_with_unit(0.15, variable)


def test_html_suffix_survives_the_static_plot_sanitizer():
    """make_static_plots.bar_plots de-HTMLs plot_forecast_hours' axis
    title; the two must agree on the tag spelling."""
    for variable in plot_units._UNITS:
        html = plot_units.unit_suffix(variable, html=True)
        stripped = html.replace('</i>', '').replace('<i>', '')
        assert stripped == plot_units.unit_suffix(variable)
        assert '<' not in stripped


def test_get_yaxis_label_no_longer_raises_on_unmapped_variable():
    """Regression: the old units if/elif chain had no else branch."""
    logger = logging.getLogger('plot_units_test')
    assert plot_forecast_hours.get_yaxis_label('wl', logger) == (
        'Water level', ' (<i>meters</i>)')
    assert plot_forecast_hours.get_yaxis_label('ice_conc', logger) == (
        'Ice concentration', ' (<i>%</i>)')
    assert plot_forecast_hours.get_yaxis_label('nope', logger) == (
        'Unknown', '')


def test_rmse_axis_title_keeps_the_literal_token_for_bar_plots():
    """make_static_plots.bar_plots dispatches on `'RMSE' in ytitle`."""
    logger = logging.getLogger('plot_units_test')
    label, units = plot_forecast_hours.get_yaxis_label('wl', logger)
    ytitle = label + '<br>RMSE or error' + units
    sanitized = (ytitle.replace('<br>', '\n').replace('</i>', '')
                 .replace('<i>', '').replace(' or error', ''))
    assert 'RMSE' in sanitized
    assert '<' not in sanitized
    assert sanitized == 'Water level\nRMSE (meters)'


def test_resolve_variable_prefers_the_long_name_when_it_is_mapped():
    """The long variable name is the only one that separates current
    direction from current speed."""
    assert plot_units.resolve_variable('currents_dir', 'cu') == 'currents_dir'
    assert plot_units.resolve_variable('water_level', 'wl') == 'water_level'


def test_resolve_variable_falls_back_when_the_long_name_is_unmapped():
    """An unmapped long name must never strip the unit off a plot that
    the short code can still resolve."""
    assert plot_units.resolve_variable('not_a_variable', 'wl') == 'wl'
    assert plot_units.resolve_variable(None, 'cu') == 'cu'
    assert plot_units.resolve_variable('', 'temp') == 'temp'


def test_resolve_variable_keeps_threshold_and_unit_on_one_key():
    """canonical_key of the resolved name is the error_ranges.csv row
    whose unit gets printed beside the number."""
    for preferred, fallback, expected in [
        ('currents_dir', 'cu', 'cu_dir'),
        ('currents', 'cu', 'cu'),
        ('water_level', 'wl', 'wl'),
        ('water_temperature', 'temp', 'temp'),
        ('salinity', 'salt', 'salt'),
        ('not_a_variable', 'wl', 'wl'),
    ]:
        resolved = plot_units.resolve_variable(preferred, fallback)
        assert plot_units.canonical_key(resolved) == expected
