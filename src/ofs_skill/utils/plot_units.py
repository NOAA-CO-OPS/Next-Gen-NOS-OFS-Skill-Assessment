"""Canonical variable -> physical-unit mapping for plot labels.

Single source of truth: every axis title, colorbar title, legend entry,
hover row and threshold annotation that names a physical quantity
(RMSE, mean bias, target error range, ...) resolves its unit here, so
units cannot drift between the plotly and matplotlib renderings of the
same figure, or between the 1D station plots and the skill maps.

Deliberately import-light (no numpy/pandas/plotly/matplotlib) so that
importing it never slows down ``--help`` on the CLI entry points, and so
it can be imported from both ``ofs_skill.visualization`` and
``ofs_skill.skill_assessment`` without creating a cycle.

Intentionally separate from ``ofs_skill.utils.file_headers``: the labels
there are written into ``.obs``/``.prd`` files on disk and are read back
by the header-detection helpers, so their exact text is part of the file
format rather than a display string.
"""
from __future__ import annotations

from logging import Logger
from typing import Any

# Canonical key space == the key space of nos_metrics._DEFAULT_THRESHOLDS
# and the name_var column of conf/error_ranges.csv, so a variable can
# never have a target-error threshold without a unit (asserted in
# tests/plot_units_test.py).
_UNITS: dict[str, str] = {
    'wl': 'meters',
    'temp': '\u00b0C',
    'salt': 'PSU',
    'cu': 'm/s',
    'cu_dir': 'degrees',
    'ice_conc': '%',
}

# Long variable names (get_skill / create_1dplot vocabulary) and 2D
# satellite variable names, mapped onto the canonical short keys.
# Water-level extrema share the water-level unit: slot 0 of
# metrics_paired_one_d.skill_extrema is the AMPLITUDE rmse (meters); the
# timing rmse (hours) is reported separately in slot 15.
_ALIASES: dict[str, str] = {
    'water_level': 'wl',
    'water_level_hw': 'wl',
    'water_level_lw': 'wl',
    'water_temperature': 'temp',
    'salinity': 'salt',
    'currents': 'cu',
    'currents_dir': 'cu_dir',
    'ice_concentration': 'ice_conc',
    'ssh': 'wl',
    'sst': 'temp',
    'sss': 'salt',
    'ssu': 'cu',
    'ssv': 'cu',
}

# Human-readable quantity names. Keyed on BOTH vocabularies because the
# unit collapses for the extrema variants but the NAME must not.
_QUANTITY_NAMES: dict[str, str] = {
    'wl': 'Water level',
    'temp': 'Water temperature',
    'salt': 'Salinity',
    'cu': 'Current speed',
    'cu_dir': 'Current direction',
    'ice_conc': 'Ice concentration',
    'water_level': 'Water level',
    'water_level_hw': 'Water level high water extrema',
    'water_level_lw': 'Water level low water extrema',
    'water_temperature': 'Water temperature',
    'salinity': 'Salinity',
    'currents': 'Current speed',
    'currents_dir': 'Current direction',
    'ice_concentration': 'Ice concentration',
}


def canonical_key(variable: str | None) -> str | None:
    """Normalise either vocabulary to a canonical short key.

    Accepts the short ``name_var`` codes ('wl', 'cu', 'cu_dir', ...) and
    the long variable names ('water_level', 'currents_dir', ...).
    Returns ``None`` for anything unmapped -- callers must never invent
    a unit for an unknown variable.
    """
    if not variable:
        return None
    key = str(variable).strip()
    if key in _UNITS:
        return key
    return _ALIASES.get(key.lower())


def resolve_variable(preferred: str | None,
                     fallback: str | None) -> str | None:
    """Pick whichever of two names for the same series is mapped here.

    The plotting modules carry two names for one series: the long
    variable name ('currents_dir') and the short ``name_var`` code
    ('cu').  Only the long name separates current direction from current
    speed, so it wins whenever this module recognises it.  An unmapped
    long name falls back to the short code, so a variable that used to
    get a unit can never lose one.

    Resolving the target-error threshold, the axis label and the unit
    through this single key is what stops a plot printing one
    variable's number in another variable's unit.
    """
    return preferred if canonical_key(preferred) else fallback


def unit(variable: str | None, logger: Logger | None = None) -> str:
    """Bare unit text, e.g. 'meters'.  Empty string if unknown."""
    key = canonical_key(variable)
    if key is None:
        if logger is not None:
            logger.warning(
                'No unit mapping for variable %r -- plot labels will omit '
                'the unit. Add it to ofs_skill.utils.plot_units._UNITS.',
                variable,
            )
        return ''
    return _UNITS[key]


def unit_suffix(variable: str | None, html: bool = False,
                logger: Logger | None = None) -> str:
    """Parenthesised unit suffix: ' (meters)', or ' (<i>meters</i>)'
    when *html*.  Empty string when the unit is unknown, so a caller can
    always concatenate blindly without rendering an empty '()'.
    """
    text = unit(variable, logger)
    if not text:
        return ''
    return f' (<i>{text}</i>)' if html else f' ({text})'


def with_unit(label: str, variable: str | None, html: bool = False,
              logger: Logger | None = None) -> str:
    """'RMSE' -> 'RMSE (meters)'."""
    return f'{label}{unit_suffix(variable, html=html, logger=logger)}'


def value_with_unit(value: Any, variable: str | None,
                    logger: Logger | None = None) -> str:
    """'0.15' -> '0.15 meters'.

    Used for threshold annotations and for plotly hover tokens, e.g.
    ``value_with_unit('%{y:.3f}', variable)``.  Falls back to the bare
    value when the unit is unknown.
    """
    return f'{value} {unit(variable, logger)}'.strip()


def quantity_label(variable: str | None, logger: Logger | None = None,
                   fallback: str = 'Unknown') -> str:
    """Human-readable quantity name, e.g. 'Water level'.

    Prefers an exact match so the high/low-water extrema keep their
    distinct names, then falls back to the canonical short key's name,
    then to *fallback*.  Callers that would rather echo an unmapped
    variable name than print 'Unknown' pass ``fallback=variable``.
    """
    if variable:
        exact = _QUANTITY_NAMES.get(str(variable).strip())
        if exact:
            return exact
        key = canonical_key(variable)
        if key is not None and key in _QUANTITY_NAMES:
            return _QUANTITY_NAMES[key]
    if logger is not None:
        logger.error('Unknown variable %r for plot labeling!', variable)
    return fallback
