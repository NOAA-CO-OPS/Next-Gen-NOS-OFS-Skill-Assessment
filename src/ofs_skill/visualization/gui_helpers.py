"""
Reusable helpers for the skill-assessment Tkinter GUI.

This module contains the pure-logic helpers and the cross-platform DateEntry
widget used by create_gui.py. Keeping them here lets the GUI module focus on
layout and wiring, and lets the helpers be unit tested without spinning up Tk.

Key Features:
    - Cross-platform tkcalendar DateEntry wrapper with macOS Sonoma fixes
    - Per-OFS default datum and most-recent-cycle computation for Quick Run
    - Conf-driven datum list lookup with hardcoded fallback
    - Pure-logic validators that return error message strings (or None) so
      callers decide how to surface them (messagebox, assertion, log, etc.)

Functions:
    quick_run_datum: Pick a sensible default vertical datum per OFS family
    compute_recent_cycle: Compute most recent available forecast cycle
    read_datum_list: Load [datums] datum_list from conf with fallback
    format_date: Format a date object and hour into the CLI's ISO string
    build_utc_datetime: Combine date + hour into a UTC-aware datetime
    validate_date_order: Check that start is strictly before end
    validate_start_not_future: Check that start is not in the future (UTC)
    validate_horizon_requires_stations: Enforce Horizon_Skill + stations rule

Author: TSR
Created: Extracted from create_gui.py for modularity
"""
from __future__ import annotations

import logging
import sys
import tkinter as tk
from datetime import date as date_type
from datetime import datetime, timedelta, timezone

from tkcalendar import DateEntry as _TkDateEntry

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours
from ofs_skill.obs_retrieval import utils

# OFS groupings used to pick sensible per-OFS defaults in Quick Run mode.
GREAT_LAKES_OFS = ('leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs')
STOFS_OFS = ('stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac')

# Datum fallback if the [datums] section of the conf cannot be read.
DEFAULT_DATUMS = (
    'MHHW', 'MHW', 'MLLW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B'
)


class DateEntry(_TkDateEntry):
    """Drop-in replacement for ``tkcalendar.DateEntry`` with cross-platform
    calendar-popup fixes (macOS Sonoma+ rendering, focus, and color
    inheritance)."""

    _CAL_COLOR_DEFAULTS = {
        'normalbackground':     'white',
        'normalforeground':     'black',
        'selectbackground':     '#1a73e8',
        'selectforeground':     'white',
        'weekendbackground':    '#f0f0f0',
        'weekendforeground':    'black',
        'headersbackground':    '#e0e0e0',
        'headersforeground':    'black',
        'othermonthbackground': '#fafafa',
        'othermonthforeground': 'gray50',
        'bordercolor':          'gray60',
    }

    def __init__(self, master=None, **kw):
        for key, value in self._CAL_COLOR_DEFAULTS.items():
            kw.setdefault(key, value)
        super().__init__(master, **kw)
        if sys.platform == 'darwin':
            try:
                self._top_cal.overrideredirect(False)
            except (tk.TclError, AttributeError):
                pass

    def drop_down(self):
        super().drop_down()
        try:
            top = self._top_cal
            if not top.winfo_ismapped():
                return
            top.update_idletasks()
            top.update()
            top.lift()
            if sys.platform != 'darwin':
                top.attributes('-topmost', True)
                top.focus_force()
        except (tk.TclError, AttributeError):
            pass


def quick_run_datum(ofs: str) -> str:
    """Return a sensible default vertical datum for Quick Run mode.

    Great Lakes OFSes use IGLD85, the global/regional STOFS systems
    don't have a uniform tidal datum so fall back to NAVD88, and tidal
    coastal OFSes use MLLW.
    """
    if ofs in GREAT_LAKES_OFS:
        return 'IGLD85'
    if ofs in STOFS_OFS:
        return 'NAVD88'
    return 'MLLW'


def compute_recent_cycle(ofs: str, now: datetime | None = None):
    """Compute the most recent forecast cycle that should be available.

    Parameters
    ----------
    ofs : str
        OFS identifier (e.g. ``'cbofs'``).
    now : datetime, optional
        UTC-aware "current" time. Defaults to ``datetime.now(timezone.utc)``.
        Exposed for deterministic unit testing.

    Returns
    -------
    (start_iso, forecast_hr) : tuple of str
        ``start_iso`` is YYYY-MM-DDTHH:MM:SSZ at the chosen cycle, and
        ``forecast_hr`` is the cycle as ``'06z'`` etc. Allows a 2h delay
        for file arrival on NODD before considering a cycle "available".
    """
    _, fcstcycles = get_fcst_hours(ofs)
    cycles = sorted(int(c) for c in fcstcycles)
    if now is None:
        now = datetime.now(timezone.utc)
    now_utc = now.replace(minute=0, second=0, microsecond=0)
    cutoff = now_utc - timedelta(hours=2)
    today = now_utc.replace(hour=0)
    chosen = None
    for offset_days in (0, 1):
        day = today - timedelta(days=offset_days)
        for hr in reversed(cycles):
            cyc_dt = day.replace(hour=hr)
            if cyc_dt <= cutoff:
                chosen = cyc_dt
                break
        if chosen is not None:
            break
    if chosen is None:
        chosen = today.replace(hour=cycles[-1]) - timedelta(days=1)
    return (
        chosen.strftime('%Y-%m-%dT%H:%M:%SZ'),
        f'{chosen.hour:02d}z',
    )


def read_datum_list():
    """Read ``[datums] datum_list`` from the active conf, falling back
    to ``DEFAULT_DATUMS`` if the section is missing or unreadable."""
    log = logging.getLogger(__name__)
    try:
        section = utils.Utils(None).read_config_section('datums', log)
        raw = section.get('datum_list')
        if raw:
            return tuple(raw.split())
    except (KeyError, AttributeError, OSError):
        log.warning(
            'Could not read [datums] from conf; falling back to defaults.'
        )
    return DEFAULT_DATUMS


def format_date(date_obj, hour) -> str:
    """Format a date object and integer-ish hour into the ISO string the
    downstream CLI expects, e.g. ``'2025-11-12T06:00:00Z'``.

    Raises ``TypeError`` if ``date_obj`` is not a ``datetime.date``.
    """
    if isinstance(date_obj, date_type):
        return f"{date_obj.strftime('%Y-%m-%d')}T{int(hour):02d}:00:00Z"
    raise TypeError(f'Expected date object, got {type(date_obj)}: {date_obj}')


def build_utc_datetime(date_obj, hour) -> datetime | None:
    """Build a UTC-aware ``datetime`` from a ``date`` and integer-ish hour.

    Returns ``None`` if ``date_obj`` is falsy or if ``hour`` cannot be
    coerced to an integer. Used to normalise the two ``DateEntry`` +
    ``Scale`` widget pairs in the GUI before comparison.
    """
    if not date_obj:
        return None
    try:
        return datetime(
            date_obj.year, date_obj.month, date_obj.day,
            int(hour), tzinfo=timezone.utc,
        )
    except (TypeError, ValueError):
        return None


def validate_date_order(start_dt: datetime | None,
                        end_dt: datetime | None) -> str | None:
    """Return an error message if start is not strictly before end."""
    if start_dt is not None and end_dt is not None and start_dt >= end_dt:
        return 'Start date/hour must be before end date/hour.'
    return None


def validate_start_not_future(start_dt: datetime | None,
                              now: datetime | None = None
                              ) -> str | None:
    """Return an error message if ``start_dt`` is in the future (UTC).

    ``now`` defaults to ``datetime.now(timezone.utc)``; overridable for
    deterministic testing.
    """
    if start_dt is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    if start_dt > now:
        return 'Start date/hour cannot be in the future (UTC).'
    return None


def validate_horizon_requires_stations(horizon_skill: bool,
                                       filetype: str) -> str | None:
    """Return an error message if Horizon_Skill=True is combined with a
    non-stations file type. Horizon skill is only implemented for the
    station model output files."""
    if horizon_skill and filetype != 'stations':
        return (
            '"Assess all forecast horizons?" is only supported with '
            'the "Station" model output file type. Either change the '
            'file type to Station, or set "Assess all forecast '
            'horizons?" to No.'
        )
    return None
