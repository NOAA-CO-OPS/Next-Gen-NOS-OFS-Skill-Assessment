"""
Reusable helpers for the skill-assessment Tkinter GUI.

This module contains the pure-logic helpers and the cross-platform DateEntry
widget used by create_gui.py. This separation lets the GUI module focus on
layout and wiring, and lets the helpers be unit tested without spinning up Tk.

Key Features:
    - Cross-platform tkcalendar DateEntry wrapper with calendar-popup fixes
    - Per-OFS default datum and most-recent-cycle computation for Quick Run
    - Conf-driven datum list lookup with hardcoded fallback
    - Pure-logic validators that return error message strings (or None) so
      callers decide how to surface them (messagebox, assertion, log, etc.)

Classes:
    DateEntry: Cross-platform tkcalendar DateEntry subclass
    ToolTip: Lightweight hover tooltip for any Tk widget
    GuiParams: Typed dataclass mirroring the argparse namespace
    GuiTheme: Shared color/font/spacing palette for the GUI

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
from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class GuiTheme:
    """Shared color/font/spacing palette for the skill-assessment GUI.
    Frozen so widgets cannot accidentally mutate the live theme."""

    themecolor: str = 'gainsboro'
    textcolor: str = 'black'
    datefield_bg: str = 'darkblue'
    datefield_fg: str = 'white'

    fontfamily: str = 'Helvetica'
    labelfontsize: int = 12
    widgetfontsize: int = 12
    hintfontsize: int = 9

    padx: int = 3
    pady: int = 10
    section_padx: int = 10
    section_pady: int = 5

    anchor: str = 'e'

    @property
    def label_font(self) -> tuple:
        return (self.fontfamily, self.labelfontsize)

    @property
    def widget_font(self) -> tuple:
        return (self.fontfamily, self.widgetfontsize)

    @property
    def section_title_font(self) -> tuple:
        return (self.fontfamily, self.labelfontsize, 'bold')

    @property
    def hint_font(self) -> tuple:
        return (self.fontfamily, self.hintfontsize, 'italic')


class DateEntry(_TkDateEntry):
    """Drop-in replacement for ``tkcalendar.DateEntry`` with cross-platform
    calendar-popup fixes."""

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

    # Grace period (ms) after opening during which a transient FocusOut on
    # the calendar is ignored. Prevents the popup from immediately closing
    # when the platform briefly redirects focus while mapping the window.
    _FOCUS_GRACE_MS = 200

    def __init__(self, master=None, **kw):
        for key, value in self._CAL_COLOR_DEFAULTS.items():
            kw.setdefault(key, value)
        super().__init__(master, **kw)
        self._drop_down_time = 0
        if sys.platform == 'darwin':
            try:
                self._top_cal.overrideredirect(False)
            except (tk.TclError, AttributeError):
                pass

    def drop_down(self):
        self._drop_down_time = self.winfo_toplevel().tk.call('clock', 'milliseconds')
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
        except (tk.TclError, AttributeError):
            pass

    def _on_focus_out_cal(self, event):
        """Ignore transient FocusOut events fired right after popup opens."""
        now = self.winfo_toplevel().tk.call('clock', 'milliseconds')
        elapsed = now - self._drop_down_time
        if elapsed < self._FOCUS_GRACE_MS:
            self._calendar.focus_set()
            return
        super()._on_focus_out_cal(event)


class ToolTip:
    """Lightweight hover tooltip for any Tk widget.

    Usage::

        ToolTip(widget, 'This field sets the vertical datum.')

    The tooltip appears after a short delay when the cursor enters the
    widget and disappears on leave. No external dependencies required.
    """

    _DELAY_MS = 400
    _BG = '#ffffe0'
    _FG = 'black'

    def __init__(self, widget: tk.Widget, text: str) -> None:
        self._widget = widget
        self._text = text
        self._tip_window: tk.Toplevel | None = None
        self._after_id: str | None = None
        widget.bind('<Enter>', self._schedule, add='+')
        widget.bind('<Leave>', self._hide, add='+')
        widget.bind('<ButtonPress>', self._hide, add='+')

    def _schedule(self, _event=None):
        self._cancel()
        self._after_id = self._widget.after(self._DELAY_MS, self._show)

    def _show(self):
        if self._tip_window is not None:
            return
        tw = tk.Toplevel(self._widget)
        tw.wm_overrideredirect(True)
        try:
            tw.wm_attributes('-topmost', True)
        except tk.TclError:
            pass
        # macOS Aqua: use the undocumented "help" window style so the
        # tooltip floats over other windows and doesn't steal focus.
        # Without this, overrideredirect Toplevels often render invisibly.
        if sys.platform == 'darwin':
            try:
                tw.tk.call(
                    '::tk::unsupported::MacWindowStyle',
                    'style', tw._w, 'help', 'noActivates',
                )
            except tk.TclError:
                pass
        label = tk.Label(
            tw, text=self._text, justify='left',
            background=self._BG, foreground=self._FG,
            relief='solid', borderwidth=1,
            font=('Helvetica', 10),
            wraplength=300,
        )
        label.pack(ipadx=4, ipady=2)
        x = self._widget.winfo_rootx() + 20
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
        tw.wm_geometry(f'+{x}+{y}')
        tw.update_idletasks()
        tw.lift()
        self._tip_window = tw

    def _hide(self, _event=None):
        self._cancel()
        if self._tip_window is not None:
            self._tip_window.destroy()
            self._tip_window = None

    def _cancel(self):
        if self._after_id is not None:
            self._widget.after_cancel(self._after_id)
            self._after_id = None


def quick_run_datum(ofs: str) -> str:
    """Default vertical datum per OFS family: IGLD85 (Great Lakes),
    NAVD88 (STOFS), else MLLW (tidal coastal)."""
    if ofs in GREAT_LAKES_OFS:
        return 'IGLD85'
    if ofs in STOFS_OFS:
        return 'NAVD88'
    return 'MLLW'


def compute_recent_cycle(ofs: str, now: datetime | None = None):
    """Return ``(start_iso, forecast_hr)`` for the most recent cycle
    available (assumes a 2h NODD delivery delay). ``now`` is overridable
    for deterministic testing; defaults to ``datetime.now(timezone.utc)``."""
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
    """Format date + hour into the CLI ISO string ``'YYYY-MM-DDTHH:00:00Z'``;
    raises ``TypeError`` if ``date_obj`` is not a ``datetime.date``."""
    if isinstance(date_obj, date_type):
        return f"{date_obj.strftime('%Y-%m-%d')}T{int(hour):02d}:00:00Z"
    raise TypeError(f'Expected date object, got {type(date_obj)}: {date_obj}')


def build_utc_datetime(date_obj, hour) -> datetime | None:
    """Build a UTC-aware ``datetime`` from date + hour, or ``None`` if
    ``date_obj`` is falsy or ``hour`` is not int-coercible."""
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
    """Error message if ``start_dt`` is in the future (UTC); ``now`` is
    overridable for deterministic testing."""
    if start_dt is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    if start_dt > now:
        return 'Start date/hour cannot be in the future (UTC).'
    return None


def validate_horizon_requires_stations(horizon_skill: bool,
                                       filetype: str) -> str | None:
    """Error message if ``Horizon_Skill=True`` is paired with a non-stations
    file type (horizon skill is only implemented for station outputs)."""
    if horizon_skill and filetype != 'stations':
        return (
            '"Assess all forecast horizons?" is only supported with '
            'the "Station" model output file type. Either change the '
            'file type to Station, or set "Assess all forecast '
            'horizons?" to No.'
        )
    return None


@dataclass
class GuiParams:
    """Typed GUI-to-CLI param schema; fields mirror argparse ``dest`` names
    in ``create_1dplot.py`` as a drop-in for ``argparse.Namespace``."""

    OFS: str | None = None
    Path: str | None = None
    StartDate_full: str | None = None
    EndDate_full: str | None = None
    Whichcasts: list[str] = field(
        default_factory=lambda: ['nowcast', 'forecast_b']
    )
    Datum: str = 'MLLW'
    FileType: str = 'stations'
    Forecast_Hr: str = 'now'
    Station_Owner: list[str] = field(
        default_factory=lambda: ['co-ops', 'ndbc', 'usgs', 'chs']
    )
    Horizon_Skill: bool = False
    Var_Selection: list[str] = field(
        default_factory=lambda: [
            'water_level', 'water_temperature', 'salinity', 'currents'
        ]
    )
    Currents_Bins_Csv: str | None = None
    Disable_Model_File_Check: bool = True
    config: str | None = None
