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
    load_recent_paths: Load remembered paths (directory/config/CSV) from disk
    save_recent_paths: Persist remembered paths back to disk
    add_to_recent: Push a path to the front of its recent-paths list
    GuiSession: Cross-GUI shared last-used form values (persisted to disk)
    load_gui_session: Load the shared session from disk
    save_gui_session: Persist the shared session to disk
    apply_gui_session: Pre-fill common widgets from the shared session
    persist_gui_session_from_run: Update session after a successful submit

Author: TSR
Created: Extracted from create_gui.py for modularity
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
import sys
import threading
import tkinter as tk
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from tkinter import messagebox, ttk
from tkinter.font import Font
from typing import Literal, cast

from tkcalendar import DateEntry as _TkDateEntry

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours
from ofs_skill.obs_retrieval import utils

# tkinter's ``anchor`` option only accepts these compass literals; alias
# used to narrow the (str) theme value for type checkers.
_tk_anchor = Literal['nw', 'n', 'ne', 'w', 'center', 'e', 'sw', 's', 'se']

# Where the GUI persists recently-used path selections so users do not
# have to re-browse on every run. Lives under the user's home folder so
# it survives across project checkouts.
RECENT_PATHS_FILE = (
    pathlib.Path.home() / '.ofs_skill_assessment' / 'recent_paths.json'
)
SESSION_FILE = RECENT_PATHS_FILE.parent / 'gui_session.json'
_RECENT_MAX = 8

# OFS groupings used to pick sensible per-OFS defaults in Quick Run mode.
GREAT_LAKES_OFS = ('leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs')
STOFS_OFS = ('stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac')

# Datum fallback if the [datums] section of the conf cannot be read.
DEFAULT_DATUMS = (
    'MHHW', 'MHW', 'MLLW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B', 'MSL'
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
    if ofs in ('stofs_3d_pac', 'stofs_2d_glo'):
        return 'MSL'
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
        now = datetime.now(UTC)
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
            int(hour), tzinfo=UTC,
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
        now = datetime.now(UTC)
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
    # Continuation runs are a CLI-only mode for now; the fields exist so
    # a GUI launch reaches _run_pipeline with the same shape argparse
    # produces rather than tripping over a missing attribute.
    Continue_Run: bool = False
    Continue_Overlap: float = 24.0
    config: str | None = None


def load_recent_paths() -> dict[str, list[str]]:
    """Load remembered path lists from ``RECENT_PATHS_FILE``.

    Returns a dict like ``{'home_directory': [...], 'config_file': [...],
    'currents_bins_csv': [...]}``. Any read/parse error returns an empty
    dict so missing files or corrupted JSON never break the GUI.
    """
    try:
        with open(RECENT_PATHS_FILE) as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        key: [str(p) for p in value]
        for key, value in data.items()
        if isinstance(value, list)
    }


def save_recent_paths(data: dict[str, list[str]]) -> None:
    """Persist the recent-paths dict; ignore write errors silently."""
    try:
        RECENT_PATHS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(RECENT_PATHS_FILE, 'w') as fh:
            json.dump(data, fh, indent=2)
    except OSError:
        pass


def add_to_recent(category: str, path: str | None) -> dict[str, list[str]]:
    """Push ``path`` to the front of ``data[category]`` (deduped, capped).

    No-op if ``path`` is empty/None. Returns the updated dict so callers
    can refresh combobox values from a single source of truth.
    """
    data = load_recent_paths()
    if not path:
        return data
    items = [p for p in data.get(category, []) if p != path]
    items.insert(0, path)
    data[category] = items[:_RECENT_MAX]
    save_recent_paths(data)
    return data


@dataclass
class GuiSession:
    """Last-used values shared across all skill-assessment sub-GUIs."""

    Path: str = ''
    config: str = 'conf/ofs_dps.conf'
    OFS: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    start_hour: int = 0
    end_hour: int = 0
    Datum: str | None = None


def load_gui_session() -> GuiSession:
    """Load shared GUI session from ``SESSION_FILE``; empty session on error."""
    try:
        with open(SESSION_FILE) as fh:
            raw = json.load(fh)
    except (OSError, json.JSONDecodeError, TypeError):
        return GuiSession()
    if not isinstance(raw, dict):
        return GuiSession()
    return GuiSession(
        Path=str(raw.get('Path', '') or ''),
        config=str(raw.get('config', 'conf/ofs_dps.conf') or 'conf/ofs_dps.conf'),
        OFS=raw.get('OFS') or None,
        start_date=raw.get('start_date') or None,
        end_date=raw.get('end_date') or None,
        start_hour=int(raw.get('start_hour', 0) or 0),
        end_hour=int(raw.get('end_hour', 0) or 0),
        Datum=raw.get('Datum') or None,
    )


def save_gui_session(session: GuiSession) -> None:
    """Persist ``session`` to ``SESSION_FILE``; ignore write errors."""
    try:
        SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SESSION_FILE, 'w') as fh:
            json.dump(session.__dict__, fh, indent=2)
    except OSError:
        pass


def _split_iso_datetime(iso: str | None) -> tuple[str | None, int | None]:
    """Split ``YYYY-MM-DDThh:mm:ssZ`` into date and hour."""
    if not iso:
        return None, None
    try:
        dt = datetime.strptime(iso, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None, None
    return dt.strftime('%Y-%m-%d'), dt.hour


def persist_gui_session_from_run(
    *,
    Path: str | None = None,
    config: str | None = None,
    OFS: str | None = None,
    StartDate_full: str | None = None,
    EndDate_full: str | None = None,
    Datum: str | None = None,
) -> GuiSession:
    """Merge successful-run values into the shared session and save."""
    session = load_gui_session()
    if Path:
        session.Path = Path
    if config:
        session.config = config
    if OFS:
        session.OFS = OFS
    if Datum:
        session.Datum = Datum
    start_date, start_hour = _split_iso_datetime(StartDate_full)
    end_date, end_hour = _split_iso_datetime(EndDate_full)
    if start_date:
        session.start_date = start_date
    if start_hour is not None:
        session.start_hour = start_hour
    if end_date:
        session.end_date = end_date
    if end_hour is not None:
        session.end_hour = end_hour
    save_gui_session(session)
    return session


def _set_date_entry(entry: DateEntry, date_str: str | None) -> None:
    if not date_str:
        return
    try:
        entry.set_date(datetime.strptime(date_str, '%Y-%m-%d').date())
    except (ValueError, tk.TclError):
        pass


def apply_gui_session(
    session: GuiSession | None = None,
    *,
    directory_var: tk.StringVar | None = None,
    config_var: tk.StringVar | None = None,
    ofs_var: tk.StringVar | None = None,
    ofs_choices: tuple[str, ...] | None = None,
    ofs_placeholder: str | None = None,
    start_entry: DateEntry | None = None,
    end_entry: DateEntry | None = None,
    s_hour_var: tk.IntVar | None = None,
    e_hour_var: tk.IntVar | None = None,
    datum_var: tk.StringVar | None = None,
    datum_choices: tuple[str, ...] | None = None,
    datum_placeholder: str | None = None,
) -> GuiSession:
    """Pre-fill common widgets from the shared session."""
    session = session or load_gui_session()
    if directory_var and session.Path:
        directory_var.set(session.Path)
    if config_var and session.config:
        config_var.set(session.config)
    if ofs_var and session.OFS:
        valid = ofs_choices is None or session.OFS in ofs_choices
        if valid and (not ofs_placeholder or session.OFS != ofs_placeholder):
            ofs_var.set(session.OFS)
    if start_entry:
        _set_date_entry(start_entry, session.start_date)
    if end_entry:
        _set_date_entry(end_entry, session.end_date)
    if s_hour_var is not None:
        s_hour_var.set(session.start_hour)
    if e_hour_var is not None:
        e_hour_var.set(session.end_hour)
    if datum_var and session.Datum:
        valid = datum_choices is None or session.Datum in datum_choices
        if valid and (not datum_placeholder or session.Datum != datum_placeholder):
            datum_var.set(session.Datum)
    return session


# ---------------------------------------------------------------------------
# Shared Tkinter layout / validation / launch helpers (all sub-GUIs)
# ---------------------------------------------------------------------------

_ERROR_TTK_CLASSES = ('TCombobox', 'TSpinbox', 'TEntry', 'TLabel')
_ERROR_BG = '#fff3f3'
_ERROR_BORDER = '#d93025'
_ERROR_LABEL_BG = '#ffd6d6'


@dataclass
class ScrollableForm:
    """Container + canvas + inner frame for a scrollable GUI form."""

    container: ttk.Frame
    canvas: tk.Canvas
    frame: ttk.Frame


def apply_window_icon(root: tk.Tk, log: logging.Logger | None = None) -> None:
    """Set the NOAA logo on ``root`` when the conf path is available."""
    log = log or logging.getLogger(__name__)
    try:
        dir_params = utils.Utils().read_config_section('directories', log)
        iconpath = os.path.join(
            dir_params['home'], 'readme_images', 'noaa_logo.png'
        )
        root.iconphoto(False, tk.PhotoImage(file=iconpath))
    except (KeyError, tk.TclError, OSError):
        log.info('GUI logo not found; defaulting to tkinter logo.')


def configure_gui_styles(root: tk.Tk, theme: GuiTheme) -> ttk.Style:
    """Apply the shared clam theme, widget fonts, and error-state styles."""
    style = ttk.Style(root)
    style.theme_use('clam')
    ff = theme.fontfamily
    wf = theme.widgetfontsize
    tc = theme.themecolor
    fg = theme.textcolor
    style.configure('TButton', background=tc, foreground=fg, font=(ff, wf))
    style.configure('TCheckbutton', background=tc, foreground=fg, font=(ff, wf))
    style.configure('TRadiobutton', background=tc, foreground=fg, font=(ff, wf))
    style.configure('TFrame', background=tc)
    style.configure('TLabel', background=tc, foreground=fg)
    style.configure('TLabelframe', background=tc)
    style.configure(
        'TLabelframe.Label', background=tc, foreground=fg,
        font=theme.section_title_font,
    )
    style.configure(
        'Error.TCombobox', fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER,
    )
    style.map('Error.TCombobox', fieldbackground=[('readonly', _ERROR_BG)])
    style.configure(
        'Error.TSpinbox', fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER,
    )
    style.configure(
        'Error.TEntry', fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER,
    )
    style.configure('Error.TLabel', background=_ERROR_LABEL_BG)
    root.option_add('*TCombobox*Listbox*Font', Font(family=ff, size=wf))
    return style


def setup_scrollable_form(root: tk.Tk, themecolor: str) -> ScrollableForm:
    """Build a vertical scroll area that fills ``root``."""
    container = ttk.Frame(root)
    container.pack(fill='both', expand=True)

    canvas = tk.Canvas(container, bg=themecolor, highlightthickness=0)
    scrollbar = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)
    frame = ttk.Frame(canvas)
    frame.bind(
        '<Configure>',
        lambda e: canvas.configure(scrollregion=canvas.bbox('all')),
    )
    canvas_win = canvas.create_window((0, 0), window=frame, anchor='nw')
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side='left', fill='both', expand=True)
    scrollbar.pack(side='right', fill='y')

    def _on_canvas_resize(event):
        canvas.itemconfigure(canvas_win, width=event.width)

    canvas.bind('<Configure>', _on_canvas_resize)

    def _on_mousewheel(event):
        widget = event.widget
        while widget is not None and widget is not canvas:
            widget = getattr(widget, 'master', None)
        if widget is None:
            return
        if event.num == 4:
            delta = -1
        elif event.num == 5:
            delta = 1
        elif sys.platform == 'darwin':
            delta = -int(event.delta)
        else:
            delta = -int(event.delta / 120)
        if delta:
            canvas.yview_scroll(delta, 'units')

    root.bind_all('<MouseWheel>', _on_mousewheel)
    root.bind_all('<Button-4>', _on_mousewheel)
    root.bind_all('<Button-5>', _on_mousewheel)
    frame.columnconfigure(0, weight=1)
    return ScrollableForm(container=container, canvas=canvas, frame=frame)


def form_section(parent: tk.Widget, theme: GuiTheme, title: str) -> ttk.LabelFrame:
    """Themed LabelFrame with a stretchable third column."""
    frame = ttk.LabelFrame(
        parent, text=title, padding=(theme.padx, theme.pady),
    )
    frame.columnconfigure(2, weight=1)
    return frame


def form_label(
    parent: tk.Widget,
    theme: GuiTheme,
    text: str,
    *,
    italic: bool = False,
    help_text: str | None = None,
) -> ttk.Label:
    """Standard row label; optional trailing help icon with tooltip."""
    font = theme.hint_font if italic else theme.label_font
    display = f'{text} \u24d8' if help_text else text
    lbl = ttk.Label(
        parent, text=display, font=font,
        anchor=cast('_tk_anchor', theme.anchor),
    )
    if help_text:
        lbl.config(cursor='question_arrow')
        ToolTip(lbl, help_text)
    return lbl


def collapsible_section(
    parent: tk.Widget,
    theme: GuiTheme,
    title: str,
    *,
    expanded: bool = False,
) -> tuple[ttk.Frame, ttk.Frame]:
    """Return ``(outer, body)`` where clicking the header toggles ``body``."""
    outer = ttk.Frame(parent)
    state = [bool(expanded)]
    header_lbl = ttk.Label(
        outer, text='', cursor='hand2', font=theme.section_title_font,
    )
    header_lbl.pack(fill='x', anchor='w', padx=theme.padx, pady=(theme.pady, 4))
    body = ttk.Frame(outer, padding=(theme.padx, theme.pady))

    def _apply():
        prefix = '\u25bc' if state[0] else '\u25b6'
        header_lbl.configure(text=f'{prefix}  {title}')
        if state[0]:
            body.pack(fill='x', expand=True)
        else:
            body.pack_forget()

    def _toggle(_event=None):
        state[0] = not state[0]
        _apply()

    header_lbl.bind('<Button-1>', _toggle)
    _apply()
    return outer, body


class GuiValidation:
    """Field highlighting and live Time Range validation."""

    def __init__(self) -> None:
        self._invalid_widgets: list[tk.Widget] = []
        self._dates_touched = False

    def mark_invalid(self, *widgets: tk.Widget | None) -> None:
        for w in widgets:
            if w is None or w in self._invalid_widgets:
                continue
            try:
                cls = w.winfo_class()
                if cls in _ERROR_TTK_CLASSES:
                    w.configure(style=f'Error.{cls}')  # type: ignore[call-arg]
                    self._invalid_widgets.append(w)
            except tk.TclError:
                pass

    def clear_invalid(self) -> None:
        while self._invalid_widgets:
            w = self._invalid_widgets.pop()
            try:
                cls = w.winfo_class()
                if cls in _ERROR_TTK_CLASSES:
                    w.configure(style=cls)  # type: ignore[call-arg]
            except tk.TclError:
                pass

    def reset_dates_touched(self) -> None:
        self._dates_touched = False

    def wire_live_dates(
        self,
        start_entry: DateEntry,
        end_entry: DateEntry,
        *,
        s_hour_var: tk.IntVar | None = None,
        e_hour_var: tk.IntVar | None = None,
        s_hour_spin: tk.Widget | None = None,
        e_hour_spin: tk.Widget | None = None,
        default_hour: int = 0,
    ) -> None:
        """Highlight invalid date ranges as the user edits Time Range fields."""
        date_widgets: set[tk.Widget] = {start_entry, end_entry}
        if s_hour_spin is not None:
            date_widgets.add(s_hour_spin)
        if e_hour_spin is not None:
            date_widgets.add(e_hour_spin)

        def _touch(*_args):
            self._dates_touched = True
            _live()

        def _live():
            desired: set[tk.Widget] = set()
            if self._dates_touched:
                sh = s_hour_var.get() if s_hour_var is not None else default_hour
                eh = e_hour_var.get() if e_hour_var is not None else default_hour
                try:
                    start_dt = build_utc_datetime(start_entry.get_date(), sh)
                    end_dt = build_utc_datetime(end_entry.get_date(), eh)
                except (ValueError, AttributeError, tk.TclError):
                    start_dt = end_dt = None
                if start_dt is not None and end_dt is not None:
                    if validate_date_order(start_dt, end_dt) is not None:
                        desired.update(date_widgets)
                    elif validate_start_not_future(start_dt) is not None:
                        desired.update(
                            {start_entry, end_entry}
                            if s_hour_spin is None
                            else {start_entry, s_hour_spin}
                        )
            current = set(self._invalid_widgets) & date_widgets
            for w in current - desired:
                try:
                    cls = w.winfo_class()
                    if cls in _ERROR_TTK_CLASSES:
                        w.configure(style=cls)
                except tk.TclError:
                    pass
                self._invalid_widgets.remove(w)
            new_invalid = desired - current
            if new_invalid:
                self.mark_invalid(*new_invalid)

        if s_hour_var is not None:
            s_hour_var.trace_add('write', _touch)
        if e_hour_var is not None:
            e_hour_var.trace_add('write', _touch)
        for de in (start_entry, end_entry):
            de.bind('<<DateEntrySelected>>', _touch)
            de.bind('<FocusOut>', _touch, add='+')


def show_summary_confirmation(
    root: tk.Tk,
    theme: GuiTheme,
    *,
    window_title: str,
    heading: str,
    rows: Sequence[tuple[str, str]],
) -> bool:
    """Modal preview of run parameters; True when the user clicks Launch."""
    confirmed = [False]
    win = tk.Toplevel(root)
    win.title(window_title)
    win.transient(root)
    win.configure(bg=theme.themecolor)
    win.resizable(False, False)

    ttk.Label(win, text=heading, font=theme.section_title_font).pack(
        padx=20, pady=(15, 8), anchor='w',
    )
    table = ttk.Frame(win)
    table.pack(padx=20, pady=(0, 10), anchor='w')
    bold_font = (*theme.widget_font, 'bold')
    for i, (key, value) in enumerate(rows):
        ttk.Label(table, text=f'{key}:', font=bold_font).grid(
            row=i, column=0, sticky='w', padx=(0, 12), pady=1,
        )
        ttk.Label(table, text=str(value), font=theme.widget_font).grid(
            row=i, column=1, sticky='w', pady=1,
        )

    btn_row = ttk.Frame(win)
    btn_row.pack(padx=20, pady=(0, 15), fill='x')

    def _cancel():
        confirmed[0] = False
        win.destroy()

    def _launch():
        confirmed[0] = True
        win.destroy()

    ttk.Button(btn_row, text='Cancel', command=_cancel).pack(
        side='right', padx=(8, 0),
    )
    launch_btn = ttk.Button(btn_row, text='Launch', command=_launch)
    launch_btn.pack(side='right')
    launch_btn.focus_set()

    win.protocol('WM_DELETE_WINDOW', _cancel)
    win.bind('<Escape>', lambda _e: _cancel())
    win.bind('<Return>', lambda _e: _launch())

    win.update_idletasks()
    x = root.winfo_rootx() + (root.winfo_width() - win.winfo_width()) // 2
    y = root.winfo_rooty() + (root.winfo_height() - win.winfo_height()) // 2
    win.geometry(f'+{max(0, x)}+{max(0, y)}')
    win.lift()
    win.attributes('-topmost', True)
    win.after(100, lambda: win.attributes('-topmost', False))
    win.grab_set()
    win.wait_window()
    return confirmed[0]


def create_action_buttons(
    parent: tk.Widget,
    *,
    reset_command: Callable[[], None],
    submit_command: Callable[[], None],
    submit_text: str,
) -> ttk.Frame:
    """Bottom row: Reset to defaults (left) and primary submit (right)."""
    row = ttk.Frame(parent)
    ttk.Button(row, text='Reset to defaults', command=reset_command).pack(
        side='left', padx=(0, 20),
    )
    ttk.Button(row, text=submit_text, command=submit_command).pack(side='left')
    return row


def launch_with_progress(
    root: tk.Tk,
    scrollable_frame: tk.Widget,
    theme: GuiTheme,
    *,
    runner: Callable[[object], None] | None,
    params: object,
    progress_title: str,
    runner_exception: list[BaseException | None],
    before_run: Callable[[], None] | None = None,
    progress_hint: str = 'Please keep this window open until the run finishes.',
    destroy_on_complete: bool = True,
) -> None:
    """Persist paths, show progress overlay, and run ``runner`` in a thread."""
    if before_run is not None:
        before_run()
    if runner is None:
        if destroy_on_complete:
            root.destroy()
        return

    for child in scrollable_frame.winfo_children():
        child.grid_remove()

    overlay = ttk.Frame(scrollable_frame, padding=(40, 60))
    overlay.grid(row=0, column=0, columnspan=4, sticky='nsew')
    scrollable_frame.grid_columnconfigure(0, weight=1)

    ttk.Label(
        overlay, text=progress_title,
        font=theme.section_title_font, anchor='center',
    ).pack(pady=(0, 15), fill='x')
    ttk.Label(
        overlay, text=progress_hint,
        font=theme.hint_font, anchor='center',
    ).pack(pady=(0, 20), fill='x')

    progress = ttk.Progressbar(overlay, mode='indeterminate', length=400)
    progress.pack()
    progress.start(10)
    root.configure(cursor='watch')
    root.update_idletasks()

    def _target():
        try:
            runner(params)
        except SystemExit:
            runner_exception[0] = RuntimeError(
                'Pipeline exited with an error. Check the terminal '
                'log for details.',
            )
        except BaseException as exc:
            runner_exception[0] = exc
            traceback.print_exc()

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()

    def _poll():
        if thread.is_alive():
            root.after(200, _poll)
            return
        try:
            progress.stop()
        except tk.TclError:
            pass
        root.configure(cursor='')
        if runner_exception[0] is not None:
            messagebox.showerror('Pipeline Error', str(runner_exception[0]))
        if destroy_on_complete:
            root.destroy()

    root.after(200, _poll)


def set_date_entry_today(entry: DateEntry) -> None:
    """Set a DateEntry to today's date, ignoring Tcl errors."""
    try:
        entry.set_date(datetime.now(UTC).date())
    except (tk.TclError, ValueError):
        pass
