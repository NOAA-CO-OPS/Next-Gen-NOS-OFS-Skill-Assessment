"""
Created on Wed Nov 12 08:39:35 2025

@author: PWL
"""
import logging
import os
import sys
import tkinter as tk
from datetime import datetime, timedelta, timezone
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font
from types import SimpleNamespace

from tkcalendar import DateEntry as _TkDateEntry

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils

# UI sentinel strings — used in multiple places, must match exactly.
_OFS_PLACEHOLDER = 'Select an OFS...'
_OFS_FIRST_PLACEHOLDER = 'Select an OFS first...'
_DATUM_PLACEHOLDER = 'Select a datum...'
_MOST_RECENT_LABEL = 'Most recent cycle available'
_WINDOW_GEOMETRY = '850x500'

# Datum fallback if conf cannot be read.
_DEFAULT_DATUMS = (
    'MHHW', 'MHW', 'MLLW', 'MLW', 'NAVD88', 'IGLD85', 'LWD', 'XGEOID20B'
)

# Per-OFS Quick Run datum defaults. Great Lakes use IGLD85; STOFS systems
# don't have a uniform tidal datum so fall back to NAVD88; tidal coastal
# OFSes use MLLW.
_GREAT_LAKES_OFS = ('leofs', 'lmhofs', 'loofs', 'loofs2', 'lsofs')
_STOFS_OFS = ('stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac')


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
        """Ignore transient FocusOut events fired right after the popup
        opens, before the user has had a chance to interact with it."""
        now = self.winfo_toplevel().tk.call('clock', 'milliseconds')
        elapsed = now - self._drop_down_time
        if elapsed < self._FOCUS_GRACE_MS:
            self._calendar.focus_set()
            return
        super()._on_focus_out_cal(event)


def _quick_run_datum(ofs):
    """Return a sensible default datum for the given OFS in Quick Run mode."""
    if ofs in _GREAT_LAKES_OFS:
        return 'IGLD85'
    if ofs in _STOFS_OFS:
        return 'NAVD88'
    return 'MLLW'


def _compute_recent_cycle(ofs):
    """Compute the most recent forecast cycle that should be available.

    Returns
    -------
    (start_iso, forecast_hr) : tuple of str
        ``start_iso`` is YYYY-MM-DDTHH:MM:SSZ at the chosen cycle, and
        ``forecast_hr`` is the cycle as ``'06z'`` etc. Allows a 2h delay
        for file arrival on NODD before considering a cycle "available".
    """
    _, fcstcycles = get_fcst_hours(ofs)
    cycles = sorted(int(c) for c in fcstcycles)
    now_utc = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
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


def _read_datum_list():
    """Read [datums] datum_list from conf, falling back to a hardcoded list.

    The shared ``read_config_section`` helper logs via the logger argument
    on failure; we pass a real logger so a missing section doesn't crash
    the GUI on launch.
    """
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
    return _DEFAULT_DATUMS


def create_gui(parser):

    def on_closing():
        if messagebox.askokcancel('Quit', 'Do you want to quit?'):
            # If the user confirms quitting, destroy the window
            root.destroy()
            print('Skill assessment run terminated by user.')
            sys.exit()

    def quick_run_submit():
        '''Bypasses standard validation and executes a pre-configured quick run.

        Defaults are tailored per-OFS where it matters: datum picks IGLD85
        for Great Lakes / NAVD88 for STOFS / MLLW for tidal coastal, and the
        forecast cycle is computed from current UTC time so the run does not
        depend on the optional S3 fallback setting.
        '''
        if not directory_path_var.get():
            messagebox.showerror('Error', 'Please select your home directory for Quick Run.')
            return
        if ofs_entry.get() == choices[0] or not ofs_entry.get():
            messagebox.showerror('Error', 'Please select an OFS for Quick Run.')
            return

        ofs = ofs_entry.get()
        start_iso, forecast_hr = _compute_recent_cycle(ofs)
        end_iso = (
            datetime.strptime(start_iso, '%Y-%m-%dT%H:%M:%SZ')
            + timedelta(hours=24)
        ).strftime('%Y-%m-%dT%H:%M:%SZ')

        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs
        args_values.Whichcasts = ['nowcast', 'forecast_a']
        args_values.Forecast_Hr = forecast_hr
        args_values.StartDate_full = start_iso
        args_values.EndDate_full = end_iso
        args_values.Datum = _quick_run_datum(ofs)
        args_values.FileType = 'stations'
        args_values.Station_Owner = ['co-ops', 'ndbc', 'usgs']
        args_values.Var_Selection = [
            'water_level', 'water_temperature', 'salinity', 'currents'
        ]
        args_values.Horizon_Skill = False
        args_values.Currents_Bins_Csv = None
        # Mirrors argparse: True means the model-file pre-check is performed.
        args_values.Disable_Model_File_Check = True
        args_values.config = config_path_var.get().strip() or None

        root.destroy()

    def submit_and_close():
        # First check for required arguments and display error if not present
        error = None

        # Build UTC-aware datetimes once for the date-ordering / future
        # checks so comparisons are unambiguous.
        start_date = start_entry.get_date()
        end_date = end_entry.get_date()
        try:
            start_dt = datetime(
                start_date.year, start_date.month, start_date.day,
                int(s_hour_scale.get()), tzinfo=timezone.utc,
            ) if start_date else None
            end_dt = datetime(
                end_date.year, end_date.month, end_date.day,
                int(e_hour_scale.get()), tzinfo=timezone.utc,
            ) if end_date else None
        except (TypeError, ValueError):
            start_dt = end_dt = None

        if directory_path_var.get() is None:
            messagebox.showerror('Error', 'Please select your home directory.')
            error = 1
        elif ofs_entry.get() == choices[0]:
            messagebox.showerror('Error', 'Please select the OFS.')
            error = 1
        elif cycle_var.get() in (_OFS_FIRST_PLACEHOLDER, ''):
            messagebox.showerror('Error', 'Please select a model cycle.')
            error = 1
        elif datum_var.get() == dchoices[0]:
            messagebox.showerror('Error', 'Please choose a datum.')
            error = 1
        elif not start_entry.get_date():
            messagebox.showerror('Error', 'Please enter a start date.')
            error = 1
        elif not end_entry.get_date():
            messagebox.showerror('Error', 'Please enter an end date.')
            error = 1
        elif start_dt is not None and end_dt is not None and start_dt >= end_dt:
            messagebox.showerror(
                'Error', 'Start date/hour must be before end date/hour.'
            )
            error = 1
        elif (start_dt is not None
              and start_dt > datetime.now(timezone.utc)):
            messagebox.showerror(
                'Error',
                'Start date/hour cannot be in the future (UTC).'
            )
            error = 1
        elif (var_now.get() == '0' and var_fore.get() == '0'
              and var_forea.get() == '0' and var_hind.get() == '0'):
            messagebox.showerror(
                'Error', 'Please select at least one whichcast.'
            )
            error = 1
        elif (var_coops.get() == '0' and var_ndbc.get() == '0'
              and var_usgs.get() == '0' and var_list.get() == '0'):
            messagebox.showerror(
                'Error',
                'Please select at least one station provider, or provide '
                'a list of station IDs.'
            )
            error = 1
        elif (var_salt.get() == '0' and var_cu.get() == '0'
              and var_temp.get() == '0' and var_wl.get() == '0'):
            messagebox.showerror(
                'Error', 'Please select at least one variable to assess.'
            )
            error = 1
        elif horizon_var.get() and filetype_var.get() != 'stations':
            messagebox.showerror(
                'Error',
                '"Assess all forecast horizons?" is only supported with '
                'the "Station" model output file type. Either change the '
                'file type to Station, or set "Assess all forecast '
                'horizons?" to No.'
            )
            error = 1

        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs_entry.get()
        args_values.StartDate_full = format_date(
            start_entry.get_date(), s_hour_scale.get()
        )
        args_values.EndDate_full = format_date(
            end_entry.get_date(), e_hour_scale.get()
        )
        args_values.Whichcasts = [
            item for item in (
                var_now.get(), var_fore.get(),
                var_forea.get(), var_hind.get(),
            ) if item != '0'
        ]
        args_values.Datum = datum_var.get()
        args_values.FileType = filetype_var.get()
        args_values.Station_Owner = [
            item for item in (
                var_coops.get(), var_ndbc.get(),
                var_usgs.get(), var_list.get(),
            ) if item != '0'
        ]
        args_values.Horizon_Skill = horizon_var.get()
        args_values.Var_Selection = [
            item for item in (
                var_wl.get(), var_temp.get(),
                var_salt.get(), var_cu.get(),
            ) if item != '0'
        ]

        selected_cycle = cycle_var.get()
        if selected_cycle == _MOST_RECENT_LABEL:
            args_values.Forecast_Hr = 'now'
        else:
            args_values.Forecast_Hr = selected_cycle

        args_values.Currents_Bins_Csv = cb_var.get() or None
        args_values.config = config_path_var.get().strip() or None

        # GUI variable is named for clarity; the namespace attribute keeps
        # the argparse name. True means the model-file pre-check runs.
        args_values.Disable_Model_File_Check = enable_file_check_var.get()

        if error is None:
            root.destroy() # Close the GUI window

    def format_date(date, hour):
        # DateEntry.get_date() returns a datetime.date object
        from datetime import date as date_type
        if isinstance(date, date_type):
            return f"{date.strftime('%Y-%m-%d')}T{int(hour):02d}:00:00Z"
        raise TypeError(f'Expected date object, got {type(date)}: {date}')

    def browse_directory():
        '''
        Opens a directory selection dialog and
        updates the directory path.
        '''
        chosen_directory = filedialog.askdirectory()
        if chosen_directory:  # Only update if a directory was selected
            directory_path_var.set(chosen_directory)

    def browse_config_file():
        '''
        Opens a file selection dialog for the .conf configuration file.
        '''
        chosen_file = filedialog.askopenfilename(
            title='Select configuration file',
            filetypes=(('Config files', '*.conf'), ('All files', '*.*')),
        )
        if chosen_file:
            config_path_var.set(chosen_file)

    def browse_csv_file():
        '''
        Opens a file selection dialog for the currents bins CSV.
        '''
        chosen_file = filedialog.askopenfilename(
            title='Select Currents Bins CSV',
            filetypes=(('CSV files', '*.csv'), ('All files', '*.*'))
        )
        if chosen_file:
            cb_var.set(chosen_file)

    def update_cycles(_event=None):
        '''Updates the cycle combobox based on the selected OFS.'''
        selected_ofs = ofs_entry.get()
        if selected_ofs and selected_ofs != _OFS_PLACEHOLDER:
            try:
                _, cycles = get_fcst_hours(selected_ofs)
            except (KeyError, ValueError):
                cycle_chosen['values'] = (_OFS_FIRST_PLACEHOLDER,)
                cycle_var.set(_OFS_FIRST_PLACEHOLDER)
                return
            cycle_choices = [f'{int(c):02d}z' for c in cycles]
            cycle_choices.insert(0, _MOST_RECENT_LABEL)
            cycle_chosen['values'] = tuple(cycle_choices)
            cycle_var.set(cycle_choices[0])
        else:
            cycle_chosen['values'] = (_OFS_FIRST_PLACEHOLDER,)
            cycle_var.set(_OFS_FIRST_PLACEHOLDER)

    def toggle_cycle_state():
        '''Enables or disables the Model Cycle dropdown based on Forecast_a selection.'''
        if var_forea.get() == 'forecast_a':
            cycle_chosen.config(state='readonly')
        else:
            cycle_chosen.config(state='disabled')

    # Track the previously selected OFS so we can restore the default
    # whichcast set when the user switches AWAY from LOOFS2 (where we
    # forced hindcast-only). Closure-captured list lets the nested
    # handler mutate the value without `nonlocal` gymnastics.
    _last_ofs = ['']

    def update_whichcast_state(_event=None):
        '''Restrict whichcast checkboxes based on the selected OFS.

        LOOFS2 currently only supports the hindcast whichcast — nowcast,
        forecast_a and forecast_b are not yet implemented. When LOOFS2
        is selected we force-uncheck the three unsupported whichcasts
        and disable their checkboxes so the user can't pick them. When
        switching away from LOOFS2, we restore the original defaults
        (nowcast + forecast_b on, forecast_a + hindcast off).
        '''
        current = ofs_entry.get()
        is_loofs2 = current == 'loofs2'
        was_loofs2 = _last_ofs[0] == 'loofs2'

        if is_loofs2:
            var_now.set('0')
            var_fore.set('0')
            var_forea.set('0')
            var_hind.set('hindcast')
            now_chk.config(state='disabled')
            fore_chk.config(state='disabled')
            forea_chk.config(state='disabled')
            hind_chk.config(state='normal')
            toggle_cycle_state()
        else:
            now_chk.config(state='normal')
            fore_chk.config(state='normal')
            forea_chk.config(state='normal')
            hind_chk.config(state='normal')
            if was_loofs2:
                var_now.set('nowcast')
                var_fore.set('forecast_b')
                var_forea.set('0')
                var_hind.set('0')
                toggle_cycle_state()

        _last_ofs[0] = current

    def on_ofs_changed(_event=None):
        '''Single handler for the OFS combobox: refreshes both the cycle
        dropdown and the whichcast checkbox enable/disable state.'''
        update_cycles(_event)
        update_whichcast_state(_event)

    root = tk.Tk()
    root.title('Skill assessment inputs')
    # Set the protocol for handling the window close event
    root.protocol('WM_DELETE_WINDOW', on_closing)
    root.geometry(_WINDOW_GEOMETRY)

    style = ttk.Style(root)
    style.theme_use('clam') # modified from vista to clam for cross OS compatibility

    log = logging.getLogger(__name__)

    # STYLING
    # Change the icon
    try:
        dir_params = utils.Utils().read_config_section('directories', log)
        iconpath = os.path.join(
            dir_params['home'], 'readme_images', 'noaa_logo.png'
        )
        icon_image = tk.PhotoImage(file=iconpath)
        root.iconphoto(False, icon_image)
    except (KeyError, tk.TclError, OSError):
        log.info('GUI logo not found; defaulting to tkinter logo.')

    datum_list = _read_datum_list()

    # Window-wide colors, text, and stuff
    anchor='e'
    themecolor = 'gainsboro'
    textcolor = 'black'
    labelfontsize = 12
    widgetfontsize = 12
    fontfamily = 'Helvetica'
    padx = 3
    pady = 10
    root.config(bg=themecolor)

    # --- SCROLLBAR AND CANVAS SETUP ---
    # Create a main container frame
    container = tk.Frame(root, bg=themecolor)
    container.pack(fill='both', expand=True)

    # Create a canvas inside the container
    canvas = tk.Canvas(container, bg=themecolor, highlightthickness=0)

    # Create the vertical scrollbar
    scrollbar = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)

    # Create the scrollable frame that will hold all widgets
    scrollable_frame = tk.Frame(canvas, bg=themecolor)

    # Bind the frame size changes to update the canvas scroll region
    scrollable_frame.bind(
        '<Configure>',
        lambda e: canvas.configure(
            scrollregion=canvas.bbox('all')
        )
    )

    # Put the frame inside a window within the canvas
    canvas.create_window((0, 0), window=scrollable_frame, anchor='nw')
    canvas.configure(yscrollcommand=scrollbar.set)

    # Pack the canvas and scrollbar
    canvas.pack(side='left', fill='both', expand=True)
    scrollbar.pack(side='right', fill='y')

    # Bind mousewheel scrolling to the canvas
    def _on_mousewheel(event):
        canvas.yview_scroll(int(-1*(event.delta/120)), 'units')
    root.bind_all('<MouseWheel>', _on_mousewheel)

    # Style for each widget type
    style = ttk.Style()
    style.configure('TButton',
                    background=themecolor,
                    foreground=textcolor,
                    font=(fontfamily, widgetfontsize))
    style.configure('TCheckbutton',
                    background=themecolor,
                    foreground=textcolor,
                    font=(fontfamily, widgetfontsize))
    style.configure('TRadiobutton',
                    background=themecolor,
                    foreground=textcolor,
                    font=(fontfamily, widgetfontsize))
    # Set font for drop-downs
    root.option_add('*TCombobox*Listbox*Font',
                    Font(family='Helvetica', size=12))
    # Set default argument values
    args_values = SimpleNamespace() # To store the values from GUI
    args_values.OFS = None
    args_values.Path = None
    args_values.StartDate_full = None
    args_values.EndDate_full = None
    args_values.Whichcasts = None
    args_values.Datum = parser.get_default('Datum')
    args_values.FileType = parser.get_default('FileType')
    args_values.Station_Owner = parser.get_default('Station_Owner')
    args_values.Horizon_Skill = parser.get_default('Horizon_Skill')
    args_values.Forecast_Hr = parser.get_default('Forecast_Hr')
    args_values.Var_Selection = parser.get_default('Var_Selection')
    args_values.Currents_Bins_Csv = parser.get_default('Currents_Bins_Csv')
    args_values.Disable_Model_File_Check = parser.get_default('Disable_Model_File_Check')
    args_values.config = None

    # Set row initial value
    row = -1

    # Set home dir, -p
    row += 1
    # Create a StringVar to hold the selected directory path
    directory_path_var = tk.StringVar()
    tk.Label(scrollable_frame,
             text='Home directory',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    ttk.Button(scrollable_frame, text='Browse...', command=browse_directory,
               style='TButton').grid(row=row, column=1,  sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(scrollable_frame, textvariable=directory_path_var).grid(row=row, column=2,
                                                          sticky='w',
                                                          padx=padx, pady=pady)

    # Config file, -c. Empty/default value means "use the default
    # conf/ofs_dps.conf"; submit_and_close() converts blanks to None
    # so create_1dplot.py's argparse default kicks in.
    row += 1
    config_path_var = tk.StringVar(value='conf/ofs_dps.conf')
    tk.Label(scrollable_frame,
             text='Config file',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    ttk.Button(scrollable_frame, text='Browse...', command=browse_config_file,
               style='TButton').grid(row=row, column=1, sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(scrollable_frame, textvariable=config_path_var).grid(
        row=row, column=2, sticky='w', padx=padx, pady=pady)

    # OFS input, -o
    row += 1
    ofs_entry = tk.StringVar()
    choices = (
        _OFS_PLACEHOLDER,
        'cbofs',
        'ciofs',
        'dbofs',
        'gomofs',
        'leofs',
        'lmhofs',
        'loofs',
        'loofs2',
        'lsofs',
        'necofs',
        'ngofs2',
        'secofs',
        'sfbofs',
        'sscofs',
        'stofs_2d_glo',
        'stofs_3d_atl',
        'stofs_3d_pac',
        'tbofs',
        'wcofs',
    )

    ofs_entry.set(_OFS_PLACEHOLDER)
    tk.Label(scrollable_frame,
             text='OFS',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(
        scrollable_frame, width=15, textvariable=ofs_entry,
        font=('Helvetica', 12), state='readonly',
    )
    ofs_chosen['values'] = choices
    ofs_chosen.grid(row=row, column=1, sticky='w', padx=padx, pady=pady)
    ofs_chosen.bind('<<ComboboxSelected>>', on_ofs_changed)

    row += 1
    tk.Label(scrollable_frame,
             text='Quick run mode assesses the most recent model cycle ➡️',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, 9, 'italic'),
             anchor=anchor).grid(
                 row=row, column=0, sticky='w', padx=padx, pady=(0, pady))
    ttk.Button(
        scrollable_frame, text='⚡ Quick Run Mode',
        command=quick_run_submit, style='TButton',
    ).grid(row=row, column=1, sticky='w', padx=padx, pady=(0, pady))

    row += 1
    ttk.Separator(scrollable_frame, orient='horizontal').grid(
        row=row, column=0, columnspan=4, sticky='ew', pady=(15, 10))

    # Start date, -s
    row += 1
    tk.Label(scrollable_frame,
             text='Start date & hour',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    start_entry = DateEntry(
        scrollable_frame, width=16, background='darkblue',
        foreground='white', bd=2, date_pattern='yyyy-mm-dd',
        font=('Helvetica', 12),
    )
    start_entry.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)
    # Create scale widget for hour selection
    s_hour_scale = tk.Scale(scrollable_frame, from_=0, to=23, orient=tk.HORIZONTAL,
                          length=100)
    s_hour_scale.grid(row=row, column=2,  sticky='w', padx=padx, pady=pady)

    # End date, -e
    row += 1
    tk.Label(scrollable_frame,
             text='End date & hour',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    end_entry = DateEntry(
        scrollable_frame, width=16, background='darkblue',
        foreground='white', bd=2, date_pattern='yyyy-mm-dd',
        font=('Helvetica', 12),
    )
    end_entry.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)
    # Create scale widget for hour selection
    e_hour_scale = tk.Scale(scrollable_frame, from_=0, to=23, orient=tk.HORIZONTAL,
                          length=100)
    e_hour_scale.grid(row=row, column=2,  sticky='w', padx=padx, pady=pady)

    # Whichcasts, -ws
    row += 1
    var_now = tk.StringVar()
    var_fore = tk.StringVar()
    var_hind = tk.StringVar()
    var_forea = tk.StringVar()
    var_now.set('nowcast')
    var_fore.set('forecast_b')

    # Set these to '0' so they are unchecked by default
    var_forea.set('0')
    var_hind.set('0')

    tk.Label(scrollable_frame,
             text='Whichcasts',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    now_chk = ttk.Checkbutton(
        scrollable_frame, text='Nowcast', variable=var_now,
        onvalue='nowcast', offvalue=0,
    )
    now_chk.grid(row=row, column=1, sticky='w', padx=padx, pady=pady)
    fore_chk = ttk.Checkbutton(
        scrollable_frame, text='Forecast_b', variable=var_fore,
        onvalue='forecast_b', offvalue=0,
    )
    fore_chk.grid(row=row, column=2, sticky='w', padx=padx, pady=pady)
    row += 1
    forea_chk = ttk.Checkbutton(
        scrollable_frame, text='Forecast_a', variable=var_forea,
        onvalue='forecast_a', offvalue=0, command=toggle_cycle_state,
    )
    forea_chk.grid(row=row, column=1, sticky='w', padx=padx, pady=pady)
    hind_chk = ttk.Checkbutton(
        scrollable_frame, text='Hindcast (LOOFS2 only)', variable=var_hind,
        onvalue='hindcast', offvalue=0,
    )
    hind_chk.grid(row=row, column=2, sticky='w', padx=padx, pady=pady)

    # Model Cycle input, -f
    row += 1
    cycle_var = tk.StringVar()
    cycle_var.set(_OFS_FIRST_PLACEHOLDER)
    tk.Label(scrollable_frame,
             text='Model cycle (forecast_a only)',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    cycle_chosen = ttk.Combobox(scrollable_frame, width=22, textvariable=cycle_var,
                              font=('Helvetica', 12), state='readonly')
    cycle_chosen.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)

    # Datums, -d
    row += 1
    datum_var = tk.StringVar()
    dchoices = (_DATUM_PLACEHOLDER,) + tuple(datum_list)
    datum_var.set(_DATUM_PLACEHOLDER)
    tk.Label(scrollable_frame,
             text='Vertical datum',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    datum_chosen = ttk.Combobox(scrollable_frame, width = 15, textvariable = datum_var,
                                font=('Helvetica', 12))
    datum_chosen['values'] = dchoices
    datum_chosen.grid(row=row, column=1, sticky='w', padx=padx, pady=pady)

    # File type, -t
    row += 1
    filetype_var = tk.StringVar(value='stations') # Default selection
    tk.Label(scrollable_frame,
             text='Model output file type',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    ttk.Radiobutton(scrollable_frame, text='Station', variable=filetype_var,
                   value='stations').grid(row=row, column=1,
                                          sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(scrollable_frame, text='Field', variable=filetype_var, value='fields').\
        grid(row=row, column=2, sticky='w', padx=padx, pady=pady)

    # Station provider, -so
    row += 1
    var_coops = tk.StringVar()
    var_ndbc = tk.StringVar()
    var_usgs = tk.StringVar()
    var_list = tk.StringVar()
    var_coops.set('co-ops')
    var_ndbc.set('ndbc')
    var_usgs.set('usgs')
    var_list.set(0)
    tk.Label(scrollable_frame,
             text='Station providers',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    tk.Label(scrollable_frame,
             text='If adding stations from list, provider selection is optional',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, 9, 'italic'),
             anchor=anchor).grid(
        row=row+1, column=0, sticky='w', padx=padx, pady=(0,pady))
    ttk.Checkbutton(scrollable_frame, text='CO-OPS',
                   variable=var_coops, onvalue='co-ops', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(pady,2))
    ttk.Checkbutton(scrollable_frame, text='NDBC',
                   variable=var_ndbc, onvalue='ndbc', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(pady,2))
    row += 1
    ttk.Checkbutton(scrollable_frame, text='USGS',
                   variable=var_usgs, onvalue='usgs', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(0,pady))
    ttk.Checkbutton(scrollable_frame, text='Add from conf file',
                   variable=var_list, onvalue='list', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(0,pady))

    # Variables, -vs
    row += 1
    var_wl = tk.StringVar()
    var_temp = tk.StringVar()
    var_salt = tk.StringVar()
    var_cu = tk.StringVar()
    var_wl.set('water_level')
    var_temp.set('water_temperature')
    var_salt.set('salinity')
    var_cu.set('currents')
    tk.Label(scrollable_frame,
             text='Variables',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(scrollable_frame, text='Water level',
                   variable=var_wl, onvalue='water_level', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(pady,2))
    ttk.Checkbutton(scrollable_frame, text='Temperature',
                   variable=var_temp, onvalue='water_temperature', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(pady,2))
    row += 1
    ttk.Checkbutton(scrollable_frame, text='Salinity',
                   variable=var_salt, onvalue='salinity', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(0,pady))
    ttk.Checkbutton(scrollable_frame, text='Current velocity',
                   variable=var_cu, onvalue='currents', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(0,pady))

    # Forecast horizon skill, -hs
    row += 1
    horizon_var = tk.BooleanVar(value=False) # Default selection
    tk.Label(scrollable_frame,
             text='Assess all forecast horizons?',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(scrollable_frame, text='No (default)', variable=horizon_var, value=False).grid(
        row=row, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(scrollable_frame, text='Yes', variable=horizon_var, value=True).grid(
        row=row, column=2, sticky='w', padx=padx, pady=pady)

    # Currents bins CSV, -cb
    row += 1
    cb_var = tk.StringVar()
    tk.Label(scrollable_frame,
             text='Currents bins CSV (Optional)',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    ttk.Button(scrollable_frame, text='Browse...', command=browse_csv_file,
               style='TButton').grid(row=row, column=1, sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(scrollable_frame, textvariable=cb_var).grid(row=row, column=2,
                                              sticky='w',
                                              padx=padx, pady=pady)

    # Model file check, -df
    # NOTE: True means the pre-check IS performed (matches the
    # action='store_false' semantics of -df in create_1dplot.py).
    row += 1
    enable_file_check_var = tk.BooleanVar(value=True)
    tk.Label(scrollable_frame,
             text='Pre-check for model output files?',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(
        scrollable_frame, text='No (disable check)',
        variable=enable_file_check_var, value=False,
    ).grid(row=row, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(
        scrollable_frame, text='Yes (default)',
        variable=enable_file_check_var, value=True,
    ).grid(row=row, column=2, sticky='w', padx=padx, pady=pady)

    # Horizontal separator
    row += 1
    separator = ttk.Separator(scrollable_frame, orient='horizontal')
    separator.grid(row=row, column=0, columnspan=4, sticky='ew', pady=pady)

    # Submit button
    row += 1
    submit_button = ttk.Button(scrollable_frame, text='Run skill assessment!',
                              command=submit_and_close)
    submit_button.grid(row=row, column=0, columnspan=4, pady=10)
    toggle_cycle_state()
    root.mainloop()
    return args_values
