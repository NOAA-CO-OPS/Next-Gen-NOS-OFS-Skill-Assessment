"""
Created on Wed Nov 12 08:39:35 2025

@author: PWL
"""
from __future__ import annotations

import logging
import os
import sys
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization.gui_helpers import (
    DateEntry,
    GuiParams,
    GuiTheme,
    ToolTip,
    build_utc_datetime,
    compute_recent_cycle,
    format_date,
    quick_run_datum,
    read_datum_list,
    validate_date_order,
    validate_horizon_requires_stations,
    validate_start_not_future,
)

# UI sentinel strings — used in multiple places, must match exactly.
_OFS_PLACEHOLDER = 'Select an OFS...'
_OFS_FIRST_PLACEHOLDER = 'Select an OFS first...'
_DATUM_PLACEHOLDER = 'Select a datum...'
_MOST_RECENT_LABEL = 'Most recent cycle available'
_WINDOW_GEOMETRY = '850x500'


def create_gui(parser):

    def on_closing():
        if messagebox.askokcancel('Quit', 'Do you want to quit?'):
            # If the user confirms quitting, destroy the window
            root.destroy()
            print('Skill assessment run terminated by user.')
            sys.exit()

    # Widgets currently flagged by _mark_invalid(); cleared on next submit.
    _invalid_widgets: list[tk.Widget] = []

    _ERROR_TTK_CLASSES = ('TCombobox', 'TSpinbox', 'TEntry', 'TLabel')

    def _mark_invalid(*widgets):
        """Highlight the given widget(s) to indicate failed validation.
        All ttk widgets switch to their Error.* style variant."""
        for w in widgets:
            if w is None or w in _invalid_widgets:
                continue
            try:
                cls = w.winfo_class()
                if cls in _ERROR_TTK_CLASSES:
                    w.configure(style=f'Error.{cls}')
                    _invalid_widgets.append(w)
            except tk.TclError:
                pass

    def _clear_invalid():
        """Restore the default styling on any previously-flagged widgets."""
        while _invalid_widgets:
            w = _invalid_widgets.pop()
            try:
                cls = w.winfo_class()
                if cls in _ERROR_TTK_CLASSES:
                    w.configure(style=cls)
            except tk.TclError:
                pass

    def quick_run_submit():
        '''Bypasses standard validation and executes a pre-configured quick run.

        Defaults are tailored per-OFS where it matters: datum picks IGLD85
        for Great Lakes / NAVD88 for STOFS / MLLW for tidal coastal, and the
        forecast cycle is computed from current UTC time so the run does not
        depend on the optional S3 fallback setting.
        '''
        _clear_invalid()
        if not directory_path_var.get():
            _mark_invalid(directory_entry)
            messagebox.showerror('Error', 'Please select your home directory for Quick Run.')
            return
        if ofs_entry.get() == choices[0] or not ofs_entry.get():
            _mark_invalid(ofs_chosen)
            messagebox.showerror('Error', 'Please select an OFS for Quick Run.')
            return

        ofs = ofs_entry.get()
        start_iso, forecast_hr = compute_recent_cycle(ofs)
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
        args_values.Datum = quick_run_datum(ofs)
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
        # Reset any field highlights from the previous submission attempt.
        _clear_invalid()
        error = None

        # Normalised UTC-aware datetimes for ordering / future checks.
        start_dt = build_utc_datetime(
            start_entry.get_date(), s_hour_var.get()
        )
        end_dt = build_utc_datetime(
            end_entry.get_date(), e_hour_var.get()
        )

        # Run the pure-logic validators independently so each can flag
        # its own offending widget(s).
        date_order_msg = validate_date_order(start_dt, end_dt)
        future_msg = validate_start_not_future(start_dt)
        horizon_msg = validate_horizon_requires_stations(
            horizon_var.get(), filetype_var.get()
        )

        if not directory_path_var.get():
            _mark_invalid(directory_entry)
            messagebox.showerror('Error', 'Please select your home directory.')
            error = 1
        elif ofs_entry.get() == choices[0]:
            _mark_invalid(ofs_chosen)
            messagebox.showerror('Error', 'Please select the OFS.')
            error = 1
        elif cycle_var.get() in (_OFS_FIRST_PLACEHOLDER, ''):
            _mark_invalid(cycle_chosen)
            messagebox.showerror('Error', 'Please select a model cycle.')
            error = 1
        elif datum_var.get() == dchoices[0]:
            _mark_invalid(datum_chosen)
            messagebox.showerror('Error', 'Please choose a datum.')
            error = 1
        elif not start_entry.get_date():
            _mark_invalid(start_entry)
            messagebox.showerror('Error', 'Please enter a start date.')
            error = 1
        elif not end_entry.get_date():
            _mark_invalid(end_entry)
            messagebox.showerror('Error', 'Please enter an end date.')
            error = 1
        elif (var_now.get() == '0' and var_fore.get() == '0'
              and var_forea.get() == '0' and var_hind.get() == '0'):
            _mark_invalid(whichcast_lbl)
            messagebox.showerror(
                'Error', 'Please select at least one whichcast.'
            )
            error = 1
        elif (var_coops.get() == '0' and var_ndbc.get() == '0'
              and var_usgs.get() == '0' and var_list.get() == '0'):
            _mark_invalid(providers_lbl)
            messagebox.showerror(
                'Error',
                'Please select at least one station provider, or provide '
                'a list of station IDs.'
            )
            error = 1
        elif (var_salt.get() == '0' and var_cu.get() == '0'
              and var_temp.get() == '0' and var_wl.get() == '0'):
            _mark_invalid(variables_lbl)
            messagebox.showerror(
                'Error', 'Please select at least one variable to assess.'
            )
            error = 1
        elif date_order_msg is not None:
            _mark_invalid(start_entry, end_entry, s_hour_spin, e_hour_spin)
            messagebox.showerror('Error', date_order_msg)
            error = 1
        elif future_msg is not None:
            _mark_invalid(start_entry, s_hour_spin)
            messagebox.showerror('Error', future_msg)
            error = 1
        elif horizon_msg is not None:
            _mark_invalid(horizon_lbl)
            messagebox.showerror('Error', horizon_msg)
            error = 1

        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs_entry.get()
        args_values.StartDate_full = format_date(
            start_entry.get_date(), s_hour_var.get()
        )
        args_values.EndDate_full = format_date(
            end_entry.get_date(), e_hour_var.get()
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

    datum_list = read_datum_list()

    # Shared theme palette (colors, fonts, spacing) — see gui_helpers.GuiTheme.
    theme = GuiTheme()
    themecolor = theme.themecolor
    textcolor = theme.textcolor
    fontfamily = theme.fontfamily
    labelfontsize = theme.labelfontsize
    widgetfontsize = theme.widgetfontsize
    padx = theme.padx
    pady = theme.pady
    anchor = theme.anchor
    root.config(bg=themecolor)

    # --- SCROLLBAR AND CANVAS SETUP ---
    # Create a main container frame
    container = ttk.Frame(root)
    container.pack(fill='both', expand=True)

    # Create a canvas inside the container
    canvas = tk.Canvas(container, bg=themecolor, highlightthickness=0)

    # Create the vertical scrollbar
    scrollbar = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)

    # Create the scrollable frame that will hold all widgets
    scrollable_frame = ttk.Frame(canvas)

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

    # Cross-platform mouse-wheel scrolling.
    # - Linux X11: wheel events arrive as <Button-4> (up) / <Button-5>
    #   (down); event.delta is unset.
    # - Windows: <MouseWheel> with event.delta in multiples of 120.
    # - macOS: <MouseWheel> with small integer deltas (often +/-1) per
    #   smooth-scroll tick — the old `delta / 120` logic truncated these
    #   to 0 so scrolling silently did nothing.
    def _on_mousewheel(event):
        # Ignore wheel events from widgets outside the form canvas
        # (e.g. the calendar popup) so they don't scroll the form.
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
                    Font(family=fontfamily, size=widgetfontsize))

    # Error-state styles used by _mark_invalid() to highlight fields
    # whose values fail validation in submit_and_close().
    _ERROR_BG = '#fff3f3'
    _ERROR_BORDER = '#d93025'
    _ERROR_LABEL_BG = '#ffd6d6'
    style.configure('Error.TCombobox',
                    fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER)
    style.map('Error.TCombobox',
              fieldbackground=[('readonly', _ERROR_BG)])
    style.configure('Error.TSpinbox',
                    fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER)
    style.configure('Error.TEntry',
                    fieldbackground=_ERROR_BG, bordercolor=_ERROR_BORDER)
    style.configure('TFrame', background=themecolor)
    style.configure('TLabel', background=themecolor, foreground=textcolor)
    style.configure('TLabelframe', background=themecolor)
    style.configure('TLabelframe.Label', background=themecolor,
                    foreground=textcolor, font=theme.section_title_font)
    style.configure('Error.TLabel', background=_ERROR_LABEL_BG)

    args_values = GuiParams()

    def _section(title):
        """Themed LabelFrame container for a group of related widgets."""
        return ttk.LabelFrame(
            scrollable_frame, text=title, padding=(padx, pady),
        )

    def _label(parent, text, italic=False, help_text=None):
        """Standard row label inside a section frame. If ``help_text`` is
        given, the label gets a trailing ⓘ icon and shows the text on hover."""
        font = theme.hint_font if italic else theme.label_font
        display = f'{text} \u24d8' if help_text else text
        lbl = ttk.Label(parent, text=display, font=font, anchor=anchor)
        if help_text:
            lbl.config(cursor='question_arrow')
            ToolTip(lbl, help_text)
        return lbl

    section_row = 0

    # === General Settings ============================================
    general_frame = _section('General Settings')
    general_frame.grid(row=section_row, column=0, columnspan=4,
                       sticky='ew', padx=theme.section_padx,
                       pady=(theme.section_padx, theme.section_pady))
    section_row += 1

    # Home directory, -p
    directory_path_var = tk.StringVar()
    _label(general_frame, 'Home directory',
           help_text='Root working directory for the skill assessment. '
                     'Output files and the ofs_extents/ folder live '
                     'under this path.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Button(general_frame, text='Browse...', command=browse_directory,
               style='TButton').grid(row=0, column=1, sticky='w',
                                     padx=padx, pady=pady)
    directory_entry = ttk.Entry(general_frame, textvariable=directory_path_var)
    directory_entry.grid(row=0, column=2, sticky='w', padx=padx, pady=pady)

    # Config file, -c. Blank/default value means "use the default
    # conf/ofs_dps.conf"; submit_and_close() converts blanks to None
    # so create_1dplot.py's argparse default kicks in.
    config_path_var = tk.StringVar(value='conf/ofs_dps.conf')
    _label(general_frame, 'Config file',
           help_text='Path to the .conf configuration file. Leave the '
                     'default (conf/ofs_dps.conf) unless you have a '
                     'custom config for this run.').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Button(general_frame, text='Browse...', command=browse_config_file,
               style='TButton').grid(row=1, column=1, sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(general_frame, textvariable=config_path_var).grid(
        row=1, column=2, sticky='w', padx=padx, pady=pady)

    # OFS, -o
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
    _label(general_frame, 'OFS',
           help_text='Operational Forecast System to assess. Pick one from '
                     'the list, then optionally use Quick Run for a '
                     'one-click recent-cycle run.').grid(
        row=2, column=0, sticky='w', padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(
        general_frame, width=15, textvariable=ofs_entry,
        font=(fontfamily, widgetfontsize), state='readonly',
    )
    ofs_chosen['values'] = choices
    ofs_chosen.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)
    ofs_chosen.bind('<<ComboboxSelected>>', on_ofs_changed)

    # Quick Run button (sits under OFS so the per-OFS context is clear).
    _label(general_frame,
           'Quick run mode assesses the most recent model cycle ➡️',
           italic=True).grid(row=3, column=0, sticky='w',
                             padx=padx, pady=(0, pady))
    ttk.Button(general_frame, text='⚡ Quick Run Mode',
               command=quick_run_submit, style='TButton').grid(
        row=3, column=1, sticky='w', padx=padx, pady=(0, pady))

    # === Time Range ==================================================
    time_frame = _section('Time Range')
    time_frame.grid(row=section_row, column=0, columnspan=4,
                    sticky='ew', padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    # Start date, -s
    _label(time_frame, 'Start date & hour',
           help_text='Assessment start date and hour (UTC). Must not be '
                     'in the future.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    start_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    start_entry.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)
    s_hour_var = tk.IntVar(value=0)
    s_hour_spin = ttk.Spinbox(
        time_frame, from_=0, to=23, wrap=True, width=4,
        textvariable=s_hour_var, state='readonly',
        font=(fontfamily, widgetfontsize),
    )
    s_hour_spin.grid(row=0, column=2, sticky='w', padx=padx, pady=pady)
    _label(time_frame, 'h (UTC)', italic=True).grid(
        row=0, column=3, sticky='w', padx=(0, padx), pady=pady)

    # End date, -e
    _label(time_frame, 'End date & hour',
           help_text='Assessment end date and hour (UTC). Must be after '
                     'the start date.').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)
    end_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    end_entry.grid(row=1, column=1, sticky='w', padx=padx, pady=pady)
    e_hour_var = tk.IntVar(value=0)
    e_hour_spin = ttk.Spinbox(
        time_frame, from_=0, to=23, wrap=True, width=4,
        textvariable=e_hour_var, state='readonly',
        font=(fontfamily, widgetfontsize),
    )
    e_hour_spin.grid(row=1, column=2, sticky='w', padx=padx, pady=pady)
    _label(time_frame, 'h (UTC)', italic=True).grid(
        row=1, column=3, sticky='w', padx=(0, padx), pady=pady)

    # === Whichcasts & Model Cycles ========================================
    wcast_frame = _section('Whichcasts & Model Cycles')
    wcast_frame.grid(row=section_row, column=0, columnspan=4,
                     sticky='ew', padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    # Whichcasts, -ws
    var_now = tk.StringVar(value='nowcast')
    var_fore = tk.StringVar(value='forecast_b')
    var_forea = tk.StringVar(value='0')
    var_hind = tk.StringVar(value='0')

    whichcast_lbl = _label(
        wcast_frame, 'Whichcasts',
        help_text='Run mode. Nowcast and Forecast_b are the common '
                  'defaults; Forecast_a uses one complete model cycle; '
                  'Hindcast is only available for LOOFS2.')
    whichcast_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    now_chk = ttk.Checkbutton(
        wcast_frame, text='Nowcast', variable=var_now,
        onvalue='nowcast', offvalue=0,
    )
    now_chk.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)
    fore_chk = ttk.Checkbutton(
        wcast_frame, text='Forecast_b', variable=var_fore,
        onvalue='forecast_b', offvalue=0,
    )
    fore_chk.grid(row=0, column=2, sticky='w', padx=padx, pady=pady)
    forea_chk = ttk.Checkbutton(
        wcast_frame, text='Forecast_a', variable=var_forea,
        onvalue='forecast_a', offvalue=0, command=toggle_cycle_state,
    )
    forea_chk.grid(row=1, column=1, sticky='w', padx=padx, pady=pady)
    hind_chk = ttk.Checkbutton(
        wcast_frame, text='Hindcast (LOOFS2 only)', variable=var_hind,
        onvalue='hindcast', offvalue=0,
    )
    hind_chk.grid(row=1, column=2, sticky='w', padx=padx, pady=pady)

    # Model cycle, -f
    cycle_var = tk.StringVar(value=_OFS_FIRST_PLACEHOLDER)
    _label(wcast_frame, 'Model cycle (forecast_a only)',
           help_text='Model cycle to assess when Forecast_a is selected. '
                     '"Most recent cycle available" picks the latest '
                     'cycle available on the S3 NODD bucket.').grid(
        row=2, column=0, sticky='w', padx=padx, pady=pady)
    cycle_chosen = ttk.Combobox(
        wcast_frame, width=22, textvariable=cycle_var,
        font=(fontfamily, widgetfontsize), state='readonly',
    )
    cycle_chosen.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)

    # === Datums & OFS File Type ==============================================
    datum_frame = _section('Datums & OFS File Type')
    datum_frame.grid(row=section_row, column=0, columnspan=4,
                     sticky='ew', padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    # Datum, -d
    datum_var = tk.StringVar()
    dchoices = (_DATUM_PLACEHOLDER,) + tuple(datum_list)
    datum_var.set(_DATUM_PLACEHOLDER)
    _label(datum_frame, 'Vertical datum',
           help_text='Vertical datum for water-level comparisons. Great '
                     'Lakes OFS require IGLD85 or LWD; tidal coastal OFS '
                     'require tidal datum, xgeoid20b, or NAVD88.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    datum_chosen = ttk.Combobox(
        datum_frame, width=15, textvariable=datum_var, font=(fontfamily, widgetfontsize),
    )
    datum_chosen['values'] = dchoices
    datum_chosen.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)

    # File type, -t
    filetype_var = tk.StringVar(value='stations')
    _label(datum_frame, 'Model output file type',
           help_text='OFS file type. "Station" uses the 6-min model '
                     'station files; "Field" uses the hourly gridded '
                     'model fields.').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(datum_frame, text='Station', variable=filetype_var,
                    value='stations').grid(row=1, column=1, sticky='w',
                                           padx=padx, pady=pady)
    ttk.Radiobutton(datum_frame, text='Field', variable=filetype_var,
                    value='fields').grid(row=1, column=2, sticky='w',
                                         padx=padx, pady=pady)

    # === Station Providers & Variables ========================================
    sv_frame = _section('Station Providers & Variables')
    sv_frame.grid(row=section_row, column=0, columnspan=4,
                  sticky='ew', padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    # Station providers, -so
    var_coops = tk.StringVar(value='co-ops')
    var_ndbc = tk.StringVar(value='ndbc')
    var_usgs = tk.StringVar(value='usgs')
    var_list = tk.StringVar(value='0')
    providers_lbl = _label(
        sv_frame, 'Station providers',
        help_text='Observation data retrieval sources. "Add from conf '
                  'file" lets you supply a custom list in the '
                  '[station_lists] section of the config file.')
    providers_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    _label(sv_frame,
           'If adding stations from list, provider selection is optional',
           italic=True).grid(row=1, column=0, sticky='w',
                             padx=padx, pady=(0, pady))
    ttk.Checkbutton(sv_frame, text='CO-OPS', variable=var_coops,
                    onvalue='co-ops', offvalue=0).grid(
        row=0, column=1, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='NDBC', variable=var_ndbc,
                    onvalue='ndbc', offvalue=0).grid(
        row=0, column=2, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='USGS', variable=var_usgs,
                    onvalue='usgs', offvalue=0).grid(
        row=1, column=1, sticky='w', padx=padx, pady=(0, pady))
    ttk.Checkbutton(sv_frame, text='Add from conf file', variable=var_list,
                    onvalue='list', offvalue=0).grid(
        row=1, column=2, sticky='w', padx=padx, pady=(0, pady))

    # Variables, -vs
    var_wl = tk.StringVar(value='water_level')
    var_temp = tk.StringVar(value='water_temperature')
    var_salt = tk.StringVar(value='salinity')
    var_cu = tk.StringVar(value='currents')
    variables_lbl = _label(
        sv_frame, 'Variables',
        help_text='Oceanographic variables to assess. Pick any '
                  'combination; the run only processes the ones '
                  'you check.')
    variables_lbl.grid(row=2, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(sv_frame, text='Water level', variable=var_wl,
                    onvalue='water_level', offvalue=0).grid(
        row=2, column=1, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='Temperature', variable=var_temp,
                    onvalue='water_temperature', offvalue=0).grid(
        row=2, column=2, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='Salinity', variable=var_salt,
                    onvalue='salinity', offvalue=0).grid(
        row=3, column=1, sticky='w', padx=padx, pady=(0, pady))
    ttk.Checkbutton(sv_frame, text='Current velocity', variable=var_cu,
                    onvalue='currents', offvalue=0).grid(
        row=3, column=2, sticky='w', padx=padx, pady=(0, pady))

    # === Advanced ====================================================
    adv_frame = _section('Advanced')
    adv_frame.grid(row=section_row, column=0, columnspan=4,
                   sticky='ew', padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    # Forecast horizon skill, -hs
    horizon_var = tk.BooleanVar(value=False)
    horizon_lbl = _label(
        adv_frame, 'Assess all forecast horizons?',
        help_text='If Yes, assesses every forecast horizon between '
                  'start and end dates. Only works with Station file '
                  'type.')
    horizon_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(adv_frame, text='No (default)', variable=horizon_var,
                    value=False).grid(row=0, column=1, sticky='w',
                                      padx=padx, pady=pady)
    ttk.Radiobutton(adv_frame, text='Yes', variable=horizon_var,
                    value=True).grid(row=0, column=2, sticky='w',
                                     padx=padx, pady=pady)

    # Currents bins CSV, -cb
    cb_var = tk.StringVar()
    _label(adv_frame, 'Currents bins CSV (Optional)',
           help_text='Optional CSV that pins which CO-OPS ADCP bins are '
                     'processed and overrides their depth/orientation/'
                     'name. Columns: station_id,bin,depth,orientation,'
                     'name.').grid(row=1, column=0, sticky='w',
                                   padx=padx, pady=pady)
    ttk.Button(adv_frame, text='Browse...', command=browse_csv_file,
               style='TButton').grid(row=1, column=1, sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(adv_frame, textvariable=cb_var).grid(
        row=1, column=2, sticky='w', padx=padx, pady=pady)

    # Model file check, -df
    # True means the pre-check IS performed (matches the
    # action='store_false' semantics of -df in create_1dplot.py).
    enable_file_check_var = tk.BooleanVar(value=True)
    _label(adv_frame, 'Pre-check for model output files?',
           help_text='If Yes (default), the run verifies model NetCDFs '
                     'exist before processing and exits early if not. '
                     'Disable only when supplying custom .prd/.obs '
                     'files without corresponding model outputs.').grid(
        row=2, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(adv_frame, text='No (disable check)',
                    variable=enable_file_check_var, value=False).grid(
        row=2, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(adv_frame, text='Yes (default)',
                    variable=enable_file_check_var, value=True).grid(
        row=2, column=2, sticky='w', padx=padx, pady=pady)

    # === Submit ======================================================
    submit_button = ttk.Button(scrollable_frame,
                               text='Run skill assessment!',
                               command=submit_and_close)
    submit_button.grid(row=section_row, column=0, columnspan=4,
                       pady=(15, 15))
    toggle_cycle_state()
    root.mainloop()
    return args_values
