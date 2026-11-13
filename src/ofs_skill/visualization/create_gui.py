"""
Created on Wed Nov 12 08:39:35 2025

@author: PWL
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import tkinter as tk
import traceback
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font

from ofs_skill.model_processing.get_fcst_cycle import get_fcst_hours
from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization.gui_helpers import (
    DateEntry,
    GuiParams,
    GuiTheme,
    ToolTip,
    add_to_recent,
    apply_gui_session,
    build_utc_datetime,
    compute_recent_cycle,
    format_date,
    load_recent_paths,
    persist_gui_session_from_run,
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


def create_gui(parser, runner=None):
    """Build and run the skill-assessment GUI.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Kept for backwards compatibility (unused in the body).
    runner : callable, optional
        ``runner(args_values)`` that executes the assessment pipeline.
        When provided, the GUI replaces the form with a live progress
        overlay that stays visible until the pipeline finishes; the
        runner is executed in a background thread so the progressbar
        keeps animating. If omitted, the GUI keeps its legacy behaviour
        (collect args, briefly flash a launch indicator, return args).
    """

    # Captures exceptions raised in the runner thread so they can be
    # re-raised in the main thread after Tk's mainloop exits.
    _runner_exception: list[BaseException | None] = [None]

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

    # Set once the user has interacted with any Time Range widget; live
    # validation only paints date/hour highlights after this flips so
    # the form does not light up on initial open.
    _dates_touched = [False]

    def _touch_dates(*_args):
        """Mark Time Range as user-touched and re-run live validation."""
        _dates_touched[0] = True
        _live_validate_dates()

    def _live_validate_dates():
        """Recompute Time Range validations and update field highlights.

        Mirrors the date-related rules in submit_and_close() but never
        raises a messagebox — submit-time popups are unchanged.
        """
        desired: set[tk.Widget] = set()

        if _dates_touched[0]:
            try:
                start_dt = build_utc_datetime(
                    start_entry.get_date(), s_hour_var.get()
                )
                end_dt = build_utc_datetime(
                    end_entry.get_date(), e_hour_var.get()
                )
            except (ValueError, AttributeError):
                start_dt = end_dt = None
            if start_dt is not None and end_dt is not None:
                if validate_date_order(start_dt, end_dt) is not None:
                    desired.update(
                        [start_entry, end_entry, s_hour_spin, e_hour_spin]
                    )
                elif validate_start_not_future(start_dt) is not None:
                    desired.update([start_entry, s_hour_spin])

        # Diff against the Time Range widgets only so we don't disturb
        # any submit-time highlights on other sections.
        date_widgets = {
            start_entry, end_entry, s_hour_spin, e_hour_spin,
        }
        current = set(_invalid_widgets) & date_widgets
        for w in current - desired:
            try:
                cls = w.winfo_class()
                if cls in _ERROR_TTK_CLASSES:
                    w.configure(style=cls)
            except tk.TclError:
                pass
            _invalid_widgets.remove(w)

        new_invalid = desired - current
        if new_invalid:
            _mark_invalid(*new_invalid)

    def _format_summary_rows(av):
        """Build (label, value) rows for the run-summary preview dialog."""
        return [
            ('Home directory',     av.Path or '\u2014'),
            ('Config',             av.config or '(default conf/ofs_dps.conf)'),
            ('OFS',                av.OFS or '\u2014'),
            ('Start (UTC)',        av.StartDate_full or '\u2014'),
            ('End (UTC)',          av.EndDate_full or '\u2014'),
            ('Whichcasts',
                ', '.join(av.Whichcasts) if av.Whichcasts else '\u2014'),
            ('Forecast cycle',     str(av.Forecast_Hr) if av.Forecast_Hr
                                   else '\u2014'),
            ('Datum',              av.Datum or '\u2014'),
            ('File type',          av.FileType or '\u2014'),
            ('Station providers',
                ', '.join(av.Station_Owner) if av.Station_Owner
                else '\u2014'),
            ('Variables',
                ', '.join(av.Var_Selection) if av.Var_Selection
                else '\u2014'),
            ('All forecast horizons',
                'Yes' if av.Horizon_Skill else 'No'),
            ('Currents bins CSV',  av.Currents_Bins_Csv or '(none)'),
            ('Pre-check model files',
                'Enabled' if av.Disable_Model_File_Check else 'Disabled'),
        ]

    def _show_summary_confirmation() -> bool:
        """Modal confirm dialog that previews the run summary.

        Returns True if the user clicks Launch, False if they cancel
        (so the caller can return the user to the form to edit).
        """
        confirmed = [False]
        win = tk.Toplevel(root)
        win.title('Confirm skill assessment run')
        win.transient(root)
        win.configure(bg=themecolor)
        win.resizable(False, False)

        ttk.Label(
            win, text='Please review your skill assessment run:',
            font=theme.section_title_font,
        ).pack(padx=20, pady=(15, 8), anchor='w')

        table = ttk.Frame(win)
        table.pack(padx=20, pady=(0, 10), anchor='w')
        bold_font = (fontfamily, widgetfontsize, 'bold')
        for i, (key, value) in enumerate(_format_summary_rows(args_values)):
            ttk.Label(table, text=f'{key}:', font=bold_font).grid(
                row=i, column=0, sticky='w', padx=(0, 12), pady=1
            )
            ttk.Label(table, text=str(value),
                      font=(fontfamily, widgetfontsize)).grid(
                row=i, column=1, sticky='w', pady=1
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
            side='right', padx=(8, 0))
        launch_btn = ttk.Button(btn_row, text='Launch', command=_launch)
        launch_btn.pack(side='right')
        launch_btn.focus_set()

        win.protocol('WM_DELETE_WINDOW', _cancel)
        win.bind('<Escape>', lambda _e: _cancel())
        win.bind('<Return>', lambda _e: _launch())

        # Center on parent and grab modal focus.
        win.update_idletasks()
        x = root.winfo_rootx() + (root.winfo_width() - win.winfo_width()) // 2
        y = root.winfo_rooty() + (root.winfo_height() - win.winfo_height()) // 2
        win.geometry(f'+{max(0, x)}+{max(0, y)}')
        win.grab_set()
        win.wait_window()
        return confirmed[0]

    def _start_launch_progress():
        """Replace the form with a centered "Running..." overlay.

        Hides every widget inside ``scrollable_frame``, then renders a
        single full-width frame with a heading and an indeterminate
        ttk.Progressbar so the user always sees launch feedback no
        matter where they were scrolled. Returns the progressbar so
        the caller can stop it before destroying the window.
        """
        for child in scrollable_frame.winfo_children():
            child.grid_remove()

        overlay = ttk.Frame(scrollable_frame, padding=(40, 60))
        overlay.grid(row=0, column=0, columnspan=4, sticky='nsew')
        scrollable_frame.grid_columnconfigure(0, weight=1)

        ttk.Label(
            overlay, text='Running skill assessment...',
            font=theme.section_title_font, anchor='center',
        ).pack(pady=(0, 15), fill='x')
        ttk.Label(
            overlay,
            text='Please keep this window open until the run finishes.',
            font=theme.hint_font, anchor='center',
        ).pack(pady=(0, 20), fill='x')

        progress = ttk.Progressbar(
            overlay, mode='indeterminate', length=400,
        )
        progress.pack()
        progress.start(10)
        root.configure(cursor='watch')
        root.update_idletasks()
        return progress

    def _run_pipeline_in_thread(progress):
        """Run ``runner(args_values)`` in a daemon thread and destroy
        the root once it completes. Captures any exception raised by
        the runner into ``_runner_exception`` for the main thread to
        re-raise after mainloop exits."""

        def _target():
            try:
                runner(args_values)
            except BaseException as exc:  # noqa: BLE001
                _runner_exception[0] = exc
                # Stream the traceback to the terminal immediately so
                # the user can see what happened even before the GUI
                # closes.
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
            root.destroy()

        root.after(200, _poll)

    def _launch():
        """Common launch path for both submit_and_close and quick_run_submit.

        With a runner: replace the form with the progress overlay and
        drive the pipeline in a background thread.
        Without a runner: legacy behaviour — flash a brief inline
        indicator below the submit button and destroy after 700ms.
        """
        # Persist the chosen paths so they appear in the recent-paths
        # dropdowns next session. add_to_recent silently no-ops on
        # empty/None values.
        add_to_recent('home_directory', args_values.Path)
        add_to_recent('config_file', args_values.config)
        add_to_recent('currents_bins_csv', args_values.Currents_Bins_Csv)
        persist_gui_session_from_run(
            Path=args_values.Path,
            config=args_values.config,
            OFS=args_values.OFS,
            StartDate_full=args_values.StartDate_full,
            EndDate_full=args_values.EndDate_full,
            Datum=args_values.Datum,
        )

        if runner is not None:
            progress = _start_launch_progress()
            _run_pipeline_in_thread(progress)
            return
        try:
            submit_button.configure(
                text='Launching skill assessment...', state='disabled'
            )
            inline = ttk.Progressbar(
                scrollable_frame, mode='indeterminate', length=300
            )
            inline.grid(row=section_row + 1, column=0, columnspan=4,
                        pady=(0, 15))
            inline.start(10)
            root.configure(cursor='watch')
            root.update_idletasks()
        except tk.TclError:
            pass
        root.after(700, root.destroy)

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
        args_values.Station_Owner = ['co-ops', 'ndbc', 'usgs', 'chs']
        args_values.Var_Selection = [
            'water_level', 'water_temperature', 'salinity', 'currents'
        ]
        args_values.Horizon_Skill = False
        args_values.Currents_Bins_Csv = None
        # Mirrors argparse: True means the model-file pre-check is performed.
        args_values.Disable_Model_File_Check = True
        args_values.config = config_path_var.get().strip() or None

        _launch()

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
              and var_usgs.get() == '0' and var_chs.get() == '0'
              and var_list.get() == '0'):
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
                var_usgs.get(), var_chs.get(), var_list.get(),
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
            # Show a summary preview so the user can review their inputs
            # before the GUI closes and the assessment starts.
            if not _show_summary_confirmation():
                return
            _launch()

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

    def reset_to_defaults():
        '''Restore every form field to its initial default value.

        Mirrors the StringVar/IntVar/BooleanVar initial values used when
        the form was first built, clears any error highlights, and
        re-applies the OFS-dependent enable/disable logic so the form
        looks exactly like a fresh launch.
        '''
        directory_path_var.set('')
        config_path_var.set('conf/ofs_dps.conf')
        ofs_entry.set(_OFS_PLACEHOLDER)
        cycle_var.set(_OFS_FIRST_PLACEHOLDER)
        datum_var.set(_DATUM_PLACEHOLDER)
        filetype_var.set('stations')

        today = datetime.now().date()
        try:
            start_entry.set_date(today)
            end_entry.set_date(today)
        except (tk.TclError, ValueError):
            pass
        s_hour_var.set(0)
        e_hour_var.set(0)

        var_now.set('nowcast')
        var_fore.set('forecast_b')
        var_forea.set('0')
        var_hind.set('0')

        var_coops.set('co-ops')
        var_ndbc.set('ndbc')
        var_usgs.set('usgs')
        var_chs.set('chs')
        var_list.set('0')

        var_wl.set('water_level')
        var_temp.set('water_temperature')
        var_salt.set('salinity')
        var_cu.set('currents')

        horizon_var.set(False)
        cb_var.set('')
        enable_file_check_var.set(True)

        _clear_invalid()
        _dates_touched[0] = False
        _last_ofs[0] = ''
        update_whichcast_state()
        toggle_cycle_state()

    root = tk.Tk()
    root.title('Skill assessment inputs')
    root.protocol('WM_DELETE_WINDOW', on_closing)
    root.geometry(_WINDOW_GEOMETRY)
    root.minsize(700, 400)

    style = ttk.Style(root)
    style.theme_use('clam')

    log = logging.getLogger(__name__)

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
    widgetfontsize = theme.widgetfontsize
    padx = theme.padx
    pady = theme.pady
    anchor = theme.anchor
    root.config(bg=themecolor)

    container = ttk.Frame(root)
    container.pack(fill='both', expand=True)
    canvas = tk.Canvas(container, bg=themecolor, highlightthickness=0)
    scrollbar = ttk.Scrollbar(container, orient='vertical', command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)
    scrollable_frame.bind(
        '<Configure>',
        lambda e: canvas.configure(scrollregion=canvas.bbox('all'))
    )
    _canvas_win = canvas.create_window((0, 0), window=scrollable_frame, anchor='nw')
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side='left', fill='both', expand=True)
    scrollbar.pack(side='right', fill='y')

    # Keep the scrollable frame width in sync with the canvas so section
    # frames that use sticky='ew' actually stretch to the window width.
    def _on_canvas_resize(event):
        canvas.itemconfigure(_canvas_win, width=event.width)

    canvas.bind('<Configure>', _on_canvas_resize)

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
    root.option_add('*TCombobox*Listbox*Font',
                    Font(family=fontfamily, size=widgetfontsize))

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

    # Recently-used paths (home dir, config file, currents bins CSV) so
    # users don't have to re-browse on every run; persisted on submit.
    _recent_paths = load_recent_paths()

    def _section(title):
        """Themed LabelFrame container for a group of related widgets."""
        frame = ttk.LabelFrame(
            scrollable_frame, text=title, padding=(padx, pady),
        )
        frame.columnconfigure(2, weight=1)
        return frame

    def _collapsible_section(title, expanded=False):
        """Like ``_section`` but the title bar toggles body visibility.

        Returns ``(outer, body)``: place ``outer`` with .grid() on the
        scrollable frame, and grid child widgets into ``body``. Clicking
        the title (or its disclosure triangle) hides/shows ``body``
        without affecting the rest of the form.
        """
        outer = ttk.Frame(scrollable_frame)
        state = [bool(expanded)]
        header_lbl = ttk.Label(
            outer, text='', cursor='hand2',
            font=theme.section_title_font,
        )
        header_lbl.pack(fill='x', anchor='w', padx=padx, pady=(pady, 4))
        body = ttk.Frame(outer, padding=(padx, pady))

        def _apply():
            prefix = '\u25bc' if state[0] else '\u25b6'
            header_lbl.configure(text=f'{prefix}  {title}')
            if state[0]:
                body.pack(fill='x', expand=True)
            else:
                body.pack_forget()

        def _toggle(_e=None):
            state[0] = not state[0]
            _apply()

        header_lbl.bind('<Button-1>', _toggle)
        _apply()
        return outer, body

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

    # Allow the scrollable frame columns to expand with the window.
    scrollable_frame.columnconfigure(0, weight=1)

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
                     'Output files are written under this path; it can '
                     'be anywhere, including an external disk. Input '
                     'assets (ofs_extents/, conf/) are found in the '
                     'installation directory automatically.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Button(general_frame, text='Browse...', command=browse_directory,
               style='TButton').grid(row=0, column=1, sticky='w',
                                     padx=padx, pady=pady)
    directory_entry = ttk.Combobox(
        general_frame, textvariable=directory_path_var, width=40,
        values=_recent_paths.get('home_directory', []),
    )
    directory_entry.grid(row=0, column=2, sticky='ew', padx=padx, pady=pady)

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
    config_entry = ttk.Combobox(
        general_frame, textvariable=config_path_var, width=40,
        values=_recent_paths.get('config_file', []),
    )
    config_entry.grid(row=1, column=2, sticky='ew', padx=padx, pady=pady)

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
    var_chs = tk.StringVar(value='chs')
    var_list = tk.StringVar(value='0')
    providers_lbl = _label(
        sv_frame, 'Station providers',
        help_text='Observation data retrieval sources. "Add from conf '
                  'file" lets you supply a custom list in the '
                  '[station_lists] section of the config file. CHS covers '
                  'Canadian Great Lakes stations.')
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
    ttk.Checkbutton(sv_frame, text='CHS', variable=var_chs,
                    onvalue='chs', offvalue=0).grid(
        row=1, column=2, sticky='w', padx=padx, pady=(0, pady))
    ttk.Checkbutton(sv_frame, text='Add from conf file', variable=var_list,
                    onvalue='list', offvalue=0).grid(
        row=2, column=1, sticky='w', padx=padx, pady=(0, pady))

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
    variables_lbl.grid(row=3, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(sv_frame, text='Water level', variable=var_wl,
                    onvalue='water_level', offvalue=0).grid(
        row=3, column=1, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='Temperature', variable=var_temp,
                    onvalue='water_temperature', offvalue=0).grid(
        row=3, column=2, sticky='w', padx=padx, pady=(pady, 2))
    ttk.Checkbutton(sv_frame, text='Salinity', variable=var_salt,
                    onvalue='salinity', offvalue=0).grid(
        row=4, column=1, sticky='w', padx=padx, pady=(0, pady))
    ttk.Checkbutton(sv_frame, text='Current velocity', variable=var_cu,
                    onvalue='currents', offvalue=0).grid(
        row=4, column=2, sticky='w', padx=padx, pady=(0, pady))

    # === Advanced ====================================================
    # Collapsed by default — most runs only need the General/Time/
    # Whichcasts/Datums/Stations sections above. Click the title to
    # reveal currents-bins CSV, forecast-horizon skill, and the
    # pre-check toggle.
    adv_outer, adv_frame = _collapsible_section('Advanced', expanded=False)
    adv_outer.grid(row=section_row, column=0, columnspan=4,
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
    cb_entry = ttk.Combobox(
        adv_frame, textvariable=cb_var, width=40,
        values=_recent_paths.get('currents_bins_csv', []),
    )
    cb_entry.grid(row=1, column=2, sticky='ew', padx=padx, pady=pady)

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
    # Bottom action row: Reset on the left (less destructive emphasis),
    # primary Run button on the right where the eye lands last.
    button_row = ttk.Frame(scrollable_frame)
    button_row.grid(row=section_row, column=0, columnspan=4,
                    pady=(15, 15))
    reset_button = ttk.Button(button_row, text='Reset to defaults',
                              command=reset_to_defaults)
    reset_button.pack(side='left', padx=(0, 20))
    submit_button = ttk.Button(button_row,
                               text='Run skill assessment!',
                               command=submit_and_close)
    submit_button.pack(side='left')

    # === Live validation wiring (Time Range only) ====================
    # Re-validate the start/end date+hour as the user edits them, so a
    # bad range (e.g. end before start, or start in the future) is
    # flagged the moment it appears rather than only at submit time.
    for _v in (s_hour_var, e_hour_var):
        _v.trace_add('write', _touch_dates)
    for _de in (start_entry, end_entry):
        _de.bind('<<DateEntrySelected>>', _touch_dates)
        _de.bind('<FocusOut>', _touch_dates, add='+')

    apply_gui_session(
        directory_var=directory_path_var,
        config_var=config_path_var,
        ofs_var=ofs_entry,
        ofs_choices=choices,
        ofs_placeholder=_OFS_PLACEHOLDER,
        start_entry=start_entry,
        end_entry=end_entry,
        s_hour_var=s_hour_var,
        e_hour_var=e_hour_var,
        datum_var=datum_var,
        datum_choices=dchoices,
        datum_placeholder=_DATUM_PLACEHOLDER,
    )

    toggle_cycle_state()
    root.mainloop()

    # If the runner raised inside the background thread, re-raise it
    # in the main thread so the script exits with the right traceback.
    if _runner_exception[0] is not None:
        raise _runner_exception[0]

    return args_values
