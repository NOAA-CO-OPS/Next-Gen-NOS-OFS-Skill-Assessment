"""
Tkinter GUI for the 2D (field-based) skill assessment pipeline.

Wraps ``bin/visualization/create_2dplot.py``. Collects OFS, date range,
whichcasts, home directory, and config file; then either returns a params
namespace or drives the pipeline via a threaded runner callback.

Author: TSR
"""
from __future__ import annotations

import logging
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from types import SimpleNamespace

from ofs_skill.visualization.gui_helpers import (
    DateEntry,
    GuiTheme,
    GuiValidation,
    add_to_recent,
    apply_gui_session,
    apply_window_icon,
    build_utc_datetime,
    configure_gui_styles,
    create_action_buttons,
    form_label,
    form_section,
    launch_with_progress,
    load_recent_paths,
    persist_gui_session_from_run,
    set_date_entry_today,
    setup_scrollable_form,
    show_summary_confirmation,
    validate_date_order,
    validate_start_not_future,
)

_OFS_PLACEHOLDER = 'Select an OFS...'
_WINDOW_GEOMETRY = '750x480'

_OFS_CHOICES = (
    _OFS_PLACEHOLDER,
    'cbofs', 'ciofs', 'dbofs', 'gomofs', 'leofs', 'lmhofs', 'loofs',
    'loofs2', 'lsofs', 'necofs', 'ngofs2', 'secofs', 'sfbofs', 'sscofs',
    'stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac', 'tbofs', 'wcofs',
)


def create_gui_2d(runner=None):
    """Build and display the 2D skill assessment GUI.

    Parameters
    ----------
    runner : callable, optional
        ``runner(params)`` executing the 2D pipeline in a background thread.
    """
    _runner_exception: list[BaseException | None] = [None]
    theme = GuiTheme()
    padx, pady = theme.padx, theme.pady
    fontfamily = theme.fontfamily
    widgetfontsize = theme.widgetfontsize

    root = tk.Tk()
    root.title('2D Skill Assessment')
    root.geometry(_WINDOW_GEOMETRY)
    root.minsize(650, 380)
    root.config(bg=theme.themecolor)

    configure_gui_styles(root, theme)
    apply_window_icon(root, logging.getLogger(__name__))
    validation = GuiValidation()
    _recent = load_recent_paths()
    scroll = setup_scrollable_form(root, theme.themecolor)
    scrollable_frame = scroll.frame

    def on_closing():
        if messagebox.askokcancel('Quit', 'Do you want to quit?'):
            root.destroy()
            sys.exit()

    root.protocol('WM_DELETE_WINDOW', on_closing)

    section_row = 0

    # === General Settings ================================================
    gen = form_section(scrollable_frame, theme, 'General Settings')
    gen.grid(row=section_row, column=0, sticky='ew',
             padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    directory_path_var = tk.StringVar()
    form_label(gen, theme, 'Home directory',
               help_text='Root working directory for the 2D assessment.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)

    def browse_directory():
        d = filedialog.askdirectory()
        if d:
            directory_path_var.set(d)

    ttk.Button(gen, text='Browse...', command=browse_directory).grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)
    directory_entry = ttk.Combobox(
        gen, textvariable=directory_path_var, width=40,
        values=_recent.get('home_directory', []),
    )
    directory_entry.grid(row=0, column=2, sticky='ew', padx=padx, pady=pady)

    config_path_var = tk.StringVar(value='conf/ofs_dps.conf')
    form_label(gen, theme, 'Config file',
               help_text='Path to the .conf configuration file.').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)

    def browse_config():
        f = filedialog.askopenfilename(
            title='Select config',
            filetypes=(('Config', '*.conf'), ('All', '*.*')),
        )
        if f:
            config_path_var.set(f)

    ttk.Button(gen, text='Browse...', command=browse_config).grid(
        row=1, column=1, sticky='w', padx=padx, pady=pady)
    config_entry = ttk.Combobox(
        gen, textvariable=config_path_var, width=40,
        values=_recent.get('config_file', []),
    )
    config_entry.grid(row=1, column=2, sticky='ew', padx=padx, pady=pady)

    ofs_var = tk.StringVar(value=_OFS_PLACEHOLDER)
    form_label(gen, theme, 'OFS',
               help_text='Operational Forecast System to assess with 2D fields.').grid(
        row=2, column=0, sticky='w', padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(
        gen, width=15, textvariable=ofs_var,
        font=(fontfamily, widgetfontsize), state='readonly',
        values=_OFS_CHOICES,
    )
    ofs_chosen.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)

    # === Time Range ======================================================
    time_frame = form_section(scrollable_frame, theme, 'Time Range')
    time_frame.grid(row=section_row, column=0, sticky='ew',
                    padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    form_label(time_frame, theme, 'Start date',
               help_text='Assessment start date (UTC).').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    start_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    start_entry.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)

    form_label(time_frame, theme, 'End date',
               help_text='Assessment end date (UTC).').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)
    end_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    end_entry.grid(row=1, column=1, sticky='w', padx=padx, pady=pady)

    # === Whichcasts ======================================================
    wcast_frame = form_section(scrollable_frame, theme, 'Whichcasts')
    wcast_frame.grid(row=section_row, column=0, sticky='ew',
                     padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    var_now = tk.StringVar(value='nowcast')
    var_fore = tk.StringVar(value='forecast_b')
    whichcast_lbl = form_label(
        wcast_frame, theme, 'Whichcasts',
        help_text='Run mode. 2D supports nowcast and forecast_b.',
    )
    whichcast_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wcast_frame, text='Nowcast', variable=var_now,
                    onvalue='nowcast', offvalue='0').grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wcast_frame, text='Forecast_b', variable=var_fore,
                    onvalue='forecast_b', offvalue='0').grid(
        row=0, column=2, sticky='w', padx=padx, pady=pady)

    apply_gui_session(
        directory_var=directory_path_var,
        config_var=config_path_var,
        ofs_var=ofs_var,
        ofs_choices=_OFS_CHOICES,
        ofs_placeholder=_OFS_PLACEHOLDER,
        start_entry=start_entry,
        end_entry=end_entry,
    )
    validation.wire_live_dates(start_entry, end_entry)

    params = SimpleNamespace(
        OFS=None, Path=None, StartDate_full=None, EndDate_full=None,
        Whichcasts=None, config=None,
    )

    def _format_summary_rows():
        return [
            ('Home directory', params.Path or '\u2014'),
            ('Config', params.config or '(default conf/ofs_dps.conf)'),
            ('OFS', params.OFS or '\u2014'),
            ('Start (UTC)', params.StartDate_full or '\u2014'),
            ('End (UTC)', params.EndDate_full or '\u2014'),
            ('Whichcasts', ', '.join(params.Whichcasts)
             if params.Whichcasts else '\u2014'),
        ]

    def _persist_before_run():
        add_to_recent('home_directory', params.Path)
        add_to_recent('config_file', params.config)
        persist_gui_session_from_run(
            Path=params.Path,
            config=params.config,
            OFS=params.OFS,
            StartDate_full=params.StartDate_full,
            EndDate_full=params.EndDate_full,
        )

    def reset_to_defaults():
        directory_path_var.set('')
        config_path_var.set('conf/ofs_dps.conf')
        ofs_var.set(_OFS_PLACEHOLDER)
        set_date_entry_today(start_entry)
        set_date_entry_today(end_entry)
        var_now.set('nowcast')
        var_fore.set('forecast_b')
        validation.clear_invalid()
        validation.reset_dates_touched()

    def submit():
        validation.clear_invalid()
        if not directory_path_var.get():
            validation.mark_invalid(directory_entry)
            messagebox.showerror('Error', 'Please select a home directory.')
            return
        if ofs_var.get() == _OFS_PLACEHOLDER:
            validation.mark_invalid(ofs_chosen)
            messagebox.showerror('Error', 'Please select an OFS.')
            return
        whichcasts = [v.get() for v in (var_now, var_fore) if v.get() != '0']
        if not whichcasts:
            validation.mark_invalid(whichcast_lbl)
            messagebox.showerror('Error', 'Select at least one whichcast.')
            return

        start_dt = build_utc_datetime(start_entry.get_date(), 0)
        end_dt = build_utc_datetime(end_entry.get_date(), 0)
        msg = validate_date_order(start_dt, end_dt)
        if msg:
            validation.mark_invalid(start_entry, end_entry)
            messagebox.showerror('Error', msg)
            return
        msg = validate_start_not_future(start_dt)
        if msg:
            validation.mark_invalid(start_entry)
            messagebox.showerror('Error', msg)
            return

        params.Path = directory_path_var.get()
        params.OFS = ofs_var.get()
        params.StartDate_full = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        params.EndDate_full = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        params.Whichcasts = whichcasts
        params.config = config_path_var.get().strip() or None
        if not show_summary_confirmation(
            root, theme,
            window_title='Confirm 2D skill assessment run',
            heading='Please review your run:',
            rows=_format_summary_rows(),
        ):
            return
        launch_with_progress(
            root, scrollable_frame, theme,
            runner=runner,
            params=params,
            progress_title='Running 2D skill assessment...',
            runner_exception=_runner_exception,
            before_run=_persist_before_run,
        )

    button_row = create_action_buttons(
        scrollable_frame,
        reset_command=reset_to_defaults,
        submit_command=submit,
        submit_text='Run 2D skill assessment!',
    )
    button_row.grid(row=section_row, column=0, columnspan=4, pady=(15, 15))

    root.mainloop()
    if _runner_exception[0] is not None:
        raise _runner_exception[0]
    return params
