"""
Tkinter GUI for the GLOFS ice skill assessment pipeline.

Wraps ``bin/skill_assessment/do_iceskill.py``. Collects Great-Lakes OFS,
date range, whichcasts, daily-average flag, timestep, and config.

Author: TSR
"""
from __future__ import annotations

import logging
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from types import SimpleNamespace

from ofs_skill.visualization.gui_helpers import (
    GREAT_LAKES_OFS,
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

_OFS_PLACEHOLDER = 'Select a Great Lakes OFS...'
_WINDOW_GEOMETRY = '700x520'

_ICE_OFS_CHOICES = (_OFS_PLACEHOLDER,) + GREAT_LAKES_OFS


def create_gui_ice(runner=None):
    """Build and display the GLOFS ice skill assessment GUI.

    Parameters
    ----------
    runner : callable, optional
        ``runner(params)`` executing the ice pipeline in a background thread.
    """
    _runner_exception: list[BaseException | None] = [None]
    theme = GuiTheme()
    padx, pady = theme.padx, theme.pady
    fontfamily = theme.fontfamily
    widgetfontsize = theme.widgetfontsize

    root = tk.Tk()
    root.title('GLOFS Ice Skill Assessment')
    root.geometry(_WINDOW_GEOMETRY)
    root.minsize(600, 420)
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

    gen = form_section(scrollable_frame, theme, 'General Settings')
    gen.grid(row=section_row, column=0, sticky='ew',
             padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    directory_path_var = tk.StringVar()
    form_label(gen, theme, 'Home directory',
               help_text='Root working directory for the ice assessment.').grid(
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
    form_label(
        gen, theme, 'OFS',
        help_text='Great Lakes OFS to assess for ice skill. Only Great '
                  'Lakes models produce ice output.',
    ).grid(row=2, column=0, sticky='w', padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(
        gen, width=18, textvariable=ofs_var,
        font=(fontfamily, widgetfontsize), state='readonly',
        values=_ICE_OFS_CHOICES,
    )
    ofs_chosen.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)

    time_frame = form_section(scrollable_frame, theme, 'Time Range')
    time_frame.grid(row=section_row, column=0, sticky='ew',
                    padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    form_label(time_frame, theme, 'Start date',
               help_text='Ice assessment start date (UTC).').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    start_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    start_entry.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)

    form_label(time_frame, theme, 'End date',
               help_text='Ice assessment end date (UTC).').grid(
        row=1, column=0, sticky='w', padx=padx, pady=pady)
    end_entry = DateEntry(
        time_frame, width=16, background=theme.datefield_bg,
        foreground=theme.datefield_fg, bd=2, date_pattern='yyyy-mm-dd',
        font=(fontfamily, widgetfontsize),
    )
    end_entry.grid(row=1, column=1, sticky='w', padx=padx, pady=pady)

    ice_frame = form_section(scrollable_frame, theme, 'Ice Parameters')
    ice_frame.grid(row=section_row, column=0, sticky='ew',
                   padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    var_now = tk.StringVar(value='nowcast')
    var_fb = tk.StringVar(value='0')
    var_hc = tk.StringVar(value='0')
    whichcast_lbl = form_label(
        ice_frame, theme, 'Whichcasts',
        help_text='Run mode. Ice skill supports nowcast, forecast_b, '
                  'and hindcast.',
    )
    whichcast_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(ice_frame, text='Nowcast', variable=var_now,
                    onvalue='nowcast', offvalue='0').grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(ice_frame, text='Forecast_b', variable=var_fb,
                    onvalue='forecast_b', offvalue='0').grid(
        row=0, column=2, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(ice_frame, text='Hindcast', variable=var_hc,
                    onvalue='hindcast', offvalue='0').grid(
        row=1, column=1, sticky='w', padx=padx, pady=pady)

    daily_avg_var = tk.BooleanVar(value=False)
    form_label(
        ice_frame, theme, 'Daily average',
        help_text='Compute daily-averaged ice concentration before '
                  'comparing to GLSEA observations.',
    ).grid(row=2, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(ice_frame, text='Enabled', variable=daily_avg_var).grid(
        row=2, column=1, sticky='w', padx=padx, pady=pady)

    timestep_var = tk.StringVar(value='daily')
    form_label(
        ice_frame, theme, 'Time step',
        help_text='Assessment time resolution: hourly or daily '
                  '(daily is the CLI default).',
    ).grid(row=3, column=0, sticky='w', padx=padx, pady=pady)
    timestep_combo = ttk.Combobox(
        ice_frame, width=10, textvariable=timestep_var,
        values=('daily', 'hourly'), state='readonly',
        font=(fontfamily, widgetfontsize),
    )
    timestep_combo.grid(row=3, column=1, sticky='w', padx=padx, pady=pady)

    apply_gui_session(
        directory_var=directory_path_var,
        config_var=config_path_var,
        ofs_var=ofs_var,
        ofs_choices=_ICE_OFS_CHOICES,
        ofs_placeholder=_OFS_PLACEHOLDER,
        start_entry=start_entry,
        end_entry=end_entry,
    )
    validation.wire_live_dates(start_entry, end_entry)

    params = SimpleNamespace(
        OFS=None, Path=None, StartDate_full=None, EndDate_full=None,
        Whichcasts=None, DailyAverage=None, TimeStep=None, config=None,
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
            ('Daily average', 'Yes' if params.DailyAverage else 'No'),
            ('Time step', params.TimeStep or '\u2014'),
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
        var_fb.set('0')
        var_hc.set('0')
        daily_avg_var.set(False)
        timestep_var.set('daily')
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
            messagebox.showerror('Error', 'Please select a Great Lakes OFS.')
            return
        whichcasts = [v.get() for v in (var_now, var_fb, var_hc) if v.get() != '0']
        if not whichcasts:
            validation.mark_invalid(whichcast_lbl)
            messagebox.showerror('Error', 'Select at least one whichcast.')
            return
        if timestep_var.get() not in ('daily', 'hourly'):
            validation.mark_invalid(timestep_combo)
            messagebox.showerror('Error', 'Time step must be daily or hourly.')
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
        params.DailyAverage = daily_avg_var.get()
        params.TimeStep = timestep_var.get()
        params.config = config_path_var.get().strip() or None
        if not show_summary_confirmation(
            root, theme,
            window_title='Confirm ice skill assessment run',
            heading='Please review your run:',
            rows=_format_summary_rows(),
        ):
            return
        launch_with_progress(
            root, scrollable_frame, theme,
            runner=runner,
            params=params,
            progress_title='Running GLOFS ice skill assessment...',
            runner_exception=_runner_exception,
            before_run=_persist_before_run,
        )

    button_row = create_action_buttons(
        scrollable_frame,
        reset_command=reset_to_defaults,
        submit_command=submit,
        submit_text='Run ice skill assessment!',
    )
    button_row.grid(row=section_row, column=0, columnspan=4, pady=(15, 15))

    root.mainloop()
    if _runner_exception[0] is not None:
        raise _runner_exception[0]
    return params
