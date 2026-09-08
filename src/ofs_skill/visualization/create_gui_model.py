"""
Tkinter GUI for model time-series extraction at specific nodes/stations.

Wraps ``bin/model_processing/get_node_cli.py``. Collects OFS, date range,
datum, whichcasts, file type, forecast hour, variables, and optional flags.

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
    collapsible_section,
    configure_gui_styles,
    create_action_buttons,
    form_label,
    form_section,
    launch_with_progress,
    load_recent_paths,
    persist_gui_session_from_run,
    read_datum_list,
    set_date_entry_today,
    setup_scrollable_form,
    show_summary_confirmation,
    validate_date_order,
    validate_start_not_future,
)

_OFS_PLACEHOLDER = 'Select an OFS...'
_DATUM_PLACEHOLDER = 'Select a datum...'
_WINDOW_GEOMETRY = '780x680'

_OFS_CHOICES = (
    _OFS_PLACEHOLDER,
    'cbofs', 'ciofs', 'dbofs', 'gomofs', 'leofs', 'lmhofs', 'loofs',
    'loofs2', 'lsofs', 'necofs', 'ngofs2', 'secofs', 'sfbofs', 'sscofs',
    'stofs_2d_glo', 'stofs_3d_atl', 'stofs_3d_pac', 'tbofs', 'wcofs',
)


def create_gui_model(runner=None):
    """Build and display the model time-series extraction GUI.

    Parameters
    ----------
    runner : callable, optional
        ``runner(params)`` executing the extraction in a background thread.
    """
    _runner_exception: list[BaseException | None] = [None]
    theme = GuiTheme()
    padx, pady = theme.padx, theme.pady
    fontfamily = theme.fontfamily
    widgetfontsize = theme.widgetfontsize

    root = tk.Tk()
    root.title('Model Time Series Extraction')
    root.geometry(_WINDOW_GEOMETRY)
    root.minsize(700, 580)
    root.config(bg=theme.themecolor)

    configure_gui_styles(root, theme)
    apply_window_icon(root, logging.getLogger(__name__))
    validation = GuiValidation()
    datum_list = read_datum_list()
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
               help_text='Root working directory.').grid(
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
        help_text='Operational Forecast System to extract model output from.',
    ).grid(row=2, column=0, sticky='w', padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(
        gen, width=15, textvariable=ofs_var,
        font=(fontfamily, widgetfontsize), state='readonly',
        values=_OFS_CHOICES,
    )
    ofs_chosen.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)

    time_frame = form_section(scrollable_frame, theme, 'Time Range')
    time_frame.grid(row=section_row, column=0, sticky='ew',
                    padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    form_label(time_frame, theme, 'Start date & hour',
               help_text='Extraction start (UTC).').grid(
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

    form_label(time_frame, theme, 'End date & hour',
               help_text='Extraction end (UTC).').grid(
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

    df_frame = form_section(scrollable_frame, theme, 'Datum & OFS File Type')
    df_frame.grid(row=section_row, column=0, sticky='ew',
                  padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    datum_var = tk.StringVar(value=_DATUM_PLACEHOLDER)
    dchoices = (_DATUM_PLACEHOLDER,) + tuple(datum_list)
    form_label(df_frame, theme, 'Vertical datum',
               help_text='Datum for water-level adjustment.').grid(
        row=0, column=0, sticky='w', padx=padx, pady=pady)
    datum_chosen = ttk.Combobox(
        df_frame, width=15, textvariable=datum_var,
        font=(fontfamily, widgetfontsize), values=dchoices,
    )
    datum_chosen.grid(row=0, column=1, sticky='w', padx=padx, pady=pady)

    filetype_var = tk.StringVar(value='stations')
    form_label(
        df_frame, theme, 'File type',
        help_text='stations: station-format NetCDF; fields: gridded output.',
    ).grid(row=1, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(df_frame, text='Stations', variable=filetype_var,
                    value='stations').grid(
        row=1, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(df_frame, text='Fields', variable=filetype_var,
                    value='fields').grid(
        row=1, column=2, sticky='w', padx=padx, pady=pady)

    wf_frame = form_section(scrollable_frame, theme, 'Whichcasts & Forecast')
    wf_frame.grid(row=section_row, column=0, sticky='ew',
                  padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    var_now = tk.StringVar(value='nowcast')
    var_fa = tk.StringVar(value='0')
    var_fb = tk.StringVar(value='forecast_b')
    var_hc = tk.StringVar(value='0')
    whichcast_lbl = form_label(
        wf_frame, theme, 'Whichcasts',
        help_text='Select one or more model run modes.',
    )
    whichcast_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wf_frame, text='Nowcast', variable=var_now,
                    onvalue='nowcast', offvalue='0').grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wf_frame, text='Forecast_a', variable=var_fa,
                    onvalue='forecast_a', offvalue='0').grid(
        row=0, column=2, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wf_frame, text='Forecast_b', variable=var_fb,
                    onvalue='forecast_b', offvalue='0').grid(
        row=1, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wf_frame, text='Hindcast', variable=var_hc,
                    onvalue='hindcast', offvalue='0').grid(
        row=1, column=2, sticky='w', padx=padx, pady=pady)

    forecast_hr_var = tk.IntVar(value=-999)
    form_label(
        wf_frame, theme, 'Forecast hour',
        help_text='Specific forecast hour (use -999 for all).',
    ).grid(row=2, column=0, sticky='w', padx=padx, pady=pady)
    forecast_hr_spin = ttk.Spinbox(
        wf_frame, from_=-999, to=120, width=5,
        textvariable=forecast_hr_var, font=(fontfamily, widgetfontsize),
    )
    forecast_hr_spin.grid(row=2, column=1, sticky='w', padx=padx, pady=pady)

    horizon_var = tk.BooleanVar(value=False)
    form_label(
        wf_frame, theme, 'Assess all forecast horizons?',
        help_text='Assess each forecast lead time independently.',
    ).grid(row=3, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(wf_frame, text='Yes', variable=horizon_var).grid(
        row=3, column=1, sticky='w', padx=padx, pady=pady)

    vars_frame = form_section(scrollable_frame, theme, 'Variables')
    vars_frame.grid(row=section_row, column=0, sticky='ew',
                    padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    var_wl = tk.StringVar(value='water_level')
    var_temp = tk.StringVar(value='water_temperature')
    var_salt = tk.StringVar(value='salinity')
    var_cu = tk.StringVar(value='currents')
    variables_lbl = form_label(
        vars_frame, theme, 'Variables',
        help_text='Oceanographic variables to extract from model output.',
    )
    variables_lbl.grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(vars_frame, text='Water level', variable=var_wl,
                    onvalue='water_level', offvalue='0').grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(vars_frame, text='Temperature', variable=var_temp,
                    onvalue='water_temperature', offvalue='0').grid(
        row=0, column=2, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(vars_frame, text='Salinity', variable=var_salt,
                    onvalue='salinity', offvalue='0').grid(
        row=1, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(vars_frame, text='Currents', variable=var_cu,
                    onvalue='currents', offvalue='0').grid(
        row=1, column=2, sticky='w', padx=padx, pady=pady)

    adv_outer, adv_frame = collapsible_section(
        scrollable_frame, theme, 'Advanced', expanded=False,
    )
    adv_outer.grid(row=section_row, column=0, sticky='ew',
                   padx=theme.section_padx, pady=theme.section_pady)
    section_row += 1

    user_input_var = tk.BooleanVar(value=False)
    form_label(
        adv_frame, theme, 'User-input location',
        help_text='Supply custom X/Y coordinates instead of station list.',
    ).grid(row=0, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(adv_frame, text='Enabled', variable=user_input_var).grid(
        row=0, column=1, sticky='w', padx=padx, pady=pady)

    apply_gui_session(
        directory_var=directory_path_var,
        config_var=config_path_var,
        ofs_var=ofs_var,
        ofs_choices=_OFS_CHOICES,
        ofs_placeholder=_OFS_PLACEHOLDER,
        start_entry=start_entry,
        end_entry=end_entry,
        s_hour_var=s_hour_var,
        e_hour_var=e_hour_var,
        datum_var=datum_var,
        datum_choices=dchoices,
        datum_placeholder=_DATUM_PLACEHOLDER,
    )
    validation.wire_live_dates(
        start_entry, end_entry,
        s_hour_var=s_hour_var, e_hour_var=e_hour_var,
        s_hour_spin=s_hour_spin, e_hour_spin=e_hour_spin,
    )

    params = SimpleNamespace(
        OFS=None, Path=None, StartDate_full=None, EndDate_full=None,
        Datum=None, Whichcasts=None, OFS_Filetype=None, Forecast_Hr=None,
        Var_Selection=None, Horizon_Skill=None, UserInput=None, config=None,
    )

    def _format_summary_rows():
        return [
            ('Home directory', params.Path or '\u2014'),
            ('Config', params.config or '(default conf/ofs_dps.conf)'),
            ('OFS', params.OFS or '\u2014'),
            ('Start (UTC)', params.StartDate_full or '\u2014'),
            ('End (UTC)', params.EndDate_full or '\u2014'),
            ('Datum', params.Datum or '\u2014'),
            ('File type', params.OFS_Filetype or '\u2014'),
            ('Whichcasts', ', '.join(params.Whichcasts)
             if params.Whichcasts else '\u2014'),
            ('Forecast hour', str(params.Forecast_Hr)),
            ('Variables', ', '.join(params.Var_Selection)
             if params.Var_Selection else '\u2014'),
            ('All forecast horizons', 'Yes' if params.Horizon_Skill else 'No'),
            ('User-input location', 'Yes' if params.UserInput else 'No'),
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
            Datum=params.Datum,
        )

    def reset_to_defaults():
        directory_path_var.set('')
        config_path_var.set('conf/ofs_dps.conf')
        ofs_var.set(_OFS_PLACEHOLDER)
        datum_var.set(_DATUM_PLACEHOLDER)
        filetype_var.set('stations')
        set_date_entry_today(start_entry)
        set_date_entry_today(end_entry)
        s_hour_var.set(0)
        e_hour_var.set(0)
        var_now.set('nowcast')
        var_fa.set('0')
        var_fb.set('forecast_b')
        var_hc.set('0')
        forecast_hr_var.set(-999)
        horizon_var.set(False)
        var_wl.set('water_level')
        var_temp.set('water_temperature')
        var_salt.set('salinity')
        var_cu.set('currents')
        user_input_var.set(False)
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
        if datum_var.get() == _DATUM_PLACEHOLDER:
            validation.mark_invalid(datum_chosen)
            messagebox.showerror('Error', 'Please select a datum.')
            return

        whichcasts = [v.get() for v in (var_now, var_fa, var_fb, var_hc)
                      if v.get() != '0']
        if not whichcasts:
            validation.mark_invalid(whichcast_lbl)
            messagebox.showerror('Error', 'Select at least one whichcast.')
            return
        variables = [v.get() for v in (var_wl, var_temp, var_salt, var_cu)
                     if v.get() != '0']
        if not variables:
            validation.mark_invalid(variables_lbl)
            messagebox.showerror('Error', 'Select at least one variable.')
            return

        start_dt = build_utc_datetime(start_entry.get_date(), s_hour_var.get())
        end_dt = build_utc_datetime(end_entry.get_date(), e_hour_var.get())
        msg = validate_date_order(start_dt, end_dt)
        if msg:
            validation.mark_invalid(
                start_entry, end_entry, s_hour_spin, e_hour_spin,
            )
            messagebox.showerror('Error', msg)
            return
        msg = validate_start_not_future(start_dt)
        if msg:
            validation.mark_invalid(start_entry, s_hour_spin)
            messagebox.showerror('Error', msg)
            return

        params.Path = directory_path_var.get()
        params.OFS = ofs_var.get()
        params.StartDate_full = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        params.EndDate_full = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        params.Datum = datum_var.get()
        params.Whichcasts = whichcasts
        params.OFS_Filetype = filetype_var.get()
        params.Forecast_Hr = forecast_hr_var.get()
        params.Var_Selection = variables
        params.Horizon_Skill = horizon_var.get()
        params.UserInput = user_input_var.get()
        params.config = config_path_var.get().strip() or None
        if not show_summary_confirmation(
            root, theme,
            window_title='Confirm model extraction run',
            heading='Please review your run:',
            rows=_format_summary_rows(),
        ):
            return
        launch_with_progress(
            root, scrollable_frame, theme,
            runner=runner,
            params=params,
            progress_title='Extracting model time series...',
            runner_exception=_runner_exception,
            before_run=_persist_before_run,
        )

    button_row = create_action_buttons(
        scrollable_frame,
        reset_command=reset_to_defaults,
        submit_command=submit,
        submit_text='Extract model time series!',
    )
    button_row.grid(row=section_row, column=0, columnspan=4, pady=(15, 15))

    root.mainloop()
    if _runner_exception[0] is not None:
        raise _runner_exception[0]
    return params
