"""
Created on Wed Nov 12 08:39:35 2025

@author: PWL
"""
import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font
from types import SimpleNamespace

from tkcalendar import DateEntry

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils


def create_gui(parser):

    def on_closing():
        '''Function called when the user closes the window.'''
        if messagebox.askokcancel('Quit', 'Do you want to quit?'):
            # If the user confirms quitting, destroy the window
            root.destroy()
            print('Skill assessment run terminated by user.')
            sys.exit()

    def submit_and_close():
        # First check for required arguments and display error if not present
        error = None
        if directory_path_var.get() is None:
            messagebox.showerror('Error', 'Please select your home directory.')
            error = 1
        if ofs_entry.get() == choices[0]:
            messagebox.showerror('Error', 'Please select the OFS.')
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
        elif var_now.get() == '0' and var_fore.get() == '0':
             messagebox.showerror('Error', 'Please select at least one '
                                  'whichcast.')
             error = 1
        elif var_coops.get() == '0' and var_ndbc.get() == '0' and \
                 var_usgs.get() == '0' and var_list.get() == '0':
             messagebox.showerror('Error', 'Please select at least one '
                                  'station provider, or provide a list of '
                                  'station IDs.')
             error = 1
        elif var_salt.get() == '0' and var_cu.get() == '0' and \
                 var_temp.get() == '0' and var_wl.get() == '0':
             messagebox.showerror('Error', 'Please select at least one '
                                  'variable to assess.')
             error = 1
        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs_entry.get()
        args_values.StartDate_full = format_date(start_entry.get_date(),
                                                 s_hour_scale.get())
        args_values.EndDate_full = format_date(end_entry.get_date(),
                                               e_hour_scale.get())
        args_values.Whichcasts = [item for item in [var_now.get(), \
                                var_fore.get()] if item != '0']
        args_values.Datum = datum_var.get()
        args_values.FileType = filetype_var.get()
        args_values.Station_Owner = [item for item in [var_coops.get(), \
                                var_ndbc.get(), var_usgs.get(), var_list.get()] if \
                                     item != '0']
        args_values.Horizon_Skill = horizon_var.get()
        args_values.Var_Selection = [item for item in [var_wl.get(), \
                                var_temp.get(), var_salt.get(), var_cu.get()] if \
                                     item != '0']
        if error is None:
            root.destroy() # Close the GUI window

    def format_date(date, hour):
        from datetime import date as date_type
        # DateEntry.get_date() returns a datetime.date object
        if isinstance(date, date_type):
            # Format date object as YYYY-MM-DDThh:mm:ssZ
            formatted_date = date.strftime('%Y-%m-%d')
            return formatted_date + 'T' + str(hour).zfill(2) + ':00:00Z'
        else:
            # Fallback: if it's a string, raise an error with helpful message
            raise TypeError(f'Expected date object, got {type(date)}: {date}')

    def browse_directory():
        '''
        Opens a directory selection dialog and
        updates the directory path.
        '''
        chosen_directory = filedialog.askdirectory()
        if chosen_directory:  # Only update if a directory was selected
            directory_path_var.set(chosen_directory)

    def get_selected_date():
        selected_date = start_entry.get_date()
        print(f'Selected date: {selected_date}')

    root = tk.Tk()
    root.title('Skill assessment inputs')
    # Set the protocol for handling the window close event
    root.protocol('WM_DELETE_WINDOW', on_closing)
    style = ttk.Style(root)
    style.theme_use('clam')
    # STYLING
    # Change the icon
    try:
        dir_params = utils.Utils().read_config_section('directories', None)
        iconpath = os.path.join(dir_params['home'],
                                'readme_images','noaa_logo.png')
        icon_image = tk.PhotoImage(file=iconpath)
        root.iconphoto(False, icon_image)
    except:
        print('GUI logo not found! Defaulting to tkinter logo...')

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

    # Set row initial value
    row = -1

    # Set home dir, -p
    row += 1
    # Create a StringVar to hold the selected directory path
    directory_path_var = tk.StringVar()
    tk.Label(root,
             text='Home directory',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    ttk.Button(root, text='Browse...', command=browse_directory,
               style='TButton').grid(row=row, column=1,  sticky='w',
                                     padx=padx, pady=pady)
    ttk.Entry(root, textvariable=directory_path_var).grid(row=row, column=2,
                                                          sticky='w',
                                                          padx=padx, pady=pady)

    # OFS input, -o
    row += 1
    ofs_entry = tk.StringVar()
    choices = ('Select an OFS...','cbofs', 'dbofs', 'gomofs', 'tbofs', 'ciofs',
               'wcofs', 'ngofs2', 'ngofs', 'leofs', 'lmhofs', 'loofs', 'lsofs',
               'sfbofs', 'sscofs','stofs_3d_atl', 'stofs_3d_pac',
               'loofs-nextgen')
    ofs_entry.set('Select an OFS...')
    tk.Label(root,
             text='OFS',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    ofs_chosen = ttk.Combobox(root, width = 15, textvariable = ofs_entry,
                              font=('Helvetica', 12))
    ofs_chosen['values'] = choices
    ofs_chosen.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)

    # Start date, -s
    row += 1
    tk.Label(root,
             text='Start date & hour',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    start_entry = DateEntry(root, width=16, background='darkblue',
                    foreground='white', bd=2, date_pattern='yyyy-mm-dd',
                    font=('Helvetica', 12))
    start_entry.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)
    # Create scale widget for hour selection
    s_hour_scale = tk.Scale(root, from_=0, to=23, orient=tk.HORIZONTAL,
                          length=100)
    s_hour_scale.grid(row=row, column=2,  sticky='w', padx=padx, pady=pady)

    # End date, -e
    row += 1
    tk.Label(root,
             text='End date & hour',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0,  sticky='w',
                                 padx=padx, pady=pady)
    end_entry = DateEntry(root, width=16, background='darkblue',
                    foreground='white', bd=2, date_pattern='yyyy-mm-dd',
                    font=('Helvetica', 12))
    end_entry.grid(row=row, column=1,  sticky='w', padx=padx, pady=pady)
    # Create scale widget for hour selection
    e_hour_scale = tk.Scale(root, from_=0, to=23, orient=tk.HORIZONTAL,
                          length=100)
    e_hour_scale.grid(row=row, column=2,  sticky='w', padx=padx, pady=pady)

    # Whichcasts, -ws
    row += 1
    var_now = tk.StringVar()
    var_fore = tk.StringVar()
    var_now.set('nowcast')
    var_fore.set('forecast_b')
    tk.Label(root,
             text='Whichcasts (choose one or more)',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    ttk.Checkbutton(root, text='Nowcast', variable=var_now, onvalue='nowcast',
                   offvalue=0).grid(row=row, column=1, sticky='w',
                                    padx=padx, pady=pady)
    ttk.Checkbutton(root, text='Forecast', variable=var_fore, onvalue=
                   'forecast_b', offvalue=0).grid(row=row, column=2,
                                                  sticky='w', padx=padx,
                                                  pady=pady)
    # Datums, -d
    row += 1
    datum_var = tk.StringVar()
    dchoices = ('Select a datum...','MLLW', 'MLW', 'MHW', 'MHHW', 'XGEOID20b',
               'IGLD85', 'LWD')
    datum_var.set('Select a datum...')
    tk.Label(root,
             text='Vertical datum',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    datum_chosen = ttk.Combobox(root, width = 15, textvariable = datum_var,
                                font=('Helvetica', 12))
    datum_chosen['values'] = dchoices
    datum_chosen.grid(row=row, column=1, sticky='w', padx=padx, pady=pady)

    # File type, -t
    row += 1
    filetype_var = tk.StringVar(value='stations') # Default selection
    tk.Label(root,
             text='Model output file type',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(row=row, column=0, sticky='w',
                                 padx=padx, pady=pady)
    ttk.Radiobutton(root, text='Station', variable=filetype_var,
                   value='stations').grid(row=row, column=1,
                                          sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(root, text='Field', variable=filetype_var, value='fields').\
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
    tk.Label(root,
             text='Station providers (choose one or more)',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    tk.Label(root,
             text='If adding stations from list, provider selection is optional',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, 9, 'italic'),
             anchor=anchor).grid(
        row=row+1, column=0, sticky='w', padx=padx, pady=(0,pady))
    ttk.Checkbutton(root, text='CO-OPS',
                   variable=var_coops, onvalue='co-ops', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(pady,2))
    ttk.Checkbutton(root, text='NDBC',
                   variable=var_ndbc, onvalue='ndbc', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(pady,2))
    row += 1
    ttk.Checkbutton(root, text='USGS',
                   variable=var_usgs, onvalue='usgs', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(0,pady))
    ttk.Checkbutton(root, text='Add from list',
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
    tk.Label(root,
             text='Variables (choose one or more)',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Checkbutton(root, text='Water level',
                   variable=var_wl, onvalue='water_level', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(pady,2))
    ttk.Checkbutton(root, text='Temperature',
                   variable=var_temp, onvalue='water_temperature', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(pady,2))
    row += 1
    ttk.Checkbutton(root, text='Salinity',
                   variable=var_salt, onvalue='salinity', offvalue=0).grid(
                              row=row, column=1, sticky='w',
                              padx=padx, pady=(0,pady))
    ttk.Checkbutton(root, text='Current velocity',
                   variable=var_cu, onvalue='currents', offvalue=0).grid(
                              row=row, column=2, sticky='w',
                              padx=padx, pady=(0,pady))

    # Forecast horizon skill, -hs
    row += 1
    horizon_var = tk.BooleanVar(value=False) # Default selection
    tk.Label(root,
             text='Assess all forecast horizons?',
             bg=themecolor,
             fg=textcolor,
             font=(fontfamily, labelfontsize),
             anchor=anchor).grid(
        row=row, column=0, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(root, text='No', variable=horizon_var, value=False).grid(
        row=row, column=1, sticky='w', padx=padx, pady=pady)
    ttk.Radiobutton(root, text='Yes', variable=horizon_var, value=True).grid(
        row=row, column=2, sticky='w', padx=padx, pady=pady)

    # Horizontal separator
    row += 1
    separator = ttk.Separator(root, orient='horizontal')
    separator.grid(row=row, column=0, columnspan=4, sticky='ew', pady=pady)

    # Submit button
    row += 1
    submit_button = ttk.Button(root, text='Run skill assessment!',
                              command=submit_and_close)
    submit_button.grid(row=row, column=0, columnspan=4, pady=10)

    root.mainloop()
    return args_values
