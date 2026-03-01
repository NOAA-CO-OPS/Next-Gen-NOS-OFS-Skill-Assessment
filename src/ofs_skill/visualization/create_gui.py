"""
Created on Wed Nov 12 08:39:35 2025

@author: PWL

Enhanced GUI with professional layout, cross-platform themes, and UX improvements.
"""
import os
import sys
import platform
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font
from types import SimpleNamespace

from tkcalendar import DateEntry

# Import from ofs_skill package
from ofs_skill.obs_retrieval import utils


def get_cross_platform_theme():
    """
    Determine the best theme based on the operating system.
    
    Returns
    -------
    str
        Theme name optimized for the current platform
    """
    system = platform.system()
    
    if system == 'Windows':
        return 'vista'
    elif system == 'Darwin':  # macOS
        return 'clam'
    else:  # Linux and others
        return 'clam'


def validate_inputs(args_values):
    """
    Validate GUI inputs and return error messages if any.
    
    Parameters
    ----------
    args_values : SimpleNamespace
        Object containing all GUI input values
        
    Returns
    -------
    list
        List of error messages, empty if all inputs are valid
    """
    errors = []
    
    if not args_values.Path or args_values.Path == '':
        errors.append('Please select your home directory.')
    
    if not args_values.OFS or args_values.OFS == 'Select an OFS...':
        errors.append('Please select the OFS.')
    
    if not args_values.Datum or args_values.Datum == 'Select a datum...':
        errors.append('Please choose a datum.')
    
    if not args_values.StartDate_full:
        errors.append('Please enter a start date.')
    
    if not args_values.EndDate_full:
        errors.append('Please enter an end date.')
    
    if not args_values.Whichcasts or len(args_values.Whichcasts) == 0:
        errors.append('Please select at least one whichcast.')
    
    if not args_values.Station_Owner or len(args_values.Station_Owner) == 0:
        errors.append('Please select at least one station provider, or provide a list of station IDs.')
    
    if not args_values.Var_Selection or len(args_values.Var_Selection) == 0:
        errors.append('Please select at least one variable to assess.')
    
    return errors


def create_gui(parser):

    def on_closing():
        '''Function called when the user closes the window.'''
        if messagebox.askokcancel('Quit', 'Do you want to quit?'):
            # If the user confirms quitting, destroy the window
            root.destroy()
            print('Skill assessment run terminated by user.')
            sys.exit()

    def submit_and_close():
        # Validate all inputs first
        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs_entry.get()
        args_values.StartDate_full = format_date(start_entry.get_date(), s_hour_scale.get())
        args_values.EndDate_full = format_date(end_entry.get_date(), e_hour_scale.get())
        args_values.Whichcasts = [item for item in [var_now.get(), var_fore.get()] if item != '0']
        args_values.Datum = datum_var.get()
        args_values.FileType = filetype_var.get()
        args_values.Station_Owner = [item for item in [var_coops.get(), var_ndbc.get(), var_usgs.get(), var_list.get()] if item != '0']
        args_values.Horizon_Skill = horizon_var.get()
        args_values.Var_Selection = [item for item in [var_wl.get(), var_temp.get(), var_salt.get(), var_cu.get()] if item != '0']
        
        # Validate inputs
        errors = validate_inputs(args_values)
        
        if errors:
            # Show all errors in a single dialog
            error_message = "Please fix the following issues:\n\n" + "\n".join(f"• {error}" for error in errors)
            messagebox.showerror('Input Validation Error', error_message)
        else:
            # Update status bar
            status_var.set("Running skill assessment...")
            root.update_idletasks()
            
            # Close GUI window
            root.destroy()

    def reset_fields():
        """Reset all form fields to default values."""
        if messagebox.askyesno('Reset Fields', 'Are you sure you want to reset all fields to default values?'):
            # Reset directory
            directory_path_var.set('')
            
            # Reset OFS selection
            ofs_entry.set('Select an OFS...')
            
            # Reset dates to current date
            from datetime import date
            today = date.today()
            start_entry.set_date(today)
            end_entry.set_date(today)
            s_hour_scale.set(0)
            e_hour_scale.set(0)
            
            # Reset whichcasts
            var_now.set('0')
            var_fore.set('0')
            
            # Reset datum
            datum_var.set('Select a datum...')
            
            # Reset file type
            filetype_var.set('stations')
            
            # Reset station providers
            var_coops.set('0')
            var_ndbc.set('0')
            var_usgs.set('0')
            var_list.set('0')
            
            # Reset variables
            var_wl.set('0')
            var_temp.set('0')
            var_salt.set('0')
            var_cu.set('0')
            
            # Reset horizon skill
            horizon_var.set(False)
            
            # Update status
            status_var.set("All fields reset to defaults")

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

    # Create main window
    root = tk.Tk()
    root.title('NOAA OFS Skill Assessment - Professional Edition')
    root.geometry('800x700')  # Set initial window size
    root.minsize(700, 600)     # Set minimum window size
    
    # Set the protocol for handling the window close event
    root.protocol('WM_DELETE_WINDOW', on_closing)
    
    # Cross-platform theme handling
    try:
        style = ttk.Style(root)
        preferred_theme = get_cross_platform_theme()
        available_themes = style.theme_names()
        
        if preferred_theme in available_themes:
            style.theme_use(preferred_theme)
        else:
            # Fallback to 'clam' if preferred theme not available
            if 'clam' in available_themes:
                style.theme_use('clam')
            else:
                style.theme_use(available_themes[0])  # Use first available theme
    except Exception as e:
        print(f'Theme setup failed: {e}. Using default theme.')

    # Professional color scheme
    bg_color = '#f0f0f0'      # Light gray background
    fg_color = '#333333'      # Dark gray text
    accent_color = '#0066cc'   # Professional blue
    frame_bg = '#ffffff'       # White for frames
    border_color = '#cccccc'   # Light border

    root.config(bg=bg_color)

    # Configure styles for professional appearance
    style.configure('TFrame', background=frame_bg, relief='raised', borderwidth=1)
    style.configure('TLabelframe', background=frame_bg, foreground=fg_color, borderwidth=1)
    style.configure('TLabelframe.Label', background=frame_bg, foreground=accent_color, 
                   font=('Helvetica', 10, 'bold'))
    style.configure('TLabel', background=frame_bg, foreground=fg_color, font=('Helvetica', 10))
    style.configure('TButton', background=accent_color, foreground='white', 
                   font=('Helvetica', 10, 'bold'), borderwidth=1)
    style.map('TButton', background=[('active', '#0052a3'), ('pressed', '#003d7a')])
    style.configure('TCheckbutton', background=frame_bg, foreground=fg_color, font=('Helvetica', 10))
    style.configure('TRadiobutton', background=frame_bg, foreground=fg_color, font=('Helvetica', 10))
    style.configure('TCombobox', fieldbackground='white', background=frame_bg, borderwidth=1)
    style.configure('TSeparator', background=border_color)

    # Set font for drop-downs
    root.option_add('*TCombobox*Listbox*Font', Font(family='Helvetica', size=10))

    # Create status bar at bottom
    status_frame = ttk.Frame(root, style='TFrame')
    status_frame.pack(side='bottom', fill='x', padx=5, pady=5)
    
    status_var = tk.StringVar()
    status_var.set("Ready - Configure your skill assessment parameters")
    status_label = ttk.Label(status_frame, textvariable=status_var, relief='sunken', anchor='w')
    status_label.pack(fill='x', padx=5)

    # Create main scrollable frame
    main_canvas = tk.Canvas(root, bg=bg_color)
    scrollbar = ttk.Scrollbar(root, orient='vertical', command=main_canvas.yview)
    scrollable_frame = ttk.Frame(main_canvas, style='TFrame')

    scrollable_frame.bind(
        "<Configure>",
        lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all"))
    )

    main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    main_canvas.configure(yscrollcommand=scrollbar.set)

    # Pack canvas and scrollbar
    main_canvas.pack(side="left", fill="both", expand=True, padx=5, pady=5)
    scrollbar.pack(side="right", fill="y")

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

    # Try to load NOAA logo
    try:
        dir_params = utils.Utils().read_config_section('directories', None)
        iconpath = os.path.join(dir_params['home'], 'readme_images', 'noaa_logo.png')
        icon_image = tk.PhotoImage(file=iconpath)
        root.iconphoto(False, icon_image)
    except:
        print('GUI logo not found! Using default tkinter logo...')

    # Header frame
    header_frame = ttk.Frame(scrollable_frame, style='TFrame')
    header_frame.pack(fill='x', padx=10, pady=10)
    
    title_label = ttk.Label(header_frame, text="NOAA Operational Forecast System Skill Assessment", 
                           font=('Helvetica', 16, 'bold'), foreground=accent_color)
    title_label.pack(pady=5)
    
    subtitle_label = ttk.Label(header_frame, text="Configure parameters for model skill assessment", 
                              font=('Helvetica', 10))
    subtitle_label.pack(pady=2)

    # Model Configuration Frame
    model_frame = ttk.LabelFrame(scrollable_frame, text="Model Configuration", style='TLabelframe')
    model_frame.pack(fill='x', padx=10, pady=5)

    # Home directory
    dir_frame = ttk.Frame(model_frame, style='TFrame')
    dir_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(dir_frame, text="Home Directory:", width=15, anchor='w').pack(side='left', padx=5)
    directory_path_var = tk.StringVar()
    ttk.Entry(dir_frame, textvariable=directory_path_var, width=50).pack(side='left', padx=5, fill='x', expand=True)
    ttk.Button(dir_frame, text="Browse...", command=browse_directory).pack(side='left', padx=5)

    # OFS selection
    ofs_frame = ttk.Frame(model_frame, style='TFrame')
    ofs_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(ofs_frame, text="OFS System:", width=15, anchor='w').pack(side='left', padx=5)
    ofs_entry = tk.StringVar()
    choices = ('Select an OFS...','cbofs', 'dbofs', 'gomofs', 'tbofs', 'ciofs',
               'wcofs', 'ngofs2', 'ngofs', 'leofs', 'lmhofs', 'loofs', 'lsofs',
               'sfbofs', 'sscofs','stofs_3d_atl', 'stofs_3d_pac', 'loofs-nextgen')
    ofs_entry.set('Select an OFS...')
    ofs_chosen = ttk.Combobox(ofs_frame, textvariable=ofs_entry, width=20, font=('Helvetica', 10))
    ofs_chosen['values'] = choices
    ofs_chosen.pack(side='left', padx=5, fill='x', expand=True)

    # Observation Settings Frame
    obs_frame = ttk.LabelFrame(scrollable_frame, text="Observation Settings", style='TLabelframe')
    obs_frame.pack(fill='x', padx=10, pady=5)

    # Station providers
    provider_frame = ttk.Frame(obs_frame, style='TFrame')
    provider_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(provider_frame, text="Station Providers:", width=15, anchor='w').pack(side='left', padx=5)
    
    provider_check_frame = ttk.Frame(provider_frame, style='TFrame')
    provider_check_frame.pack(side='left', fill='x', expand=True)
    
    var_coops = tk.StringVar()
    var_ndbc = tk.StringVar()
    var_usgs = tk.StringVar()
    var_list = tk.StringVar()
    var_coops.set('0')
    var_ndbc.set('0')
    var_usgs.set('0')
    var_list.set('0')
    
    ttk.Checkbutton(provider_check_frame, text="CO-OPS", variable=var_coops, onvalue='co-ops', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(provider_check_frame, text="NDBC", variable=var_ndbc, onvalue='ndbc', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(provider_check_frame, text="USGS", variable=var_usgs, onvalue='usgs', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(provider_check_frame, text="Custom List", variable=var_list, onvalue='list', offvalue='0').pack(side='left', padx=5)

    # Variables selection
    vars_frame = ttk.Frame(obs_frame, style='TFrame')
    vars_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(vars_frame, text="Variables:", width=15, anchor='w').pack(side='left', padx=5)
    
    var_check_frame = ttk.Frame(vars_frame, style='TFrame')
    var_check_frame.pack(side='left', fill='x', expand=True)
    
    var_wl = tk.StringVar()
    var_temp = tk.StringVar()
    var_salt = tk.StringVar()
    var_cu = tk.StringVar()
    var_wl.set('0')
    var_temp.set('0')
    var_salt.set('0')
    var_cu.set('0')
    
    ttk.Checkbutton(var_check_frame, text="Water Level", variable=var_wl, onvalue='water_level', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(var_check_frame, text="Temperature", variable=var_temp, onvalue='water_temperature', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(var_check_frame, text="Salinity", variable=var_salt, onvalue='salinity', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(var_check_frame, text="Currents", variable=var_cu, onvalue='currents', offvalue='0').pack(side='left', padx=5)

    # Time Selection Frame
    time_frame = ttk.LabelFrame(scrollable_frame, text="Time Selection", style='TLabelframe')
    time_frame.pack(fill='x', padx=10, pady=5)

    # Start date and hour
    start_frame = ttk.Frame(time_frame, style='TFrame')
    start_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(start_frame, text="Start Date:", width=15, anchor='w').pack(side='left', padx=5)
    start_entry = DateEntry(start_frame, width=15, background='darkblue', foreground='white', 
                           bd=2, date_pattern='yyyy-mm-dd', font=('Helvetica', 10))
    start_entry.pack(side='left', padx=5)
    
    ttk.Label(start_frame, text="Hour:").pack(side='left', padx=5)
    s_hour_scale = tk.Scale(start_frame, from_=0, to=23, orient=tk.HORIZONTAL, length=150)
    s_hour_scale.pack(side='left', padx=5)

    # End date and hour
    end_frame = ttk.Frame(time_frame, style='TFrame')
    end_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(end_frame, text="End Date:", width=15, anchor='w').pack(side='left', padx=5)
    end_entry = DateEntry(end_frame, width=15, background='darkblue', foreground='white', 
                         bd=2, date_pattern='yyyy-mm-dd', font=('Helvetica', 10))
    end_entry.pack(side='left', padx=5)
    
    ttk.Label(end_frame, text="Hour:").pack(side='left', padx=5)
    e_hour_scale = tk.Scale(end_frame, from_=0, to=23, orient=tk.HORIZONTAL, length=150)
    e_hour_scale.pack(side='left', padx=5)

    # Output Options Frame
    output_frame = ttk.LabelFrame(scrollable_frame, text="Output Options", style='TLabelframe')
    output_frame.pack(fill='x', padx=10, pady=5)

    # Whichcasts
    whichcast_frame = ttk.Frame(output_frame, style='TFrame')
    whichcast_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(whichcast_frame, text="Assessment Mode:", width=15, anchor='w').pack(side='left', padx=5)
    
    var_now = tk.StringVar()
    var_fore = tk.StringVar()
    var_now.set('0')
    var_fore.set('0')
    
    whichcast_check_frame = ttk.Frame(whichcast_frame, style='TFrame')
    whichcast_check_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Checkbutton(whichcast_check_frame, text="Nowcast", variable=var_now, onvalue='nowcast', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(whichcast_check_frame, text="Forecast", variable=var_fore, onvalue='forecast_b', offvalue='0').pack(side='left', padx=5)

    # Vertical datum
    datum_frame = ttk.Frame(output_frame, style='TFrame')
    datum_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(datum_frame, text="Vertical Datum:", width=15, anchor='w').pack(side='left', padx=5)
    datum_var = tk.StringVar()
    dchoices = ('Select a datum...','MLLW', 'MLW', 'MHW', 'MHHW', 'XGEOID20b', 'IGLD85', 'LWD')
    datum_var.set('Select a datum...')
    datum_chosen = ttk.Combobox(datum_frame, textvariable=datum_var, width=20, font=('Helvetica', 10))
    datum_chosen['values'] = dchoices
    datum_chosen.pack(side='left', padx=5, fill='x', expand=True)

    # File type
    filetype_frame = ttk.Frame(output_frame, style='TFrame')
    filetype_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(filetype_frame, text="File Type:", width=15, anchor='w').pack(side='left', padx=5)
    filetype_var = tk.StringVar(value='stations')
    
    filetype_radio_frame = ttk.Frame(filetype_frame, style='TFrame')
    filetype_radio_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Radiobutton(filetype_radio_frame, text="Station Files", variable=filetype_var, value='stations').pack(side='left', padx=5)
    ttk.Radiobutton(filetype_radio_frame, text="Field Files", variable=filetype_var, value='fields').pack(side='left', padx=5)

    # Forecast horizon skill
    horizon_frame = ttk.Frame(output_frame, style='TFrame')
    horizon_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(horizon_frame, text="All Forecast Horizons:", width=15, anchor='w').pack(side='left', padx=5)
    horizon_var = tk.BooleanVar(value=False)
    
    horizon_radio_frame = ttk.Frame(horizon_frame, style='TFrame')
    horizon_radio_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Radiobutton(horizon_radio_frame, text="No", variable=horizon_var, value=False).pack(side='left', padx=5)
    ttk.Radiobutton(horizon_radio_frame, text="Yes", variable=horizon_var, value=True).pack(side='left', padx=5)

    # Action Buttons Frame
    button_frame = ttk.Frame(scrollable_frame, style='TFrame')
    button_frame.pack(fill='x', padx=10, pady=20)

    # Reset button
    reset_button = ttk.Button(button_frame, text="Reset Fields", command=reset_fields)
    reset_button.pack(side='left', padx=5)

    # Submit button (main action)
    submit_button = ttk.Button(button_frame, text="Run Skill Assessment", command=submit_and_close)
    submit_button.pack(side='right', padx=5)

    # Configure mouse wheel scrolling
    def _on_mousewheel(event):
        main_canvas.yview_scroll(int(-1*(event.delta/120)), "units")

    def _bind_to_mousewheel(event):
        main_canvas.bind_all("<MouseWheel>", _on_mousewheel)

    def _unbind_from_mousewheel(event):
        main_canvas.unbind_all("<MouseWheel>")

    main_canvas.bind('<Enter>', _bind_to_mousewheel)
    main_canvas.bind('<Leave>', _unbind_from_mousewheel)

    # Start the GUI
    root.mainloop()
    return args_values
