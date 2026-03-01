"""
Great Lakes Ice Skill Assessment GUI Prototype

This module provides a professional GUI for the Great Lakes ice concentration 
skill assessment system. It allows users to configure all parameters for 
comparing model ice concentration output to GLSEA satellite observations.

Author: GSoC Contributor Evaluation
Created: March 2026
"""
import os
import sys
import platform
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.font import Font
from types import SimpleNamespace
from datetime import date

from tkcalendar import DateEntry


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


def validate_ice_inputs(args_values):
    """
    Validate ice skill assessment inputs and return error messages if any.
    
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
    
    if not args_values.OFS or args_values.OFS == 'Select a Great Lakes OFS...':
        errors.append('Please select the Great Lakes OFS.')
    
    if not args_values.StartDate_full:
        errors.append('Please enter a start date.')
    
    if not args_values.EndDate_full:
        errors.append('Please enter an end date.')
    
    if not args_values.Whichcasts or len(args_values.Whichcasts) == 0:
        errors.append('Please select at least one assessment mode.')
    
    # Validate date range
    if args_values.StartDate_full and args_values.EndDate_full:
        try:
            start_date = datetime.strptime(args_values.StartDate_full.split('T')[0], '%Y-%m-%d')
            end_date = datetime.strptime(args_values.EndDate_full.split('T')[0], '%Y-%m-%d')
            if start_date >= end_date:
                errors.append('Start date must be before end date.')
            
            # Check for reasonable ice season (Nov-Apr for Great Lakes)
            if not (11 <= start_date.month <= 12 or 1 <= start_date.month <= 4):
                errors.append('Start date should be during ice season (Nov-Apr).')
            if not (11 <= end_date.month <= 12 or 1 <= end_date.month <= 4):
                errors.append('End date should be during ice season (Nov-Apr).')
                
        except ValueError:
            errors.append('Invalid date format.')
    
    return errors


def create_ice_skill_gui():
    """
    Create and display the Great Lakes Ice Skill Assessment GUI.
    
    Returns
    -------
    SimpleNamespace
        Object containing all user-configured parameters
    """
    def on_closing():
        '''Function called when the user closes the window.'''
        if messagebox.askokcancel('Quit', 'Do you want to quit the Ice Skill Assessment GUI?'):
            root.destroy()
            print('Ice skill assessment run terminated by user.')
            sys.exit()

    def submit_and_close():
        # Validate all inputs first
        args_values.Path = directory_path_var.get()
        args_values.OFS = ofs_entry.get()
        args_values.StartDate_full = format_date(start_entry.get_date(), s_hour_scale.get())
        args_values.EndDate_full = format_date(end_entry.get_date(), e_hour_scale.get())
        args_values.Whichcasts = [item for item in [var_now.get(), var_fore_a.get(), var_fore_b.get()] if item != '0']
        args_values.DailyAverage = daily_average_var.get()
        args_values.TimeStep = timestep_var.get()
        
        # Validate inputs
        errors = validate_ice_inputs(args_values)
        
        if errors:
            # Show all errors in a single dialog
            error_message = "Please fix the following issues:\n\n" + "\n".join(f"• {error}" for error in errors)
            messagebox.showerror('Input Validation Error', error_message)
        else:
            # Update status bar
            status_var.set("Running ice skill assessment...")
            root.update_idletasks()
            
            # Close GUI window
            root.destroy()

    def reset_fields():
        """Reset all form fields to default values."""
        if messagebox.askyesno('Reset Fields', 'Are you sure you want to reset all fields to default values?'):
            # Reset directory
            directory_path_var.set('')
            
            # Reset OFS selection
            ofs_entry.set('Select a Great Lakes OFS...')
            
            # Reset dates to ice season start
            start_entry.set_date(date(2024, 11, 15))  # Mid-November
            end_entry.set_date(date(2025, 4, 15))     # Mid-April
            s_hour_scale.set(0)
            e_hour_scale.set(0)
            
            # Reset whichcasts
            var_now.set('0')
            var_fore_a.set('0')
            var_fore_b.set('0')
            
            # Reset other options
            daily_average_var.set(False)
            timestep_var.set('daily')
            
            # Update status
            status_var.set("All fields reset to ice season defaults")

    def format_date(date, hour):
        """Format date object as ISO8601 string."""
        formatted_date = date.strftime('%Y-%m-%d')
        return formatted_date + 'T' + str(hour).zfill(2) + ':00:00Z'

    def browse_directory():
        '''Open a directory selection dialog and update the directory path.'''
        chosen_directory = filedialog.askdirectory()
        if chosen_directory:
            directory_path_var.set(chosen_directory)

    # Create main window
    root = tk.Tk()
    root.title('NOAA Great Lakes Ice Skill Assessment - Professional Edition')
    root.geometry('800x650')
    root.minsize(700, 550)
    
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
            if 'clam' in available_themes:
                style.theme_use('clam')
            else:
                style.theme_use(available_themes[0])
    except Exception as e:
        print(f'Theme setup failed: {e}. Using default theme.')

    # Professional color scheme
    bg_color = '#f0f0f0'
    fg_color = '#333333'
    accent_color = '#0066cc'
    frame_bg = '#ffffff'
    border_color = '#cccccc'

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
    status_var.set("Ready - Configure Great Lakes ice skill assessment parameters")
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
    args_values = SimpleNamespace()
    args_values.OFS = None
    args_values.Path = None
    args_values.StartDate_full = None
    args_values.EndDate_full = None
    args_values.Whichcasts = None
    args_values.DailyAverage = False
    args_values.TimeStep = 'daily'

    # Header frame
    header_frame = ttk.Frame(scrollable_frame, style='TFrame')
    header_frame.pack(fill='x', padx=10, pady=10)
    
    title_label = ttk.Label(header_frame, text="Great Lakes Ice Concentration Skill Assessment", 
                           font=('Helvetica', 16, 'bold'), foreground=accent_color)
    title_label.pack(pady=5)
    
    subtitle_label = ttk.Label(header_frame, text="Compare model ice concentration to GLSEA satellite observations", 
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

    # Great Lakes OFS selection
    ofs_frame = ttk.Frame(model_frame, style='TFrame')
    ofs_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(ofs_frame, text="Great Lakes OFS:", width=15, anchor='w').pack(side='left', padx=5)
    ofs_entry = tk.StringVar()
    great_lakes_choices = ('Select a Great Lakes OFS...', 'lmhofs', 'lsofs', 'loofs')
    ofs_entry.set('Select a Great Lakes OFS...')
    ofs_chosen = ttk.Combobox(ofs_frame, textvariable=ofs_entry, width=20, font=('Helvetica', 10))
    ofs_chosen['values'] = great_lakes_choices
    ofs_chosen.pack(side='left', padx=5, fill='x', expand=True)

    # Time Selection Frame
    time_frame = ttk.LabelFrame(scrollable_frame, text="Time Selection", style='TLabelframe')
    time_frame.pack(fill='x', padx=10, pady=5)

    # Start date and hour
    start_frame = ttk.Frame(time_frame, style='TFrame')
    start_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(start_frame, text="Start Date:", width=15, anchor='w').pack(side='left', padx=5)
    start_entry = DateEntry(start_frame, width=15, background='darkblue', foreground='white', 
                           bd=2, date_pattern='yyyy-mm-dd', font=('Helvetica', 10))
    start_entry.set_date(date(2024, 11, 15))  # Default to ice season start
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
    end_entry.set_date(date(2025, 4, 15))  # Default to ice season end
    end_entry.pack(side='left', padx=5)
    
    ttk.Label(end_frame, text="Hour:").pack(side='left', padx=5)
    e_hour_scale = tk.Scale(end_frame, from_=0, to=23, orient=tk.HORIZONTAL, length=150)
    e_hour_scale.pack(side='left', padx=5)

    # Assessment Mode Frame
    mode_frame = ttk.LabelFrame(scrollable_frame, text="Assessment Mode", style='TLabelframe')
    mode_frame.pack(fill='x', padx=10, pady=5)

    # Whichcasts
    whichcast_frame = ttk.Frame(mode_frame, style='TFrame')
    whichcast_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(whichcast_frame, text="Assessment Mode:", width=15, anchor='w').pack(side='left', padx=5)
    
    var_now = tk.StringVar()
    var_fore_a = tk.StringVar()
    var_fore_b = tk.StringVar()
    var_now.set('0')
    var_fore_a.set('0')
    var_fore_b.set('0')
    
    whichcast_check_frame = ttk.Frame(whichcast_frame, style='TFrame')
    whichcast_check_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Checkbutton(whichcast_check_frame, text="Nowcast", variable=var_now, onvalue='nowcast', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(whichcast_check_frame, text="Forecast A", variable=var_fore_a, onvalue='forecast_a', offvalue='0').pack(side='left', padx=5)
    ttk.Checkbutton(whichcast_check_frame, text="Forecast B", variable=var_fore_b, onvalue='forecast_b', offvalue='0').pack(side='left', padx=5)

    # Processing Options Frame
    options_frame = ttk.LabelFrame(scrollable_frame, text="Processing Options", style='TLabelframe')
    options_frame.pack(fill='x', padx=10, pady=5)

    # Daily average option
    daily_frame = ttk.Frame(options_frame, style='TFrame')
    daily_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(daily_frame, text="Daily Average:", width=15, anchor='w').pack(side='left', padx=5)
    daily_average_var = tk.BooleanVar(value=False)
    
    daily_radio_frame = ttk.Frame(daily_frame, style='TFrame')
    daily_radio_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Radiobutton(daily_radio_frame, text="No (single hour)", variable=daily_average_var, value=False).pack(side='left', padx=5)
    ttk.Radiobutton(daily_radio_frame, text="Yes (24-hour average)", variable=daily_average_var, value=True).pack(side='left', padx=5)

    # Time step option
    timestep_frame = ttk.Frame(options_frame, style='TFrame')
    timestep_frame.pack(fill='x', padx=10, pady=5)
    
    ttk.Label(timestep_frame, text="Time Step:", width=15, anchor='w').pack(side='left', padx=5)
    timestep_var = tk.StringVar(value='daily')
    
    timestep_radio_frame = ttk.Frame(timestep_frame, style='TFrame')
    timestep_radio_frame.pack(side='left', fill='x', expand=True)
    
    ttk.Radiobutton(timestep_radio_frame, text="Daily", variable=timestep_var, value='daily').pack(side='left', padx=5)
    ttk.Radiobutton(timestep_radio_frame, text="Hourly", variable=timestep_var, value='hourly').pack(side='left', padx=5)

    # Information Frame
    info_frame = ttk.LabelFrame(scrollable_frame, text="Information", style='TLabelframe')
    info_frame.pack(fill='x', padx=10, pady=5)

    info_text = ttk.Label(info_frame, 
                         text="This tool compares Great Lakes model ice concentration output to GLSEA satellite observations.\n"
                             "Recommended assessment period: November 15 - April 15 (ice season)\n"
                             "Minimum assessment duration: 5 days for statistics and plots",
                         font=('Helvetica', 9, 'italic'),
                         foreground='#666666')
    info_text.pack(padx=10, pady=5)

    # Action Buttons Frame
    button_frame = ttk.Frame(scrollable_frame, style='TFrame')
    button_frame.pack(fill='x', padx=10, pady=20)

    # Reset button
    reset_button = ttk.Button(button_frame, text="Reset Fields", command=reset_fields)
    reset_button.pack(side='left', padx=5)

    # Submit button (main action)
    submit_button = ttk.Button(button_frame, text="Run Ice Skill Assessment", command=submit_and_close)
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


if __name__ == '__main__':
    """
    Run the ice skill GUI prototype independently for testing.
    """
    print("Starting Great Lakes Ice Skill Assessment GUI Prototype...")
    print("This is a prototype GUI for demonstration purposes.")
    print("No backend processing will be performed.")
    
    try:
        args = create_ice_skill_gui()
        print("\nGUI Configuration Summary:")
        print(f"OFS: {args.OFS}")
        print(f"Path: {args.Path}")
        print(f"Start Date: {args.StartDate_full}")
        print(f"End Date: {args.EndDate_full}")
        print(f"Whichcasts: {args.Whichcasts}")
        print(f"Daily Average: {args.DailyAverage}")
        print(f"Time Step: {args.TimeStep}")
        print("\nGUI prototype completed successfully!")
    except Exception as e:
        print(f"Error running GUI prototype: {e}")
