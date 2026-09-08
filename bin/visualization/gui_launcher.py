"""
Launcher window for the NOS OFS Skill Assessment GUI suite.

Presents a menu of available tools and opens the selected GUI module.

Author: TSR
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import tkinter as tk
from tkinter import ttk

from ofs_skill.obs_retrieval import utils
from ofs_skill.visualization.gui_helpers import GuiTheme

_WINDOW_GEOMETRY = '520x460'


def launch():
    """Display the main launcher window and return the user's selection."""

    theme = GuiTheme()
    themecolor = theme.themecolor
    textcolor = theme.textcolor

    root = tk.Tk()
    root.title('NOS OFS Skill Assessment')
    root.geometry(_WINDOW_GEOMETRY)
    root.minsize(480, 400)
    root.config(bg=themecolor)
    root.resizable(False, False)

    style = ttk.Style(root)
    style.theme_use('clam')
    style.configure('TFrame', background=themecolor)
    style.configure('TLabel', background=themecolor, foreground=textcolor)
    style.configure('TButton', background=themecolor, foreground=textcolor,
                    font=(theme.fontfamily, theme.widgetfontsize))
    style.configure('Title.TLabel', background=themecolor,
                    foreground=textcolor,
                    font=(theme.fontfamily, 16, 'bold'))
    style.configure('Subtitle.TLabel', background=themecolor,
                    foreground='#555',
                    font=(theme.fontfamily, 10, 'italic'))
    style.configure('Launch.TButton',
                    font=(theme.fontfamily, 12),
                    padding=(12, 8))

    log = logging.getLogger(__name__)
    try:
        dir_params = utils.Utils().read_config_section('directories', log)
        iconpath = os.path.join(
            dir_params['home'], 'readme_images', 'noaa_logo.png'
        )
        icon_image = tk.PhotoImage(file=iconpath)
        root.iconphoto(False, icon_image)
    except (KeyError, tk.TclError, OSError):
        pass

    selection = [None]

    def _select(name):
        selection[0] = name
        root.destroy()

    def on_closing():
        log.info('Launcher closed by user.')
        root.destroy()
        sys.exit()

    root.protocol('WM_DELETE_WINDOW', on_closing)

    frame = ttk.Frame(root, padding=30)
    frame.pack(fill='both', expand=True)

    ttk.Label(
        frame, text='NOS OFS Skill Assessment', style='Title.TLabel',
    ).pack(pady=(0, 4))
    ttk.Label(
        frame, text='Select a module to launch', style='Subtitle.TLabel',
    ).pack(pady=(0, 25))

    buttons = [
        ('1D Skill Assessment (Station)',  '1d'),
        ('2D Skill Assessment (Field)',    '2d'),
        ('GLOFS Ice Skill Assessment',     'ice'),
        ('Observation Retrieval',          'obs'),
        ('Model Time Series Extraction',   'model'),
    ]

    for label, key in buttons:
        ttk.Button(
            frame, text=label, style='Launch.TButton',
            command=lambda k=key: _select(k),
        ).pack(fill='x', pady=5)

    root.mainloop()
    return selection[0]


def _open_selected_gui(key: str) -> None:
    """Open the sub-GUI for ``key`` with its pipeline runner wired."""
    if key == '1d':

        from bin.visualization.create_1dplot import _run_pipeline

        from ofs_skill.visualization import create_gui
        parser = argparse.ArgumentParser()
        create_gui.create_gui(parser, runner=_run_pipeline)
    elif key == '2d':
        from bin.visualization.create_2dplot import _run_pipeline

        from ofs_skill.visualization import create_gui_2d
        create_gui_2d.create_gui_2d(runner=_run_pipeline)
    elif key == 'ice':
        from bin.skill_assessment.do_iceskill import _run_pipeline

        from ofs_skill.visualization import create_gui_ice
        create_gui_ice.create_gui_ice(runner=_run_pipeline)
    elif key == 'obs':
        from bin.obs_retrieval.get_station_observations_cli import _run_pipeline

        from ofs_skill.visualization import create_gui_obs
        create_gui_obs.create_gui_obs(runner=_run_pipeline)
    elif key == 'model':
        from bin.model_processing.get_node_cli import _run_pipeline

        from ofs_skill.visualization import create_gui_model
        create_gui_model.create_gui_model(runner=_run_pipeline)
    else:
        raise ValueError(f'Unknown GUI module: {key!r}')


def main() -> None:
    """Entry point for the ofs-skill-gui console script."""
    result = launch()
    if result is None:
        return
    _open_selected_gui(result)


if __name__ == '__main__':
    main()
