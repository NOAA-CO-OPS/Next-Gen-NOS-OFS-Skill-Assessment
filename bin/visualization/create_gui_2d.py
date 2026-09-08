"""
Entry point for the 2D skill assessment GUI.

Run without arguments to open the GUI directly:
    python bin/visualization/create_gui_2d.py

Author: TSR
"""
from bin.visualization.create_2dplot import _run_pipeline

from ofs_skill.visualization import create_gui_2d

if __name__ == '__main__':
    create_gui_2d.create_gui_2d(runner=_run_pipeline)
