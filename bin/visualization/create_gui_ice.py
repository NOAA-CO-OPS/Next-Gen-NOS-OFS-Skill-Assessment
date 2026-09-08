"""
Entry point for the GLOFS ice skill assessment GUI.

Run without arguments to open the GUI directly:
    python bin/visualization/create_gui_ice.py

Author: TSR
"""
from bin.skill_assessment.do_iceskill import _run_pipeline

from ofs_skill.visualization import create_gui_ice

if __name__ == '__main__':
    create_gui_ice.create_gui_ice(runner=_run_pipeline)
