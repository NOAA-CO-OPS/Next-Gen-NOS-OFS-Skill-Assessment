"""
Entry point for the observation retrieval GUI.

Run without arguments to open the GUI directly:
    python bin/visualization/create_gui_obs.py

Author: TSR
"""
from bin.obs_retrieval.get_station_observations_cli import _run_pipeline

from ofs_skill.visualization import create_gui_obs

if __name__ == '__main__':
    create_gui_obs.create_gui_obs(runner=_run_pipeline)
