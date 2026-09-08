"""
Entry point for the model time-series extraction GUI.

Run without arguments to open the GUI directly:
    python bin/visualization/create_gui_model.py

Author: TSR
"""
from bin.model_processing.get_node_cli import _run_pipeline

from ofs_skill.visualization import create_gui_model

if __name__ == '__main__':
    create_gui_model.create_gui_model(runner=_run_pipeline)
