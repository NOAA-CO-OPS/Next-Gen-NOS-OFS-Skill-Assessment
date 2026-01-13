"""
Visualization Module

Provides plotting and visualization functionality for OFS skill assessment.

Note: create_1dplot and create_2dplot are CLI entry points in bin/visualization/
and are not part of this package.
"""

# Import only library modules, not CLI scripts
from . import plotting_functions, plotting_scalar, plotting_vector

__all__ = [
    'plotting_scalar',
    'plotting_vector',
    'plotting_functions',
]
