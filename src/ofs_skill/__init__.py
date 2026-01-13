"""
Next-Gen NOS OFS Skill Assessment Package

Provides tools for:
- Model data processing
- Observation retrieval
- Skill assessment metrics
- Visualization
"""

__version__ = '1.4.0'

# Expose commonly used functionality at package level
from ofs_skill.model_processing.model_properties import ModelProperties

__all__ = [
    'ModelProperties',
]
