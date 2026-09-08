"""
Model Format Properties

This module defines the ModelFormatProperties class which holds
model format configuration.
"""

from typing import Any


class ModelFormatProperties:
    """Model time/obs format metadata used when reading NetCDF output.

    Attributes:
        model_time: Time-coordinate description or units string.
        model_obs: Observation-related format hint.
        model_lang: Model framework label (e.g. ``python``, ``fortran``).
        data_model: Optional loaded model data object.
    """

    def __init__(self):
        """Initialize empty format fields."""
        self.model_time: str = ''
        self.model_obs: str = ''
        self.model_lang: str = ''
        self.data_model: Any | None = None

    def __repr__(self) -> str:
        """String representation of ModelFormatProperties."""
        return (f"ModelFormatProperties(model_time='{self.model_time}', "
                f"model_lang='{self.model_lang}')")
