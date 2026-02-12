"""
Model Format Properties

This module defines the ModelFormatProperties class which holds
model format configuration.
"""

from typing import Any, Optional


class ModelFormatProperties:
    """
    Properties for model format configuration.

    This class stores model format-specific properties including
    time format, observation format, and model language.

    Attributes
    ----------
    model_time : str
        Model time format specification
    model_obs : str
        Model observation format specification
    model_lang : str
        Model language/framework (e.g., 'python', 'fortran')
    data_model : Any, optional
        Model data object or structure

    Examples
    --------
    >>> format_props = ModelFormatProperties()
    >>> format_props.model_time = "hours since 1970-01-01"
    >>> format_props.model_lang = "python"
    """

    def __init__(self):
        """Initialize ModelFormatProperties with default values."""
        self.model_time: str = ''
        self.model_obs: str = ''
        self.model_lang: str = ''
        self.data_model: Optional[Any] = None

    def __repr__(self) -> str:
        """String representation of ModelFormatProperties."""
        return (f"ModelFormatProperties(model_time='{self.model_time}', "
                f"model_lang='{self.model_lang}')")
