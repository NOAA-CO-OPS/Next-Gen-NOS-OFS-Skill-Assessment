"""
Properties for retrieve station operations.

This module defines the base properties class used for retrieving station data.
"""


class RetrieveProperties:
    """Base property bag for single-station observation retrieval.

    Attributes:
        station: Station identifier.
        year: Year string for retrieval.
        variable: Variable name (e.g. ``water_level``, ``temperature``).
        month_num: Month number string.
        month: Month name string.
        start_date: Retrieval start.
        end_date: Retrieval end.
        datum: Vertical datum when applicable.
    """

    def __init__(self):
        """Initialize empty retrieval fields."""
        self.station: str = ''
        self.year: str = ''
        self.variable: str = ''
        self.month_num: str = ''
        self.month: str = ''
        self.start_date: str = ''
        self.end_date: str = ''
        self.datum: str = ''
