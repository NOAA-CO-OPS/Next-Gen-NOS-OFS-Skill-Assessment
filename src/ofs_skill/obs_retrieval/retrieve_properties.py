"""
Properties for retrieve station operations.

This module defines the base properties class used for retrieving station data.
"""


class RetrieveProperties:
    """
    Base properties class for station data retrieval.

    Attributes:
        station: Station identifier
        year: Year for retrieval
        variable: Variable to retrieve (e.g., 'water_level', 'temperature')
        month_num: Month number
        month: Month name
        start_date: Start date for retrieval
        end_date: End date for retrieval
        datum: Vertical datum reference
    """

    def __init__(self):
        self.station: str = ''
        self.year: str = ''
        self.variable: str = ''
        self.month_num: str = ''
        self.month: str = ''
        self.start_date: str = ''
        self.end_date: str = ''
        self.datum: str = ''
