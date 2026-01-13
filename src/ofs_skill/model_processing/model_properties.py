"""
OFS Model Properties

This module defines the ModelProperties class which holds configuration
and path information for OFS model operations.
"""



class ModelProperties:
    """
    Properties and configuration for OFS Model operations.

    This class stores all configuration needed for model data processing,
    including OFS identifier, file paths, time ranges, and data paths.

    Attributes
    ----------
    ofs : str
        OFS identifier (e.g., 'cbofs', 'ngofs2')
    whichcast : str
        Forecast type (e.g., 'nowcast', 'forecast_a')
    whichcasts : str
        Comma-separated list of forecast types
    forecast_hr : str
        Forecast hour
    path : Path or str
        Root working directory path
    datum : str
        Vertical datum (e.g., 'MLLW', 'NAVD88')
    datum_list : str
        List of datums to process
    start_date_full : str
        Start date in ISO format (YYYY-MM-DDTHH:MM:SSZ)
    end_date_full : str
        End date in ISO format
    startdate : str
        Start date (shortened format)
    enddate : str
        End date (shortened format)
    ofsfiletype : str
        File type ('stations' or 'fields')
    control_files_path : str
        Path to control files
    model_path : str
        Path to model data
    ofs_extents_path : str
        Path to OFS extent shapefiles
    data_model_1d_node_path : str
        Path for 1D model node data
    data_model_2d_json_path : str
        Path for 2D model JSON data
    data_observations_1d_station_path : str
        Path for 1D observation station data
    data_observations_2d_station_path : str
        Path for 2D observation station data
    data_observations_2d_json_path : str
        Path for 2D observation JSON data
    data_skill_1d_pair_path : str
        Path for 1D skill paired data
    data_skill_1d_table_path : str
        Path for 1D skill tables
    data_skill_stats_path : str
        Path for skill statistics
    data_skill_2d_json_path : str
        Path for 2D skill JSON data
    visuals_1d_station_path : str
        Path for 1D station visualizations
    visuals_2d_station_path : str
        Path for 2D station visualizations
    data_skill_ice1dpair_path : str
        Path for ice 1D paired data
    visuals_maps_ice_path : str
        Path for ice map visualizations
    visuals_1d_ice_path : str
        Path for 1D ice visualizations
    visuals_stats_ice_path : str
        Path for ice statistics visualizations
    data_observations_2d_satellite_path : str
        Path for satellite observation data
    data_model_ice_path : str
        Path for model ice data
    model_source : str
        Model source type (e.g., 'fvcom', 'roms', 'schism')

    Examples
    --------
    >>> prop = ModelProperties()
    >>> prop.ofs = "cbofs"
    >>> prop.datum = "MLLW"
    >>> prop.path = Path("./")
    """

    def __init__(self):
        """Initialize ModelProperties with default values."""
        self.ofs: str = ''
        self.whichcast: str = ''
        self.whichcasts: str = ''
        self.forecast_hr: str = ''
        self.path: str = ''
        self.datum: str = ''
        self.datum_list: str = ''
        self.start_date_full: str = ''
        self.end_date_full: str = ''
        self.startdate: str = ''
        self.enddate: str = ''
        self.ofsfiletype: str = ''

        # Path attributes
        self.control_files_path: str = ''
        self.model_path: str = ''
        self.ofs_extents_path: str = ''
        self.data_model_1d_node_path: str = ''
        self.data_model_2d_json_path: str = ''
        self.data_observations_1d_station_path: str = ''
        self.data_observations_2d_station_path: str = ''
        self.data_observations_2d_json_path: str = ''
        self.data_skill_1d_pair_path: str = ''
        self.data_skill_1d_table_path: str = ''
        self.data_skill_stats_path: str = ''
        self.data_skill_2d_json_path: str = ''
        self.visuals_1d_station_path: str = ''
        self.visuals_2d_station_path: str = ''

        # Ice-specific paths
        self.data_skill_ice1dpair_path: str = ''
        self.visuals_maps_ice_path: str = ''
        self.visuals_1d_ice_path: str = ''
        self.visuals_stats_ice_path: str = ''
        self.data_observations_2d_satellite_path: str = ''
        self.data_model_ice_path: str = ''

        self.model_source: str = ''

    def __repr__(self) -> str:
        """String representation of ModelProperties."""
        return f"ModelProperties(ofs='{self.ofs}', datum='{self.datum}')"
