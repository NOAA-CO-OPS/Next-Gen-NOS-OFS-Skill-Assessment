"""
OFS Model Properties

This module defines the ModelProperties class which holds configuration
and path information for OFS model operations.
"""

import copy
from typing import Any

# Attributes excluded from deepcopy. ``_cached_model`` holds the loaded
# xarray model dataset (a dask graph referencing hundreds of backing
# files); deep-copying it is expensive and no consumer needs a private
# copy — every access goes through ``getattr(..., None)`` and treats a
# missing attribute as a cache miss. Without this guard, the per-station
# ``copy.deepcopy(prop)`` in the plotting fan-out duplicated the whole
# dataset twice per station.
_DEEPCOPY_SKIP_ATTRS = frozenset({'_cached_model', '_cached_model_key'})


class ModelProperties:
    """Configuration bag for OFS model processing and skill runs.

    Holds the OFS id, whichcast mode, vertical datum, time window, and
    derived directory paths used across model extraction, pairing, and
    plotting. Callers typically construct an instance and set fields from
    CLI args or a config file before passing ``prop`` into pipeline
    functions.

    Attributes:
        ofs: OFS identifier (e.g. ``cbofs``, ``ngofs2``).
        whichcast: Forecast type (``nowcast``, ``forecast_a``, ``forecast_b``).
        whichcasts: Comma-separated list of forecast types when running several.
        forecast_hr: Forecast hour string when applicable.
        path: Root working directory.
        datum: Vertical datum (e.g. ``MLLW``, ``NAVD88``).
        datum_list: Space-separated datums from config when batching.
        start_date_full: Run start in ISO form ``YYYY-MM-DDTHH:MM:SSZ``.
        end_date_full: Run end in ISO form.
        ofsfiletype: Model file kind (``stations`` or ``fields``).
        model_source: Framework once detected (``roms``, ``fvcom``, ``schism``).
        control_files_path: Directory for ``*.ctl`` files.
        model_path: Directory for downloaded/extracted model files.
        data_model_1d_node_path: 1D model node/time-series outputs.
        data_observations_1d_station_path: 1D observation station outputs.
        data_skill_1d_pair_path: Paired ``.int`` skill inputs.
        data_skill_1d_table_path: Skill tables.
        visuals_1d_station_path: 1D station plot outputs.
        startdate: Run start in compact form used by file naming.
        enddate: Run end in compact form used by file naming.
        stationowner: Observation provider filter (e.g. ``CO-OPS``, ``USGS``).
        user_input_location: User-supplied location/bbox override, when set.
        horizonskill: Forecast-horizon skill mode flag.
        var_list: Variables selected for the run.
        filecheck: Flag controlling model-file availability checks.
        filepath: Caller-supplied file path override, when set.
        currents_bins_csv: Optional ADCP bin-override CSV path.
        continue_run: Extend existing artifacts instead of regenerating
            them, fetching only the tail missing from the run window.
        continue_overlap_hours: Hours of already-retrieved data a
            continuation run re-fetches before the seam.
        config_file: Path to the config file in use, when set.
        ofs_extents_path: Directory of OFS extent shapefiles.
        data_model_2d_json_path: 2D model JSON outputs.
        data_observations_2d_station_path: 2D observation station outputs.
        data_observations_2d_json_path: 2D observation JSON outputs.
        data_skill_stats_path: Summary skill statistics outputs.
        data_skill_2d_json_path: 2D skill JSON outputs.
        visuals_2d_station_path: 2D station plot outputs.
        ice_dt: Ice time-step setting.
        dailyavg: Daily-average toggle for ice runs.
        data_skill_ice1dpair_path: Paired 1D ice skill inputs.
        data_model_ice_path: Ice model outputs.
        data_observations_2d_satellite_path: 2D satellite observation outputs.
        visuals_maps_ice_path: Ice map plot outputs.
        visuals_1d_ice_path: 1D ice plot outputs.
        visuals_stats_ice_path: Ice statistics plot outputs.

    Example:
        ```python
        prop = ModelProperties()
        prop.ofs = "cbofs"
        prop.datum = "MLLW"
        prop.path = "./"
        prop.whichcast = "nowcast"
        prop.start_date_full = "2025-07-01T00:00:00Z"
        prop.end_date_full = "2025-07-02T00:00:00Z"
        ```
    """

    def __init__(self):
        """Create an empty ``ModelProperties`` with blank/default fields."""
        # Many of these attributes are reassigned downstream to bool/None
        # values (e.g. user_input_location is a bool once set from the CLI,
        # forecast_hr is Optional[str]). Typed as Any so dynamic attribute
        # sets from argparse don't trigger mypy assignment errors.
        self.ofs: Any = ''
        self.whichcast: Any = ''
        self.whichcasts: Any = ''
        self.forecast_hr: Any = ''
        self.path: Any = ''
        self.datum: Any = ''
        self.datum_list: Any = ''
        self.start_date_full: Any = ''
        self.end_date_full: Any = ''
        self.startdate: Any = ''
        self.enddate: Any = ''
        self.ofsfiletype: Any = ''
        self.stationowner: Any = ''
        self.user_input_location: Any = ''
        self.horizonskill: Any = ''
        self.horizon_extra_plots: Any = False
        self.var_list: Any = ''
        self.filecheck: Any = ''
        # Extension attrs set dynamically by various CLI entrypoints.
        self.currents_bins_csv: Any = None
        self.filepath: Any = ''
        self.continue_run: Any = False
        self.continue_overlap_hours: Any = 24.0

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

        # Ice-specific paths & variables
        self.ice_dt: str = ''
        self.dailyavg: str = ''
        self.data_skill_ice1dpair_path: str = ''
        self.visuals_maps_ice_path: str = ''
        self.visuals_1d_ice_path: str = ''
        self.visuals_stats_ice_path: str = ''
        self.data_observations_2d_satellite_path: str = ''
        self.data_model_ice_path: str = ''

        self.model_source: str = ''
        self.config_file = None

    def __repr__(self) -> str:
        """String representation of ModelProperties."""
        return f"ModelProperties(ofs='{self.ofs}', datum='{self.datum}')"

    def __deepcopy__(self, memo):
        """Deep-copy all attributes except the cached model dataset.

        Copies come back without ``_cached_model`` / ``_cached_model_key``;
        consumers read those via ``getattr(..., None)`` and treat their
        absence as a cache miss, so behavior is unchanged apart from the
        copy no longer dragging a full dask graph along.
        """
        cls = self.__class__
        clone = cls.__new__(cls)
        memo[id(self)] = clone
        for key, value in self.__dict__.items():
            if key in _DEEPCOPY_SKIP_ATTRS:
                continue
            setattr(clone, key, copy.deepcopy(value, memo))
        return clone
