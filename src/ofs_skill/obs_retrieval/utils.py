"""
Observation Retrieval Utilities

Utility class for configuration management and helper functions.
"""

import configparser
import logging
import math
import os
import sys
import threading
from pathlib import Path

# Parsed-config cache keyed on file path, invalidated by (mtime_ns, size).
# The pipeline re-reads config sections constantly — per variable, per
# station in the obs fan-out, per plot dispatch — and each call was a
# fresh configparser.read() from disk. Caching the parsed object keeps
# every call after the first in memory while an edited file (new mtime)
# still reparses.
_CONFIG_PARSE_CACHE: dict[Path, tuple[tuple[int, int],
                                      configparser.ConfigParser]] = {}
_CONFIG_PARSE_LOCK = threading.Lock()


def _read_config_cached(config_file: Path) -> configparser.ConfigParser:
    """Return a parsed ConfigParser for ``config_file``, cached by mtime.

    Raises ``OSError`` when the file is missing, matching what callers
    already handle for unreadable configs.
    """
    stat = os.stat(config_file)
    key = (stat.st_mtime_ns, stat.st_size)
    with _CONFIG_PARSE_LOCK:
        cached = _CONFIG_PARSE_CACHE.get(config_file)
        if cached is not None and cached[0] == key:
            return cached[1]
    config = configparser.ConfigParser()
    config.read(config_file)
    with _CONFIG_PARSE_LOCK:
        _CONFIG_PARSE_CACHE[config_file] = (key, config)
    return config


class Utils:
    """Helpers for reading ``ofs_dps.conf`` and related path resolution.

    Attributes:
        config_file: Path to the INI config (default ``conf/ofs_dps.conf``).

    Example:
        ```python
        utils = Utils()
        dir_params = utils.read_config_section("directories", logger)
        home = dir_params["home"]
        ```
    """

    def __init__(self, config_file=None):
        """
        Initialize Utils with path to configuration file.

        Args:
            config_file: Optional path to a config file. If None,
                defaults to conf/ofs_dps.conf relative to the package root.
                Falls back to conf/ofs_dps.conf.example if the local conf
                is missing.
        """
        project_root = Path(__file__).parent.parent.parent.parent

        if config_file is not None:
            self.config_file = Path(config_file).resolve()
            if not self.config_file.is_file():
                raise FileNotFoundError(
                    f'Configuration file not found: {self.config_file}'
                )
        else:
            config_file_path = (project_root / 'conf' / 'ofs_dps.conf').resolve()
            example_file = (project_root / 'conf' / 'ofs_dps.conf.example').resolve()
            if config_file_path.exists():
                self.config_file = config_file_path
            elif example_file.exists():
                logging.getLogger(__name__).warning(
                    'conf/ofs_dps.conf not found — falling back to '
                    'conf/ofs_dps.conf.example. Copy it to conf/ofs_dps.conf '
                    'and set home= to your working directory.'
                )
                self.config_file = example_file
            else:
                self.config_file = config_file_path  # will error later

    def get_config_file(self) -> Path:
        """
        Get the path to the configuration file.

        Returns
        -------
        Path
            Absolute path to the configuration file

        Examples
        --------
        >>> utils = Utils()
        >>> config_path = utils.get_config_file()
        >>> assert config_path.exists()
        """
        return self.config_file

    def read_config_section(
        self,
        section: str,
        logger: logging.Logger
    ) -> dict[str, str]:
        """
        Read a configuration file section and return as dictionary.

        Reads all options from the specified section of the configuration file
        and returns them as a dictionary.

        Parameters
        ----------
        section : str
            Name of the section to read (e.g., 'directories', 'urls')
        logger : logging.Logger
            Logger instance for error reporting

        Returns
        -------
        Dict[str, str]
            Dictionary with configuration parameters from the section.
            Returns empty dict if section not found or file cannot be read.

        Examples
        --------
        >>> import logging
        >>> logger = logging.getLogger(__name__)
        >>> utils = Utils()
        >>> params = utils.read_config_section('directories', logger)
        >>> print(params.keys())
        dict_keys(['home', 'data_dir', 'model_dir', ...])

        >>> # Access individual parameters
        >>> home_dir = params.get('home', './')
        >>> print(home_dir)
        ./

        Notes
        -----
        Common configuration sections:

        - 'directories': File system paths
        - 'urls': Remote data sources
        - 'stations': Station configuration
        - 'parameters': Processing parameters

        If the section doesn't exist or the file cannot be read,
        an error is logged and an empty dictionary is returned.
        """
        params = {}

        try:
            config = _read_config_cached(self.config_file)
            options = config.options(section)

            for option in options:
                try:
                    params[option] = config.get(section, option)
                    if params[option] == -1:
                        logger.error(f'Could not read option: {option}')
                except RuntimeError:
                    logger.error(
                        f'Exception reading option {option}!',
                        exc_info=True
                    )
                    params[option] = ''

            if section == 'directories':
                # Directory settings may be relative (subdir of the working
                # directory) or absolute (used as-is, e.g. an external disk).
                # Expand ~ so users can write home-relative paths.
                params = {
                    key: os.path.expanduser(value)
                    for key, value in params.items()
                }

        except configparser.NoSectionError as nse:
            logger.error(
                f"No section '{section}' found reading {self.config_file}: {nse}",
                exc_info=True,
            )
        except OSError as ioe:
            logger.error(
                f'Config file not found: {self.config_file}: {ioe}',
                exc_info=True
            )

        return params

    def validate_config(self, logger: logging.Logger) -> bool:
        """
        Validate that the configuration file exists and is readable.

        Parameters
        ----------
        logger : logging.Logger
            Logger instance

        Returns
        -------
        bool
            True if configuration file exists and is readable, False otherwise
        """
        if not self.config_file.exists():
            logger.error(f'Configuration file not found: {self.config_file}')
            return False

        if not self.config_file.is_file():
            logger.error(f'Configuration path is not a file: {self.config_file}')
            return False

        try:
            config = configparser.ConfigParser()
            config.read(self.config_file)
            logger.info(f'Configuration file validated: {self.config_file}')
            return True
        except Exception as e:
            logger.error(f'Error reading configuration file: {e}', exc_info=True)
            return False


def get_project_root() -> Path:
    """
    Return the absolute path to the installation (repository) root.

    This is the directory that contains the packaged input assets shipped
    with the skill assessment: ``ofs_extents/``, ``conf/``, and
    ``src/wcofs_msl.nc``.
    """
    return Path(__file__).resolve().parent.parent.parent.parent


def resolve_asset_path(base_path, *parts) -> str:
    """
    Resolve the path to a packaged input asset.

    Input assets (``ofs_extents/`` shapefiles, ``conf/logging.conf``,
    ``conf/error_ranges.csv``, ``src/wcofs_msl.nc``) ship with the
    installation, while the working directory (``-p`` / ``home=``) may be
    anywhere — including an external disk. Looks under *base_path* first so
    a user can still override an asset by placing a copy in their working
    directory, then falls back to the installation root.

    Parameters
    ----------
    base_path : str or Path or None
        The working directory (usually ``prop.path``). May be None.
    *parts : str
        Path components of the asset relative to either root,
        e.g. ``('conf', 'error_ranges.csv')``.

    Returns
    -------
    str
        The working-directory candidate if it exists, otherwise the
        installation-root candidate. The returned path is not guaranteed
        to exist — callers that require the asset should check.
    """
    relative = os.path.join(*parts)
    if base_path is not None:
        candidate = os.path.join(str(base_path), relative)
        if os.path.exists(candidate):
            return candidate
    return str(get_project_root() / relative)


def get_s3_cache_dir(config_file=None, logger=None) -> str:
    """
    Return the directory used for the fsspec cache of streamed model files.

    Reads ``s3_cache_dir`` from the ``[directories]`` config section. When
    the key is missing or blank, defaults to ``~/.ofs_cache/s3``. Point
    this at a large disk if your home directory has a size quota — streamed
    NODD model files can accumulate several GB here.

    Parameters
    ----------
    config_file : str or Path or None
        Optional path to an ofs_dps.conf-style file. Pass
        ``getattr(prop, 'config_file', None)`` from callers so ``-c``
        selects the file the cache setting is read from.
    logger : logging.Logger or None
        Logger instance. If None, a module-level logger is used.
    """
    default = os.path.join(os.path.expanduser('~'), '.ofs_cache', 's3')
    if logger is None:
        logger = logging.getLogger(__name__)
    dir_params = Utils(config_file).read_config_section('directories', logger)
    configured = (dir_params.get('s3_cache_dir') or '').strip()
    if not configured:
        return default
    return configured


def redact_secrets(
    text: str | None, env_keys: tuple[str, ...] = ('API_USGS_PAT',)
) -> str | None:
    """Return ``text`` with known secret env values replaced by ``***``.

    Use before logging exceptions or writing diagnostics so tokens never
    appear in CI logs, tracebacks, or uploaded artifacts.

    Args:
        text: Raw string that may contain secret values, or ``None``.
        env_keys: Environment variable names whose values should be redacted.

    Returns:
        Redacted string, or ``None`` if ``text`` was ``None``.

    Note:
        Only values with length ``>= 6`` are redacted, so a pathologically
        short or empty secret cannot mangle unrelated log text. This is
        belt-and-suspenders on top of GitHub ``::add-mask::``.
    """
    if text is None:
        return None
    out = str(text)
    for key in env_keys:
        value = os.environ.get(key, '').strip()
        if len(value) >= 6:
            out = out.replace(value, '***')
    return out


def load_api_keys(config_filename='conf/api_keys.conf'):
    """Load API keys from a config file into environment variables.

    Reads ``KEY=VALUE`` lines and sets each key only if it is not already
    present in the environment (conda/CI env vars win).

    Args:
        config_filename: Path relative to the project root, or absolute.
            Default ``conf/api_keys.conf``.

    Returns:
        None. Side effect: may set ``os.environ`` entries.

    Note:
        Comment/blank lines and empty values are skipped. Missing files are
        ignored (debug log only). Values are never logged — only key names.
    """
    logger = logging.getLogger(__name__)

    config_path = Path(config_filename)
    if not config_path.is_absolute():
        # Navigate from src/ofs_skill/obs_retrieval/ up to project root
        project_root = Path(__file__).parent.parent.parent.parent
        config_path = (project_root / config_path).resolve()

    if not config_path.is_file():
        logger.debug('API keys config file not found: %s', config_path)
    else:
        with open(config_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip()
                if not key or not value:
                    continue
                if key not in os.environ:
                    os.environ[key] = value
                    logger.info('Loaded %s from %s', key, config_path)
                else:
                    logger.info(
                        '%s already set in environment, ignoring value from '
                        'config file',
                        key,
                    )

    if 'API_USGS_PAT' not in os.environ:
        logger.warning(
            'API_USGS_PAT is not set. USGS API requests will be limited to '
            '50/hour. Set it in conf/api_keys.conf or as an environment variable.'
        )


def _auto_workers(key):
    """Return an automatic worker count for *key* based on CPU count.

    I/O-bound pools (obs retrieval, model download, plotting) can safely
    exceed the CPU count because threads spend most of their time waiting
    on network or disk.  CPU-bound pools (harmonic analysis) are capped
    at ``cpu_count - 1`` (max 8) to leave headroom for the main process.

    Parameters
    ----------
    key : str
        Config key name, e.g. ``'obs_coops_workers'``.

    Returns
    -------
    int
        Positive worker count (always >= 1).
    """
    cpus = os.cpu_count() or 2

    # CPU-bound: leave one core free, cap at 8
    if key == 'ha_workers':
        return max(1, min(cpus - 1, 8))

    # I/O-bound defaults scale with CPU count
    io_defaults = {
        'obs_coops_workers': min(cpus * 2, 12),
        'obs_usgs_workers': min(max(cpus // 2, 2), 6),
        'obs_ndbc_workers': min(cpus * 2, 12),
        'obs_chs_workers': min(max(cpus // 4, 1), 4),
        'model_download_workers': min(cpus, 8),
        'skill_workers': min(cpus, 8),
        'plot_workers': min(cpus, 8),
    }
    return max(1, io_defaults.get(key, min(cpus, 4)))


def get_parallel_config(logger=None, config_file=None):
    """
    Read parallelization settings from the [parallelization] config section.

    Returns a dict with integer worker counts and boolean flags.
    If the section is missing or unreadable, returns safe defaults
    (parallel_enabled=True with conservative worker counts).

    Parameters
    ----------
    logger : logging.Logger or None
        Logger instance. If None, a module-level logger is used.
    config_file : str or Path or None
        Optional path to an ofs_dps.conf-style file. Pass
        ``getattr(prop, 'config_file', None)`` from CLI entry points so the
        ``-c <conf>`` flag actually selects which parallelization knobs
        are read. When None, falls back to the repo default.
    """
    defaults = {
        'parallel_enabled': True,
        'obs_coops_workers': 6,
        'obs_usgs_workers': 2,
        'obs_ndbc_workers': 6,
        'obs_chs_workers': 1,
        'model_download_workers': 4,
        'skill_workers': 4,
        'ha_workers': _auto_workers('ha_workers'),
        'plot_workers': 4,
        'parallel_variables': False,
        'parallel_workflow': False,
        'parallel_stations': False,
        'parallel_plotting': False,
        'parallel_forecast_cycles': False,
        'parallel_obs_variables': False,
        'parallel_2d_interp': False,
        'parallel_extract_slots': 2,
        'coops_request_concurrency': 2,
        'coops_request_gap_sec': 0.5,
    }

    if logger is None:
        logger = logging.getLogger(__name__)

    raw = Utils(config_file=config_file).read_config_section(
        'parallelization', logger)
    if not raw:
        return defaults

    result = dict(defaults)

    # Parse parallel_enabled
    val = raw.get('parallel_enabled', 'true').strip().lower()
    result['parallel_enabled'] = val in ('true', '1', 'yes')

    # Parse parallel_variables
    val = raw.get('parallel_variables', 'false').strip().lower()
    result['parallel_variables'] = val in ('true', '1', 'yes')

    # Parse parallel_workflow
    val = raw.get('parallel_workflow', 'false').strip().lower()
    result['parallel_workflow'] = val in ('true', '1', 'yes')

    # Parse parallel_stations
    val = raw.get('parallel_stations', 'false').strip().lower()
    result['parallel_stations'] = val in ('true', '1', 'yes')

    # Parse parallel_plotting
    val = raw.get('parallel_plotting', 'false').strip().lower()
    result['parallel_plotting'] = val in ('true', '1', 'yes')

    # Parse parallel_forecast_cycles
    val = raw.get('parallel_forecast_cycles', 'false').strip().lower()
    result['parallel_forecast_cycles'] = val in ('true', '1', 'yes')

    # Parse parallel_obs_variables
    val = raw.get('parallel_obs_variables', 'false').strip().lower()
    result['parallel_obs_variables'] = val in ('true', '1', 'yes')

    # Parse parallel_2d_interp
    val = raw.get('parallel_2d_interp', 'false').strip().lower()
    result['parallel_2d_interp'] = val in ('true', '1', 'yes')

    # Parse integer worker counts — all support "auto"
    int_keys = [
        'obs_coops_workers', 'obs_usgs_workers', 'obs_ndbc_workers',
        'obs_chs_workers', 'model_download_workers', 'skill_workers',
        'ha_workers', 'plot_workers',
    ]
    for key in int_keys:
        val = raw.get(key, '').strip().lower()
        if val == 'auto':
            result[key] = _auto_workers(key)
        elif val:
            try:
                result[key] = min(max(1, int(val)), 64)
            except ValueError:
                pass

    # Parse parallel_extract_slots — max concurrent dask.compute calls
    # across variable threads during model extraction. Only takes effect
    # under the thread-safe h5netcdf engine (thread-unsafe engines are
    # always fully serialized); bounds peak memory, since each slot holds
    # one variable's time-window materialization.
    val = raw.get('parallel_extract_slots', '').strip().lower()
    if val:
        try:
            result['parallel_extract_slots'] = min(max(1, int(val)), 8)
        except ValueError:
            pass

    # Parse CO-OPS rate-limit overrides. These feed the process-wide
    # limiter in retrieve_t_and_c_station (concurrent in-flight GETs and
    # minimum start-to-start gap). Defaults match the values tuned
    # against CO-OPS throttling on long historical windows; raising them
    # risks HTTP 403 chunk drops, so they are opt-in per run.
    val = raw.get('coops_request_concurrency', '').strip().lower()
    if val:
        try:
            result['coops_request_concurrency'] = min(max(1, int(val)), 8)
        except ValueError:
            pass
    val = raw.get('coops_request_gap_sec', '').strip().lower()
    if val:
        try:
            result['coops_request_gap_sec'] = min(max(0.0, float(val)), 10.0)
        except ValueError:
            pass

    # If parallelization is globally disabled, force all workers to 1
    if not result['parallel_enabled']:
        for key in int_keys + ['ha_workers']:
            result[key] = 1
        result['parallel_extract_slots'] = 1
        result['coops_request_concurrency'] = 1

    return result


def get_station_match_max_dist(logger=None, config_file=None):
    """Read the station-matching distance cutoff (km) from ``[settings]``.

    Returns the ``station_match_max_dist_km`` value from the ``[settings]``
    section of the config, falling back to the package default when the key
    is absent, blank, or unparseable. The same value drives both the exact
    great-circle match cutoff and the (latitude-aware) candidate pre-filter
    box in ``index_nearest_station``, so matching stays consistent.

    Parameters
    ----------
    logger : logging.Logger or None
        Logger instance. If None, a module-level logger is used.
    config_file : str or Path or None
        Optional path to an ofs_dps.conf-style file (typically
        ``getattr(prop, 'config_file', None)``). When None, falls back to the
        repo default config.

    Returns
    -------
    float
        The cutoff distance in kilometers (always finite and > 0).
    """
    # Imported here (not at module top) so that importing obs_retrieval.utils
    # never pulls in the heavier model_processing package at module-import
    # time (utils is imported very early, including by CLI --help paths).
    from ofs_skill.model_processing.indexing import (  # pylint: disable=import-outside-toplevel
        STATION_MATCH_MAX_DIST_KM,
    )

    default = STATION_MATCH_MAX_DIST_KM

    if logger is None:
        logger = logging.getLogger(__name__)

    raw = Utils(config_file=config_file).read_config_section(
        'settings', logger)
    val = (raw or {}).get('station_match_max_dist_km', '').strip()
    if not val:
        return default
    try:
        parsed = float(val)
    except ValueError:
        logger.warning(
            'station_match_max_dist_km=%r is not a number; using default '
            '%.1f km', val, default,
        )
        return default
    if not math.isfinite(parsed) or parsed <= 0:
        logger.warning(
            'station_match_max_dist_km=%r must be a finite positive number; '
            'using default %.1f km', parsed, default,
        )
        return default
    return parsed


def parse_arguments_to_list(
    argument: str | list[str],
    logger: logging.Logger
) -> list[str]:
    """
    Parse a string argument into a list of strings.

    Takes a user-supplied argument string and parses it to a list of strings.
    Handles bracket notation, spaces, and comma separation.

    Parameters
    ----------
    argument : Union[str, List[str]]
        String to parse (e.g., "[item1, item2, item3]") or already a list
    logger : logging.Logger
        Logger instance for error reporting

    Returns
    -------
    List[str]
        Parsed list of strings

    Examples
    --------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> parse_arguments_to_list("[item1, item2, item3]", logger)
    ['item1', 'item2', 'item3']

    >>> parse_arguments_to_list("item1,item2,item3", logger)
    ['item1', 'item2', 'item3']

    >>> parse_arguments_to_list(['item1', 'item2'], logger)
    ['item1', 'item2']

    Notes
    -----
    - Removes brackets [ ] from input
    - Removes spaces
    - Converts to lowercase
    - Splits on commas
    - If input is already a list, returns it unchanged
    """
    if not isinstance(argument, str):
        logger.info(
            'Input argument (%s) being parsed from str to list is '
            'already a list. Moving on...', argument
        )
        return argument
    parsed = argument.lower().replace('[', '').replace(']', '').replace(
        ' ', ''
    ).split(',')
    try:
        parsed[0]
        return parsed
    except IndexError:
        logger.error(
            'Cannot parse input argument %s! Correct formatting and '
            'try again.', parsed
        )
        sys.exit(-1)
