"""
Observation Retrieval Utilities

Utility class for configuration management and helper functions.
"""

import configparser
import logging
import os
import sys
from pathlib import Path
from typing import Union


class Utils:
    """
    Utility class for configuration file management.

    Provides methods to read and parse configuration files that define
    directory paths, URLs, and other system parameters.

    Attributes
    ----------
    config_file : Path
        Path to the main configuration file (conf/ofs_dps.conf)

    Examples
    --------
    >>> utils = Utils()
    >>> config_path = utils.get_config_file()
    >>> print(config_path)
    /path/to/conf/ofs_dps.conf

    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> dir_params = utils.read_config_section('directories', logger)
    >>> print(dir_params['home'])
    ./

    Notes
    -----
    The configuration file is expected to be in INI format with sections:

    [directories]
    home = ./
    data_dir = data
    ...

    [urls]
    nodd_s3 = https://noaa-nos-ofs-pds.s3.amazonaws.com/
    ...

    [stations]
    ... station configuration ...
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
        config = configparser.ConfigParser()

        try:
            config.read(self.config_file)
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
                    params[option] = None

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


def load_api_keys(config_filename='conf/api_keys.conf'):
    """
    Load API keys from a config file into environment variables.

    Reads a simple KEY=VALUE config file and sets each key as an
    environment variable, but only if it is not already set.
    This allows environment variables (e.g., from conda or CI) to
    take precedence over the config file.

    Parameters
    ----------
    config_filename : str
        Path to the config file, relative to the project root, or an
        absolute path. Default: ``"conf/api_keys.conf"``.

    Notes
    -----
    - Lines starting with ``#`` and blank lines are skipped.
    - Keys with empty values (e.g., ``API_USGS_PAT=``) are skipped.
    - If the file does not exist, a debug message is logged.
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
                    logger.info('%s already set in environment, ignoring value from config file', key)

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


def get_parallel_config(logger=None):
    """
    Read parallelization settings from the [parallelization] config section.

    Returns a dict with integer worker counts and boolean flags.
    If the section is missing or unreadable, returns safe defaults
    (parallel_enabled=True with conservative worker counts).

    Parameters
    ----------
    logger : logging.Logger or None
        Logger instance. If None, a module-level logger is used.

    Returns
    -------
    dict
        Keys: parallel_enabled (bool), obs_coops_workers, obs_usgs_workers,
        obs_ndbc_workers, obs_chs_workers, model_download_workers,
        skill_workers, ha_workers, plot_workers (all int),
        parallel_variables (bool).
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
    }

    if logger is None:
        logger = logging.getLogger(__name__)

    raw = Utils().read_config_section('parallelization', logger)
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

    # If parallelization is globally disabled, force all workers to 1
    if not result['parallel_enabled']:
        for key in int_keys + ['ha_workers']:
            result[key] = 1

    return result


def parse_arguments_to_list(
    argument: Union[str, list[str]],
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
    try:
        argument = argument.lower().replace('[', '').replace(']', '').replace(' ', '').split(',')
    except AttributeError:  # If argument is not a string
        logger.info(
            'Input argument (%s) being parsed from str to list is '
            'already a list. Moving on...', argument
        )
        return argument
    try:
        argument[0]
        return argument
    except IndexError:
        logger.error(
            'Cannot parse input argument %s! Correct formatting and '
            'try again.', argument
        )
        sys.exit(-1)
