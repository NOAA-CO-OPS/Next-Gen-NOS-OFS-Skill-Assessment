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

    def __init__(self):
        """
        Initialize Utils with path to configuration file.

        The config file is located relative to the package root:
        <package_root>/conf/ofs_dps.conf

        Falls back to conf/ofs_dps.conf.example if the local config
        file does not exist, with a warning.
        """
        project_root = Path(__file__).parent.parent.parent.parent
        config_file = (project_root / 'conf' / 'ofs_dps.conf').resolve()
        if not config_file.is_file():
            example = (project_root / 'conf' / 'ofs_dps.conf.example').resolve()
            if example.is_file():
                import warnings
                warnings.warn(
                    f'conf/ofs_dps.conf not found — falling back to '
                    f'conf/ofs_dps.conf.example. Copy the example file '
                    f'and set your local paths:\n'
                    f'  cp {example} {config_file}',
                    stacklevel=2,
                )
                config_file = example
        self.config_file = config_file

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
        'ha_workers': max(1, min((os.cpu_count() or 2) - 1, 8)),
        'plot_workers': 4,
        'parallel_variables': False,
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

    # Parse integer worker counts
    int_keys = [
        'obs_coops_workers', 'obs_usgs_workers', 'obs_ndbc_workers',
        'obs_chs_workers', 'model_download_workers', 'skill_workers',
        'plot_workers',
    ]
    for key in int_keys:
        val = raw.get(key, '').strip()
        if val:
            try:
                result[key] = max(1, int(val))
            except ValueError:
                pass

    # Parse ha_workers (supports "auto")
    val = raw.get('ha_workers', 'auto').strip().lower()
    if val == 'auto':
        result['ha_workers'] = max(1, min((os.cpu_count() or 2) - 1, 8))
    else:
        try:
            result['ha_workers'] = max(1, int(val))
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
