"""
Observation Retrieval Utilities

Utility class for configuration management and helper functions.
"""

import configparser
import logging
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
        """
        config_file = 'conf/ofs_dps.conf'
        # Navigate from src/ofs_skill/obs_retrieval/ up to project root
        self.config_file = (Path(__file__).parent.parent.parent.parent / config_file).resolve()

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
