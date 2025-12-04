"""
Filename:   utils.py
Created:    2019/06/18

Utility class for various helper functions

Revision History:

"""


# System library imports
import configparser
from pathlib import Path


class Utils:
    """
    Utility class for various helper functions
    """
    config_file = "conf/ofs_dps.conf"
    config_file = (Path(__file__).parent.parent.parent / config_file).resolve()

    def get_config_file(self):
        """get_config_file"""
        return self.config_file

    def read_config_section(self, section, logger):
        """
        Read a configuration file section (denoted as [section]) and
        store in a dictionary {}. Returns empty hash if no parameters found
        Args:
          config_file (str): configuration file to read
          section (str): name of the section to read

        Returns:
          Dictionary with configuration parameters from requested section

        Raises:
          None
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
                        logger.error("Could not read option: %s", option)
                except RuntimeError:
                    logger.error("Exception reading option %s!",
                                 option, exc_info=True)
                    params[option] = None
        except configparser.NoSectionError as nse:
            logger.error(
                "No section %s found reading %s: %s",
                section,
                self.config_file,
                nse,
                exc_info=True,
            )
        except IOError as ioe:
            logger.error(
                "Config file not found: %s: %s", self.config_file,
                ioe, exc_info=True
            )

        return params
