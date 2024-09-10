import os

from importlib.metadata import version
from toolviper.utils.logger import setup_logger

__version__ = version("toolviper")

# Setup default logger instance for module
if not os.getenv("VIPER_LOGGER_NAME"):
    os.environ["VIPER_LOGGER_NAME"] = "toolviper"
    setup_logger(
        logger_name="toolviper",
        log_to_term=True,
        log_to_file=False,
        log_file="toolviper-logfile",
        log_level="INFO",
    )