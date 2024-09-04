import os

from importlib.metadata import version
from vipercore.utils.logger import setup_logger

__version__ = version("vipercore")

# Setup default logger instance for module
if not os.getenv("VIPER_LOGGER_NAME"):
    os.environ["VIPER_LOGGER_NAME"] = "vipercore"
    setup_logger(
        logger_name="vipercore",
        log_to_term=True,
        log_to_file=False,
        log_file="vipercore-logfile",
        log_level="INFO",
    )