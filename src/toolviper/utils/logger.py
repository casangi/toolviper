#   Copyright 2019 AUI, Inc. Washington DC, USA
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Union

import dask
import dask.distributed
from contextvars import ContextVar
from dask.distributed import get_worker

from toolviper.utils.console import Colorize, add_verbose_info

# Global verbosity flag
verbosity: ContextVar[Optional[bool]] = ContextVar("message_verbosity", default=None)

# Constants for default values
DEFAULT_LOGGER_NAME = "viperlog"
LOGGER_ENV_VAR = "VIPER_LOGGER_NAME"


def set_verbosity(state: Optional[bool] = None) -> None:
    """
    Set the global verbosity state.

    Parameters
    ----------
    state : bool, optional
        The verbosity state to set. If None, it uses the default.
    """
    verbosity.set(state)


def _log_message(
    level: str, message: str, verbose: bool = False, color: Optional[str] = None
) -> None:
    """
    Helper function to process and log a message.

    Parameters
    ----------
    level : str
        The logging level (e.g., 'info', 'debug', 'warning').
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    color : str, optional
        The color to use for verbose information.
    """
    logger_name = os.getenv(LOGGER_ENV_VAR, DEFAULT_LOGGER_NAME)

    current_verbosity = verbosity.get()
    if current_verbosity is not None:
        verbose = current_verbosity

    if verbose and color:
        message = add_verbose_info(message=message, color=color)

    logger = get_logger(logger_name=logger_name)
    log_func = getattr(logger, level.lower())
    log_func(message)


def info(message: str, verbose: bool = False) -> None:
    """
    Log an info level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    """
    _log_message("info", message, verbose, color="blue")


def log(message: str, verbose: bool = False) -> None:
    """
    Log a message at the current logger's level.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    """
    logger_name = os.getenv(LOGGER_ENV_VAR, DEFAULT_LOGGER_NAME)
    current_verbosity = verbosity.get()
    if current_verbosity is not None:
        verbose = current_verbosity

    if verbose:
        message = add_verbose_info(message=message, color="blue")

    logger = get_logger(logger_name=logger_name)
    logger.log(logger.level, message)


def exception(message: str, verbose: bool = False) -> None:
    """
    Log an exception level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    """
    _log_message("exception", message, verbose, color="blue")


def debug(message: str, verbose: bool = False) -> None:
    """
    Log a debug level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    """
    _log_message("debug", message, verbose, color="green")


def warning(message: str, verbose: bool = False) -> None:
    """
    Log a warning level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to False.
    """
    _log_message("warning", message, verbose, color="orange")


def error(message: str, verbose: bool = True) -> None:
    """
    Log an error level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to True.
    """
    _log_message("error", message, verbose, color="red")


def critical(message: str, verbose: bool = True) -> None:
    """
    Log a critical level message.

    Parameters
    ----------
    message : str
        The message to log.
    verbose : bool, optional
        Whether to include verbose information. Defaults to True.
    """
    _log_message("critical", message, verbose, color="alert")


class ColorLoggingFormatter(logging.Formatter):
    """
    A logging formatter that adds colors to the output based on the log level.
    """

    colorize = Colorize()

    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None):
        super().__init__(fmt, datefmt)
        self.start_msg = f"[{self.colorize.purple('%(asctime)s')}] "

        self.FORMATS = {
            logging.DEBUG: self.start_msg
            + self.colorize.green("%(levelname)8s")
            + self.colorize.grey("  %(name)10s: ")
            + " %(message)s",
            logging.INFO: self.start_msg
            + self.colorize.blue("%(levelname)8s")
            + self.colorize.grey("  %(name)10s: ")
            + " %(message)s ",
            logging.WARNING: self.start_msg
            + self.colorize.orange("%(levelname)8s")
            + self.colorize.grey("  %(name)10s: ")
            + " %(message)s ",
            logging.ERROR: self.start_msg
            + self.colorize.red("%(levelname)8s")
            + self.colorize.grey("  %(name)10s: ")
            + " %(message)s",
            logging.CRITICAL: self.start_msg
            + self.colorize.format(
                text="%(levelname)8s", color=[220, 60, 20], highlight=True
            )
            + self.colorize.grey("  %(name)10s: ")
            + " %(message)s",
        }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno, self._fmt)
        formatter = logging.Formatter(log_fmt, self.datefmt)

        return formatter.format(record)


class LoggingFormatter(logging.Formatter):
    """
    A standard logging formatter for file output.
    """

    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None):
        super().__init__(fmt, datefmt)
        self.start_msg = "[%(asctime)s] "
        self.middle_msg = "%(levelname)8s"

        self.FORMATS = {
            level: f"{self.start_msg}{self.middle_msg}  %(name)10s:  %(message)s"
            for level in [
                logging.DEBUG,
                logging.INFO,
                logging.WARNING,
                logging.ERROR,
                logging.CRITICAL,
            ]
        }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno, self._fmt)
        formatter = logging.Formatter(log_fmt, self.datefmt)

        return formatter.format(record)


def get_logger(logger_name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance by name, with fallback to environment or defaults.

    Parameters
    ----------
    logger_name : str, optional
        The name of the logger to retrieve.

    Returns
    -------
    logging.Logger
        The logger instance.
    """
    if logger_name is None:
        logger_name = os.getenv(LOGGER_ENV_VAR, DEFAULT_LOGGER_NAME)

    try:
        worker = get_worker()
        # If we're on a worker, try to get the worker-specific logger from the plugin
        if hasattr(worker, "plugins") and "worker_logger" in worker.plugins:
            return worker.plugins["worker_logger"].get_logger()

    except (ValueError, AttributeError, KeyError):
        # Not on a worker, or worker logger plugin not available
        pass

    logger = logging.getLogger(logger_name)

    # If the logger has no handlers, it hasn't been set up yet.
    if not logger.handlers:
        # Default to a simple stream handler if not explicitly set up
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(ColorLoggingFormatter())
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

    return logger


def setup_logger(
    logger_name: Optional[str] = None,
    log_to_term: bool = False,
    log_to_file: bool = True,
    log_file: str = "logger",
    log_level: str = "INFO",
) -> logging.Logger:
    """
    Configure and return a logger.

    Parameters
    ----------
    logger_name : str, optional
        The name of the logger to set up.
    log_to_term : bool, optional
        Whether to log to the terminal. Defaults to False.
    log_to_file : bool, optional
        Whether to log to a file. Defaults to True.
    log_file : str, optional
        The base name of the log file.
    log_level : str, optional
        The logging level (e.g., 'DEBUG', 'INFO'). Defaults to 'INFO'.

    Returns
    -------
    logging.Logger
        The configured logger.
    """
    if logger_name is None:
        logger_name = DEFAULT_LOGGER_NAME

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    if log_to_term:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(ColorLoggingFormatter())
        logger.addHandler(stream_handler)

    if log_to_file:
        timestamp = datetime.today().strftime("%Y%m%d_%H%M%S")
        full_log_file = f"{log_file}{timestamp}.log"
        log_handler = logging.FileHandler(full_log_file)
        log_handler.setFormatter(LoggingFormatter())
        logger.addHandler(log_handler)

    return logger


def get_worker_logger_name(logger_name: Optional[str] = None) -> str:
    """
    Generate a unique logger name for a Dask worker.

    Parameters
    ----------
    logger_name : str, optional
        The base logger name.

    Returns
    -------
    str
        The worker-specific logger name.
    """
    if logger_name is None:
        logger_name = DEFAULT_LOGGER_NAME

    try:
        worker_id = get_worker().id
        return f"{logger_name}_{worker_id}"

    except (ValueError, AttributeError):
        return logger_name


def setup_worker_logger(
    logger_name: str,
    log_to_term: bool,
    log_to_file: bool,
    log_file: str,
    log_level: str,
    worker: "dask.distributed.worker.Worker",
) -> logging.Logger:
    """
    Configure and return a logger for a Dask worker.

    Parameters
    ----------
    logger_name : str
        The base name of the logger.
    log_to_term : bool
        Whether to log to the terminal.
    log_to_file : bool
        Whether to log to a file.
    log_file : str
        The base name of the log file.
    log_level : str
        The logging level.
    worker : dask.distributed.worker.Worker
        The Dask worker instance.

    Returns
    -------
    logging.Logger
        The configured worker logger.
    """
    parallel_logger_name = f"{logger_name}_{worker.name}"

    logger = logging.getLogger(parallel_logger_name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    if log_to_term:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(ColorLoggingFormatter())
        logger.addHandler(stream_handler)

    if log_to_file:
        timestamp = datetime.today().strftime("%Y%m%d_%H%M%S")
        full_log_file = f"{log_file}_{worker.name}_{timestamp}_{worker.ip}.log"
        log_handler = logging.FileHandler(full_log_file)
        log_handler.setFormatter(LoggingFormatter())
        logger.addHandler(log_handler)

    return logger
