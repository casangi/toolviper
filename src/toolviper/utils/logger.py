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

import os
import sys
import dask
import logging

from datetime import datetime
from toolviper.utils.console import Colorize
from toolviper.utils.console import add_verbose_info

from dask.distributed import get_worker

from contextvars import ContextVar

from typing import Union

VERBOSE = True
DEFAULT = False

# global verbosity flag
verbosity: Union[ContextVar[bool], ContextVar[None]] = ContextVar(
    "message_verbosity",
    default=None
)


def set_verbosity(state: Union[None, bool] = None):
    print(f"Setting verbosity to {state}")

    verbosity.set(state)


def info(message: str, verbose: bool = False):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="blue")

    logger = get_logger(logger_name=logger_name)
    logger.info(message)


def log(message: str, verbose: bool = False):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="blue")

    logger = get_logger(logger_name=logger_name)

    logger.log(logger.level, message)


def exception(message: str, verbose: bool = False):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="blue")

    logger = get_logger(logger_name=logger_name)

    logger.exception(message)


def debug(message: str, verbose: bool = False):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="green")

    logger = get_logger(logger_name=logger_name)
    logger.debug(message)


def warning(message: str, verbose: bool = False):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="orange")

    logger = get_logger(logger_name=logger_name)
    logger.warning(message)


def error(message: str, verbose: bool = True):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="red")

    logger = get_logger(logger_name=logger_name)
    logger.error(message)


def critical(message: str, verbose: bool = True):
    logger_name = os.getenv("LOGGER_NAME")

    if verbosity.get() is True or False:
        verbose = verbosity.get()

    if verbose:
        message = add_verbose_info(message=message, color="alert")

    logger = get_logger(logger_name=logger_name)
    logger.critical(message)


class ColorLoggingFormatter(logging.Formatter):
    colorize = Colorize()

    function = " [{function}] ".format(function=colorize.blue("%(funcName)s"))
    verbose = " [{exechain}] ".format(
        exechain=colorize.blue("%(filename)s:%(lineno)s : %(module)s.%(funcName)s")
    )

    start_msg = "[{time}] ".format(time=colorize.purple("%(asctime)s"))
    middle_msg = "{level}".format(level="%(levelname)8s")
    execution_msg = " {name} [ {filename} ]: {exec_info}: ".format(
        name="%(name)10s",
        filename="%(filename)-20s",
        exec_info=colorize.blue("%(callchain)-45s"),
    )

    FORMATS = {
        logging.DEBUG: start_msg
        + colorize.green(middle_msg)
        + colorize.grey("  %(name)10s: ")
        + " %(message)s",
        logging.INFO: start_msg
        + colorize.blue(middle_msg)
        + colorize.grey("  %(name)10s: ")
        + " %(message)s ",
        logging.WARNING: start_msg
        + colorize.orange(middle_msg)
        + colorize.grey("  %(name)10s: ")
        + " %(message)s ",
        logging.ERROR: start_msg
        + colorize.red(middle_msg)
        + colorize.grey("  %(name)10s: ")
        + " %(message)s",
        logging.CRITICAL: start_msg
        + colorize.format(text=middle_msg, color=[220, 60, 20], highlight=True)
        + colorize.grey("  %(name)10s: ")
        + " %(message)s",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class LoggingFormatter(logging.Formatter):
    function = " [{function}] ".format(function="%(funcName)s")
    verbose = " [{exechain}] ".format(
        exechain="%(filename)s:%(lineno)s : %(module)s.%(funcName)s"
    )

    start_msg = "[{time}] ".format(time="%(asctime)s")
    middle_msg = "{level}".format(level="%(levelname)8s")
    execution_msg = " {name} [ {filename} ]: {exec_info}: ".format(
        name="%(name)10s", filename="%(filename)-20s", exec_info="%(callchain)-45s"
    )

    FORMATS = {
        logging.DEBUG: start_msg + middle_msg + "  %(name)10s: " + " %(message)s",
        logging.INFO: start_msg + middle_msg + "  %(name)10s: " + " %(message)s ",
        logging.WARNING: start_msg + middle_msg + "  %(name)10s: " + " %(message)s ",
        logging.ERROR: start_msg + middle_msg + "  %(name)10s: " + " %(message)s",
        logging.CRITICAL: start_msg + middle_msg + "  %(name)10s: " + " %(message)s",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_logger(logger_name: Union[str, None] = None):
    if logger_name is None:
        if os.getenv("LOGGER_NAME"):
            # Return default logger from env if none is specified.
            logger_name = os.getenv("LOGGER_NAME")
        else:
            logger_name = "viperlog"

    try:
        worker = get_worker()

    except ValueError:
        # Scheduler processes
        logger_dict = logging.Logger.manager.loggerDict
        if logger_name in logger_dict:
            logger = logging.getLogger(logger_name)
        else:
            # If main logger is not started using client function it defaults to printing to term.
            logger = logging.getLogger(logger_name)
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(ColorLoggingFormatter())
            logger.addHandler(stream_handler)
            logger.setLevel(logging.getLevelName("INFO"))

        return logger

    try:
        logger = worker.plugins["worker_logger"].get_logger()

        return logger

    except Exception as e:
        print("Could not load worker logger: {}".format(e))
        print(worker.plugins.keys())

        return logging.getLogger()


def setup_logger(
    logger_name: Union[str, None] = None,
    log_to_term: bool = False,
    log_to_file: bool = True,
    log_file: str = "logger",
    log_level: str = "INFO",
):
    """To set up as many loggers as you want"""
    if logger_name is None:
        logger_name = "viperlog"

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.getLevelName(log_level))

    logger.handlers.clear()

    if log_to_term:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(ColorLoggingFormatter())
        logger.addHandler(stream_handler)

    if log_to_file:
        log_file = log_file + datetime.today().strftime("%Y%m%d_%H%M%S") + ".log"
        log_handler = logging.FileHandler(log_file)
        log_handler.setFormatter(LoggingFormatter())
        logger.addHandler(log_handler)

    return logger


def get_worker_logger_name(logger_name: Union[str, None] = None):
    if logger_name is None:
        logger_name = "viperlog"

    return "_".join((logger_name, str(get_worker().id)))


def setup_worker_logger(
    logger_name: str,
    log_to_term: bool,
    log_to_file: bool,
    log_file: str,
    log_level: str,
    worker: dask.distributed.worker.Worker,
):
    parallel_logger_name = "_".join((logger_name, str(worker.name)))

    logger = logging.getLogger(parallel_logger_name)
    logger.setLevel(logging.getLevelName(log_level))

    if log_to_term:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(ColorLoggingFormatter())
        logger.addHandler(stream_handler)

    if log_to_file:
        logger.info(f"log_to_file: {log_file}")
        dask.distributed.print(f"log_to_file: {log_to_file}")

        log_file = (
            log_file
            + "_"
            + str(worker.name)
            + "_"
            + datetime.today().strftime("%Y%m%d_%H%M%S")
            + "_"
            + str(worker.ip)
            + ".log"
        )
        log_handler = logging.FileHandler(log_file)
        log_handler.setFormatter(LoggingFormatter())
        logger.addHandler(log_handler)

    return logger
