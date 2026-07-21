import functools
import os
from importlib import import_module
from importlib.util import find_spec
from typing import Dict, Union, Any, Optional

import dask
import distributed

import toolviper.dask.menrva
import toolviper.utils.console as console
import toolviper.utils.logger as logger

colorize = console.Colorize()

DEFAULT_CLIENT_LOG_PARAMS = {
    "logger_name": "client",
    "log_to_term": True,
    "log_level": "INFO",
    "log_to_file": False,
    "log_file": "client.log",
}

DEFAULT_WORKER_LOG_PARAMS = {
    "logger_name": "worker",
    "log_to_term": True,
    "log_level": "INFO",
    "log_to_file": False,
    "log_file": "client_worker.log",
}


def _get_log_params(
    log_params: Optional[Dict[str, Any]], defaults: Dict[str, Any]
) -> Dict[str, Any]:
    if log_params is None:
        log_params = {}
    return {**defaults, **log_params}


def load_libraries(name: str, libs: Union[str, list[str]]) -> dict[str, bool]:
    """Load libraries if they were installed and can be loaded.

    Parameters
    ----------
    name : str
        A library group name based on a function of a distributed environment.
    libs : Union[str, list[str]]
        A library or a list of libraries to import.

    Returns
    -------
    dict[str, bool]
        A dictionary mapping the group name to a boolean indicating if all libraries were loaded successfully.
    """

    def _load_library(_lib):
        if find_spec(_lib) is not None:
            import_module(_lib)
            return True, f"   {colorize.blue(_lib)} is available"
        return False, f"   {colorize.blue(_lib)} is unavailable"

    if isinstance(libs, str):
        libs = [libs]

    results = [_load_library(lib) for lib in libs]
    all_available = all(res[0] for res in results)

    logger.info(f"Loading module: {name} -- {'Success' if all_available else 'Fail'}")
    for _, message in results:
        logger.info(message)

    return {name: all_available}


def print_libraries_availability(spec: dict[str, bool]):
    """Print the contents of available_specs.

    Parameters
    ----------
    spec : dict[str, bool]
        an instance of available_specs
    """
    loaded_lib = [k for k, v in spec.items() if v]
    logger.debug(
        f"{colorize.green('Available functions of this environment')}: {', '.join(loaded_lib)}"
    )


def get_thread_info() -> Dict[str, float]:
    # This just brings the built-in thread info function into the client module.
    return toolviper.dask.menrva.MenrvaClient.thread_info()


def get_client() -> Union[None, distributed.Client]:
    """
    Get a toolviper client instance
    Returns: None or a toolviper client instance

    """
    try:
        client = distributed.Client.current()

    except ValueError:
        client = None

    if client is None:
        logger.info("There are currently no client instances.")
        return None

    return client


def get_cluster() -> Union[None, distributed.LocalCluster]:
    """
    Get a toolviper cluster instance
    Returns: None or a toolviper cluster instance

    """
    cluster = None

    if get_client() is not None:
        cluster = distributed.Client.current().cluster

    if cluster is None:
        logger.info("There are currently no cluster instances.")
        return None

    return cluster


def distributed_client(
    scheduler,
    dask_local_dir: Optional[str] = None,
    log_params: Optional[Dict[str, Any]] = None,
    worker_log_params: Optional[Dict[str, Any]] = None,
    workers=None,
    worker=None,
    asynchronous=False,
    loop=None,
    security=None,
    silence_logs=False,
    name=None,
    shutdown_on_close=True,
    scheduler_sync_interval=1,
    shutdown_scheduler=True
) -> distributed.Client:
    """Setup dask cluster and logger.

    Parameters
    ----------
    cluster : Any
        An existing dask cluster instance.
    dask_local_dir : str, optional
        Where Dask should store temporary files.
    log_params : dict, optional
        The logger for the main process.
    worker_log_params : dict, optional
        The logger for the workers.

    Returns
    -------
    distributed.Client
        Dask Distributed Client
    """

    cluster = distributed.SpecCluster(
        workers=workers,
        scheduler=scheduler,
        worker=worker,
        asynchronous=asynchronous,
        loop=loop,
        security=security,
        silence_logs=False,
        name=name,
        shutdown_on_close=True,
        scheduler_sync_interval=1,
        shutdown_scheduler=True
    )

    log_params = _get_log_params(log_params, DEFAULT_CLIENT_LOG_PARAMS)
    worker_log_params = _get_log_params(worker_log_params, DEFAULT_WORKER_LOG_PARAMS)

    # If the user wants to change the global logger name from the
    # default value of toolviper
    os.environ["VIPER_LOGGER_NAME"] = log_params["logger_name"]

    logger.setup_logger(**log_params)

    if dask_local_dir is None:
        logger.warning(
            f"It is recommended that the local cache directory be set using "
            f"the {colorize.blue('dask_local_dir')} parameter."
        )

    _set_up_dask(dask_local_dir)

    logger.debug(colorize.green("Checking functions availability:"))
    available_specs = {
        **load_libraries("slurm", "dask_jobqueue"),
        **load_libraries("dask_ssh", ["asyncssh", "jupyter_server_proxy", "paramiko"]),
        **load_libraries("CUDA", "dask_cuda"),
    }

    print_libraries_availability(available_specs)

    client = toolviper.dask.menrva.MenrvaClient(cluster)
    client.get_versions(check=True)
    logger.info("Created client " + str(client))
    return client

def _set_up_dask(local_directory):
    if local_directory:
        dask.config.set({"temporary_directory": local_directory})

    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.scheduler.unknown-task-duration": "99m"})
    dask.config.set({"distributed.worker.memory.pause": False})
    dask.config.set({"distributed.worker.memory.terminate": False})
    # dask.config.set({"distributed.worker.memory.recent-to-old-time": "999s"})
    dask.config.set({"distributed.comm.timeouts.connect": "3600s"})
    dask.config.set({"distributed.comm.timeouts.tcp": "3600s"})
    dask.config.set({"distributed.nanny.environ.OMP_NUM_THREADS": 1})
    dask.config.set({"distributed.nanny.environ.MKL_NUM_THREADS": 1})
    # https://docs.dask.org/en/stable/how-to/customize-initialization.html
