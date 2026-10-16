import functools
import multiprocessing
import os
import pathlib
from importlib import import_module
from importlib.util import find_spec
from typing import Any

import dask
import dask_jobqueue
import distributed
import psutil

import toolviper.dask.menrva
import toolviper.utils.console as console
import toolviper.utils.logger as logger
import toolviper.utils.parameter as parameter

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
    log_params: dict[str, Any] | None, defaults: dict[str, Any]
) -> dict[str, Any]:
    if log_params is None:
        log_params = {}
    return {**defaults, **log_params}


# Dask's Nanny sets every key of distributed.nanny.pre-spawn-environ in the
# environment of each worker process it spawns, and Dask ships
# MALLOC_TRIM_THRESHOLD_=65536 there. Setting any MALLOC_* tunable through the
# environment turns off glibc's dynamic mmap threshold for the life of the
# worker. Every buffer above 128 KiB is then mmapped, unmapped when freed and
# faulted in again by the next task.
# https://github.com/dask/distributed/issues/9345
_MALLOC_TRIM_THRESHOLD_KEY = (
    "distributed.nanny.pre-spawn-environ.MALLOC_TRIM_THRESHOLD_"
)
_DASK_DEFAULT_MALLOC_TRIM_THRESHOLD = 65536

# Nannies on other hosts (SLURM jobs) read their own Dask config, so the job
# script sets it through the environment. The value is a dict because Dask
# lowercases nested keys taken from DASK_* variable names, and a lowercase key
# does not match MALLOC_TRIM_THRESHOLD_.
_NANNY_UNSETS_MALLOC_TRIM_THRESHOLD_PROLOGUE = (
    "export DASK_DISTRIBUTED__NANNY__PRE_SPAWN_ENVIRON="
    "\"{'MALLOC_TRIM_THRESHOLD_': None}\""
)


def _keep_glibc_dynamic_mmap_threshold() -> bool:
    """Stop the Dask Nanny from setting MALLOC_TRIM_THRESHOLD_ in its workers.

    Replaces Dask's built-in default (65536) with None in the Dask config. The
    Nanny then leaves the variable unset, and glibc keeps adapting its mmap
    threshold in the workers. A value the user chose is kept: a
    MALLOC_TRIM_THRESHOLD_ already in the environment, or another value in
    the Dask config.

    Returns
    -------
    bool
        True if Nannies started from now on leave the variable unset.
    """
    if "MALLOC_TRIM_THRESHOLD_" in os.environ:
        return False

    value = dask.config.get(_MALLOC_TRIM_THRESHOLD_KEY, default=None)
    if value is not None and str(value) != str(_DASK_DEFAULT_MALLOC_TRIM_THRESHOLD):
        return False

    dask.config.set({_MALLOC_TRIM_THRESHOLD_KEY: None})
    return True


def load_libraries(name: str, libs: str | list[str]) -> dict[str, bool]:
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


def get_thread_info() -> dict[str, float]:
    # This just brings the built-in thread info function into the client module.
    return toolviper.dask.menrva.MenrvaClient.thread_info()


def get_client() -> None | distributed.Client:
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


def get_cluster() -> None | distributed.LocalCluster:
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


@parameter.validate()
def local_client(
    cores: int | None = None,
    memory_limit: str | None = None,
    autorestrictor: bool = False,
    dask_local_dir: str | None = None,
    local_dir: str | None = None,
    wait_for_workers: bool = True,
    log_params: dict[str, Any] | None = None,
    worker_log_params: dict[str, Any] | None = None,
    dashboard_address: str = ":8787",
    serial_execution: bool = False,
    asynchronous: bool = False,
) -> distributed.Client | None:
    """Create a local client, scheduler and workers using Dask Distributed LocalCluster.

    With Dask configuration tuned for VIPER and the option to use autorestrictor plugin and local cache.
    See https://docs.dask.org/en/stable/deploying-python.html#reference for more details.

    Parameters
    ----------
    cores : int, optional
        Number of cores in Dask cluster. Defaults to number of physical cores.
    memory_limit : str, optional
        Amount of memory per core. Suggested: '8GB'. Defaults to available memory divided by cores.
    autorestrictor : bool, optional
        Whether to use the autorestrictor plugin. Defaults to False.
    dask_local_dir : str, optional
        Temporary files directory for Dask. Defaults to None.
    local_dir : str, optional
        Node-local directory for the local-disk cache. When set, the
        ``VIPER_LOCAL_DIR`` environment variable is exported and local caching
        is enabled (graphviper's ``map`` then adds ``viper_local_dir``,
        ``date_time`` and ``node_ip`` to each task's input parameters).
        Defaults to None.
    wait_for_workers : bool, optional
        Whether to wait for workers to start. Defaults to True.
    log_params : dict, optional
        Logger configuration for the main process.
    worker_log_params : dict, optional
        Logger configuration for workers.
    dashboard_address : str, optional
        Address for the Bokeh diagnostics server (e.g., 'localhost:8787'). Defaults to ':8787'.
    serial_execution : bool, optional
        If True, runs Dask in serial mode (synchronous) for debugging. Defaults to False.

    Returns
    -------
    distributed.Client or None
        Dask Distributed Client, or None if serial_execution is True.

    Notes
    -----
    Dask's Nanny would set MALLOC_TRIM_THRESHOLD_=65536 in every worker, which
    turns off glibc's dynamic mmap threshold. toolviper leaves it unset. To
    keep Dask's behaviour, set MALLOC_TRIM_THRESHOLD_ in the environment before
    calling local_client, or set
    distributed.nanny.pre-spawn-environ.MALLOC_TRIM_THRESHOLD_ to another value
    in your Dask config.
    """

    log_params = _get_log_params(log_params, DEFAULT_CLIENT_LOG_PARAMS)
    worker_log_params = _get_log_params(worker_log_params, DEFAULT_WORKER_LOG_PARAMS)

    # If the user wants to change the global logger name from the
    # default value of toolviper
    os.environ["VIPER_LOGGER_NAME"] = log_params["logger_name"]

    if local_dir:
        os.environ["VIPER_LOCAL_DIR"] = local_dir
        local_cache = True
    else:
        local_cache = False

    logger.setup_logger(**log_params)

    if dask_local_dir is None:
        logger.warning(
            f"It is recommended that the local cache directory be set using "
            f"the {colorize.blue('dask_local_dir')} parameter."
        )

    _set_up_dask(dask_local_dir)

    # This will work as long as the scheduler path isn't in some outside directory. Being that it is a plugin specific
    # to this module, I think keeping it static in the module directory it good.
    plugin_path = str(pathlib.Path(__file__).parent.resolve().joinpath("plugins/"))

    if local_cache or autorestrictor:
        dask.config.set(
            {"distributed.scheduler.preload": os.path.join(plugin_path, "scheduler.py")}
        )

        dask.config.set(
            {
                "distributed.scheduler.preload-argv": [
                    "--local_cache",
                    local_cache,
                    "--autorestrictor",
                    autorestrictor,
                ]
            }
        )

    if serial_execution:
        # Override the default behavior for debugging purposes and run synchronous
        dask.config.set(scheduler="synchronous")

        logger.info("Running client in synchronous mode.")

        return None

    # setup dask.distributed based multiprocessing environment
    if cores is None:
        cores = multiprocessing.cpu_count()

    if memory_limit is None:
        memory_limit = "".join(
            (str(round((psutil.virtual_memory().available / (1024**2)) / cores)), "MB")
        )

    _keep_glibc_dynamic_mmap_threshold()

    try:
        cluster = distributed.Client.current().cluster

    except ValueError:
        cluster = distributed.LocalCluster(
            n_workers=cores,
            threads_per_worker=1,
            processes=True,
            memory_limit=memory_limit,
            asynchronous=asynchronous,
            dashboard_address=dashboard_address,
        )

    try:
        client = distributed.Client.current()

    except ValueError:
        client = toolviper.dask.menrva.MenrvaClient(cluster)
        client.get_versions(check=True)

    # When constructing a graph that has local-cache enabled, all workers need to be up and running.
    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=cores)

    # logger.debug(f"These are the worker log parameters:\n")
    # logger.debug(f"{display.DataDict.from_dict(worker_log_params).display(interactive=False)}")

    if local_cache or worker_log_params:
        # Use the client-agnostic helper rather than client.load_plugin(): the
        # client returned by distributed.Client.current() above may be a plain
        # distributed.Client (e.g. one created elsewhere in the session), which
        # does not have the MenrvaClient.load_plugin method.
        toolviper.dask.menrva.load_plugin(
            client,
            directory=plugin_path,
            plugin="worker",
            name="worker_logger",
            local_cache=local_cache,
            log_params=worker_log_params,
        )

    logger.info("Client " + str(client))

    return client


@parameter.validate()
def distributed_client(
    cluster: Any,
    dask_local_dir: str | None = None,
    log_params: dict[str, Any] | None = None,
    worker_log_params: dict[str, Any] | None = None,
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
    # The cluster already exists, so this reaches only workers that Nannies in
    # this process spawn later, for example when a LocalCluster scales up.
    _keep_glibc_dynamic_mmap_threshold()

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


@parameter.validate()
def slurm_cluster_client(
    workers_per_node: int,
    cores_per_node: int,
    memory_per_node: str,
    number_of_nodes: int,
    queue: str,
    interface: str,
    python_env_dir: str,
    dask_local_dir: str,
    dask_log_dir: str,
    exclude_nodes: str = "",
    dashboard_port: int = 8787,
    local_dir: str | None = None,
    autorestrictor: bool = False,
    wait_for_workers: bool = True,
    log_params: dict[str, Any] | None = None,
    worker_log_params: dict[str, Any] | None = None,
) -> distributed.Client:
    """Create a SLURM cluster and return a client.

    Parameters
    ----------
    workers_per_node : int
        Number of workers per node.
    cores_per_node : int
        Number of cores per node.
    memory_per_node : str
        Memory per node (e.g., '64GB').
    number_of_nodes : int
        Number of nodes to request.
    queue : str
        SLURM queue name.
    interface : str
        Network interface to use (e.g., 'ib0').
    python_env_dir : str
        Path to the python executable in the environment.
    dask_local_dir : str
        Local directory for dask workers.
    dask_log_dir : str
        Directory for dask logs.
    exclude_nodes : str, optional
        Comma-separated list of nodes to exclude.
    dashboard_port : int, optional
        Port for the dask dashboard.
    local_dir : str, optional
        Client local directory.
    autorestrictor : bool, optional
        Whether to use the autorestrictor plugin.
    wait_for_workers : bool, optional
        Whether to wait for workers to start.
    log_params : dict, optional
        Logger parameters for the client.
    worker_log_params : dict, optional
        Logger parameters for the workers.

    Returns
    -------
    distributed.Client
        The dask client connected to the SLURM cluster.

    Notes
    -----
    The job script makes the worker Nannies leave MALLOC_TRIM_THRESHOLD_ unset,
    as local_client does, by exporting DASK_DISTRIBUTED__NANNY__PRE_SPAWN_ENVIRON.
    The export is left out when MALLOC_TRIM_THRESHOLD_ is set in the environment
    of the calling process, or set to another value in its Dask config.
    """

    # https://github.com/dask/dask/issues/5577

    # from distributed import Client

    log_params = _get_log_params(log_params, DEFAULT_CLIENT_LOG_PARAMS)
    worker_log_params = _get_log_params(worker_log_params, DEFAULT_WORKER_LOG_PARAMS)

    if local_dir:
        os.environ["VIPER_LOCAL_DIR"] = local_dir
        local_cache = True
    else:
        local_cache = False

    logger.setup_logger(**log_params)

    _set_up_dask(dask_local_dir)

    """
    load libraries related functions of a distributed environment
    'available_specs' contains the function name and a flag that the function was loaded successfully
    """

    logger.debug(colorize.green("Checking functions availability:"))
    available_specs = {
        **load_libraries("slurm", "dask_jobqueue"),
        **load_libraries("dask_ssh", ["asyncssh", "jupyter_server_proxy", "paramiko"]),
        **load_libraries("CUDA", "dask_cuda"),
    }

    print_libraries_availability(available_specs)

    plugin_path = str(pathlib.Path(__file__).parent.resolve().joinpath("plugins/"))

    if local_cache or autorestrictor:
        dask.config.set(
            {"distributed.scheduler.preload": os.path.join(plugin_path, "scheduler.py")}
        )
        dask.config.set(
            {
                "distributed.scheduler.preload-argv": [
                    "--local_cache",
                    local_cache,
                    "--autorestrictor",
                    autorestrictor,
                ]
            }
        )

    job_script_prologue = None
    if _keep_glibc_dynamic_mmap_threshold():
        # Passing job_script_prologue replaces the configured one, so keep it.
        job_script_prologue = list(
            dask.config.get("jobqueue.slurm.job-script-prologue", default=None)
            or dask.config.get("jobqueue.slurm.env-extra", default=None)
            or []
        ) + [_NANNY_UNSETS_MALLOC_TRIM_THRESHOLD_PROLOGUE]

    cluster = dask_jobqueue.SLURMCluster(
        processes=workers_per_node,
        cores=cores_per_node,
        interface=interface,
        memory=memory_per_node,
        walltime="24:00:00",
        queue=queue,
        name="viper",
        python=python_env_dir,
        local_directory=dask_local_dir,
        log_directory=dask_log_dir,
        job_extra_directives=["--exclude=" + exclude_nodes],
        job_script_prologue=job_script_prologue,
        scheduler_options={"dashboard_address": ":" + str(dashboard_port)},
    )

    client = toolviper.dask.menrva.MenrvaClient(cluster)
    cluster.scale(workers_per_node * number_of_nodes)

    # When constructing a graph that has local cache enabled all workers need to be up and running.
    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=workers_per_node * number_of_nodes)

    if local_cache or worker_log_params:
        toolviper.dask.menrva.load_plugin(
            client,
            directory=plugin_path,
            plugin="worker",
            name="worker_logger",
            local_cache=local_cache,
            log_params=worker_log_params,
        )

    logger.info("Created client " + str(client))

    return client


def auto_client():
    """
    A decorator that automatically manages a Dask client for the decorated function.

    If a client already exists, it uses the existing one.
    Otherwise, it creates a new local_client and shuts it down after the function completes.
    """

    def function_wrapper(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            client = get_client()
            persistent_client = client is not None

            if not persistent_client:
                # Get client inputs if they exist
                if "client" in kwargs:
                    client_kwargs = kwargs["client"]
                    if isinstance(client_kwargs, dict):
                        client = local_client(**client_kwargs)
                    else:
                        client = local_client()
                else:
                    client = local_client()

            try:
                if client:
                    logger.info(f"Dask dashboard started at: {client.dashboard_link}")

                # Run the decorated function
                return function(*args, **kwargs)
            finally:
                # Ensure the client is closed even if the function raises an exception
                if not persistent_client and client:
                    client.shutdown()

        return wrapper

    return function_wrapper


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
