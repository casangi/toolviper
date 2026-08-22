import importlib
import importlib.util
import inspect
import pathlib
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from typing import Any

import distributed
import psutil
from dask.widgets import get_template
from distributed.diagnostics.plugin import WorkerPlugin
from packaging.version import parse as parse_version

import toolviper.utils.console as console
import toolviper.utils.logger as logger

colorize = console.Colorize()


class MenrvaClient(distributed.Client):
    """
    This and extended version of the general Dask distributed client that will allow for
    plugin management and more extended features.
    """

    def __init__(self, cluster):
        super().__init__(cluster)

        self.n_workers = len(cluster.workers.keys())

    def _repr_html_(self):
        try:
            distributed.Client.current()

        except ValueError:
            logger.debug("<No Dask Client>")
            return None

        try:
            dle_version = parse_version(version("dask-labextension"))
            JUPYTERLAB = False if dle_version < parse_version("6.0.0") else True

        except PackageNotFoundError:
            JUPYTERLAB = False

        scheduler, info = self._get_scheduler_info(self.n_workers)

        return get_template("client.html.j2").render(
            id=self.id,
            scheduler=scheduler,
            info=info,
            cluster=self.cluster,
            scheduler_file=self.scheduler_file,
            dashboard_link=self.dashboard_link,
            jupyterlab=JUPYTERLAB,
        )

    @staticmethod
    def thread_info() -> dict[str, Any]:
        try:
            client = distributed.Client.current()

        except ValueError:  # Using default Dask schedular.
            logger.warning(
                "Couldn't find a current client instance, calculating thread information based on current system."
            )

            cpu_cores = psutil.cpu_count()
            total_memory = psutil.virtual_memory().total / (1024**3)

            thread_info = {
                "n_threads": cpu_cores,
                "memory_per_thread": total_memory / cpu_cores,
            }

            return thread_info

        memory_per_thread = -1
        n_threads = 0

        # client.cluster only exists for LocalCluster
        if client.cluster is None:
            worker_items = client.scheduler_info()["workers"].items()

        else:
            worker_items = client.cluster.scheduler_info["workers"].items()

        for _worker_name, worker in worker_items:
            temp_memory_per_thread = (worker["memory_limit"] / worker["nthreads"]) / (
                1024**3
            )
            n_threads = n_threads + worker["nthreads"]

            if (memory_per_thread == -1) or (
                memory_per_thread > temp_memory_per_thread
            ):
                memory_per_thread = temp_memory_per_thread

        thread_info = {"n_threads": n_threads, "memory_per_thread": memory_per_thread}

        return thread_info

    @staticmethod
    def call(func: Callable, *args: tuple[Any], **kwargs: dict[str, Any]):
        try:
            params = inspect.signature(func).bind(*args, **kwargs)
            return func(*params.args, **params.kwargs)

        except TypeError as e:
            logger.error(f"There was an error calling the function: {e}")

    @staticmethod
    def instantiate_module(
        plugin: str, plugin_file: str, *args: tuple[Any], **kwargs: dict[str, Any]
    ) -> WorkerPlugin | None:
        """

        Args:
            plugin (str): Name of plugin module.
            plugin_file (str): Name of a module file. ** This should be moved into the module itself, not passed **
            *args (tuple (Any)): This is any *arg that needs to be passed to the plugin module.
            **kwargs (dict[str, Any]): This is any **kwarg default values that need to be passed to the plugin module.

        Returns:
            Instance of plugin-class.
        """
        spec = importlib.util.spec_from_file_location(plugin, plugin_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for member in inspect.getmembers(module, predicate=inspect.isclass):
            plugin_instance = getattr(module, member[0])
            logger.debug(f"Loading plugin module: {plugin_instance}")
            return MenrvaClient.call(plugin_instance, *args, **kwargs)

        return None

    def load_plugin(
        self,
        directory: str,
        plugin: str,
        name: str,
        *args: tuple[Any] | Any,
        **kwargs: dict[str, Any] | Any,
    ):
        """Register a worker plugin from ``directory`` on this client.

        Thin wrapper around the module-level :func:`load_plugin`, which works
        with any ``distributed.Client`` instance.
        """
        load_plugin(self, directory, plugin, name, *args, **kwargs)


def load_plugin(
    client: distributed.Client,
    directory: str,
    plugin: str,
    name: str,
    *args: tuple[Any] | Any,
    **kwargs: dict[str, Any] | Any,
):
    """Instantiate a worker plugin module and register it on ``client``.

    Loads ``<directory>/<plugin>.py``, instantiates its plugin class and
    registers it via the supported ``distributed.Client.register_plugin`` API
    (``register_worker_plugin`` is deprecated).

    Unlike :meth:`MenrvaClient.load_plugin`, this accepts *any*
    ``distributed.Client`` instance -- including the plain client returned by
    ``distributed.Client.current()``. Callers that may reuse an already-running
    client (e.g. :func:`toolviper.dask.client.local_client`) therefore do not
    depend on that client being a :class:`MenrvaClient`.

    Parameters
    ----------
    client : distributed.Client
        The client the plugin is registered on.
    directory : str
        Directory containing ``<plugin>.py``.
    plugin : str
        Plugin module name (without the ``.py`` suffix).
    name : str
        Name the plugin is registered under.
    *args, **kwargs
        Forwarded to the plugin class constructor.
    """
    plugin_file = ".".join((plugin, "py"))
    if pathlib.Path(directory).joinpath(plugin_file).exists():
        plugin_instance = MenrvaClient.instantiate_module(
            plugin,
            "/".join((directory, plugin_file)),
            *args,
            **kwargs,
        )
        client.register_plugin(plugin_instance, name=name)
    else:
        logger.error(f"Cannot find plugins directory: {colorize.red(directory)}")


def port_is_free(port):
    import errno
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind(("127.0.0.1", port))
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            logger.warning("Port is already in use.")
            return False
        else:
            # something else raised the socket.error exception
            logger.exception(e)
            return False

    logger.debug("Socket is free.")
    s.close()

    return True


def close_port(port):
    from signal import SIGKILL

    import psutil
    from psutil import process_iter

    for proc in process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    proc.send_signal(SIGKILL)
                    continue

        except psutil.AccessDenied:
            pass
