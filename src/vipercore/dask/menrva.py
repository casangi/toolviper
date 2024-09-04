import sys

import psutil
import distributed
import inspect
import importlib
import importlib.util
import pathlib

import vipercore.utils.logger as logger
import vipercore.utils.console as console

from importlib.metadata import PackageNotFoundError, version

from packaging.version import parse as parse_version
from distributed.diagnostics.plugin import WorkerPlugin

from dask.widgets import get_template

from typing import Callable, Tuple, Dict, Any, Union

colorize = console.Colorize()

class MenrvaClient(distributed.Client):
    """
    This and extended version of the general Dask distributed client that will allow for
    plugin management and more extended features.
    """

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

        scheduler, info = self._get_scheduler_info()

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
    def thread_info()->Dict[str, Any]:

        try:
            client = distributed.Client.current()

        except ValueError:  # Using default Dask schedular.
            logger.warning("Couldn't find a current client instance, calculating thread information based on current system.")

            cpu_cores = psutil.cpu_count()
            total_memory = psutil.virtual_memory().total / (1024 ** 3)

            thread_info = {
                'n_threads': cpu_cores,
                'memory_per_thread': total_memory / cpu_cores
            }

            return thread_info

        memory_per_thread = -1
        n_threads = 0

        # client.cluster only exists for LocalCluster
        if client.cluster is None:
            worker_items = client.scheduler_info()['workers'].items()

        else:
            worker_items = client.cluster.scheduler_info['workers'].items()

        for worker_name, worker in worker_items:
            temp_memory_per_thread = (worker['memory_limit'] / worker['nthreads']) / (1024 ** 3)
            n_threads = n_threads + worker['nthreads']

            if (memory_per_thread == -1) or (memory_per_thread > temp_memory_per_thread):
                memory_per_thread = temp_memory_per_thread

        thread_info = {
            'n_threads': n_threads,
            'memory_per_thread': memory_per_thread
        }

        return thread_info

    @staticmethod
    def call(func: Callable, *args: Tuple[Any], **kwargs: Dict[str, Any]):
        try:
            params = inspect.signature(func).bind(*args, **kwargs)
            return func(*params.args, **params.kwargs)

        except TypeError as e:
            logger.error("There was an error calling the function: {}".format(e))

    @staticmethod
    def instantiate_module(
        plugin: str, plugin_file: str, *args: Tuple[Any], **kwargs: Dict[str, Any]
    ) -> WorkerPlugin:
        """

        Args:
            plugin (str): Name of plugin module.
            plugin_file (str): Name of module file. ** This should be moved into the module itself not passed **
            *args (tuple(Any)): This is any *arg that needs to be passed to the plugin module.
            **kwargs (dict[str, Any]): This is any **kwarg default values that need to be passed to the plugin module.

        Returns:
            Instance of plugin class.
        """
        spec = importlib.util.spec_from_file_location(plugin, plugin_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for member in inspect.getmembers(module, predicate=inspect.isclass):
            plugin_instance = getattr(module, member[0])
            logger.debug("Loading plugin module: {}".format(plugin_instance))
            return MenrvaClient.call(plugin_instance, *args, **kwargs)

    def load_plugin(
        self,
        directory: str,
        plugin: str,
        name: str,
        *args: Union[Tuple[Any], Any],
        **kwargs: Union[Dict[str, Any], Any],
    ):
        plugin_file = ".".join((plugin, "py"))
        if pathlib.Path(directory).joinpath(plugin_file).exists():
            plugin_instance = MenrvaClient.instantiate_module(
                plugin=plugin,
                plugin_file="/".join((directory, plugin_file)),
                *args,
                **kwargs,
            )
            logger.debug(f"{plugin}")
            if sys.version_info.major == 3:
                if sys.version_info.minor > 8:
                    self.register_plugin(plugin_instance, name=name)

                else:
                    self.register_worker_plugin(plugin_instance, name=name)
            else:
                logger.warning("Python version may not be supported.")
        else:
            logger.error(
                "Cannot find plugins directory: {}".format(colorize.red(directory))
            )


def port_is_free(port):
    import socket
    import errno

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind(("127.0.0.1", port))
    except socket.error as e:
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
    import psutil
    from psutil import process_iter
    from signal import SIGKILL

    for proc in process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    proc.send_signal(SIGKILL)
                    continue

        except psutil.AccessDenied:
            pass
