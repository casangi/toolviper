import sys
import click

import toolviper.utils.logger as logger
import distributed

from distributed.diagnostics.plugin import WorkerPlugin


class DaskWorker(WorkerPlugin):
    def __init__(self, *args, **kwargs):
        self.worker = None
        self.logger = None

        self.local_cache = True

        self.logger_name = "worker"
        self.log_to_term = True
        self.log_to_file = False
        self.log_file = None
        self.log_level = "INFO"

        if "local_cache" in kwargs:
            self.local_cache = kwargs["local_cache"]

        # Update parameters if needed.
        self.set_log_parameters(kwargs)

    def set_log_parameters(self, kwargs):
        for param in kwargs["log_params"]:
            if param in self.__dict__:
                setattr(self, param, kwargs["log_params"][param])

    def get_logger(self):
        return self.logger

    def setup(self, worker: distributed.worker.Worker):
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """

        self.logger = logger.setup_worker_logger(
            logger_name=self.logger_name,
            log_to_term=self.log_to_term,
            log_to_file=self.log_to_file,
            log_file=self.log_file,
            log_level=self.log_level,
            worker=worker,
        )

        self.logger.debug(
            "Logger created on worker " + str(worker.id) + ",*," + str(worker.address)
        )

        # Documentation https://distributed.dask.org/en/stable/worker.html#distributed.worker.Worker
        self.worker = worker

        if self.local_cache:
            ip = worker.address[
                worker.address.rfind("/") + 1 : worker.address.rfind(":")
            ]

            self.logger.debug(str(worker.id) + ",*," + ip)

            worker.state.available_resources = {
                **worker.state.available_resources,
                **{ip: 1},
            }


# https://github.com/dask/distributed/issues/4169
@click.command()
@click.option("--local_cache", default=False)
@click.option("--log_to_term", default=True)
@click.option("--log_to_file", default=False)
@click.option("--log_file", default="worker-")
@click.option("--log_level", default="INFO")
async def dask_setup(
    worker: distributed.worker.Worker,
    local_cache: str,
    log_to_term: str,
    log_to_file: str,
    log_file: str,
    log_level: str,
):
    log_params = {
        "log_to_term": log_to_term,
        "log_to_file": log_to_file,
        "log_file": log_file,
        "log_level": log_level,
    }

    plugin = DaskWorker(local_cache, log_params)

    if sys.version_info.major == 3:
        if sys.version_info.minor > 8:
            await worker.client.register_plugin(plugin, name="worker_logger")

        else:
            await worker.client.register_worker_plugin(plugin, name="worker_logger")

    else:
        logger.warning("Python version may not be supported.")
