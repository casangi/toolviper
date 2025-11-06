import os
import re
import pathlib
import distributed

from toolviper.dask import menrva
from toolviper.dask.client import local_client


class TestToolViperMenerva:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given test class
        such as fetching test data"""
        pass

    @classmethod
    def teardown_class(cls):
        """teardown any state that was previously setup with a call to setup_class
        such as deleting test data"""
        # cls.client.shutdown()
        pass

    def setup_method(self):
        """setup any state specific to all methods of the given class"""
        pass

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        pass

    def test_thread_info(self):
        log_params = {
            "logger_name": "main-logger",
            "log_level": "INFO",
            "log_to_term": True,
            "log_to_file": False,
        }

        worker_log_params = {
            "logger_name": "worker-logger",
            "log_level": "INFO",
            "log_to_term": True,
            "log_to_file": False,
            "log_file": None,
        }

        client = local_client(
            cores=2,
            memory_limit="8GB",
            log_params=log_params,
            worker_log_params=worker_log_params,
            serial_execution=False,
        )

        memory_per_thread = -1
        n_threads = 0

        # Not sure if this test is deterministic. The tests are done using GitHub actions, and I am sure the container
        # environment will change over time. Hopefully, the test pulls out the most consistently calculated case.

        worker_items = client.cluster.scheduler_info["workers"].items()

        for worker_name, worker in worker_items:
            temp_memory_per_thread = (worker["memory_limit"] / worker["nthreads"]) / (
                1024**3
            )
            n_threads = n_threads + worker["nthreads"]

            if (memory_per_thread == -1) or (
                memory_per_thread > temp_memory_per_thread
            ):
                memory_per_thread = temp_memory_per_thread

        assert menrva.MenrvaClient.thread_info()["n_threads"] == n_threads
        assert (
            menrva.MenrvaClient.thread_info()["memory_per_thread"] == memory_per_thread
        )

        client.shutdown()
