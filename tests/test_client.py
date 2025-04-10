import os
import re
import pathlib
import distributed

from toolviper.dask.client import local_client


class TestToolViperClient:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given test class
        such as fetching test data"""
        pass

    @classmethod
    def teardown_class(cls):
        """teardown any state that was previously setup with a call to setup_class
        such as deleting test data"""
        #cls.client.shutdown()
        pass

    def setup_method(self):
        """setup any state specific to all methods of the given class"""
        pass

    def teardown_method(self):
        """teardown any state that was previously setup for all methods of the given class"""
        pass

    def test_client_spawn(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB to create an instance of the
        astrohack Dask client.
        """

        log_params = {
            "log_level": "DEBUG",
            "log_to_file": True,
            "log_file": "toolviper_log_file",
        }

        path = pathlib.Path(".").cwd() / "dask_test_dir"

        client = local_client(
            cores=2,
            memory_limit="8GB",
            dask_local_dir=str(path),
            log_params=log_params,
        )

        try:
            if distributed.Client.current() is None:
                raise OSError

        except OSError:
            assert False

        client.shutdown()

    def test_client_get(self):
        """
        Test the get_client() function.
        """
        from toolviper.dask.client import get_client

        client = local_client(
            cores=2,
            memory_limit="4GB",
        )

        assert get_client() == distributed.Client.current()

        client.shutdown()

    def test_cluster_get(self):
        """
        Test the get_client() function.
        """
        from toolviper.dask.client import get_cluster

        client = local_client(
            cores=2,
            memory_limit="4GB",
        )

        assert get_cluster() == distributed.Client.current().cluster

        client.shutdown()

    def test_client_thread_info(self):
        """
        Test that thread_info() function returns the values that were set in the client instantiation.
        """

        client = local_client(
            cores=2,
            memory_limit="4GB",
        )

        memory_per_thread = -1
        n_threads = 0

        # Not sure if this test is deterministic. The tests are done using github actions and, I am sure the container
        # environment will change over time. Hopefully, the test pulls out the most consistently calculated case.

        worker_items = client.cluster.scheduler_info['workers'].items()

        for worker_name, worker in worker_items:
            temp_memory_per_thread = (worker['memory_limit'] / worker['nthreads']) / (1024 ** 3)
            n_threads = n_threads + worker['nthreads']

            if (memory_per_thread == -1) or (memory_per_thread > temp_memory_per_thread):
                memory_per_thread = temp_memory_per_thread

        assert client.thread_info() == {
            "n_threads": 2,
            "memory_per_thread": memory_per_thread
        }

        client.shutdown()


    def test_client_dask_dir(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB to create an instance of the
        astrohack Dask client. Check that temporary files are written to dask_local_dir.
        """

        try:

            path = pathlib.Path(".").cwd() / "dask_test_dir"

            if path.exists() is False:
                raise FileNotFoundError

        except FileNotFoundError:
            assert False

    def test_client_logger(self):
        """
        Run astrohack_local_client with N cores and with a memory_limit of M GB without any errors and the messages
        will be logged in the terminal.
        """

        files = os.listdir(".")

        try:
            for file in files:
                if re.match("^toolviper_log_file+[0-9].*log", file) is not None:
                    return

            raise FileNotFoundError

        except FileNotFoundError:
            assert False
