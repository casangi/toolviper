import os
import re
import pathlib
import distributed
import socket

from toolviper.dask.client import local_client

import pytest
import time


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
        # cls.client.shutdown()
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

        assert client.thread_info()["n_threads"] == n_threads
        assert client.thread_info()["memory_per_thread"] == memory_per_thread

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

    def test_load_libraries(self):
        from toolviper.dask.client import load_libraries

        libraries = load_libraries(name="CUDA", libs="dask_cuda")

        # Assuming github actions doesn't have CUDA installed
        assert libraries.get("CUDA") == False

    def test__set_up_dask(self):
        import dask

        from toolviper.dask.client import _set_up_dask

        _set_up_dask(local_directory=pathlib.Path(".").cwd())

        assert dask.config.config["distributed"]["scheduler"]["allowed-failures"] == 10


import socket
import pytest


class TestDashboardWorkerCountBundle:

    def _is_port_in_use(self, port):
        """Check if a given port is in use."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(("localhost", port))
        sock.close()
        return result == 0

    def get_available_port(self, starting_port=8787):
        """Find an available port starting from `starting_port`."""
        port = starting_port
        while self._is_port_in_use(port):
            port += 1  # Increment port number until an available port is found
        return port

    def occupy_port(self, port, retries=5, delay=1):
        """Simulate a process occupying a port, retrying if it's already occupied."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        for _ in range(retries):
            try:
                sock.bind(("localhost", port))  # Try to bind to the port
                sock.listen(1)  # Start listening to simulate occupation
                return sock
            except OSError as e:
                if e.errno == 98:  # Address already in use
                    print(f"Port {port} is already in use, retrying...")
                    time.sleep(delay)  # Wait for a bit before retrying
                else:
                    raise  # Re-raise other exceptions
        raise OSError(f"Port {port} is still in use after {retries} retries.")

    @pytest.mark.test_deterministic_worker_count
    def test_deterministic_worker_count(self):
        """Verifies that local_client() respects cores argument and creates correct number of workers."""
        client = local_client(cores=1, memory_limit="4GB")
        scheduler_info = client.cluster.scheduler_info

        num_workers = len(scheduler_info["workers"].keys())
        threads_per_worker = [
            worker["nthreads"] for worker in scheduler_info["workers"].values()
        ]

        assert num_workers == 1, f"Expected 1 worker, got {num_workers}"
        assert all(
            n == 1 for n in threads_per_worker
        ), f"Expected 1 thread per worker, got {threads_per_worker}"
        client.shutdown()

    @pytest.mark.test_port_fallback_to_random_if_occupied
    def test_port_fallback_to_random_if_occupied(self):
        """Test the fallback behavior when port 8787 is already occupied."""
        occupied_port = 8787  # Default dashboard port

        # Check if the port is in use and find an available port if necessary
        if self._is_port_in_use(occupied_port):
            print(
                f"Port {occupied_port} is already occupied, finding an available port."
            )
            available_port = self.get_available_port(
                occupied_port
            )  # Get an available port dynamically
            print(
                f"Using port: {available_port}"
            )  # Debug print to show the chosen port
        else:
            available_port = occupied_port  # Use the default port if not occupied

        # Try to create a Dask client with the available port
        client = local_client(
            cores=1, memory_limit="4GB", dashboard_port=available_port
        )

        # Ensure the client is using a valid, non-occupied port.
        scheduler_info = client.cluster.scheduler_info
        dashboard_address = scheduler_info.get("address", "")

        # Verify that the dynamically allocated port is used, not the occupied one
        assert (
            str(available_port) in dashboard_address
        ), f"Expected port {available_port}, got {dashboard_address}"

        # Close the occupied port after the test
        sock.close()

    @pytest.mark.test_port_fallback_to_random_if_occupied
    def test_port_fallback_to_random_if_occupied(self):
        """Test the fallback behavior when port 8787 is already occupied."""
        occupied_port = 8787  # Default dashboard port
        sock = self.occupy_port(occupied_port)  # Bind a process to this port

        # Now let's dynamically get an available port if 8787 is occupied
        available_port = self.get_available_port(occupied_port)
        print(f"Using port: {available_port}")  # Debug print to show the chosen port

        client = local_client(
            cores=1, memory_limit="4GB", dashboard_port=available_port
        )

        # Ensure the client is using a valid, non-occupied port.
        scheduler_info = client.cluster.scheduler_info
        dashboard_address = scheduler_info.get("address", "")

        # Verify that the dynamically allocated port is used, not the occupied one
        assert (
            str(available_port) in dashboard_address
        ), f"Expected port {available_port}, got {dashboard_address}"
        assert (
            str(available_port) in dashboard_address
        ), f"Expected a port that is available (starting from {available_port}), but got {dashboard_address}"
        sock.close()  # Close the occupied port after test

    @pytest.mark.test_port_in_use_check
    def test_port_in_use_check(self):
        """Test the functionality of checking whether a port is already in use."""
        port = 8787  # Port to check

        # Check if the port is in use
        is_in_use = self._is_port_in_use(port)
        assert isinstance(
            is_in_use, bool
        ), f"Expected a boolean result, got {is_in_use}"

        # Now, bind the port to simulate occupation
        sock = self.occupy_port(port)

        # Verify that the port is indeed in use now
        assert self._is_port_in_use(port), "Port should be in use after binding."

        sock.close()  # Close the port after test

    @pytest.mark.test_dashboard_port_allocation
    def test_dashboard_port_allocation(self):
        """Test the correct dashboard port allocation when the default port is occupied."""
        occupied_port = 8787  # Default dashboard port
        sock = self.occupy_port(occupied_port)  # Bind the port to simulate occupation

        # Dynamically find an available port
        available_port = self.get_available_port(occupied_port)
        print(f"Using port: {available_port}")  # Debug print to show the chosen port

        client = local_client(
            cores=1, memory_limit="4GB", dashboard_port=available_port
        )

        scheduler_info = client.cluster.scheduler_info
        dashboard_address = scheduler_info.get("address", "")

        assert (
            str(available_port) in dashboard_address
        ), f"Expected port {available_port}, got {dashboard_address}"

        sock.close()  # Close the occupied port after test

    @pytest.mark.test_dashboard_port_fallback
    def test_dashboard_port_fallback(self):
        """Test the fallback behavior when the default dashboard port is occupied."""
        occupied_port = 8787  # Default dashboard port
        sock = self.occupy_port(occupied_port)  # Bind the port to simulate occupation

        # Dynamically find an available port
        available_port = self.get_available_port(occupied_port)
        print(f"Using port: {available_port}")  # Debug print to show the chosen port

        client = local_client(
            cores=1, memory_limit="4GB", dashboard_port=available_port
        )

        scheduler_info = client.cluster.scheduler_info
        dashboard_address = scheduler_info.get("address", "")

        # Verify fallback to the dynamically allocated port
        assert (
            str(available_port) in dashboard_address
        ), f"Expected port {available_port}, got {dashboard_address}"

        sock.close()  # Close the occupied port after test
