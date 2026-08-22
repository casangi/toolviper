from unittest.mock import MagicMock, patch

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

        for _worker_name, worker in worker_items:
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


def test_port_is_free():
    import socket

    from toolviper.dask.menrva import port_is_free

    # Test with a definitely free port (hopefully)
    # We can use port 0 to let the OS pick a free port, but port_is_free binds it and closes it.
    # Let's try to bind a port ourselves and then check if it's free.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]

    # Since 's' is holding the port, port_is_free should return False
    assert port_is_free(port) is False

    s.close()
    # Now it should be free
    assert port_is_free(port) is True


def test_close_port():
    import socket

    from toolviper.dask.menrva import close_port, port_is_free

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.listen(1)

    assert port_is_free(port) is False

    # close_port tries to kill the process holding the port.
    # Since it's our own process, this might be dangerous if not careful,
    # but here it's just a socket in the same process.
    # Actually, close_port uses psutil to find processes with that port and SIGKILLs them.
    # We should probably mock psutil for this test to avoid killing ourselves.

    with patch("psutil.process_iter") as mock_iter:
        mock_proc = MagicMock()
        mock_conn = MagicMock()
        mock_conn.laddr.port = port
        mock_proc.connections.return_value = [mock_conn]
        mock_iter.return_value = [mock_proc]

        close_port(port)

        mock_proc.send_signal.assert_called_once()


def test_menrva_client_call():
    from toolviper.dask.menrva import MenrvaClient

    def my_func(a, b=1):
        return a + b

    assert MenrvaClient.call(my_func, 2, b=3) == 5
    assert MenrvaClient.call(my_func, 2) == 3
