import ast
import os
import pathlib
import re
import subprocess
import sys
from unittest.mock import patch

import dask
import distributed
import pytest

from toolviper.dask.client import (
    _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD,
    _MALLOC_TRIM_THRESHOLD_KEY,
    _NANNY_UNSETS_MALLOC_TRIM_THRESHOLD_PROLOGUE,
    _keep_glibc_dynamic_mmap_threshold,
    local_client,
)


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
            raise AssertionError() from None

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

        for _worker_name, worker in worker_items:
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
            raise AssertionError() from None

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
            raise AssertionError() from None

    def test_load_libraries(self):
        from toolviper.dask.client import load_libraries

        libraries = load_libraries(name="CUDA", libs="dask_cuda")

        # Assuming github actions doesn't have CUDA installed
        assert libraries.get("CUDA") is False

    def test__set_up_dask(self):
        import dask

        from toolviper.dask.client import _set_up_dask

        _set_up_dask(local_directory=pathlib.Path(".").cwd())

        assert dask.config.config["distributed"]["scheduler"]["allowed-failures"] == 10


def test_print_libraries_availability():
    import toolviper.utils.logger as logger
    from toolviper.dask.client import print_libraries_availability

    with patch.object(logger, "debug") as mock_debug:
        print_libraries_availability({"CUDA": True, "MPI": False})
        mock_debug.assert_called_once()
        args, kwargs = mock_debug.call_args
        assert "CUDA" in args[0]
        assert "MPI" not in args[0]


def test_get_client_none():
    from toolviper.dask.client import get_client

    with patch("distributed.Client.current", side_effect=ValueError):
        assert get_client() is None


def test_get_cluster_none():
    from toolviper.dask.client import get_cluster

    with patch("toolviper.dask.client.get_client", return_value=None):
        assert get_cluster() is None


def test_keep_glibc_dynamic_mmap_threshold_replaces_dask_default(monkeypatch):
    monkeypatch.delenv("MALLOC_TRIM_THRESHOLD_", raising=False)

    with dask.config.set(
        {_MALLOC_TRIM_THRESHOLD_KEY: _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD}
    ):
        assert _keep_glibc_dynamic_mmap_threshold() is True
        assert dask.config.get(_MALLOC_TRIM_THRESHOLD_KEY) is None


@pytest.mark.parametrize("user_value", [0, "131072"])
def test_keep_glibc_dynamic_mmap_threshold_respects_user_config(
    monkeypatch, user_value
):
    monkeypatch.delenv("MALLOC_TRIM_THRESHOLD_", raising=False)

    with dask.config.set({_MALLOC_TRIM_THRESHOLD_KEY: user_value}):
        assert _keep_glibc_dynamic_mmap_threshold() is False
        assert dask.config.get(_MALLOC_TRIM_THRESHOLD_KEY) == user_value


def test_keep_glibc_dynamic_mmap_threshold_respects_user_environment(monkeypatch):
    monkeypatch.setenv("MALLOC_TRIM_THRESHOLD_", "65536")

    with dask.config.set(
        {_MALLOC_TRIM_THRESHOLD_KEY: _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD}
    ):
        assert _keep_glibc_dynamic_mmap_threshold() is False
        assert (
            dask.config.get(_MALLOC_TRIM_THRESHOLD_KEY)
            == _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD
        )


def test_local_client_workers_without_malloc_trim_threshold(monkeypatch):
    monkeypatch.delenv("MALLOC_TRIM_THRESHOLD_", raising=False)

    # Start from Dask's default, whatever earlier tests left in the config.
    with dask.config.set(
        {_MALLOC_TRIM_THRESHOLD_KEY: _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD}
    ):
        client = local_client(cores=2, memory_limit="1GB")

        try:
            worker_env = client.run(
                lambda: {
                    key: os.environ.get(key)
                    for key in ("MALLOC_TRIM_THRESHOLD_", "OMP_NUM_THREADS")
                }
            )
        finally:
            client.shutdown()

    assert len(worker_env) == 2
    for env in worker_env.values():
        assert env["MALLOC_TRIM_THRESHOLD_"] is None
        assert env["OMP_NUM_THREADS"] == "1"

    # The Nanny copies its pre-spawn environment into this process too.
    assert "MALLOC_TRIM_THRESHOLD_" not in os.environ


def test_slurm_job_script_prologue_unsets_malloc_trim_threshold(monkeypatch, tmp_path):
    import toolviper.dask.client as client_module

    monkeypatch.delenv("MALLOC_TRIM_THRESHOLD_", raising=False)
    captured = {}

    class Submitted(Exception):
        pass

    def fake_slurm_cluster(**kwargs):
        captured.update(kwargs)
        raise Submitted

    monkeypatch.setattr(client_module.dask_jobqueue, "SLURMCluster", fake_slurm_cluster)
    # Keep _set_up_dask from pointing the global temporary_directory at tmp_path.
    monkeypatch.setattr(client_module, "_set_up_dask", lambda local_directory: None)

    with dask.config.set(
        {
            _MALLOC_TRIM_THRESHOLD_KEY: _DASK_DEFAULT_MALLOC_TRIM_THRESHOLD,
            "jobqueue.slurm.job-script-prologue": ["module load python"],
        }
    ):
        # client.param.json has no schema for slurm_cluster_client, so call it
        # past the parameter.validate decorator.
        with pytest.raises(Submitted):
            client_module.slurm_cluster_client.__wrapped__(
                workers_per_node=1,
                cores_per_node=1,
                memory_per_node="1GB",
                number_of_nodes=1,
                queue="debug",
                interface="lo",
                python_env_dir=sys.executable,
                dask_local_dir=str(tmp_path),
                dask_log_dir=str(tmp_path),
            )

    assert captured["job_script_prologue"] == [
        "module load python",
        _NANNY_UNSETS_MALLOC_TRIM_THRESHOLD_PROLOGUE,
    ]


def test_slurm_job_script_prologue_reaches_nanny_config():
    # Run the job script line in a shell, as the SLURM job does, and read the
    # Dask config that a Nanny started after it would see.
    env = {k: v for k, v in os.environ.items() if not k.startswith("DASK_")}
    env.pop("MALLOC_TRIM_THRESHOLD_", None)
    code = (
        "import dask, distributed; "
        "print(dask.config.get('distributed.nanny.pre-spawn-environ'))"
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'{_NANNY_UNSETS_MALLOC_TRIM_THRESHOLD_PROLOGUE}\n"$0" -c "$1"',
            sys.executable,
            code,
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    pre_spawn_environ = ast.literal_eval(result.stdout)

    assert pre_spawn_environ["MALLOC_TRIM_THRESHOLD_"] is None
    assert str(pre_spawn_environ["OMP_NUM_THREADS"]) == "1"
