import pytest
from unittest.mock import MagicMock, patch
from toolviper.dask.plugins.worker import DaskWorker
from toolviper.dask.plugins.scheduler import Scheduler, unravel_deps, get_node_depths


def test_dask_worker_init():
    log_params = {
        "log_level": "DEBUG",
        "log_to_term": False,
        "log_to_file": True,
        "log_file": "test.log",
    }
    plugin = DaskWorker(local_cache=True, log_params=log_params)
    assert plugin.local_cache is True
    assert plugin.log_level == "DEBUG"
    assert plugin.log_to_term is False
    assert plugin.log_to_file is True
    assert plugin.log_file == "test.log"


def test_dask_worker_setup():
    plugin = DaskWorker(log_params={"log_level": "INFO"})
    mock_worker = MagicMock()
    mock_worker.id = "worker-1"
    mock_worker.address = "tcp://127.0.0.1:1234"
    mock_worker.state.available_resources = {}

    with patch("toolviper.utils.logger.setup_worker_logger") as mock_setup_logger:
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger

        plugin.setup(mock_worker)

        mock_setup_logger.assert_called_once()
        assert plugin.worker == mock_worker
        # Check if resource for IP was added
        assert "127.0.0.1" in mock_worker.state.available_resources


def test_scheduler_init():
    scheduler = Scheduler(autorestrictor=True, local_cache=False)
    assert scheduler.autorestrictor is True
    assert scheduler.local_cache is False


def test_unravel_deps():
    hlg_deps = {
        "task1": {"task2", "task3"},
        "task2": {"task4"},
        "task3": set(),
        "task4": set(),
    }
    unravelled = unravel_deps(hlg_deps, "task1")
    assert unravelled == {"task2", "task3", "task4"}


def test_get_node_depths():
    dependencies = {"A": set(), "B": {"A"}, "C": {"B"}, "D": {"A"}}
    root_nodes = {"A"}
    # metrics[node][-1] is the "depth" of the node from terminal nodes (as calculated by graph_metrics)
    # get_node_depths calculates: max(metrics[r][-1] - metrics[k][-1] for r in roots)
    # For a simple chain A -> B -> C:
    # C is terminal, depth 0 in metrics.
    # B depends on A, so B's depth in metrics is 1.
    # A is root, depth 2 in metrics.
    metrics = {
        "A": [0, 2],  # depth 2
        "B": [0, 1],  # depth 1
        "C": [0, 0],  # depth 0
        "D": [0, 1],  # depth 1
    }

    node_depths = get_node_depths(dependencies, root_nodes, metrics)
    assert node_depths["A"] == 0
    # For B: roots is {'A'}. node_depths['B'] = max(metrics['A'][1] - metrics['B'][1]) = 2 - 1 = 1
    assert node_depths["B"] == 1
    # For C: roots is {'A'}. node_depths['C'] = max(metrics['A'][1] - metrics['C'][1]) = 2 - 0 = 2
    assert node_depths["C"] == 2
    # For D: roots is {'A'}. node_depths['D'] = max(metrics['A'][1] - metrics['D'][1]) = 2 - 1 = 1
    assert node_depths["D"] == 1
