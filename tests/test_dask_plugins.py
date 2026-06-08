import pytest
from unittest.mock import MagicMock, patch
from toolviper.dask.plugins.worker import DaskWorker
from toolviper.dask.plugins.scheduler import (
    Scheduler,
    ViperGraphPlugin,
    unravel_deps,
    get_node_depths,
    graph_metrics,
    dask_setup,
)


class FakeTask:
    """Stand-in for a distributed ``TaskState`` exposing only the attributes the
    scheduler plugins read/write."""

    def __init__(self, priority=None):
        self.worker_restrictions = None
        self.loose_restrictions = None
        self.priority = priority


class FakeScheduler:
    """Minimal scheduler exposing ``workers`` and ``tasks`` mappings."""

    def __init__(self, workers, tasks):
        # Values are irrelevant; only the keys (addresses) are used.
        self.workers = {w: object() for w in workers}
        self.tasks = tasks


def _run_autorestrictor(dependencies, workers, task_keys=None):
    """Run ``Scheduler.update_graph`` (autorestrictor on) over a fake graph and
    return the populated ``FakeScheduler``."""
    if task_keys is None:
        task_keys = list(dependencies.keys())
    scheduler = FakeScheduler(workers, {k: FakeTask() for k in task_keys})
    plugin = Scheduler(autorestrictor=True, local_cache=False)
    plugin.update_graph(scheduler, dependencies=dependencies)
    return scheduler


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


def test_unravel_deps_accumulates_into_existing_set():
    """A pre-seeded accumulator is extended in place (and returned)."""
    hlg_deps = {"a": {"b"}, "b": set()}
    seed = {"preexisting"}
    out = unravel_deps(hlg_deps, "a", unravelled_deps=seed)
    assert out is seed
    assert out == {"preexisting", "b"}


def test_get_node_depths_node_without_roots_is_zero():
    """A node whose dependencies contain no root node gets depth 0."""
    dependencies = {"A": set(), "B": {"A"}}
    # 'B' depends on 'A' but we declare an unrelated root set, so roots & deps is
    # empty for both keys -> the ``else 0`` branch is taken.
    node_depths = get_node_depths(
        dependencies, root_nodes={"Z"}, metrics={"A": [0], "B": [0]}
    )
    assert node_depths == {"A": 0, "B": 0}


# ---------------------------------------------------------------------------
# Scheduler.add_worker
# ---------------------------------------------------------------------------
def test_scheduler_add_worker_local_cache_adds_ip_resource():
    scheduler = Scheduler(autorestrictor=False, local_cache=True)
    mock_scheduler = MagicMock()
    scheduler.add_worker(mock_scheduler, "tcp://192.168.1.5:34567")
    mock_scheduler.add_resources.assert_called_once_with(
        worker="tcp://192.168.1.5:34567", resources={"192.168.1.5": 1}
    )


def test_scheduler_add_worker_without_local_cache_is_noop():
    scheduler = Scheduler(autorestrictor=False, local_cache=False)
    mock_scheduler = MagicMock()
    scheduler.add_worker(mock_scheduler, "tcp://192.168.1.5:34567")
    mock_scheduler.add_resources.assert_not_called()


# ---------------------------------------------------------------------------
# Scheduler.update_graph (autorestrictor)
# ---------------------------------------------------------------------------
def test_update_graph_autorestrictor_disabled_is_noop():
    scheduler = FakeScheduler(["w0", "w1"], {"t": FakeTask()})
    Scheduler(autorestrictor=False, local_cache=False).update_graph(
        scheduler, dependencies={"t": set()}
    )
    assert scheduler.tasks["t"].worker_restrictions is None
    assert scheduler.tasks["t"].loose_restrictions is None


def test_update_graph_no_dependencies_is_noop():
    scheduler = FakeScheduler(["w0", "w1"], {"t": FakeTask()})
    # autorestrictor on, but an empty dependency mapping -> the ``if dependencies``
    # guard short-circuits before any assignment.
    Scheduler(autorestrictor=True, local_cache=False).update_graph(
        scheduler, dependencies={}
    )
    assert scheduler.tasks["t"].worker_restrictions is None


def test_update_graph_assigns_worker_restrictions_map_reduce():
    # Two independent map chains feeding a single reduce.
    dependencies = {
        "r0": set(),
        "r1": set(),
        "m0": {"r0"},
        "m1": {"r1"},
        "reduce": {"m0", "m1"},
    }
    scheduler = _run_autorestrictor(dependencies, ["w0", "w1"])

    # Every task is restricted to exactly one or both workers, never loose.
    for task in scheduler.tasks.values():
        assert task.loose_restrictions is False
        assert task.worker_restrictions  # non-empty set

    r0_wr = scheduler.tasks["r0"].worker_restrictions
    r1_wr = scheduler.tasks["r1"].worker_restrictions
    # A root and the map task that depends on it land on the same worker.
    assert scheduler.tasks["m0"].worker_restrictions == r0_wr
    assert scheduler.tasks["m1"].worker_restrictions == r1_wr
    # The two independent chains are load-balanced onto different workers.
    assert len(r0_wr) == 1 and len(r1_wr) == 1
    assert r0_wr != r1_wr
    # The shared reduce task is a member of both groups -> restricted to both.
    assert scheduler.tasks["reduce"].worker_restrictions == {"w0", "w1"}


def test_update_graph_independent_roots_special_case():
    # Two independent terminal roots (no dependencies) -> the "no dependencies"
    # special case path; each is balanced onto its own worker.
    scheduler = _run_autorestrictor({"a": set(), "b": set()}, ["w0", "w1"])
    a_wr = scheduler.tasks["a"].worker_restrictions
    b_wr = scheduler.tasks["b"].worker_restrictions
    assert len(a_wr) == 1 and len(b_wr) == 1
    assert a_wr | b_wr == {"w0", "w1"}  # different workers
    for task in scheduler.tasks.values():
        assert task.loose_restrictions is False


def test_update_graph_subset_roots_share_group():
    # t0's roots {r0} are a strict subset of t1's roots {r0, r1}; they must end
    # up in the same task group and therefore on the same worker.
    dependencies = {
        "r0": set(),
        "r1": set(),
        "t0": {"r0"},
        "t1": {"r0", "r1"},
    }
    scheduler = _run_autorestrictor(dependencies, ["w0", "w1"])
    restrictions = {frozenset(t.worker_restrictions) for t in scheduler.tasks.values()}
    # A single shared group -> a single worker assignment across all tasks.
    assert len(restrictions) == 1
    (only,) = restrictions
    assert len(only) == 1


def test_update_graph_reduction_fallback_assigns_nothing():
    # A deep linear chain with more workers than partition nodes walks back
    # through the graph, exhausts the depth, and falls back (early return)
    # without assigning any restrictions.
    scheduler = _run_autorestrictor(
        {"r": set(), "a": {"r"}, "b": {"a"}}, ["w0", "w1", "w2", "w3"]
    )
    for task in scheduler.tasks.values():
        assert task.worker_restrictions is None
        assert task.loose_restrictions is None


def test_update_graph_missing_task_key_is_skipped():
    # A graph key with no corresponding scheduler task must be skipped silently
    # (the KeyError branch), without affecting the other assignments.
    dependencies = {
        "r0": set(),
        "r1": set(),
        "m0": {"r0"},
        "m1": {"r1"},
        "reduce": {"m0", "m1"},
    }
    task_keys = [k for k in dependencies if k != "reduce"]  # 'reduce' absent
    scheduler = FakeScheduler(["w0", "w1"], {k: FakeTask() for k in task_keys})
    Scheduler(autorestrictor=True, local_cache=False).update_graph(
        scheduler, dependencies=dependencies
    )
    # No KeyError, and the present tasks still received restrictions.
    for task in scheduler.tasks.values():
        assert task.worker_restrictions


# ---------------------------------------------------------------------------
# ViperGraphPlugin.update_graph
# ---------------------------------------------------------------------------
def test_viper_graph_plugin_name():
    assert ViperGraphPlugin.name == "viper-graph-plugin"


def test_viper_graph_plugin_no_annotations_is_noop():
    scheduler = FakeScheduler(["w0"], {"t": FakeTask(priority=(3,))})
    ViperGraphPlugin().update_graph(scheduler)  # no annotations kwarg
    assert scheduler.tasks["t"].priority == (3,)


def test_viper_graph_plugin_no_viper_annotations_is_noop():
    scheduler = FakeScheduler(["w0"], {"t": FakeTask(priority=(3,))})
    # Annotations exist but none carry viper keys; a non-dict annotation is also
    # tolerated (skipped). load_group_by_key stays empty -> early return.
    annotations = {"t": {"resources": {"GPU": 1}}, "bad": "not-a-dict"}
    ViperGraphPlugin().update_graph(scheduler, annotations=annotations)
    assert scheduler.tasks["t"].priority == (3,)


def test_viper_graph_plugin_sets_priorities():
    tasks = {
        "load-0": FakeTask(priority=(5,)),
        "map-0-0": FakeTask(priority=(7,)),
        "map-1-2": FakeTask(priority=None),
    }
    scheduler = FakeScheduler(["w0"], tasks)
    annotations = {
        # Load node: only viper_load_group -> priority (g,) prepended.
        "load-0": {"viper_load_group": 0},
        # Map node: (g, pair) prepended, existing priority preserved.
        "map-0-0": {"viper_load_group": 0, "viper_map_pair": 0},
        # Map node with no existing priority -> existing treated as ().
        "map-1-2": {"viper_load_group": 1, "viper_map_pair": 2},
        # Annotated key absent from scheduler.tasks -> skipped, no error.
        "absent": {"viper_load_group": 9},
        # Non-dict annotation -> skipped.
        "bad": "ignore-me",
    }
    ViperGraphPlugin().update_graph(scheduler, annotations=annotations)
    assert tasks["load-0"].priority == (0, 5)
    assert tasks["map-0-0"].priority == (0, 0, 7)
    assert tasks["map-1-2"].priority == (1, 2)


def test_viper_graph_plugin_coerces_annotation_values_to_int():
    """String annotation values are coerced via ``int(...)``."""
    tasks = {"map-0-0": FakeTask(priority=None)}
    scheduler = FakeScheduler(["w0"], tasks)
    annotations = {"map-0-0": {"viper_load_group": "2", "viper_map_pair": "3"}}
    ViperGraphPlugin().update_graph(scheduler, annotations=annotations)
    assert tasks["map-0-0"].priority == (2, 3)


# ---------------------------------------------------------------------------
# dask_setup
# ---------------------------------------------------------------------------
def test_dask_setup_registers_scheduler_plugin():
    mock_scheduler = MagicMock()
    # Invoke the underlying callback directly (bypassing the click wrapper).
    dask_setup.callback(mock_scheduler, autorestrictor=True, local_cache=False)
    mock_scheduler.add_plugin.assert_called_once()
    plugin = mock_scheduler.add_plugin.call_args[0][0]
    assert isinstance(plugin, Scheduler)
    assert plugin.autorestrictor is True
    assert plugin.local_cache is False


# ---------------------------------------------------------------------------
# graph_metrics
# ---------------------------------------------------------------------------
def test_graph_metrics_docstring_example():
    from dask.core import get_deps
    from dask.order import ndependencies

    inc = lambda x: x + 1
    dsk = {"a1": 1, "b1": (inc, "a1"), "b2": (inc, "a1"), "c1": (inc, "b1")}
    dependencies, dependents = get_deps(dsk)
    _, total_dependencies = ndependencies(dependencies, dependents)

    metrics = graph_metrics(dependencies, dependents, total_dependencies)

    # Values quoted from the function docstring (covers both the single-parent
    # and the multi-parent zip branches; 'a1' has two dependents).
    assert metrics == {
        "a1": (4, 2, 3, 1, 2),
        "b1": (2, 3, 3, 1, 1),
        "b2": (1, 2, 2, 0, 0),
        "c1": (1, 3, 3, 0, 0),
    }
