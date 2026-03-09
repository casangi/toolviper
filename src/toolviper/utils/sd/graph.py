import dask
import collections
import toolviper

import toolviper.utils.logger as logger


class Graph:
    """A class representing a directed graph for dependency management."""

    def __init__(self):
        self._graph = None
        self._results = collections.defaultdict(list)

    def source(self, job, axes, connect=False, node=None):
        function_name = job["function"].__name__
        previous = None

        if connect:
            previous = self._graph

            if node is not None:
                try:
                    previous = self._results[node]

                except KeyError:
                    logger.error(f"Node {node} not found in results.")

        self._graph = toolviper.utils.sd.distribute(
            job=job, axes=axes, function=job["function"], previous=previous
        )

        self._results[function_name].append(self._graph)

    def sink(self, function, edges=None):
        self._graph = dask.delayed(function)(self._graph)

    def visualize(self):
        return dask.visualize(self._graph)

    def compute(self):
        return dask.compute(self._graph)

    @property
    def nodes(self):
        return list(self._results.keys())
