import dask
import collections
import toolviper

import toolviper.utils.logger as logger


class Graph:
    """A class representing a directed graph for dependency management."""

    def __init__(self):
        self._graph = None
        self._results = collections.defaultdict(list)

    def source(self, job, axes, connect=False, type="", node=None):
        function_name = job["function"].__name__
        previous = None

        logger.info(f"Adding sink node for function: {function_name}")
        if connect:
            previous = self._graph
            logger.info(f"Connecting to previous node: {previous}")

            if node is not None:
                try:
                    logger.info(f"Connecting to user-supplied node: {node}")
                    previous = self._results[node]

                except KeyError:
                    logger.error(f"Node {node} not found in results.")

        logger.info(f"Distributing function: {function_name} on axes: {axes}")
        if type == "tree":
            for _previous in previous:
                self._graph = toolviper.utils.sd.distribute(
                    job=job, axes=axes, function=job["function"], previous=_previous
                )
        else:
            self._graph = toolviper.utils.sd.distribute(
                job=job, axes=axes, function=job["function"], previous=previous
            )

        self._results[function_name].append(self._graph)

    def sink(self, function, edges=None):
        logger.info(f"Adding sink node for function: {function.__name__}")
        self._results[function.__name__].append(self._graph)
        self._graph = dask.delayed(function)(self._graph)

    def visualize(self):
        return dask.visualize(self._graph)

    def compute(self):
        return dask.compute(self._graph)

    @property
    def nodes(self):
        return list(self._results.keys())
