import dataclasses

import dask
import operator
import collections

from rich.jupyter import display

import toolviper

import xarray as xr
import graphviper.graph_tools as graph_tools
import toolviper.utils.logger as logger

from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow


class Graph:
    def __init__(self):
        self._nodes = {}

        self._node_mapping = None
        self._coordinates = None
        self._graph = None
        self._dataset = None

    def __getitem__(self, item):

        # convert to a list so we can parse the input
        item = list(item)
        trees = {
            key: value
            for key, value in zip(item, list(operator.itemgetter(*item)(self._dataset)))
        }

        return xr.DataTree.from_dict(data=trees)

    def filter(self, leaves=None):
        drop = set(self._dataset.keys()) ^ set(leaves)

        logger.info(f"Dropping nodes: {drop}")
        self._dataset = self._dataset.drop_nodes(names=drop)

    @classmethod
    def from_dataset(cls, dataset):
        logger.info("Creating graph from dataset ...")
        graph = cls()
        graph._dataset = dataset
        return graph

    def build_node(self, ps_partition=None):
        """Builds a node from the dataset and coordinates"""
        if self._coordinates is None:
            raise ValueError("Coordinates must be set before building node")

        if self._dataset is None:
            raise ValueError("Dataset must be set before building node")

        self._node_mapping = (
            graph_tools.coordinate_utils.interpolate_data_coords_onto_parallel_coords(
                self._coordinates, self._dataset, ps_partition=ps_partition
            )
        )

    def make_coordinates(self, coords):
        self._coordinates = dict.fromkeys(coords)

        for coord in coords:
            if coord == "antenna_name":
                logger.info("Making antenna coordinate ...")
                xds = self._dataset.xr_ps.get_combined_antenna_xds()
                self._coordinates[coord] = (
                    graph_tools.coordinate_utils.make_parallel_coord(
                        coord=xds.antenna_name, n_chunks=xds.antenna_name.shape[0]
                    )
                )

            elif coord == "field_name":
                logger.info("Making field coordinate ...")
                xds = self._dataset.xr_ps.get_combined_field_and_source_xds()
                self._coordinates[coord] = (
                    graph_tools.coordinate_utils.make_parallel_coord(
                        coord=xds.field_name, n_chunks=xds.field_name.shape[0]
                    )
                )

            elif coord == "polarization":
                logger.info("Making polarization coordinate ...")
                xds = self._dataset.xr_ps.get_combined_field_and_source_xds()
                self._coordinates[coord] = (
                    graph_tools.coordinate_utils.make_parallel_coord(
                        coord=xds.polarization, n_chunks=xds.polarization.values.shape[0]
                    )
                )

            else:
                logger.error(f"Coordinate {coord} not found in dataset.")

    def map(self, function, parameters=None, connect=None, make_workflow=False):

        name = function.__name__

        self._graph = graph_tools.map(
            input_data=self._dataset,
            node_task_data_mapping=self._node_mapping,
            node_task=function,
            input_params=parameters,
            previous=self._nodes[connect].result if connect is not None else None,
        )

        if parameters is None:
            parameters = {}

        if make_workflow:
            self._graph = generate_dask_workflow(self._graph)

        self._nodes[name] = GraphNode(
            parameters=parameters,
            function=function,
            previous=None if connect is None else self._nodes[connect].result,
            result=self._graph,
        )

    def reduce(self, function, parameters=None, connect=None, mode="tree"):
        name = function.__name__

        graph_tools.reduce(
            graph=self._graph,
            reduce_node_task=function,
            input_params=parameters,
            mode=mode,
        )

        self._graph = generate_dask_workflow(self._graph)

        self._nodes[name] = GraphNode(
            parameters=parameters,
            function=function,
            previous=None if connect is None else self._nodes[connect].result,
            result=self._graph,
        )

    def reset(self):
        self._nodes = {}
        self._graph = None
        #self._dataset = None
        self._coordinates = None
        self._node_mapping = None

    def compute(self):
        return dask.compute(self._graph)

    @property
    def datatree(self):
        return self._dataset

    @property
    def coordinates(self):
        return toolviper.utils.display.DataDict.html(self._coordinates)

    @property
    def node_mapping(self):
        return toolviper.utils.display.DataDict.html(self._node_mapping)

    @property
    def graph(self):
        return self._graph


@dataclasses.dataclass
class GraphNode:
    """A class representing a node in a graph."""

    parameters: dict
    function: callable
    result: dict | list | None
    previous: dict | list | None


# class Graph:
#     """A class representing a directed graph for dependency management."""
#
#     def __init__(self):
#         self._graph = None
#         self._results = collections.defaultdict(list)
#
#     def source(self, job, axes, connect=False, type="", node=None):
#         function_name = job["function"].__name__
#         previous = None
#
#         logger.info(f"Adding sink node for function: {function_name}")
#         if connect:
#             previous = self._graph
#             logger.info(f"Connecting to previous node: {previous}")
#
#             if node is not None:
#                 try:
#                     logger.info(f"Connecting to user-supplied node: {node}")
#                     previous = self._results[node]
#
#                 except KeyError:
#                     logger.error(f"Node {node} not found in results.")
#
#         logger.info(f"Distributing function: {function_name} on axes: {axes}")
#         if type == "tree":
#             for _previous in previous:
#                 self._graph = toolviper.utils.sd.distribute(
#                     job=job, axes=axes, function=job["function"], previous=_previous
#                 )
#         else:
#             self._graph = toolviper.utils.sd.distribute(
#                 job=job, axes=axes, function=job["function"], previous=previous
#             )
#
#         self._results[function_name].append(self._graph)
#
#     def sink(self, function, edges=None):
#         logger.info(f"Adding sink node for function: {function.__name__}")
#         self._results[function.__name__].append(self._graph)
#         self._graph = dask.delayed(function)(self._graph)
#
#     def visualize(self):
#         return dask.visualize(self._graph)
#
#     def compute(self):
#         return dask.compute(self._graph)
#
#     @property
#     def nodes(self):
#         return list(self._results.keys())
