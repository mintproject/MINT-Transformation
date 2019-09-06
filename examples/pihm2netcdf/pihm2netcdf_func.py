#!/usr/bin/python
# -*- coding: utf-8 -*-
import bisect
import datetime
import math
from collections import defaultdict
from copy import deepcopy
from typing import List, Dict, Tuple, Union

import numpy as np
from drepr import Graph
from drepr.graph import Node
from scipy import stats

from dtran import IFunc, ArgType


class Pihm2NetCDFFunc(IFunc):
    id = "pihm2netcdf_func"
    inputs = {
        "point_graph": ArgType.Graph(None),
        "surf_graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "threshold": ArgType.Number,
    }
    outputs = {"flooding_array": ArgType.NDimArray}

    def __init__(
        self,
        point_graph: Graph,
        surf_graph: Graph,
        mean_space: str,
        start_time: datetime.datetime,
        threshold: float,
    ):
        self.point_graph = point_graph
        self.surf_graph = surf_graph

        if mean_space != "auto":
            mean_space = float(mean_space)
        self.mean_space = mean_space
        self.start_time = start_time
        self.threshold = threshold

    def exec(self) -> dict:
        matrix, point2idx, xlong, ylat = self._points2matrix(self.mean_space)
        max_flooding = 0

        time_to_node = defaultdict(lambda: Node(len(time_to_node), {}, [], []))

        for node in self.surf_graph.iter_nodes():

            recorded_at = self.start_time + datetime.timedelta(minutes=node.data["schema:recordedAt"] - 1440)

            flooding_value = 1.0 if node["mint:flooding"] >= self.threshold else 0.0

            max_flooding = max(max_flooding, flooding_value)

            if "mint:grid_flooding" not in node.data:
                time_to_node[recorded_at].data["mint:grid_flooding"] = [flooding_value]
            else:
                time_to_node[recorded_at].data["mint:grid_flooding"].append(flooding_value)


        for node in time_to_node.values():
                xi, yi = point2idx[node.data["mint:index"]]
                node.data["mint:grid_flooding"] = stats.mode(node.data["mint:grid_flooding"]).mode[0]

            flood_matrix = flood_matrix.transpose()

            if np.max(flood_matrix) == 0.0:
                flood_matrix.fill(0.0)

    def _points2matrix(
        self, mean_space: Union[str, float] = "auto"
    ) -> Tuple[np.ndarray, Dict[int, Tuple[int, int]], List[float], List[float]]:

        ylat = sorted({float(n.data["schema:latitude"]) for n in self.point_graph.iter_nodes()})
        xlong = sorted({float(n.data["schema:latitude"]) for n in self.point_graph.iter_nodes()})

        if mean_space == "auto":
            mean_space_long = np.mean([i - j for i, j in zip(xlong[1:], xlong[:-1])])
            mean_space_lat = np.mean([i - j for i, j in zip(ylat[1:], ylat[:-1])])
        else:
            mean_space_lat, mean_space_long = mean_space, mean_space

        xlong = Pihm2NetCDFFunc._get_evenly_spacing_axis(min(xlong), max(xlong), mean_space_long, True)
        ylat = Pihm2NetCDFFunc._get_evenly_spacing_axis(min(ylat), max(ylat), mean_space_lat, True)

        point2idx = {}
        matrix = np.ones((len(ylat), len(xlong))) * -999.0

        for node in self.point_graph.iter_nodes():
            xi = bisect.bisect(xlong, float(node.data["schema:latitude"])) - 1
            yi = bisect.bisect(ylat, float(node.data["schema:latitude"])) - 1
            point2idx[int(node.data["mint:index"])] = (xi, yi)

        return matrix, point2idx, xlong, ylat

    @staticmethod
    def _get_evenly_spacing_axis(
        vmin: float, vmax: float, spacing: float, is_rounding_point: bool
    ) -> List[float]:
        if is_rounding_point:
            vmin = vmin - vmin % spacing

        n_values = math.ceil((vmax - vmin) / spacing) + 1
        axis = [vmin + spacing * i for i in range(n_values + 1)]
        if axis[-2] > vmax:
            axis.pop()
        return axis
