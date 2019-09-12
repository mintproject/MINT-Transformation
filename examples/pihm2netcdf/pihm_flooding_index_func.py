#!/usr/bin/python
# -*- coding: utf-8 -*-
import bisect
import datetime
import math
from typing import List, Dict, Tuple, Union

import numpy as np
import xarray as xr
from drepr import Graph
from scipy import stats

from dtran import IFunc, ArgType


class PihmFloodingIndexFunc(IFunc):
    id = "pihm_flooding_index_func"
    inputs = {
        "graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "threshold": ArgType.Number,
    }
    outputs = {"graph": ArgType.NDimArray}

    def __init__(
        self,
        graph: Graph,
        mean_space: str,
        start_time: datetime.datetime,
        threshold: float,
    ):
        self.graph = graph

        if mean_space != "auto":
            mean_space = float(mean_space)
        self.mean_space = mean_space
        self.start_time = start_time
        self.threshold = threshold

    def exec(self) -> dict:
        matrix, point2idx, xlong, ylat = self._points2matrix(self.mean_space)
        max_flooding = 0

        flood_ndarray = np.ones((365, len(xlong), len(ylat), 1)) * -999.0

        for node in self.surf_graph.iter_nodes():
            xi, yi = point2idx[node.data["mint:index"]]
            recorded_at = (
                self.start_time + datetime.timedelta(minutes=node.data["schema:recordedAt"] - 1440)
            ).day

            flooding_value = 1.0 if node["mint:flooding"] >= self.threshold else 0.0

            max_flooding = max(max_flooding, flooding_value)

            if flood_ndarray[recorded_at][xi][yi][0] == -999.0:
                flood_ndarray[recorded_at][xi][yi][0] = [flooding_value]
            else:
                flood_ndarray[recorded_at][xi][yi][0].append(flooding_value)

        for x in np.nditer(flood_ndarray, op_flags=["readwrite"]):
            x[...] = stats.mode(x)[0]

        flood_ndarray = xr.DataArray(
            flood_ndarray,
            coords=[("time", [i for i in range(0, 366)]), ("Y", ylat), ("X", xlong)],
            attrs={
                "standard_name": "land_water_surface__height_flood_index",
                "long_name": "Flooding Index",
                "units": "m",
                "vmin": 0.0,
                "vmax": 1.0,
            },
        )

        flood_ndarray = xr.Dataset(
            data_vars={"flood": flood_ndarray},
            attrs={
                "missing_values": -999.0,
                "title": "Flooding-Index",
                "comment": "Outputs generated from the workflow",
            },
        )

        return {"data": flood_ndarray}

    def _points2matrix(
        self, mean_space: Union[str, float] = "auto"
    ) -> Tuple[np.ndarray, Dict[int, Tuple[int, int]], List[float], List[float]]:
        ylat = sorted({float(n.data["schema:latitude"]) for n in self.graph.iter_nodes()})
        xlong = sorted({float(n.data["schema:latitude"]) for n in self.graph.iter_nodes()})

        if mean_space == "auto":
            mean_space_long = np.mean([i - j for i, j in zip(xlong[1:], xlong[:-1])])
            mean_space_lat = np.mean([i - j for i, j in zip(ylat[1:], ylat[:-1])])
        else:
            mean_space_lat, mean_space_long = mean_space, mean_space

        xlong = PihmFloodingIndexFunc._get_evenly_spacing_axis(min(xlong), max(xlong), mean_space_long, True)
        ylat = PihmFloodingIndexFunc._get_evenly_spacing_axis(min(ylat), max(ylat), mean_space_lat, True)

        point2idx = {}
        matrix = np.ones((len(ylat), len(xlong))) * -999.0

        for node in self.graph.iter_nodes():
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

    def validate(self) -> bool:
        return True
