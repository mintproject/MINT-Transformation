#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime

import numpy as np
import xarray as xr
from drepr import Graph
from scipy import stats

from dtran import ArgType
from examples.pihm2netcdf.pihm_flooding_index_func import PihmFloodingIndexFunc


class PihmMonthlyFloodingFunc(PihmFloodingIndexFunc):
    id = "pihm_monthly_flooding_func"
    inputs = {
        "point_graph": ArgType.Graph(None),
        "surf_graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "threshold": ArgType.Number,
    }
    outputs = {"data": ArgType.NDimArray}

    def __init__(
        self,
        point_graph: Graph,
        surf_graph: Graph,
        mean_space: str,
        start_time: datetime.datetime,
        threshold: float,
    ):
        super().__init__(point_graph, surf_graph, mean_space, start_time, threshold)
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

        flood_ndarray = np.ones((365, len(xlong), len(ylat), 1)) * -999.0

        for node in self.surf_graph.iter_nodes():
            xi, yi = point2idx[node.data["mint:index"]]
            recorded_at = (
                self.start_time + datetime.timedelta(minutes=node.data["schema:recordedAt"] - 1440)
            ).month

            flooding_value = 1.0 if node["mint:flooding"] >= self.threshold else 0.0

            max_flooding = max(max_flooding, flooding_value)

            if flood_ndarray[recorded_at][xi][yi][0] == -999.0:
                flood_ndarray[recorded_at][xi][yi][0] = [flooding_value]
            else:
                flood_ndarray[recorded_at][xi][yi][0].append(flooding_value)

        for x in np.nditer(flood_ndarray, op_flags=["readwrite"]):
            x[...] = np.mean(x)

        flood_ndarray = xr.DataArray(
            flood_ndarray,
            coords=[("time", [i for i in range(0, 13)]), ("Y", ylat), ("X", xlong)],
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

    def validate(self) -> bool:
        return True
