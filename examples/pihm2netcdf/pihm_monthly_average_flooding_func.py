#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
from collections import defaultdict

import numpy as np
import xarray as xr
from drepr import Graph
from scipy import interpolate

from dtran import ArgType
from pihm_flooding_index_func import PihmFloodingIndexFunc


class PihmMonthlyAverageFloodingFunc(PihmFloodingIndexFunc):
    id = "pihm_monthly_average_flooding_func"
    inputs = {
        "graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "end_time": ArgType.DateTime,
    }
    outputs = {"data": ArgType.NDimArray}

    def __init__(
        self, graph: Graph, mean_space: str, start_time: datetime.datetime, end_time: datetime.datetime
    ):
        super().__init__(graph, mean_space, start_time, threshold=0)
        self.graph = graph

        if mean_space != "auto":
            mean_space = float(mean_space)
        self.mean_space = mean_space
        self.start_time = start_time
        self.end_time = end_time

    def exec(self) -> dict:
        matrix, point2idx, xlong, ylat = self._points2matrix(self.mean_space)
        max_flooding = 0

        indices = defaultdict(lambda: [])
        flood_ndarray = np.ones((12, len(xlong), len(ylat)), dtype=np.float32) * -999.0

        for node in self.graph.iter_nodes():
            xi, yi = point2idx[node.data["mint:index"]]
            recorded_at = (
                self.start_time + datetime.timedelta(minutes=node.data["schema:recordedAt"] - 1440)
            ).month - 1

            flooding_value = node.data["mint:flooding"]
            max_flooding = max(max_flooding, flooding_value)

            indices[(recorded_at, xi, yi)].append(flooding_value)

        for (i, j, k), val in indices.items():
            flood_ndarray[i][j][k] = np.max(val)

        for i in range(flood_ndarray.shape[0]):
            array = flood_ndarray[i]
            print(i, np.max(array))
            array = PihmMonthlyAverageFloodingFunc._idw_interpolate(array)
            flood_ndarray[i] = array

        for (i, j, k), val in indices.items():
            if flood_ndarray[i][j][k] <= 0.0:
                flood_ndarray[i][j][k] = 0

        time = [
            datetime.date(self.start_time.year, month, self.start_time.day).strftime("%Y-%m-%dT%H:%M:%SZ")
            for month in range(1, 13)
        ]

        flood_var = xr.DataArray(
            flood_ndarray,
            coords=[("time", time), ("X", xlong), ("Y", ylat)],
            attrs={
                "title": "Surface Inundation",
                "standard_name": "land_water_surface__height_flood_index",
                "long_name": "Surface Inundation",
                "units": "m",
                "valid_min": 0.0,
                "valid_max": np.max(flood_ndarray),
                "missing_value": -999.0,
                "fill_value": -999.0,
            },
        )

        time_resolution = "P1M"

        flood_dataset = xr.Dataset(
            data_vars={"flood": flood_var},
            attrs={
                "time_coverage_start": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "time_coverage_end": self.end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "time_coverage_resolution": time_resolution,
            },
        )

        x_attrs = {
            "standard_name": "longitude",
            "long_name": "longitude",
            "axis": "X",
            "units": "degrees_east",
        }
        y_attrs = {
            "standard_name": "latitude",
            "long_name": "latitude",
            "axis": "Y",
            "units": "degrees_north",
        }

        flood_dataset.X.attrs.update(x_attrs)
        flood_dataset.Y.attrs.update(y_attrs)

        return {"data": flood_dataset}

    def validate(self) -> bool:
        return True

    @staticmethod
    def _idw_interpolate(array):
        known_indices = np.argwhere(array != -999)
        unknown_indices = np.argwhere(array == -999)

        dist = np.sqrt(
            (known_indices[:, 0][None, :] - unknown_indices[:, 0][:, None]) ** 2
            + (known_indices[:, 1][None, :] - unknown_indices[:, 1][:, None]) ** 2
        )

        idist = 1.0 / (dist + 1e-12) ** 2
        array[array == -999] = np.sum(array[array != -999] * idist, axis=1) / np.sum(idist, axis=1)
        return array
