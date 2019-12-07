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
        "graph": ArgType.Graph(None),
        "mean_space": ArgType.String,
        "start_time": ArgType.DateTime,
        "end_time": ArgType.DateTime,
        "threshold": ArgType.Number,
    }
    outputs = {"data": ArgType.NDArrayGraph}

    def __init__(
            self,
            graph: Graph,
            mean_space: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            threshold: float,
    ):
        super().__init__(graph, mean_space, start_time, threshold)
        self.graph = graph

        if mean_space != "auto":
            mean_space = float(mean_space)
        self.mean_space = mean_space
        self.start_time = start_time
        self.end_time = end_time
        self.threshold = threshold

    def exec(self) -> dict:
        matrix, point2idx, xlong, ylat = self._points2matrix(self.mean_space)
        max_flooding = 0

        flood_ndarray = np.ones((12, len(xlong), len(ylat)), dtype=object) * -999.0

        for node in self.graph.iter_nodes():
            xi, yi = point2idx[node.data["mint:index"]]
            recorded_at = (self.start_time + datetime.timedelta(
                minutes=node.data["schema:recordedAt"] - 1440)).month - 1

            flooding_value = 1.0 if node.data["mint:flooding"] >= self.threshold else 0.0

            max_flooding = max(max_flooding, flooding_value)

            if flood_ndarray[recorded_at][xi][yi] == -999.0:
                flood_ndarray[recorded_at][xi][yi] = [flooding_value]
            else:
                flood_ndarray[recorded_at][xi][yi].append(flooding_value)

        for i in range(12):
            for j in range(len(xlong)):
                for k in range(len(ylat)):
                    flood_ndarray[i][j][k] = np.max(flood_ndarray[i][j][k])

        if max_flooding == 0:
            for i in range(12):
                for j in range(len(xlong)):
                    for k in range(len(ylat)):
                        flood_ndarray[i][j][k] = 0

        flood_ndarray = xr.DataArray(
            flood_ndarray,
            coords=[("time",
                     [datetime.date(self.start_time.year, month, self.start_time.day).strftime('%Y-%m-%dT%H:%M:%SZ') for
                      month in range(1, 13)]),
                    ("X", xlong), ("Y", ylat)],
            attrs={
                "title": "Surface Inundation",
                "standard_name": "land_water_surface__height_flood_index",
                "long_name": "Surface Inundation",
                "units": "m",
                "valid_min": 0.0,
                "valid_max": 1.0,
                "missing_value": -999.0,
                "fill_value": -999.0,
            },
        )

        time_resolution = "P1M"

        flood_ndarray = xr.Dataset(
            data_vars={"flood": flood_ndarray.load()},
            attrs={
                "time_coverage_start": self.start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "time_coverage_end": self.end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
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

        flood_ndarray.X.attrs.update(x_attrs)
        flood_ndarray.Y.attrs.update(y_attrs)

        return {"data": flood_ndarray}

    def validate(self) -> bool:
        return True
